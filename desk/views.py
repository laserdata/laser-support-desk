"""Readers of the one stream: the event tail (drives the UI and the CLI lanes),
the readers panel (memory, context, state, analytics) and the evals."""

from __future__ import annotations

import asyncio
import json

import laser_sdk as ls

from . import config as C
from .core import Desk, readable


class EventTail:
    """Tails the events topic from now on, calling on_event(record) for each."""

    def __init__(self, laser: ls.Laser, on_event) -> None:
        self.cursor = laser.topic(C.EVENTS_TOPIC).replay()
        self.on_event = on_event
        self.by_ticket: dict[str, list[dict]] = {}

    async def skip_history(self, keep: set[str] | frozenset = frozenset()) -> None:
        """Move to the end of the log, keeping the past events of the tickets in
        `keep` (tickets still open when the server restarted)."""
        while messages := await self.cursor.poll():
            for message in messages:
                if not keep:
                    break
                try:
                    record = message.json()
                except ls.CodecError:
                    continue
                if isinstance(record, dict) and record.get("ticket") in keep:
                    self.by_ticket.setdefault(record["ticket"], []).append(record)

    async def run(self) -> None:
        # One bad poll or handler must not stop the tail: everything that reads
        # the log through it (the browser, the evals) would go quiet for good.
        while True:
            try:
                messages = await self.cursor.poll()
            except Exception as error:  # noqa: BLE001
                print(f"  event tail: poll failed, retrying: {error!r}", flush=True)
                await asyncio.sleep(1)
                continue
            if not messages:
                await asyncio.sleep(0.05)
                continue
            for message in messages:
                try:
                    record = message.json()
                except ls.CodecError:
                    continue
                if not isinstance(record, dict):
                    continue
                self.by_ticket.setdefault(record.get("ticket", "-"), []).append(record)
                try:
                    result = self.on_event(record)
                    if asyncio.iscoroutine(result):
                        await result
                except Exception as error:  # noqa: BLE001
                    print(f"  event tail: handler failed on {record.get('step')}: {error!r}", flush=True)


async def readers(desk: Desk, t: str) -> dict:
    """Everything reading the same stream, for one ticket."""
    session = desk.sessions.open(t)
    customer = await desk.get_customer(t)
    out: dict = {"customer": customer}
    out["memory"] = await desk.history_recent(customer, 4)
    facts = [item.text() for item in await session.memory().recall(limit=3)]
    out["session_memory"] = facts[-1] if facts else None
    turns = await session.context()
    out["context"] = [{"kind": turn.display, "text": turn.text()} for turn in readable(turns)]
    out["state"] = await desk.get_status(t)
    out["analytics"] = await analytics(desk.laser, t)
    return out


def _rows(result, *fields) -> list[dict]:
    return [{f: result.value_text(row, f) for f in fields} for row in result.rows]


def _clean_csat(row: dict) -> dict:
    """An average over no ratings comes back as NaN: report it as none yet."""
    count = row.get("count") or "0"
    try:
        avg = float(row.get("avg"))
    except (TypeError, ValueError):
        avg = None
    if avg is not None and (avg != avg or count in ("0", 0)):  # NaN, or nothing rated
        avg = None
    return {"avg": None if avg is None else round(avg, 2), "count": count}


async def analytics(laser: ls.Laser, t: str | None = None) -> dict:
    """Managed queries over the events index."""
    out: dict = {}
    q = lambda: laser.query(C.EVENTS_INDEX)  # noqa: E731
    try:
        by_status = await q().filter_eq("step", "status").count().group_by(["status"]).fetch()
        out["status_changes"] = _rows(by_status, "status", "count")
        csat = await q().filter_eq("step", "csat").avg("score").count().fetch()
        row = _rows(csat, "avg", "count")
        out["csat"] = _clean_csat(row[0] if row else {})
        if t:
            per_agent = await q().filter_eq("ticket", t).count().group_by(["agent"]).fetch()
            out["steps_per_agent"] = _rows(per_agent, "agent", "count")
            times = await q().filter_eq("ticket", t).filter_eq("step", "status").order_asc("ts").fetch()
            stamps = [int(v) for v in (times.value_text(r, "ts") for r in times.rows) if v]
            out["resolution_seconds"] = round((stamps[-1] - stamps[0]) / 1e6, 1) if len(stamps) > 1 else None
    except ls.LaserError as error:
        out["error"] = str(error)
    return out


async def evaluate(desk: Desk, t: str, events: list[dict], text: str,
                   expect: tuple[str, int] | None) -> dict:
    """Deterministic checks from the log, plus an LLM judge on the replies."""
    steps = [e["step"] for e in events]
    replies = [e.get("text", "") for e in events if e["step"] in ("reply", "ok")]
    final = await desk.get_status(t)
    # A billing killed between announcing a refund and marking it done
    # announces it again on redelivery: count refunds by key, not by event.
    refunds = list({e.get("key") or id(e): e for e in events if e["step"] == "refund"}.values())
    checks: dict[str, bool] = {}
    if expect:
        status, cents = expect
        checks[f"ticket ended {status} (was {final})"] = final == status
        if cents:
            checks[f"refund of ${cents / 100:.2f} fired exactly once"] = (
                len(refunds) == 1 and refunds[0].get("cents") == cents)
        else:
            checks["no refund issued"] = not refunds
    else:
        checks["refund, if any, fired at most once"] = len(refunds) <= 1
    if "crash" in steps or "killed" in steps:
        # Recovered means the ticket still finished. If the kill came after the
        # checkpoint, the new resolver also shows a replay step.
        how = "resumed from checkpoint" if "replay" in steps else "restarted from the top"
        checks[f"recovered after the kill ({how})"] = final in C.TERMINAL
    checks["customer got replies"] = len(replies) >= 2
    # The note is written before the last step, but recall catches up a moment
    # later: give it a few seconds before calling it missing.
    customer = await desk.get_customer(t)
    for _ in range(10):
        recent = await desk.history_recent(customer, 20)
        if any(t[:12] in note for note in recent):
            break
        await asyncio.sleep(0.5)
    checks["resolution written back to memory"] = any(t[:12] in note for note in recent)

    judge = await desk.think_json(t, (
        "You are evaluating a customer support desk. Score its handling from 1 (bad) to 5 "
        "(excellent): accurate, consistent with the outcome, polite, no false promises.\n"
        f"Customer wrote: {text}\nReplies, in order: {json.dumps(replies)}\n"
        f"Final ticket status: {final}\n"
        "Return JSON with keys: score (integer 1-5), reason (one sentence)."),
        {"score": None, "reason": "no judge available (mock LLM)"})
    await desk.event(t, "eval", agent="evals", score=judge.get("score"),
                     text=f"judge {judge.get('score')}/5: {judge.get('reason')}")
    return {"checks": checks, "judge": judge, "final": final}
