"""The three agents (and the auto-approver), one per process:

    python -m desk.agents triage|resolver|billing|approver

The supervisor in the server (or the CLI runner) starts them and restarts any
that die. They share nothing but the log, KV and memory.
"""

from __future__ import annotations

import asyncio
import json
import os
import re
import signal
import sys
import time
import traceback

import laser_sdk as ls

from . import config as C
from . import telemetry
from .core import Desk, connect, is_addressed, setup
from .llm import choose_llm


def cents(value) -> str:
    return f"${int(value) / 100:.2f}"


# ---------------------------------------------------------------- triage

def triage(desk: Desk):
    async def handler(ctx, message):
        body = is_addressed(message, "triage")
        if body is None:
            return
        t, text = body["ticket"], body["text"]
        with telemetry.span("triage", parent=body.get("trace"), ticket=t):
            session = desk.sessions.open(t)
            await desk.event(t, "msg", text=text)
            history = await desk.history_recall(f"{C.CUSTOMER} {text}", 3)
            kb = await desk.kb_recall(text, 2)
            await desk.event(t, "kb", text=f"{len(kb)} notes, {len(history)} from customer history")
            order = re.search(r"\b(\d{3,})\b", text)
            amount = re.search(r"\$\s?(\d+(?:\.\d{1,2})?)", text)
            default = {
                "intent": "refund" if re.search(r"refund|charged", text, re.I) else "question",
                "order": order.group(1) if order else None,
                "amount_cents": int(round(float(amount.group(1)) * 100)) if amount else None,
                "summary": text,
                "reply": "Thanks, I've logged this and passed it to a resolver.",
            }
            triaged = await desk.think_json(t, (
                "You are the triage agent on a customer support desk.\n"
                f"Customer {C.CUSTOMER} wrote: \"{text}\"\n"
                f"Customer history: {history}\nKnowledge base: {kb}\n"
                "Return JSON with keys: intent (refund, question or complaint), order (the order id "
                "as a string, or null), amount_cents (integer, or null), summary (one sentence of "
                "what the customer is asking, in their terms only, nothing from history), "
                "reply (a short friendly reply saying a resolver will look into it; do not promise "
                "an outcome and do not say what has or has not happened before)."), default)
            await desk.event(t, "intent", text=f"{triaged['intent']}, order {triaged['order']}, "
                                              f"amount {triaged['amount_cents']}")
            # The facts go in the ticket's session memory: any agent, or a
            # restarted one, reads them from the session, not from a message.
            await session.memory().remember(json.dumps(
                {k: triaged[k] for k in ("intent", "order", "amount_cents", "summary")}))
            await session.append("response", triaged["reply"])
            await desk.event(t, "reply", text=triaged["reply"])
            await session.checkpoint()
            await desk.event(t, "ckpt")
            await desk.status(t, "triaged")
            await desk.handoff(t, "resolver", {**body, **triaged})
            await desk.event(t, "route", text="handoff to resolver")
    return handler


# ---------------------------------------------------------------- resolver

def resolver(desk: Desk):
    async def handler(ctx, message):
        body = is_addressed(message, "resolver")
        if body is None:
            return
        with telemetry.span("resolver", parent=body.get("trace"), ticket=body["ticket"]):
            if await desk.get_status(body["ticket"]) in C.TERMINAL:
                # A redelivery for a ticket that already finished: nothing to do.
                await desk.event(body["ticket"], "skip", text="ticket already closed")
                return
            if "decision" in body:
                await decided(desk, body)
            else:
                await resolve(desk, body)
    return handler


async def resolve(desk: Desk, body: dict) -> None:
    t = body["ticket"]
    session = desk.sessions.open(t)
    await desk.event(t, "recv", text=body.get("summary", ""))
    saved = await desk.ckpts.get(t)
    if saved is not None:
        # A previous resolver died mid-case. The log redelivered the handoff to
        # us; the checkpoint says how far it got.
        cp = ls.Checkpoint.from_json(json.loads(bytes(saved).decode())["cp"])
        since = await session.turns_since(cp)
        await desk.event(t, "replay", text=f"from checkpoint, {len(since)} turn(s) since: "
                         + "; ".join(turn.text() for turn in since))
        await finish(desk, t, body)
        return
    await desk.status(t, "resolving")
    turns = await session.context()
    await desk.event(t, "ctx", text=f"{len(turns)} turns assembled")
    record = await desk.get_order(body.get("order"))
    await desk.event(t, "order", text=f"order {body.get('order')}: {record or 'not found'}")
    cp = await session.checkpoint()
    await desk.ckpts.set(t).payload(json.dumps({"cp": cp.to_json()})).send()
    await desk.event(t, "ckpt", text="checkpoint saved to KV")
    await session.append("tool.call", f"order lookup {body.get('order')}: {record}")
    if C.CRASH_ONCE and await desk.crashes.get(t) is None:
        await desk.crashes.set(t).payload(b"1").send()
        await desk.event(t, "crash", text=f"resolver pid {os.getpid()} killed (SIGKILL)")
        os.kill(os.getpid(), signal.SIGKILL)
    if C.POLICY_PAUSE > 0:
        await desk.event(t, "policy", text=f"checking refund policy (holding {C.POLICY_PAUSE:g}s: kill me now)")
        await asyncio.sleep(C.POLICY_PAUSE)
    else:
        await desk.event(t, "policy", text="checking refund policy")
    await finish(desk, t, body)


async def finish(desk: Desk, t: str, body: dict) -> None:
    session = desk.sessions.open(t)
    for item in await session.memory().recall(limit=5):
        try:
            facts = json.loads(item.text)
        except ValueError:
            continue
        if isinstance(facts, dict):
            body = {**body, **{k: v for k, v in facts.items() if v is not None}}
            await desk.event(t, "memory", text=f"session facts: order {facts.get('order')}, "
                                              f"amount {facts.get('amount_cents')}")
            break
    record = await desk.get_order(body.get("order"))
    if body.get("intent") != "refund":
        await answer(desk, t, body, record)
        return
    policy = await desk.kb_recall("refund policy duplicate charge manager", 3)
    await desk.event(t, "kb", text="; ".join(policy))
    turns = await session.context()
    default = refund_rule(record)
    # One LLM call decides and writes every reply the outcome might need, so the
    # human's answer later needs no second call.
    decision = await desk.think_json(t, (
        "You are the resolver agent on a customer support desk.\n"
        f"Customer on this ticket: {C.CUSTOMER}\n"
        "Conversation so far:\n" + "\n".join(turn.text() for turn in turns) + "\n"
        f"The customer wrote: {body['text']}\n"
        f"Order record (charges and refunds in cents): {record}\nPolicy: {policy}\n"
        "The order record is the source of truth for charges and refunds: base the decision on it, "
        "not on anything you remember about earlier tickets. "
        "Refund only charges beyond the first of a set of identical charges, minus any refunds "
        "already made. Never refund an order that belongs to a different customer, or one that "
        "is not in the system. Decide whether a refund is due (a human approves it next).\n"
        "Return JSON with keys: refund (true or false), amount_cents (integer), reason (one sentence), "
        "and short replies to the customer (two sentences at most each): "
        "reply_no_refund (if refund is false: no refund, why, and what they can do next), "
        "reply_approved (if refund is true: the refund is approved and billing will issue it), "
        "reply_declined (if refund is true: not approved at the desk, refunds this size go to a "
        "manager who will be in touch)."),
        default)
    reason = decision.get("reason") or default["reason"]
    amount = int(decision.get("amount_cents") or 0)
    replies = {
        "no_refund": f"I've checked order {body.get('order')}: {reason}",
        "approved": f"Your {cents(amount)} refund was approved, billing will issue it.",
        "declined": f"Your {cents(amount)} refund needs a manager's approval; they will be in touch.",
    }
    for key in replies:
        text = " ".join(str(decision.get(f"reply_{key}") or "").split())
        if text:
            replies[key] = text
    decision["reason"] = reason
    if not decision.get("refund") or amount <= 0:
        reply = replies["no_refund"]
        await session.append("response", reply)
        await desk.event(t, "reply", text=reply)
        await desk.status(t, "closed")
        await desk.learn(f"{C.CUSTOMER}: {time.strftime('%Y-%m-%d')} ticket {t[:12]}, order "
                         f"{body.get('order')}, no refund: {decision['reason']}")
        return
    # Never wait for a human inside the handler: that would block the agent
    # for every other ticket. Save the pending decision, ask on the log, and
    # return. The answer comes back as a message and decided() carries on.
    pending = {"amount": amount, "reason": decision["reason"], "order": body.get("order"),
               "text": body["text"], "replies": replies}
    await desk.ckpts.set(f"{t}:pending").payload(json.dumps(pending)).send()
    await desk.status(t, "awaiting approval")
    await desk.laser.send_agent(ls.Topics.HUMAN_INPUT, json.dumps({
        "ticket": t, "text": f"approve a {cents(amount)} refund to {C.CUSTOMER} for order "
                            f"{body.get('order')}? {decision['reason']}"}).encode(),
        ls.Provenance(conversation_id=t, agent="resolver", target_agent_id="human"))
    await desk.event(t, "approve", text=f"asked a human: {cents(amount)}, {decision['reason']}")


async def decided(desk: Desk, body: dict) -> None:
    """The human's answer, back on the log. Picks up from the saved pending decision."""
    t = body["ticket"]
    session = desk.sessions.open(t)
    raw = await desk.ckpts.get(f"{t}:pending")
    pending = json.loads(bytes(raw).decode()) if raw is not None else None
    if not pending:
        await desk.event(t, "skip", text="no approval pending (already decided)")
        return
    await desk.ckpts.set(f"{t}:pending").payload("null").send()
    amount, reason = pending["amount"], pending["reason"]
    body = {**body, "order": pending["order"], "text": pending["text"]}
    approved = body["decision"] == "approved"
    decision = {"reason": reason}
    await desk.event(t, "human", text="approved" if approved else "declined")
    # The reply was written with the decision, before the human was asked.
    replies = pending.get("replies") or {}
    reply = replies.get("approved" if approved else "declined") or (
        f"Your {cents(amount)} refund was approved, billing will issue it." if approved else
        f"Your {cents(amount)} refund needs a manager's approval; they will be in touch.")
    await session.append("response", reply)
    await desk.event(t, "reply", text=reply)
    if approved:
        await desk.handoff(t, "billing", {"ticket": t, "amount": amount, "order": body.get("order"),
                                          "reason": decision["reason"]}, key=f"refund-{t}")
        await desk.event(t, "route", text="handoff to billing")
    else:
        await desk.status(t, "declined")
        await desk.learn(f"{C.CUSTOMER}: {time.strftime('%Y-%m-%d')} ticket {t[:12]}, order "
                         f"{body.get('order')}, {cents(amount)} refund declined at the desk, "
                         "escalated to a manager")


def refund_rule(record) -> dict:
    """The rule-based answer the LLM is checked against (and the fallback with the mock)."""
    if not record:
        return {"refund": False, "amount_cents": 0, "reason": "the order is not in the system"}
    if record["customer"] != C.CUSTOMER:
        return {"refund": False, "amount_cents": 0,
                "reason": f"the order belongs to a different customer ({record['customer']})"}
    charges = record["charges"]
    amount = max(set(charges), key=charges.count)
    extra = (charges.count(amount) - 1) * amount
    due = max(0, extra - sum(record.get("refunds", [])))
    if extra and not due:
        return {"refund": False, "amount_cents": 0, "reason": "the duplicate charge was already refunded"}
    if not due:
        return {"refund": False, "amount_cents": 0, "reason": "no duplicate charge found"}
    return {"refund": True, "amount_cents": due,
            "reason": f"{charges.count(amount)} identical charges of {cents(amount)}, "
                      f"{cents(due)} beyond the first"}


async def answer(desk: Desk, t: str, body: dict, record) -> None:
    """Not a refund: answer from the knowledge base and the order record."""
    session = desk.sessions.open(t)
    notes = await desk.kb_recall(body["text"], 3)
    await desk.event(t, "kb", text="; ".join(notes))
    reply = await desk.write(t, (
        f"You are a support agent. Customer {C.CUSTOMER} asked: {body['text']}\n"
        f"Order record: {record}\nKnowledge base: {notes}\n"
        "Answer them in two or three sentences, using only what the record and the knowledge base say."),
        "Thanks for asking. " + (notes[0] if notes else "We'll follow up by email."))
    await session.append("response", reply)
    await desk.event(t, "reply", text=reply)
    await desk.status(t, "answered")
    await desk.learn(f"{C.CUSTOMER}: {time.strftime('%Y-%m-%d')} ticket {t[:12]}, asked: {body['text']}")


# ---------------------------------------------------------------- billing

def billing_dedup(desk: Desk):
    """Durable fires-once: the key lives in KV, so it survives a billing restart."""
    store = desk.laser.kv(C.NS_DEDUP)

    async def observe(key: str) -> bool:
        if await store.get(key) is not None:
            ticket = key.split("refund-")[-1]
            await desk.event(ticket, "dedup", text=f"duplicate {key} skipped")
            return False
        await store.set(key).payload(b"1").ttl(86400.0).send()
        return True
    return observe


def billing(desk: Desk):
    async def handler(ctx, message):
        body = is_addressed(message, "billing")
        if body is None:
            return
        t = body["ticket"]
        with telemetry.span("billing", parent=body.get("trace"), ticket=t):
            session = desk.sessions.open(t)
            await desk.event(t, "recv", text=cents(body["amount"]))
            turns = await session.context()
            await desk.event(t, "ctx", text=f"{len(turns)} turns")
            raw = await desk.accounts.get(C.CUSTOMER)
            refunded = int(bytes(raw).decode()) if raw is not None else 0
            await desk.event(t, "acct", text=f"{C.CUSTOMER} refunded so far: {cents(refunded)}")
            memo = await desk.write(t, (
                f"Write a one-line ledger memo for a {cents(body['amount'])} refund to {C.CUSTOMER}, "
                f"ticket {t[:12]}. Reason: {body.get('reason', '')}."),
                f"refund {cents(body['amount'])}: {body.get('reason', '')}")
            await session.checkpoint()
            await desk.event(t, "ckpt")
            await desk.accounts.set(f"ledger:{message.idempotency_key}").payload(
                json.dumps({"ticket": t, "cents": body["amount"], "memo": memo})).send()
            await desk.event(t, "ledger", text=memo)
            await desk.accounts.set(C.CUSTOMER).payload(str(refunded + body["amount"])).send()
            await desk.record_refund(body.get("order"), body["amount"])
            await desk.event(t, "refund", text=f"{cents(body['amount'])} to {C.CUSTOMER} (fires once)",
                             cents=body["amount"])
            await desk.status(t, "refunded")
            await desk.learn(f"{C.CUSTOMER}: {time.strftime('%Y-%m-%d')} ticket {t[:12]}, order "
                             f"{body.get('order')}, refunded {cents(body['amount'])}: {body.get('reason', '')}")
            reply = f"Your refund of {cents(body['amount'])} has been issued."
            await session.append("response", reply)
            await desk.event(t, "ok", text=reply)
    return handler


# ---------------------------------------------------------------- auto approver (CLI runs)

def approver(desk: Desk):
    """Stands in for the human in CLI runs: approves up to the desk's limit, and
    sends the decision back to the resolver on the log."""
    async def handler(ctx, message):
        try:
            asked = json.loads(bytes(message.payload).decode("utf-8", "replace"))
        except ValueError:
            return
        amount = re.search(r"\$(\d+(?:\.\d{1,2})?)", asked.get("text", ""))
        value = int(round(float(amount.group(1)) * 100)) if amount else 0
        decision = "approved" if value <= C.APPROVAL_LIMIT_CENTS else "declined"
        await desk.handoff(asked["ticket"], "resolver", {"ticket": asked["ticket"], "decision": decision})
    return handler


# ---------------------------------------------------------------- process entry

def guarded(desk: Desk, name: str, handler):
    """Log a handler failure on the events topic instead of dying silently."""
    async def run(ctx, message):
        try:
            await handler(ctx, message)
        except Exception as error:  # noqa: BLE001
            traceback.print_exc()
            await desk.event("-", "error", text=f"{name}: {type(error).__name__}: {error}")
    return run


async def main(name: str) -> None:
    telemetry.init(f"support-desk-{name}")
    laser = await connect()
    await setup(laser, with_index=False)
    llm = choose_llm() if name != "approver" else None
    desk = Desk(laser, name, llm)
    if name == "approver":
        handle = laser.spawn_agent("approver", ls.Topics.HUMAN_INPUT, guarded(desk, name, approver(desk)),
                                   poll_interval_ms=20)
    else:
        make = {"triage": triage, "resolver": resolver, "billing": billing}[name]
        extra = {"dedup": billing_dedup(desk)} if name == "billing" else {}
        handle = laser.spawn_agent(name, C.DESK_TOPIC, guarded(desk, name, make(desk)),
                                   poll_interval_ms=20, **extra)
    await handle.ready()
    await desk.event("-", "ready", text=llm.name if llm else "auto-approve")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    joined = asyncio.ensure_future(handle.join())
    stopped = asyncio.ensure_future(stop.wait())
    await asyncio.wait([joined, stopped], return_when=asyncio.FIRST_COMPLETED)
    if not joined.done():
        await handle.shutdown()


if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in ("triage", "resolver", "billing", "approver"):
        sys.exit("usage: python -m desk.agents triage|resolver|billing|approver")
    asyncio.run(main(sys.argv[1]))
