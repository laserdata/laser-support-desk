"""Run one scenario end to end from the command line, no browser:

    python -m desk.run --list
    python -m desk.run --scenario over-limit
    python -m desk.run --scenario duplicate --no-crash
    python -m desk.run --chat

The agents run as separate processes. Unless --no-crash, the resolver SIGKILLs
itself once, mid-case; the supervisor restarts it, the log redelivers, and it
resumes from its checkpoint. Humans are replaced by an auto-approver that
declines refunds over the desk's limit.
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sys
import time

from . import config as C
from . import telemetry
from .core import Desk, connect, setup
from .llm import choose_llm
from .supervisor import Supervisor
from .views import EventTail, evaluate, readers

COLOURS = {"triage": "36", "resolver": "33", "billing": "35", "desk": "32", "evals": "34",
           "supervisor": "31"}


def lane(agent: str, step: str, note: str = "") -> None:
    note = " ".join(str(note).split())
    if len(note) > 170:
        note = note[:167] + "..."
    print(f"  \033[{COLOURS.get(agent, '0')}m{agent:>10}\033[0m  {step:<8} {note}", flush=True)


def phase(title: str) -> None:
    print(f"\n\033[1;36m▸ {title}\033[0m", flush=True)


async def main() -> None:
    parser = argparse.ArgumentParser(description="Support desk on Laser SDK: one scenario, no browser")
    parser.add_argument("--scenario", default="duplicate", choices=sorted(C.SCENARIOS))
    parser.add_argument("--list", action="store_true", help="list the scenarios")
    parser.add_argument("--chat", action="store_true", help="type the customer message yourself")
    parser.add_argument("--no-crash", action="store_true", help="do not kill the resolver")
    parser.add_argument("--reset", action="store_true",
                        help="clean slate first: orders as configured, fresh customer history")
    parser.add_argument("--no-agents", action="store_true",
                        help="use the agents the web server is already running (watch it in the UI; "
                             "approve there, kill from there)")
    args = parser.parse_args()
    if args.list:
        for name, (about, message, status, _) in C.SCENARIOS.items():
            print(f"  {name:<14} {about}\n  {'':<14} \"{message}\"")
        return
    about, scripted, expect_status, expect_cents = C.SCENARIOS[args.scenario]

    tracing = telemetry.init("support-desk-runner")
    laser = await connect()
    await setup(laser, with_index=True)
    desk = Desk(laser, "desk", choose_llm())
    print(f"  LLM: {desk.llm.name}   tracing: {'on' if tracing else 'off'}")
    if not args.chat:
        print(f"  scenario: {args.scenario} ({about})")

    phase("memory, read back from the log")
    kb, history = await (desk.reset_demo() if args.reset else desk.seed_memory())
    print(f"  knowledge base: {kb} notes, customer history: {history} notes")

    ticket_box: dict = {}
    ready: set = set()
    pids: dict[str, int] = {}

    def on_event(record: dict) -> None:
        agent, step = record.get("agent", "?"), record.get("step", "?")
        if step == "ready":
            ready.add(agent)
            pids[agent] = record.get("pid")
            return
        if record.get("ticket") != ticket_box.get("t") and step != "error":
            return
        pid = record.get("pid")
        if agent in ("triage", "resolver", "billing") and pid and pids.get(agent) != pid:
            pids[agent] = pid
        lane(agent, step, record.get("text") or record.get("status") or "")

    tail = EventTail(laser, on_event)
    await tail.skip_history()
    tail_task = asyncio.create_task(tail.run())

    def on_change(name, state, pid, code=None) -> None:
        if state == "exited" and ticket_box.get("t"):
            lane("supervisor", "restart", f"{name} pid {pid} died (exit {code}), starting a new one")

    supervisor = None
    if args.no_agents:
        phase("using the agents the web server is running: approve and kill from the UI")
    else:
        phase("starting triage, resolver, billing and the approver as separate processes")
        env = {"DESK_POLICY_PAUSE": "0", "DESK_CRASH_ONCE": "0" if args.no_crash else "1"}
        supervisor = Supervisor(["triage", "resolver", "billing", "approver"], env, on_change)
        await supervisor.start()
    try:
        deadline = time.monotonic() + 120
        while supervisor and len(ready) < 4:
            if time.monotonic() > deadline:
                sys.exit(f"agents not ready after 120s (ready: {sorted(ready)}); see logs/*.log")
            await asyncio.sleep(0.2)
        for name in ("triage", "resolver", "billing", "approver"):
            if supervisor:
                print(f"  {name:<9} pid {supervisor.pid(name)}")

        text = (input("  customer> ").strip() if args.chat else "") or scripted
        if not args.chat:
            await desk.reset_orders(text)  # the scenario starts from its configured order
        session = desk.sessions.start()
        t = session.conversation
        ticket_box["t"] = t
        phase(f"ticket {t[:12]}")
        with telemetry.span("ticket", ticket=t, scenario=args.scenario) as root:
            await session.append("instruction", text)
            await desk.status(t, "open")
            await desk.handoff(t, "triage", {"ticket": t, "customer": C.CUSTOMER, "text": text})
            deadline = time.monotonic() + 900
            while True:
                status = await desk.get_status(t)
                steps = [e["step"] for e in tail.by_ticket.get(t, [])]
                if status in C.TERMINAL and (status != "refunded" or "ok" in steps):
                    break
                if time.monotonic() > deadline:
                    sys.exit(f"ticket stuck at {status}; see logs/*.log")
                await asyncio.sleep(0.2)
            trace = telemetry.trace_id(root)

        if status == "refunded":
            phase("restart billing and redeliver the refund: it must not fire twice")
            if supervisor:
                await supervisor.restart("billing")
                ready.discard("billing")
                while "billing" not in ready:
                    await asyncio.sleep(0.2)
            events = tail.by_ticket.get(t, [])
            refund = next(e for e in events if e["step"] == "refund")
            await desk.handoff(t, "billing", {"ticket": t, "amount": refund["cents"],
                                              "order": None, "reason": "redelivery"}, key=f"refund-{t}")
            for _ in range(50):
                if "dedup" in [e["step"] for e in tail.by_ticket.get(t, [])]:
                    break
                await asyncio.sleep(0.2)

        phase("readers on the same stream")
        seen = await readers(desk, t)
        print("  memory (customer history, newest last):")
        for note in seen["memory"]:
            print(f"    {note}")
        print(f"  session memory: {seen['session_memory']}")
        print(f"  context: {len(seen['context'])} turns")
        for turn in seen["context"]:
            print(f"    {turn['kind']:<10} {turn['text'][:140]}")
        print(f"  state: {seen['state']}")
        a = seen["analytics"]
        if "error" in a:
            print(f"  analytics: {a['error']}")
        else:
            print(f"  analytics: steps per agent {a.get('steps_per_agent')}, resolution "
                  f"{a.get('resolution_seconds')}s, status changes {a.get('status_changes')}, "
                  f"CSAT {a.get('csat')}")

        phase("evals")
        await asyncio.sleep(0.5)
        result = await evaluate(desk, t, tail.by_ticket.get(t, []), text,
                                None if args.chat else (expect_status, expect_cents))
        for name, ok in result["checks"].items():
            print(f"  {'PASS' if ok else 'FAIL'}  {name}")
        judge = result["judge"]
        print(f"  judge  {judge.get('score')}/5  {judge.get('reason')}")
        if judge.get("score"):
            # No customer to ask in a CLI run: record the judge's score as a simulated CSAT.
            await desk.event(t, "csat", agent="evals", score=int(judge["score"]), text="simulated from the judge")
        if trace:
            endpoint = os.environ.get("DESK_JAEGER_UI", "http://localhost:16686")
            print(f"\n  trace: {endpoint}/trace/{trace}")
    finally:
        if supervisor:
            await supervisor.stop()
        tail_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
