"""Smoke tests against an in-memory fake of laser_sdk: every scenario end to end
with the mock LLM, and the resolver crash-and-resume path. No Laser Stack needed.

    python tests/smoke.py
"""
import asyncio, os, sys
from pathlib import Path
HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "fake")); sys.path.insert(1, str(HERE.parent))
os.environ.update(DESK_LLM="mock", DESK_OTEL="0", DESK_POLICY_PAUSE="0", DESK_EMBEDDER="toy")
import laser_sdk as ls
assert "fake" in ls.__file__, "tests must run against the fake"
from desk import config as C, agents as A
from desk.core import Desk
from desk.llm import choose_llm
from desk.views import evaluate


async def run(scenario):
    ls.KVS.clear(); ls.MEM.clear()
    laser = ls.Laser()
    runner = Desk(laser, "desk", choose_llm())
    await runner.seed_memory()
    desks = {n: Desk(laser, n, choose_llm()) for n in ("triage","resolver","billing","approver")}
    handlers = {"triage": A.triage(desks["triage"]), "resolver": A.resolver(desks["resolver"]),
                "billing": A.billing(desks["billing"])}
    handlers["human"] = A.approver(desks["approver"])  # approvals are messages now
    dedup = A.billing_dedup(desks["billing"])
    about, text, status, cents = C.SCENARIOS[scenario]
    s = runner.sessions.start(); t = s.conversation
    await s.append("instruction", text); await runner.status(t, "open")
    await runner.handoff(t, "triage", {"ticket": t, "customer": C.CUSTOMER, "text": text})
    while laser.sent:
        m = laser.sent.pop(0)
        target = m.provenance.target_agent_id
        if target == "billing" and m.idempotency_key and not await dedup("billing"+m.idempotency_key):
            continue
        await handlers[target](None, m)
    # redeliver billing
    if status == "refunded":
        await runner.handoff(t, "billing", {"ticket": t, "amount": cents}, key=f"refund-{t}")
        m = laser.sent.pop(0)
        if await dedup("billing"+m.idempotency_key): await handlers["billing"](None, m)
    events = [e for e in laser.log["support.events"] if e["ticket"] == t]
    res = await evaluate(runner, t, events, text, (status, cents))
    print(scenario, "->", res["final"], {k: v for k, v in res["checks"].items()})
    assert all(res["checks"].values()), res
    print("   steps:", " ".join(f'{e["agent"][0]}.{e["step"]}' for e in events))


class Killed(Exception):
    pass


def fake_kill(pid, sig):
    raise Killed()


async def crash_and_resume():
    C.CRASH_ONCE = True
    A.C.CRASH_ONCE = True
    A.os.kill = fake_kill
    laser = ls.Laser(); d = Desk(laser, "resolver", choose_llm()); tri = Desk(laser, "triage", choose_llm())
    s = d.sessions.start(); t = s.conversation
    text = C.SCENARIOS["duplicate"][1]
    await tri.handoff(t, "triage", {"ticket": t, "text": text})
    await A.triage(tri)(None, laser.sent.pop(0))
    msg = laser.sent.pop(0)
    try:
        await A.resolver(d)(None, msg)
    except Killed:
        print("killed as expected")
    await A.resolver(d)(None, msg)   # redelivery to the new process
    ask = laser.sent.pop(0); assert ask.provenance.target_agent_id == "human"
    await A.approver(d)(None, ask)   # the decision comes back as a message
    await A.resolver(d)(None, laser.sent.pop(0))
    ev = [e["step"] for e in laser.log["support.events"] if e["ticket"] == t and e["agent"] == "resolver"]
    print(ev); assert "replay" in ev and ev.count("recv") == 2 and "route" in ev


async def main():
    for scenario in C.SCENARIOS:
        await run(scenario)
    await crash_and_resume()
    print("all smoke tests passed")


asyncio.run(main())
