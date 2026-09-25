"""The demo: a customer chat, the poster's three lanes filling live off the log,
a kill button per agent, a human Approve/Decline, the readers panel, evals and
CSAT.

    python -m desk.server          # then open http://localhost:8000
"""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path

import laser_sdk as ls
from aiohttp import WSMsgType, web

from . import config as C
from . import telemetry
from .chaos import ChaosMonkey
from .core import Desk, auto_decision, connect, setup
from .demo import DemoLoop
from .llm import choose_llm
from .supervisor import Supervisor
from .views import EventTail, evaluate, readers

STATIC = Path(__file__).resolve().parent / "static"

# macOS: when a browser opens and drops a speculative connection, aiohttp's
# SO_KEEPALIVE call on the already-closed socket raises EINVAL and the
# connection is abandoned. Keep-alive is an optimisation; ignore the failure.
import aiohttp.web_protocol as _web_protocol  # noqa: E402

_tcp_keepalive = _web_protocol.tcp_keepalive


def _safe_keepalive(transport) -> None:
    try:
        _tcp_keepalive(transport)
    except OSError:
        pass


_web_protocol.tcp_keepalive = _safe_keepalive
AGENTS = ["triage", "resolver", "billing"]


class App:
    def __init__(self) -> None:
        self.sockets: set[web.WebSocketResponse] = set()
        self.tickets: dict[str, dict] = {}
        self.pending: dict[str, dict] = {}
        self.pids: dict[str, int | None] = {}
        self.demo = DemoLoop(self)

    async def broadcast(self, message: dict) -> None:
        data = json.dumps(message, default=str)
        for ws in list(self.sockets):
            try:
                await ws.send_str(data)
            except (ConnectionResetError, RuntimeError):
                self.sockets.discard(ws)

    # ---- startup and shutdown
    async def start(self, _app) -> None:
        telemetry.init("support-desk-server")
        if C.DEMO:
            # Before the LLM is chosen and the agents start: they read these.
            os.environ["DESK_LLM"] = C.DEMO_LLM
            os.environ.setdefault("DESK_MOCK_DELAY", str(C.DEMO_STEP))
        self.laser = await connect()
        await setup(self.laser, with_index=True)
        self.desk = Desk(self.laser, "desk", choose_llm())
        self.kb_count, self.history_count = await self.desk.seed_memory()
        self.tail = EventTail(self.laser, self.on_event)
        await self.tail.skip_history(keep=await self.restore_approvals())
        self.tail_task = asyncio.create_task(self.tail.run())
        env = {"DESK_CRASH_ONCE": "0",
               "DESK_POLICY_PAUSE": os.environ.get("DESK_POLICY_PAUSE", "0")}
        self.supervisor = Supervisor(AGENTS, env, self.on_agent)
        await self.supervisor.start()
        self.chaos = ChaosMonkey(self.supervisor, on_kill=self.on_chaos, may_kill=self.chaos_may_kill)
        if C.CHAOS:
            self.chaos.start()
        # The human in the loop: approval requests land here and wait for a click.
        self.human = self.laser.spawn_agent("human", ls.Topics.HUMAN_INPUT, self.on_approval,
                                            poll_interval_ms=20)
        await self.human.ready()
        if C.DEMO:
            self.demo.start()
        print(f"  LLM: {self.desk.llm.name}{', demo loop on' if C.DEMO else ''}. Open http://localhost:{os.environ.get('DESK_PORT', '8000')}")

    async def stop(self, _app) -> None:
        self.demo.stop()
        self.chaos.stop()
        await self.supervisor.stop()
        self.tail_task.cancel()
        await self.human.shutdown()

    # ---- the log, the supervisor, the human
    async def on_event(self, record: dict) -> None:
        await self.broadcast({"type": "event", **record})
        t = record.get("ticket")
        ticket = self.tickets.get(t)
        if not ticket or ticket.get("evaluated"):
            return
        step, status = record.get("step"), record.get("status")
        finished = step == "ok" or (step == "status" and status in C.TERMINAL - {"refunded"})
        if finished:
            ticket["evaluated"] = True
            asyncio.create_task(self.finish(t))

    async def finish(self, t: str) -> None:
        ticket = self.tickets[t]
        await asyncio.sleep(0.5)
        result = await evaluate(self.desk, t, self.tail.by_ticket.get(t, []), ticket["text"],
                                ticket.get("expect"))
        telemetry.end_root(ticket.get("root"), status=result["final"])
        await self.broadcast({"type": "eval", "ticket": t, **result})
        ticket.setdefault("done", asyncio.Event()).set()

    async def wait_finished(self, t: str, timeout: float) -> bool:
        """Until the ticket is finished and evaluated (the demo loop waits on this)."""
        done = self.tickets[t].setdefault("done", asyncio.Event())
        try:
            await asyncio.wait_for(done.wait(), timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def on_agent(self, name: str, state: str, pid: int, code=None) -> None:
        self.pids[name] = pid if state == "started" else None
        await self.broadcast({"type": "agent", "name": name, "state": state, "pid": pid, "code": code})

    def chaos_may_kill(self) -> bool:
        """In the demo loop, only while a ticket is in flight: a kill in the
        pause between tickets shows nothing but a restart."""
        if not self.demo.running:
            return True
        return any(not info.get("evaluated") for info in self.tickets.values())

    async def on_chaos(self, name: str, pid: int) -> None:
        """A chaos kill goes on the log like a Kill click, against the newest open ticket."""
        live = [t for t, info in self.tickets.items() if not info.get("evaluated")]
        await self.desk.event(live[-1] if live else "-", "killed", agent="supervisor", by="chaos",
                              text=f"chaos monkey killed {name} pid {pid} (SIGKILL)", target=name)

    # Approvals wait on a person, maybe longer than this server runs. They are
    # saved in KV and brought back on start, so a restart loses none.
    async def save_approvals(self) -> None:
        saved = {rid: {"approval": p["approval"], "ticket": p.get("ticket")} for rid, p in self.pending.items()}
        await self.desk.meta.set("approvals").payload(json.dumps(saved)).send()

    async def restore_approvals(self) -> set[str]:
        raw = await self.desk.meta.get("approvals")
        saved = json.loads(bytes(raw).decode()) if raw is not None else {}
        for request_id, item in saved.items():
            t = item["approval"]["ticket"]
            still = await self.desk.ckpts.get(f"{t}:pending")
            if still is None or json.loads(bytes(still).decode()) is None:
                continue  # decided since it was saved
            self.pending[request_id] = item
            if item.get("ticket"):
                self.tickets[t] = {**item["ticket"], "root": None}
        await self.save_approvals()
        if self.pending:
            print(f"  {len(self.pending)} approval(s) still waiting from before the restart")
        return {p["approval"]["ticket"] for p in self.pending.values()}

    async def on_approval(self, ctx, message) -> None:
        request_id = message.message_id
        raw = bytes(message.payload).decode("utf-8", "replace")
        try:
            asked = json.loads(raw)
        except ValueError:
            asked = {"ticket": message.conversation_id, "text": raw}
        approval = {"type": "approval", "id": request_id, "ticket": asked.get("ticket"),
                    "text": asked.get("text", raw)}
        # One open approval per ticket: a re-ask (after a resolver restart) replaces it.
        for other, pending in list(self.pending.items()):
            if pending["approval"]["ticket"] == approval["ticket"]:
                self.pending.pop(other)
        ticket = self.tickets.get(approval["ticket"])
        about = {k: ticket.get(k) for k in ("text", "expect", "customer")} if ticket else None
        self.pending[request_id] = {"approval": approval, "ticket": about}
        await self.save_approvals()
        await self.broadcast(approval)
        if self.demo.running:
            asyncio.create_task(self.auto_decide(request_id, approval["text"]))
        # Return straight away: the decision goes back to the resolver as a
        # message when someone clicks, whenever that is.

    async def auto_decide(self, request_id: str, text: str) -> None:
        """Demo loop: nobody is at the keyboard, so the desk's rule decides. A
        person who clicks first still wins."""
        await asyncio.sleep(C.DEMO_APPROVE_DELAY)
        await self.decide(request_id, auto_decision(text), auto=True)

    async def auto_decide_waiting(self) -> None:
        """Demo loop starting: approvals already waiting (from before it started,
        or from before a restart) are decided too, or their tickets never finish."""
        for request_id, pending in list(self.pending.items()):
            asyncio.create_task(self.auto_decide(request_id, pending["approval"]["text"]))

    async def decide(self, request_id: str, decision: str, auto: bool = False) -> bool:
        pending = self.pending.pop(request_id, None)
        if not pending:
            return False
        t = pending["approval"]["ticket"]
        await self.desk.handoff(t, "resolver", {"ticket": t, "decision": decision})
        await self.save_approvals()
        await self.broadcast({"type": "approval_done", "id": request_id, "decision": decision, "auto": auto})
        return True

    # ---- HTTP
    async def index(self, _request) -> web.FileResponse:
        # No stale page after an edit: the browser revalidates every load.
        return web.FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-cache"})

    async def config(self, _request) -> web.Response:
        return web.json_response({
            "scenarios": {k: {"about": v[0], "message": v[1], "customer": C.scenario_customer(k)}
                          for k, v in C.SCENARIOS.items()},
            "customers": C.CUSTOMERS, "customer": C.CUSTOMER,
            "llm": self.desk.llm.name, "pids": self.pids,
            "memory": {"kb": self.kb_count, "history": self.history_count},
            "jaeger": os.environ.get("DESK_JAEGER_UI", "http://localhost:16686"),
            "pause": os.environ.get("DESK_POLICY_PAUSE", "0"), "demo": self.demo.running})

    async def new_ticket(self, request) -> web.Response:
        body = await request.json()
        scenario = body.get("scenario")
        text = (body.get("text") or "").strip()
        if not text and scenario in C.SCENARIOS:
            text = C.SCENARIOS[scenario][1]
        if not text:
            return web.json_response({"error": "empty message"}, status=400)
        return web.json_response(await self.open_ticket(scenario, text, body.get("customer")))

    async def open_ticket(self, scenario: str | None, text: str | None = None,
                          customer: str | None = None, demo: bool = False) -> dict:
        """Open a ticket and hand it to triage. Returns what the page needs to show it."""
        text = text or C.SCENARIOS[scenario][1]
        if customer not in C.CUSTOMERS:
            customer = C.scenario_customer(scenario)
        expect = None
        if (scenario in C.SCENARIOS and text == C.SCENARIOS[scenario][1]
                and customer == C.scenario_customer(scenario)):
            expect = (C.SCENARIOS[scenario][2], C.SCENARIOS[scenario][3])
        if expect:
            # A scripted scenario starts from its configured order, whatever earlier runs did.
            await self.desk.reset_orders(text)
        session = self.desk.sessions.start()
        t = session.conversation
        root = telemetry.start_root("ticket", ticket=t, scenario=scenario or "chat")
        self.tickets[t] = {"text": text, "expect": expect, "customer": customer, "root": root,
                           "done": asyncio.Event()}
        opened = {"ticket": t, "text": text, "customer": customer, "scenario": scenario,
                  "trace": telemetry.trace_id(root)}
        if demo:
            # Before the first event, so every open page follows this ticket from its start.
            await self.broadcast({"type": "ticket", "demo": True, **opened})
        await session.append("instruction", text)
        await self.desk.set_customer(t, customer)
        await self.desk.status(t, "open")
        await self.desk.handoff(t, "triage", {"ticket": t, "customer": customer, "text": text,
                                              "trace": telemetry.carrier(root)})
        return opened

    async def reset_data(self) -> None:
        self.kb_count, self.history_count = await self.desk.reset_demo()

    async def reset(self, _request) -> web.Response:
        await self.reset_data()
        return web.json_response({"kb": self.kb_count, "history": self.history_count})

    async def demo_toggle(self, request) -> web.Response:
        body = await request.json()
        if body.get("on"):
            self.demo.start()
        else:
            self.demo.stop()
        return web.json_response({"demo": body.get("on") and self.demo.running})

    async def clear_memory(self, request) -> web.Response:
        body = await request.json()
        result = await self.desk.clear_memory(kb=bool(body.get("kb")))
        self.kb_count, self.history_count = await self.desk.seed_memory()
        return web.json_response({**result, "kb": self.kb_count, "history": self.history_count})

    async def kill(self, request) -> web.Response:
        body = await request.json()
        name, t = body.get("agent"), body.get("ticket") or "-"
        if name not in AGENTS:
            return web.json_response({"error": "unknown agent"}, status=400)
        pid = await self.supervisor.kill(name)
        if pid:
            await self.desk.event(t, "killed", agent="supervisor",
                                  text=f"{name} pid {pid} killed (SIGKILL)", target=name)
        return web.json_response({"killed": pid})

    async def approve(self, request) -> web.Response:
        body = await request.json()
        decision = "approved" if body.get("decision") == "approved" else "declined"
        if not await self.decide(body.get("id"), decision):
            return web.json_response({"error": "no such approval"}, status=404)
        return web.json_response({"decision": decision})

    async def csat(self, request) -> web.Response:
        body = await request.json()
        score = max(1, min(5, int(body.get("score", 0))))
        await self.desk.event(body["ticket"], "csat", agent="customer", score=score, text=f"{score}/5")
        return web.json_response({"score": score})

    async def readers(self, request) -> web.Response:
        t = request.query.get("ticket")
        if not t:
            return web.json_response({"error": "ticket required"}, status=400)
        try:
            return web.json_response(await readers(self.desk, t))
        except ls.InvalidError:
            return web.json_response({"error": "not a ticket id"}, status=400)

    async def websocket(self, request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse(heartbeat=20)
        await ws.prepare(request)
        self.sockets.add(ws)
        await ws.send_str(json.dumps({"type": "hello", "pids": self.pids, "demo": self.demo.running}))
        for pending in list(self.pending.values()):
            await ws.send_str(json.dumps(pending["approval"]))
        async for msg in ws:
            if msg.type == WSMsgType.ERROR:
                break
        self.sockets.discard(ws)
        return ws


def make_app() -> web.Application:
    state = App()
    app = web.Application()
    app.on_startup.append(state.start)
    app.on_cleanup.append(state.stop)
    app.add_routes([
        web.get("/", state.index),
        web.get("/api/config", state.config),
        web.post("/api/ticket", state.new_ticket),
        web.post("/api/kill", state.kill),
        web.post("/api/reset", state.reset),
        web.post("/api/clear-memory", state.clear_memory),
        web.post("/api/approve", state.approve),
        web.post("/api/demo", state.demo_toggle),
        web.post("/api/csat", state.csat),
        web.get("/api/readers", state.readers),
        web.get("/ws", state.websocket),
        web.static("/static", STATIC),
    ])
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host=os.environ.get("DESK_HOST", "localhost"), port=int(os.environ.get("DESK_PORT", "8000")),
                print=lambda *_: None)
