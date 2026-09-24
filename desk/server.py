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
from .core import Desk, connect, setup
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
        self.laser = await connect()
        await setup(self.laser, with_index=True)
        self.desk = Desk(self.laser, "desk", choose_llm())
        self.kb_count, self.history_count = await self.desk.seed_memory()
        self.tail = EventTail(self.laser, self.on_event)
        await self.tail.skip_history()
        self.tail_task = asyncio.create_task(self.tail.run())
        env = {"DESK_CRASH_ONCE": "0",
               "DESK_POLICY_PAUSE": os.environ.get("DESK_POLICY_PAUSE", "0")}
        self.supervisor = Supervisor(AGENTS, env, self.on_agent)
        await self.supervisor.start()
        # The human in the loop: approval requests land here and wait for a click.
        self.human = self.laser.spawn_agent("human", ls.Topics.HUMAN_INPUT, self.on_approval,
                                            poll_interval_ms=20)
        await self.human.ready()
        print(f"  LLM: {self.desk.llm.name}. Open http://localhost:{os.environ.get('DESK_PORT', '8000')}")

    async def stop(self, _app) -> None:
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

    async def on_agent(self, name: str, state: str, pid: int, code=None) -> None:
        self.pids[name] = pid if state == "started" else None
        await self.broadcast({"type": "agent", "name": name, "state": state, "pid": pid, "code": code})

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
        self.pending[request_id] = {"approval": approval}
        await self.broadcast(approval)
        # Return straight away: the decision goes back to the resolver as a
        # message when someone clicks, whenever that is.

    # ---- HTTP
    async def index(self, _request) -> web.FileResponse:
        # No stale page after an edit: the browser revalidates every load.
        return web.FileResponse(STATIC / "index.html", headers={"Cache-Control": "no-cache"})

    async def config(self, _request) -> web.Response:
        return web.json_response({
            "scenarios": {k: {"about": v[0], "message": v[1]} for k, v in C.SCENARIOS.items()},
            "llm": self.desk.llm.name, "pids": self.pids,
            "memory": {"kb": self.kb_count, "history": self.history_count},
            "jaeger": os.environ.get("DESK_JAEGER_UI", "http://localhost:16686"),
            "pause": os.environ.get("DESK_POLICY_PAUSE", "0")})

    async def new_ticket(self, request) -> web.Response:
        body = await request.json()
        scenario = body.get("scenario")
        text = (body.get("text") or "").strip()
        if not text and scenario in C.SCENARIOS:
            text = C.SCENARIOS[scenario][1]
        if not text:
            return web.json_response({"error": "empty message"}, status=400)
        expect = None
        if scenario in C.SCENARIOS and text == C.SCENARIOS[scenario][1]:
            expect = (C.SCENARIOS[scenario][2], C.SCENARIOS[scenario][3])
        if expect:
            # A scripted scenario starts from its configured order, whatever earlier runs did.
            await self.desk.reset_orders(text)
        session = self.desk.sessions.start()
        t = session.conversation
        root = telemetry.start_root("ticket", ticket=t, scenario=scenario or "chat")
        self.tickets[t] = {"text": text, "expect": expect, "root": root}
        await session.append("instruction", text)
        await self.desk.status(t, "open")
        await self.desk.handoff(t, "triage", {"ticket": t, "customer": C.CUSTOMER, "text": text,
                                              "trace": telemetry.carrier(root)})
        return web.json_response({"ticket": t, "text": text, "trace": telemetry.trace_id(root)})

    async def reset(self, _request) -> web.Response:
        self.kb_count, self.history_count = await self.desk.reset_demo()
        return web.json_response({"kb": self.kb_count, "history": self.history_count})

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
        pending = self.pending.pop(body.get("id"), None)
        if not pending:
            return web.json_response({"error": "no such approval"}, status=404)
        decision = "approved" if body.get("decision") == "approved" else "declined"
        t = pending["approval"]["ticket"]
        await self.desk.handoff(t, "resolver", {"ticket": t, "decision": decision})
        await self.broadcast({"type": "approval_done", "id": body.get("id"), "decision": decision})
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
        await ws.send_str(json.dumps({"type": "hello", "pids": self.pids}))
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
        web.post("/api/csat", state.csat),
        web.get("/api/readers", state.readers),
        web.get("/ws", state.websocket),
        web.static("/static", STATIC),
    ])
    return app


if __name__ == "__main__":
    web.run_app(make_app(), host=os.environ.get("DESK_HOST", "localhost"), port=int(os.environ.get("DESK_PORT", "8000")),
                print=lambda *_: None)
