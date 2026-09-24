"""Shared plumbing: connection, topics, the events index, memory, and the Desk
helpers every process (agents, server, CLI runner) uses."""

from __future__ import annotations

import asyncio
import json
import os
import time

import laser_sdk as ls

from . import config as C
from . import telemetry
from .llm import is_mock, parse_json

INDEX_FIELDS = ["ticket", "agent", "step", "ts", "status", "score"]


async def connect() -> ls.Laser:
    return await ls.Laser.connect(C.connection_string(), stream=C.STREAM)


async def setup(laser: ls.Laser, *, with_index: bool) -> None:
    """Idempotent: topics, and (for the server and runner) the events index."""
    await laser.bootstrap(C.PARTITIONS)
    await laser.topic(C.DESK_TOPIC).ensure(partitions=C.PARTITIONS)
    await laser.topic(C.EVENTS_TOPIC).ensure(partitions=C.PARTITIONS)
    if with_index:
        await start_projector(laser)


async def start_projector(laser: ls.Laser) -> None:
    pid = f"{C.EVENTS_INDEX}.v1"
    try:
        await laser.register_projection({
            "id": pid, "name": C.EVENTS_INDEX, "version": 1, "content_type": "json",
            "extraction": {"fields": [{"name": f, "pointer": f"/{f}"} for f in INDEX_FIELDS],
                           "inline_payload": False},
            "inline_payload_default": False})
        await laser.apply_binding({
            "source": {"stream": laser.default_stream, "topic": C.EVENTS_TOPIC},
            "allowed_projections": [pid], "default_projection": pid,
            "notify": True, "index": C.EVENTS_INDEX})
    except ls.LaserError as error:
        # Already registered by an earlier run: fine, it keeps materializing.
        if "exist" not in str(error).lower() and "conflict" not in str(error).lower():
            print(f"  events index: {error} (continuing)")
    deadline = time.monotonic() + 60
    while True:
        try:
            await laser.query(C.EVENTS_INDEX).limit(1).fetch()
            return
        except ls.LaserError:
            if time.monotonic() > deadline:
                raise
            await asyncio.sleep(0.2)


async def embed(text: str) -> list[float]:
    """Deterministic bag-of-words embedder. A real model is item 4 on the list."""
    dims, vector, token = 64, [0.0] * 64, ""
    for char in text.lower() + " ":
        if char.isalnum():
            token += char
            continue
        if token:
            h = 0x811C9DC5
            for byte in token.encode():
                h = ((h ^ byte) * 0x01000193) & 0xFFFFFFFF
            vector[h % dims] += 1.0
            token = ""
    norm = sum(v * v for v in vector) ** 0.5
    return [v / norm for v in vector] if norm else vector


def is_addressed(message, agent: str):
    """All three agents read the one log; each acts only on what is routed to it."""
    if message.provenance.target_agent_id != agent:
        return None
    try:
        payload = message.json()
    except ls.CodecError:
        return None
    return payload if isinstance(payload, dict) else None


class Desk:
    """What an agent (or the server) needs: the log, state, memory, the LLM."""

    def __init__(self, laser: ls.Laser, agent: str, llm=None) -> None:
        self.laser = laser
        self.agent = agent
        self.llm = llm
        self.sessions = laser.sessions()
        self.state = laser.kv(C.NS_TICKETS)
        self.accounts = laser.kv(C.NS_ACCOUNTS)
        self.ckpts = laser.kv(C.NS_CKPT)
        self.crashes = laser.kv(C.NS_CRASH)
        self.orders = laser.kv(C.NS_ORDERS)
        self.meta = laser.kv(C.NS_META)
        # Durable memory: named sessions, so the same name reaches the same
        # conversation from any process, run after run. Only conversation-scoped
        # memory reads back on Laser SDK 0.4.0 (see memory_probe.py).
        self.kb_mem = self.sessions.create(C.KB_SCOPE).memory()
        self._kb_index = None

    # ---- the log
    async def event(self, t: str, step: str, agent: str | None = None, **extra) -> None:
        record = {"ticket": t, "agent": agent or self.agent, "step": step,
                  "ts": time.time_ns() // 1000, "pid": os.getpid(), **extra}
        await self.laser.topic(C.EVENTS_TOPIC).publish().json(record).send()

    async def status(self, t: str, value: str) -> None:
        await self.state.set(t).payload(value).send()
        await self.event(t, "status", agent="desk", status=value)

    async def get_status(self, t: str) -> str | None:
        raw = await self.state.get(t)
        return bytes(raw).decode() if raw is not None else None

    async def handoff(self, t: str, target: str, body: dict, key: str | None = None) -> None:
        body = {**body, "trace": telemetry.carrier() or body.get("trace", {})}
        prov = ls.Provenance(conversation_id=t, agent=self.agent, target_agent_id=target,
                             idempotency_key=key)
        await self.laser.send_agent(C.DESK_TOPIC, json.dumps(body).encode(), prov)

    # ---- the order system (state in KV, seeded from config.ORDERS)
    async def get_order(self, order) -> dict | None:
        if order is None:
            return None
        raw = await self.orders.get(str(order))
        if raw is not None:
            return json.loads(bytes(raw).decode())
        seed = C.ORDERS.get(str(order))
        if seed is not None:
            await self.orders.set(str(order)).payload(json.dumps(seed)).send()
        return json.loads(json.dumps(seed)) if seed is not None else None

    async def record_refund(self, order, amount: int) -> None:
        record = await self.get_order(order)
        if record is None:
            return
        record.setdefault("refunds", []).append(amount)
        await self.orders.set(str(order)).payload(json.dumps(record)).send()

    async def reset_orders(self, text: str | None = None) -> list[str]:
        """Put orders back to config.ORDERS: those named in `text`, or all of them."""
        ids = [o for o in C.ORDERS if text is None or o in text]
        for order in ids:
            await self.orders.set(order).payload(json.dumps(C.ORDERS[order])).send()
        return ids

    async def reset_demo(self) -> tuple[int, int]:
        """A clean slate: orders as configured, customer history cleared and reseeded."""
        await self.reset_orders()
        await self.clear_memory(kb=False)
        return await self.seed_memory()

    async def clear_memory(self, *, kb: bool = False) -> dict:
        """Forget every customer history note (and the knowledge base with kb=True).
        Memory is a log, so forgetting appends a tombstone per item; recall stops
        returning them. If anything survives, move to a fresh memory epoch instead."""
        # Session memory is laser.memory(<the sessions' namespace>) scoped to the
        # session's conversation: the default namespace is agent.session.
        store = self.laser.memory("agent.session")
        targets = [self.sessions.create(self._history_name(await self.epoch())).conversation]
        if kb:
            targets.append(self.sessions.create(C.KB_SCOPE).conversation)
        removed = 0
        for conversation in targets:
            for item in await store.recall(limit=2000, conversation=conversation):
                await store.forget(item.id, conversation=conversation)
                removed += 1
        left = 0
        for conversation in targets:
            left += len(await store.recall(limit=10, conversation=conversation))
        if left:
            await self.meta.set("epoch").payload(str(await self.epoch() + 1)).send()
        self._kb_index = None
        return {"forgotten": removed, "still_recalled": left, "new_epoch": bool(left)}

    # ---- memory
    async def epoch(self) -> int:
        raw = await self.meta.get("epoch")
        return int(bytes(raw).decode()) if raw is not None else 0

    @staticmethod
    def _history_name(epoch: int) -> str:
        return C.HISTORY_SCOPE if epoch == 0 else f"{C.HISTORY_SCOPE}-e{epoch}"

    async def history_mem(self):
        return self.sessions.create(self._history_name(await self.epoch())).memory()

    async def seed_memory(self) -> tuple[int, int]:
        counts = []
        for mem, seed in ((self.kb_mem, C.KB_NOTES), (await self.history_mem(), C.HISTORY_NOTES)):
            have = {item.text for item in await mem.recall(limit=500)}
            missing = [note for note in seed if note not in have]
            for note in missing:
                await mem.remember(note)
            counts.append(len(have) + len(missing))
        return counts[0], counts[1]

    async def kb_recall(self, query: str, limit: int = 3) -> list[str]:
        if self._kb_index is None:
            self._kb_index = self.laser.vector_memory(embed)
            notes = {item.text for item in await self.kb_mem.recall(limit=500)} or set(C.KB_NOTES)
            for note in notes:
                await self._kb_index.remember(note)
        return [m.text for m in await self._kb_index.recall(semantic=query, limit=limit)]

    async def history_recall(self, query: str, limit: int = 3) -> list[str]:
        # Reloaded each time: another process may have written to it since.
        index = self.laser.vector_memory(embed)
        for item in await (await self.history_mem()).recall(limit=500):
            await index.remember(item.text)
        return [m.text for m in await index.recall(semantic=query, limit=limit)]

    async def history_recent(self, n: int = 3) -> list[str]:
        items = [item.text for item in await (await self.history_mem()).recall(limit=500)]
        return items[-n:]  # recall returns oldest first

    async def learn(self, note: str) -> None:
        await (await self.history_mem()).remember(note)

    # ---- the LLM
    async def think(self, t: str, prompt: str) -> str:
        with telemetry.span(f"{self.agent}.llm", model=self.llm.name):
            text = await self.llm.complete(prompt)
        await self.event(t, "llm", text=self.llm.last[:400])
        return text

    async def think_json(self, t: str, prompt: str, default: dict) -> dict:
        text = await self.think(t, prompt + "\n\nReply with only a JSON object, no prose, no code fences.")
        return parse_json(text, default)

    async def write(self, t: str, prompt: str, fallback: str) -> str:
        """Customer-facing text from the LLM, with a plain fallback."""
        text = await self.think(t, prompt + "\nReply text only.")
        return fallback if is_mock(text) else " ".join(text.split())
