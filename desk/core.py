"""Shared plumbing: connection, topics, the events index, memory, and the Desk
helpers every process (agents, server, CLI runner) uses."""

from __future__ import annotations

import asyncio
import json
import os
import re
import threading
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
    await laser.sessions().bootstrap(C.PARTITIONS, ls.TopicRetention.expire_after(C.SESSION_RETENTION_MS))
    await laser.topic(C.DESK_TOPIC).ensure(partitions=C.PARTITIONS)
    await laser.topic(C.EVENTS_TOPIC).ensure(partitions=C.PARTITIONS)
    await laser.topic(C.HUMAN_TOPIC).ensure(partitions=1)
    if with_index:
        await start_projector(laser)


async def start_projector(laser: ls.Laser) -> None:
    pid = f"{C.EVENTS_INDEX}.v1"
    try:
        await laser.projections().register({
            "id": pid, "name": C.EVENTS_INDEX, "version": 1, "content_type": "json",
            "extraction": {"fields": [{"name": f, "pointer": f"/{f}"} for f in INDEX_FIELDS],
                           "inline_payload": False},
            "inline_payload_default": False})
        await laser.bindings().apply({
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


# Semantic recall uses a small local model: fastembed running BAAI/bge-small-en-v1.5
# on the CPU (about 64 MB, downloaded once to ~/.cache/fastembed, offline after).
# DESK_EMBEDDER=toy, or fastembed not installed, falls back to bag-of-words.
EMBED_MODEL = "BAAI/bge-small-en-v1.5"
_model = None  # loaded on first use; False once it is known to be unavailable
_model_lock = threading.Lock()


def load_embedder():
    """The embedding model, loaded once per process (None means bag-of-words)."""
    global _model
    with _model_lock:
        if _model is None:
            _model = False
            if os.environ.get("DESK_EMBEDDER", "fastembed") != "toy":
                try:
                    from fastembed import TextEmbedding
                    cache = os.environ.get("FASTEMBED_CACHE_PATH",
                                           os.path.expanduser("~/.cache/fastembed"))
                    _model = TextEmbedding(EMBED_MODEL, cache_dir=cache)
                except Exception as error:  # noqa: BLE001
                    print(f"  semantic recall: {EMBED_MODEL} unavailable ({error}); "
                          "using the bag-of-words embedder", flush=True)
        return _model or None


def embedder_name() -> str:
    return EMBED_MODEL if load_embedder() else "bag-of-words"


async def embed(text: str) -> list[float]:
    model = await asyncio.to_thread(load_embedder)
    if model is None:
        return bag_of_words(text)
    vector = await asyncio.to_thread(lambda: next(iter(model.embed([text]))))
    return [float(v) for v in vector]


def bag_of_words(text: str) -> list[float]:
    """Deterministic fallback: shared words only, no meaning."""
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


async def cas_update(kv, key: str, change):
    """Read a JSON value, change it, and write it back only if nobody else wrote
    in between; if they did, read again and retry. change(value) returns the new
    value, or None to leave it as it is. Returns the value as it now stands."""
    while True:
        entry = await kv.get_entry(key)
        current = entry.decode_value() if entry is not None else None
        new = change(current)
        if new is None:
            return current
        write = kv.set(key).json(new)
        write = write.expect_version(entry.version) if entry is not None else write.expect_absent()
        try:
            await write.commit()
            return new
        except ls.KvError as error:
            if not getattr(error, "version_conflict", False):
                raise


def auto_decision(text: str) -> str:
    """Stands in for a person on an approval: approve up to the desk's limit, decline over it."""
    amount = re.search(r"\$(\d+(?:\.\d{1,2})?)", text)
    value = int(round(float(amount.group(1)) * 100)) if amount else 0
    return "approved" if value <= C.APPROVAL_LIMIT_CENTS else "declined"


def readable(turns) -> list:
    """The turns worth showing a model or a person: not the session's own
    lifecycle records."""
    return [turn for turn in turns if not turn.display.startswith("session")]


def is_addressed(message, agent: str):
    """All three agents read the one log; each acts only on what is routed to it."""
    if message.provenance.target_agent_id != agent:
        return None
    try:
        payload = json.loads(bytes(message.body()))
    except ValueError:
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
        # conversation from any process, run after run.
        self.kb_mem = self.named(C.KB_SCOPE).memory()
        self._kb_index = None
        # Customer history, embedded once per note per process (see history_recall),
        # one index per customer: (index, notes seen, epoch).
        self._history: dict[str, tuple] = {}

    # ---- the ticket's session
    async def open_ticket(self) -> str:
        """Start the session for a new ticket; its id is the ticket id. Every
        agent then writes into it as itself."""
        session, lease = await self.sessions.start().agent(self.agent).begin()
        lease.release()
        return session.conversation

    def session(self, t: str):
        return self.sessions.open(t).as_agent(self.agent)

    def named(self, label: str):
        return self.sessions.open(self.sessions.create(label).id())

    async def say(self, t: str, kind: str, text: str) -> None:
        """Record a turn on the ticket's session."""
        await self.session(t).append(ls.event_envelope(
            ls.new_conversation_id(), t, self.agent, text.encode(), operation=kind))

    # ---- the log
    async def event(self, t: str, step: str, agent: str | None = None, **extra) -> None:
        record = {"ticket": t, "agent": agent or self.agent, "step": step,
                  "ts": time.time_ns() // 1000, "pid": os.getpid(), **extra}
        await self.laser.topic(C.EVENTS_TOPIC).publish().json(record).send()

    async def status(self, t: str, value: str) -> None:
        await self.state.set(t).bytes(value).send()
        await self.event(t, "status", agent="desk", status=value)

    async def get_status(self, t: str) -> str | None:
        raw = await self.state.get(t)
        return bytes(raw).decode() if raw is not None else None

    async def set_customer(self, t: str, customer: str) -> None:
        await self.state.set(f"{t}:customer").bytes(customer).send()

    async def get_customer(self, t: str) -> str:
        raw = await self.state.get(f"{t}:customer")
        return bytes(raw).decode() if raw is not None else C.CUSTOMER

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
            record = json.loads(bytes(raw).decode())
            record.pop("refund_keys", None)  # bookkeeping, not part of the order
            return record
        seed = C.ORDERS.get(str(order))
        if seed is not None:
            await self.orders.set(str(order)).bytes(json.dumps(seed)).send()
        return json.loads(json.dumps(seed)) if seed is not None else None

    async def record_refund(self, order, amount: int, key: str) -> None:
        """Add the refund to the order, once per key: a redelivered refund that
        already got this far changes nothing."""
        seed = C.ORDERS.get(str(order))

        def change(record):
            if record is None and seed is not None:
                record = json.loads(json.dumps(seed))
            if record is None or key in record.get("refund_keys", []):
                return None
            record.setdefault("refunds", []).append(amount)
            record.setdefault("refund_keys", []).append(key)
            return record
        if order is not None:
            await cas_update(self.orders, str(order), change)

    # ---- the customer's account: total refunded, and which refunds are in it
    @staticmethod
    def _account(value) -> dict:
        if isinstance(value, dict):
            return value
        return {"cents": int(value or 0), "keys": []}  # written by an older version

    async def refunded(self, customer: str) -> int:
        entry = await self.accounts.get_entry(customer)
        return self._account(entry.decode_value() if entry is not None else None)["cents"]

    async def add_refund(self, customer: str, amount: int, key: str) -> int:
        """Add to the customer's total, once per key. Compare-and-swap, so two
        writers never overwrite each other's refunds."""
        def change(value):
            account = self._account(value)
            if key in account["keys"]:
                return None
            return {"cents": account["cents"] + amount, "keys": account["keys"] + [key]}
        return self._account(await cas_update(self.accounts, customer, change))["cents"]

    async def reset_orders(self, text: str | None = None) -> list[str]:
        """Put orders back to config.ORDERS: those named in `text`, or all of them."""
        ids = [o for o in C.ORDERS if text is None or o in text]
        for order in ids:
            await self.orders.set(order).bytes(json.dumps(C.ORDERS[order])).send()
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
        epoch = await self.epoch()
        targets = [self.named(self._history_name(c, epoch)).conversation for c in C.CUSTOMERS]
        if kb:
            targets.append(self.named(C.KB_SCOPE).conversation)
        removed = 0
        for conversation in targets:
            for item in await store.recall(limit=2000, conversation=conversation):
                await store.forget(item.id, conversation=conversation)
                removed += 1
        left = 0
        for conversation in targets:
            left += len(await store.recall(limit=10, conversation=conversation))
        if left:
            await self.meta.set("epoch").bytes(str(await self.epoch() + 1)).send()
        self._kb_index = None
        self._history = {}
        return {"forgotten": removed, "still_recalled": left, "new_epoch": bool(left)}

    # ---- memory
    async def epoch(self) -> int:
        raw = await self.meta.get("epoch")
        return int(bytes(raw).decode()) if raw is not None else 0

    @staticmethod
    def _history_name(customer: str, epoch: int) -> str:
        scope = C.history_scope(customer)
        return scope if epoch == 0 else f"{scope}-e{epoch}"

    async def history_mem(self, customer: str):
        return self.named(self._history_name(customer, await self.epoch())).memory()

    async def seed_memory(self) -> tuple[int, int]:
        """Knowledge base notes, and customer history notes across all customers."""
        counts = []
        stores = [(self.kb_mem, C.KB_NOTES)] + [
            (await self.history_mem(c), C.HISTORY_NOTES[c]) for c in C.CUSTOMERS]
        for mem, seed in stores:
            have = {item.text() for item in await mem.recall(limit=500)}
            missing = [note for note in seed if note not in have]
            for note in missing:
                await mem.remember(note)
            counts.append(len(have) + len(missing))
        return counts[0], sum(counts[1:])

    async def kb_recall(self, query: str, limit: int = 3) -> list[str]:
        if self._kb_index is None:
            self._kb_index = ls.VectorMemory.vector(embed)
            notes = {item.text() for item in await self.kb_mem.recall(limit=500)} or set(C.KB_NOTES)
            for note in notes:
                await self._kb_index.remember(note)
        return [m.text() for m in await self._kb_index.recall(semantic=query, limit=limit)]

    async def history_recall(self, customer: str, query: str, limit: int = 3) -> list[str]:
        # Another process may have written notes since the last call, so read the
        # durable memory each time, but only embed the notes this index has not
        # seen. A cleared memory (or a new epoch) rebuilds it.
        epoch = await self.epoch()
        notes = [item.text() for item in await (await self.history_mem(customer)).recall(limit=500)]
        index, seen, built = self._history.get(customer, (None, set(), None))
        if index is None or built != epoch or not seen <= set(notes):
            index, seen = ls.VectorMemory.vector(embed), set()
            self._history[customer] = (index, seen, epoch)
        for note in notes:
            if note not in seen:
                seen.add(note)
                await index.remember(note)
        return [m.text() for m in await index.recall(semantic=query, limit=limit)]

    async def history_recent(self, customer: str, n: int = 3) -> list[str]:
        items = [item.text() for item in await (await self.history_mem(customer)).recall(limit=500)]
        return items[-n:]  # recall returns oldest first

    async def learn(self, customer: str, note: str) -> None:
        await (await self.history_mem(customer)).remember(note)

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
