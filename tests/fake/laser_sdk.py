"""In-memory stand-in for the slice of laser_sdk the desk uses. Test only."""
import asyncio, json, itertools

class LaserError(Exception): pass
class CodecError(LaserError): pass
class KvError(LaserError):
    def __init__(self, msg, version_conflict=False): super().__init__(msg); self.version_conflict=version_conflict
class UnsupportedError(LaserError): pass

class TopicRetention:
    @staticmethod
    def expire_after(ms): return TopicRetention()

_ids = itertools.count(1)
def new_conversation_id(): return f"01TEST{next(_ids):020d}"

class Provenance:
    def __init__(self, *, conversation_id=None, agent=None, target_agent_id=None, idempotency_key=None, **kw):
        self.conversation_id=conversation_id; self.agent=agent; self.target_agent_id=target_agent_id; self.idempotency_key=idempotency_key

class Message:
    def __init__(self, payload, prov, topic=None):
        self.payload=list(payload); self.provenance=prov; self.topic=topic
        self.id=MessageId(0, next(_ids))
    def body(self): return self.payload
    def json(self):
        try: return json.loads(bytes(self.payload))
        except Exception: raise CodecError("not json")

class MessageId:
    def __init__(self, partition_id, offset): self.partition_id=partition_id; self.offset=offset

class Checkpoint:
    def __init__(self, n): self.n=n
    def to_json(self): return json.dumps({"n": self.n})
    @staticmethod
    def from_json(s): return Checkpoint(json.loads(s)["n"])

class Turn:
    def __init__(self, kind, text): self.display=kind; self._t=text
    def text(self): return self._t

def event_envelope(record, conversation, source, body, *, operation=None, **kw):
    return {"kind": "event", "record": record, "conversation": conversation, "source": source,
            "body": bytes(body).decode(), "operation": operation}

class ToolCall:
    def __init__(self, turns): self.turns=turns
    async def complete(self, result): self.turns.append(Turn("tool.result", bytes(result).decode()))

class Item:
    def __init__(self, text, id=None): self._t=text; self.id=id
    def text(self): return self._t

class Scoped:
    def __init__(self, store): self.store=store
    async def remember(self, payload): self.store.append(payload if isinstance(payload,str) else bytes(payload).decode()); return "id"
    async def recall(self, limit=50, **kw): return [Item(t, str(i)) for i, t in enumerate(self.store) if t is not None][:limit]

MEM = {}

class Memory:
    async def recall(self, limit=50, conversation=None, **kw):
        return [Item(t, str(i)) for i, t in enumerate(MEM.get(conversation, [])) if t is not None][:limit]
    async def forget(self, memory_id, conversation=None, **kw):
        MEM[conversation][int(memory_id)] = None
class Session:
    def __init__(self, laser, conv): self.laser=laser; self.conversation=conv; self.turns=laser.turns.setdefault(conv, [])
    def as_agent(self, agent): return self
    async def append(self, envelope):
        assert envelope["conversation"] == self.conversation
        self.turns.append(Turn("agent.message", envelope["body"]))
    async def tool(self, name, args):
        self.turns.append(Turn("tool.call", json.dumps(args))); return ToolCall(self.turns)
    async def context(self, **kw): return list(self.turns)
    async def checkpoint(self): return Checkpoint(len(self.turns))
    async def turns_since(self, cp): return self.turns[cp.n:]
    def memory(self, m=None): return Scoped(MEM.setdefault(self.conversation, []))

class Lease:
    def release(self): pass

class SessionBuilder:
    def __init__(self, laser, conv): self.laser=laser; self.conv=conv
    def agent(self, agent): return self
    def id(self): return self.conv
    async def begin(self):
        session = Session(self.laser, self.conv); session.turns.append(Turn("session.started", ""))
        return session, Lease()

class Sessions:
    def __init__(self, laser): self.laser=laser
    async def bootstrap(self, partitions, retention): pass
    def start(self): return SessionBuilder(self.laser, new_conversation_id())
    def open(self, c): return Session(self.laser, c)
    def create(self, name): return SessionBuilder(self.laser, "named-"+name)

class KvEntry:
    def __init__(self, value, version): self.value=list(value); self.version=version
    def decode_value(self): return json.loads(bytes(self.value))

class KvSet:
    def __init__(self, kv, key): self.kv=kv; self.key=key; self.val=None; self.expect=None
    def bytes(self, v): self.val = v.encode() if isinstance(v,str) else bytes(v); return self
    def json(self, v): self.val = json.dumps(v).encode(); return self
    def ttl(self, t): return self
    def expect_version(self, v): self.expect=v; return self
    def expect_absent(self): self.expect=0; return self
    async def send(self):
        self.kv[self.key]=self.val; self.kv.versions[self.key]=self.kv.versions.get(self.key, 0)+1
    async def commit(self):
        if self.kv.versions.get(self.key, 0) != self.expect: raise KvError("version conflict", True)
        await self.send(); return self.kv.versions[self.key]

class Kv(dict):
    def __init__(self): super().__init__(); self.versions={}
    async def get(self, key): return dict.get(self, key)
    async def get_entry(self, key):
        v = dict.get(self, key)
        return None if v is None else KvEntry(v, self.versions.get(key, 0))
    def set(self, key): return KvSet(self, key)

KVS = {}
class Publish:
    def __init__(self, topic): self.topic=topic; self.body=None
    def json(self, v): self.body=v; return self
    async def send(self): self.topic.laser.log.setdefault(self.topic.name, []).append(self.body)

class Cursor:
    def __init__(self, topic): self.topic=topic; self.pos=0
    async def poll(self):
        items=self.topic.laser.log.get(self.topic.name, [])[self.pos:]; self.pos+=len(items)
        return [Message(json.dumps(i).encode(), Provenance()) for i in items]

class Topic:
    def __init__(self, laser, name): self.laser=laser; self.name=name
    def publish(self, body=None): return Publish(self)
    def replay(self, **kw): return Cursor(self)
    async def ensure(self, partitions=1): pass

class VectorMemory:
    @staticmethod
    def vector(embed): return Vector()

class Vector:
    def __init__(self): self.items=[]
    async def remember(self, t): self.items.append(t)
    async def recall(self, semantic=None, limit=50): return [Item(t) for t in self.items[:limit]]

class Laser:
    default_stream = "test"
    def __init__(self):
        self.log={}; self.turns={}; self.sent=[]; self.human=None
    @staticmethod
    async def connect(conn, stream=None): return Laser()
    def topic(self, name): return Topic(self, name)
    def kv(self, ns): return KVS.setdefault(ns, Kv())
    def sessions(self, **kw): return Sessions(self)
    def memory(self, namespace): return Memory()
    async def send_agent(self, topic, payload, prov): self.sent.append(Message(payload, prov, topic))
    async def request(self, req, rep, payload, prov, timeout_secs=30):
        return await self.human(Message(payload, prov, req))

class Q:
    def __getattr__(self, n): return lambda *a, **k: self
    async def fetch(self): raise UnsupportedError("query unavailable in fake")
Laser.query = lambda self, idx: Q()
