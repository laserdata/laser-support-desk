# Support desk on Laser SDK

Agents fail. Their sessions shouldn't.

A customer support desk with three agents (triage, resolver, billing) writing to one durable, append-only log. Kill any agent mid-case and it comes back where it left off: the log redelivers what it had not finished, and its checkpoint says how far it got. Memory, context, state, analytics and observability all read the same stream.

Built on Laser SDK for Python, running on Laser Stack locally or LaserData Cloud. The log is Apache Iggy; KV, the query index behind the analytics, and the change feed are served by Laser Stack's plane from an embedded Turso database.

## What it shows

| On the poster | In the demo |
|---|---|
| Every agent writes to one durable, append-only log | Three agent processes on the `support.desk` topic, handoffs routed by provenance |
| Sessions | One session per ticket: every turn, the facts triage extracted (session memory), checkpoints |
| Crash, replay, resume | Kill the resolver (SIGKILL). The supervisor starts a new process, the log redelivers the handoff, and it resumes from its checkpoint with `turns_since` |
| Human in the loop | The resolver asks for approval over the log; you click Approve or Decline |
| A refund that fires once | Billing's dedup key lives in KV, so a redelivery after a restart is a no-op |
| Memory | Knowledge base and customer history, durable; each resolved ticket is written back |
| Context | Assembled per turn and fed to the LLM |
| State | Ticket status in KV, shown live |
| Analytics | Managed queries over the step events: steps per agent, resolution time, CSAT average |
| Observability | OpenTelemetry traces, one per ticket, across all three processes, in Jaeger; plus evals (checks from the log and an LLM judge) |

## Run it

You need Python 3.11+, Docker, and `claude` (Claude Code) on your PATH, or an Anthropic API key.

**1. Start Laser Stack.** In a checkout of [laser-stack](https://github.com/laserdata/laser-stack):

```
cp /path/to/laser-support-desk/deploy/laser-stack.compose.override.yaml compose.override.yaml
./scripts/up
```

The override is only needed on Docker Desktop (the Iggy server otherwise crash-loops binding memory to NUMA nodes the VM does not have). `./scripts/up` prints a `LASER_CONNECTION_STRING`.

**2. Start Jaeger** (optional, for traces), from this directory:

```
docker compose up -d
```

**3. Install and run:**

```
python3 -m venv .venv && . .venv/bin/activate
pip install -e .
export LASER_CONNECTION_STRING='iggy:laser@127.0.0.1:8090'
python -m desk.server
```

Open http://localhost:8000. Pick a scenario (or type your own message) and send it. While the resolver holds at its policy step, click **Kill resolver**. Approve or decline the refund when it asks. When the ticket closes, rate it; the rating lands on the log and in the CSAT average.

## Without a browser

```
python -m desk.run --list
python -m desk.run --scenario duplicate
python -m desk.run --scenario over-limit --no-crash
python -m desk.run --chat
```

The CLI runner starts the same three agent processes plus an auto-approver (it declines refunds over $200). The resolver kills itself once, mid-case, so every run exercises crash and resume. At the end it prints the readers and the evals.

## Scenarios

| Scenario | What it tests | Expected |
|---|---|---|
| `duplicate` | Order 1042 charged twice, $49 | refunded once, even when billing restarts and the refund is redelivered |
| `no-duplicate` | Claims a double charge on 1043, charged once | closed, no refund |
| `question` | Renewal date and invoice billing | answered from the knowledge base, billing never involved |
| `over-limit` | A real duplicate on 1044, but $450 | the human declines (over the $200 limit), escalated |
| `unknown-order` | Order 9999 does not exist | closed, no refund |
| `wrong-customer` | A real duplicate, but on another customer's order (1045) | closed, no refund |
| `already-refunded` | A duplicate on 1046 that was refunded last week | closed, no second refund |
| `triple-charge` | Order 1047 charged three times, $25 each | refunded $50, the two extra charges |
| `complaint` | Checkout timing out, no refund asked for | answered from the knowledge base |
| `no-order` | A refund request with no order number | closed, the reply asks for the order number |

Scenarios, orders and knowledge base notes are in `desk/config.py`.

## LaserData Cloud

Point `LASER_CONNECTION_STRING` at your deployment (`user:password@host` or `token@host`). TLS is handled by the SDK. Nothing else changes.

## Settings

See `.env.example`. The ones you are most likely to change:

- `DESK_LLM`: `claude` (the default when `claude` is on PATH, about 5 seconds a call), `api` (needs `ANTHROPIC_API_KEY`, faster), or `mock`.
- `DESK_POLICY_PAUSE`: how long the resolver holds at its policy step so you can kill it mid-case. Default 0 (off); set it to 6 or so when presenting the kill.
- `DESK_OTEL=0`: turn tracing off.

## Layout

```
desk/config.py      scenarios, orders, knowledge base, settings
desk/agents.py      triage, resolver, billing (one process each: python -m desk.agents <name>)
desk/supervisor.py  starts the agent processes and restarts any that die
desk/core.py        the log, state, memory and LLM helpers every process shares
desk/views.py       readers of the stream: the event tail, the readers panel, the evals
desk/server.py      the web demo
desk/run.py         the CLI runner
desk/telemetry.py   OpenTelemetry, by hand (Laser SDK's Python package has none yet)
tests/smoke.py      every scenario plus crash and resume, against an in-memory fake SDK
logs/               each agent's output
```

## Known limits

- Laser SDK 0.4.0: memory reads back only when scoped to a conversation. `memory_topic`, `memory_on_topic` and agent-scoped memory accept writes but never return them (`memory_probe.py` in the prototype shows it). So durable memory here is named sessions.
- Semantic recall uses a toy bag-of-words embedder over the durable memory. A real embedding model is next.
- Python has no OTEL package in Laser SDK yet, so tracing is hand-rolled with the OpenTelemetry SDK.
