"""The chaos monkey: SIGKILLs a random agent at random intervals, so crash and
resume happens without anyone pressing Kill. It goes through the supervisor
like any other kill; on_kill(name, pid) puts the kill on the log, so the
timeline and the evals see it. may_kill() lets the caller hold it off, as the
demo loop does between tickets."""

from __future__ import annotations

import asyncio
import random

from . import config as C
from .supervisor import Supervisor


# The only processes it may kill: the desk's agents. Never the server, the
# CLI runner's auto-approver, or anything the supervisor did not start.
KILLABLE = ("triage", "resolver", "billing")


class ChaosMonkey:
    def __init__(self, supervisor: Supervisor, agents: list[str] | None = None,
                 min_s: float | None = None, max_s: float | None = None, on_kill=None,
                 may_kill=None) -> None:
        self.supervisor = supervisor
        self.on_kill = on_kill
        self.may_kill = may_kill or (lambda: True)
        self.agents = [a for a in (agents or C.CHAOS_AGENTS) if a in KILLABLE and a in supervisor.agents]
        self.min_s = C.CHAOS_MIN if min_s is None else min_s
        self.max_s = max(self.min_s, C.CHAOS_MAX if max_s is None else max_s)
        self.task: asyncio.Task | None = None

    def start(self) -> None:
        if self.agents:
            self.task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(random.uniform(self.min_s, self.max_s))
            if not self.may_kill():
                continue
            alive = [a for a in self.agents if self.supervisor.pid(a)]
            if not alive:
                continue
            name = random.choice(alive)
            pid = await self.supervisor.kill(name)
            if pid and self.on_kill:
                try:
                    await self.on_kill(name, pid)
                except Exception as error:  # noqa: BLE001
                    print(f"  chaos: could not log the kill: {error!r}", flush=True)

    def stop(self) -> None:
        if self.task:
            self.task.cancel()
