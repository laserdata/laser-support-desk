"""Runs each agent as its own OS process and restarts any that die, the way an
orchestrator would. A killed agent loses nothing: the log redelivers what it
had not finished, and its checkpoint says how far it got."""

from __future__ import annotations

import asyncio
import os
import signal
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


class Supervisor:
    def __init__(self, agents: list[str], env: dict | None = None, on_change=None) -> None:
        self.agents = agents
        self.env = {**os.environ, **(env or {})}
        self.procs: dict[str, asyncio.subprocess.Process] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.stopping = False
        self.restarts: dict[str, int] = {name: 0 for name in agents}
        # on_change(name, state, pid): "started", "exited", "killed"
        self.on_change = on_change or (lambda *a: None)
        self.log_dir = ROOT / "logs"
        self.log_dir.mkdir(exist_ok=True)

    async def start(self) -> None:
        for name in self.agents:
            self.tasks[name] = asyncio.create_task(self._keep_alive(name))

    async def _keep_alive(self, name: str) -> None:
        while not self.stopping:
            log = open(self.log_dir / f"{name}.log", "ab")
            log.write(f"\n--- start {time.strftime('%H:%M:%S')}\n".encode())
            log.flush()
            proc = await asyncio.create_subprocess_exec(
                sys.executable, "-m", "desk.agents", name, cwd=str(ROOT), env=self.env,
                stdout=log, stderr=log)
            self.procs[name] = proc
            await self._notify(name, "started", proc.pid)
            code = await proc.wait()
            log.close()
            if self.stopping:
                break
            await self._notify(name, "exited", proc.pid, code)
            self.restarts[name] += 1
            await asyncio.sleep(0.5)

    async def _notify(self, *args) -> None:
        result = self.on_change(*args)
        if asyncio.iscoroutine(result):
            await result

    def pid(self, name: str) -> int | None:
        proc = self.procs.get(name)
        return proc.pid if proc and proc.returncode is None else None

    async def kill(self, name: str) -> int | None:
        """SIGKILL: no cleanup, no goodbye. The supervisor brings it back."""
        pid = self.pid(name)
        if pid:
            await self._notify(name, "killed", pid)
            os.kill(pid, signal.SIGKILL)
        return pid

    async def restart(self, name: str) -> None:
        """A graceful restart (SIGTERM)."""
        pid = self.pid(name)
        if pid:
            os.kill(pid, signal.SIGTERM)

    async def stop(self) -> None:
        self.stopping = True
        for proc in self.procs.values():
            if proc.returncode is None:
                proc.terminate()
        for proc in self.procs.values():
            try:
                await asyncio.wait_for(proc.wait(), timeout=5)
            except asyncio.TimeoutError:
                proc.kill()
        for task in self.tasks.values():
            task.cancel()
