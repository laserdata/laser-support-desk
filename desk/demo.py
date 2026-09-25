"""The demo loop: every scenario in turn, forever, with no one at the keyboard.

It opens each ticket the way the Send button does, lets the server decide every
approval after a short delay (by the desk's limit; a person who clicks first
still decides), including any already waiting when the loop starts, waits for
the ticket to finish and be evaluated, pauses, and moves on. The demo data (orders, customer history) is reset at the
start of every round, so the scenarios keep ending the way they are meant to.
"""

from __future__ import annotations

import asyncio

from . import config as C

TICKET_TIMEOUT = 180  # seconds before a ticket that never finishes is left behind


class DemoLoop:
    def __init__(self, app) -> None:
        self.app = app  # the server: open_ticket, wait_finished, reset_data, broadcast
        self.task: asyncio.Task | None = None

    @property
    def running(self) -> bool:
        return self.task is not None and not self.task.done()

    def start(self) -> None:
        if not self.running:
            self.task = asyncio.create_task(self._run())

    def stop(self) -> None:
        if self.running:
            self.task.cancel()

    async def _say(self, state: str, **extra) -> None:
        await self.app.broadcast({"type": "demo", "state": state, **extra})

    async def _run(self) -> None:
        names = list(C.SCENARIOS)
        round_no = 0
        try:
            await self.app.auto_decide_waiting()
            while True:
                round_no += 1
                await self.app.reset_data()
                print(f"  demo loop: round {round_no}, demo data reset", flush=True)
                for i, name in enumerate(names):
                    await self._say("running", scenario=name, round=round_no)
                    t = (await self.app.open_ticket(name, demo=True))["ticket"]
                    if not await self.app.wait_finished(t, TICKET_TIMEOUT):
                        print(f"  demo loop: {name} did not finish in {TICKET_TIMEOUT}s, moving on", flush=True)
                    following = names[(i + 1) % len(names)]
                    await self._say("waiting", next=following, seconds=C.DEMO_PAUSE,
                                    reset=i == len(names) - 1)
                    await asyncio.sleep(C.DEMO_PAUSE)
        except asyncio.CancelledError:
            pass
        except Exception as error:  # noqa: BLE001
            print(f"  demo loop stopped: {error!r}", flush=True)
        finally:
            await self._say("off")
