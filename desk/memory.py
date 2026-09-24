"""Look at or clear the desk's memory from the command line.

    python -m desk.memory               # list customer history and the knowledge base
    python -m desk.memory clear         # forget customer history, reseed the starting notes
    python -m desk.memory clear --kb    # forget the knowledge base too (reseeded from config)
"""

from __future__ import annotations

import argparse
import asyncio

from .core import Desk, connect, setup


async def main() -> None:
    parser = argparse.ArgumentParser(description="The support desk's memory")
    parser.add_argument("action", nargs="?", default="list", choices=["list", "clear"])
    parser.add_argument("--kb", action="store_true", help="with clear: the knowledge base too")
    args = parser.parse_args()
    laser = await connect()
    await setup(laser, with_index=False)
    desk = Desk(laser, "desk")
    if args.action == "clear":
        result = await desk.clear_memory(kb=args.kb)
        print(f"  forgot {result['forgotten']} notes"
              + ("; some were still recalled, so moved to a fresh memory epoch" if result["new_epoch"] else ""))
        kb, history = await desk.seed_memory()
        print(f"  reseeded: {kb} knowledge base notes, {history} customer history notes")
    print("  customer history:")
    for note in await desk.history_recent(1000):
        print(f"    {note}")
    print("  knowledge base:")
    for item in await desk.kb_mem.recall(limit=1000):
        print(f"    {item.text}")


if __name__ == "__main__":
    asyncio.run(main())
