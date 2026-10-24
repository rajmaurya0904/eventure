"""Two agents coordinate through Eventure, each over its own MCP stdio connection.

Run it with:  python examples/demo.py

A "planner" and a "worker" each launch a separate `eventure serve` process. The two
processes share one SQLite database, which is what joins them into a single bus. The
planner publishes tasks, the worker consumes them and reports back, and the planner
waits for the results. Everything printed below is a real MCP tool call.
"""

from __future__ import annotations

import asyncio
import json
import sys
import tempfile
from pathlib import Path

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client


async def call(session: ClientSession, tool: str, **args):
    result = await session.call_tool(tool, args)
    if result.isError:
        raise RuntimeError(result.content[0].text)
    return json.loads(result.content[0].text)


def server(db: Path) -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable, args=["-m", "eventure.cli", "--db", str(db), "serve"]
    )


async def worker(db: Path, ready: asyncio.Event) -> None:
    async with stdio_client(server(db)) as (read, write), ClientSession(read, write) as s:
        await s.initialize()
        sub = await call(s, "subscribe", subscriber="worker", pattern="task.*")
        print(f"worker   subscribed to task.*  (handle {sub['id']})")
        ready.set()
        done = 0
        while done < 3:
            batch = await call(s, "poll", subscription_id=sub["id"], wait_seconds=10)
            for event in batch["events"]:
                n = event["payload"]["n"]
                print(f"worker   got {event['topic']} n={n}, squaring it")
                await call(
                    s,
                    "publish",
                    topic="result.square",
                    payload={"n": n, "answer": n * n},
                    publisher="worker",
                )
                done += 1


async def planner(db: Path, ready: asyncio.Event) -> None:
    async with stdio_client(server(db)) as (read, write), ClientSession(read, write) as s:
        await s.initialize()
        sub = await call(s, "subscribe", subscriber="planner", pattern="result.#")
        await ready.wait()
        for n in (3, 4, 5):
            sent = await call(
                s, "publish", topic="task.square", payload={"n": n}, publisher="planner"
            )
            print(f"planner  published task.square n={n}  -> {sent['delivered_to']} subscriber(s)")
        results = []
        while len(results) < 3:
            batch = await call(s, "poll", subscription_id=sub["id"], wait_seconds=10)
            results += [e["payload"] for e in batch["events"]]
        for r in sorted(results, key=lambda r: r["n"]):
            print(f"planner  result: {r['n']}^2 = {r['answer']}")
        stats = await call(s, "stats")
        print(
            f"bus      {stats['events_retained']} events retained, "
            f"subscribers: {', '.join(stats['subscribers'])}"
        )


async def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "bus.db"
        ready = asyncio.Event()
        await asyncio.wait_for(asyncio.gather(worker(db, ready), planner(db, ready)), 120)


if __name__ == "__main__":
    asyncio.run(main())
