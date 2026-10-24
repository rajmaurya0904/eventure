"""End to end: real server subprocesses spoken to over MCP stdio, sharing one database."""

from __future__ import annotations

import asyncio
import json
import sys

from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

TIMEOUT = 60


def params(db) -> StdioServerParameters:
    return StdioServerParameters(
        command=sys.executable, args=["-m", "eventure.cli", "--db", str(db), "serve"]
    )


async def tool(session: ClientSession, name: str, **args):
    result = await asyncio.wait_for(session.call_tool(name, args), TIMEOUT)
    return result, (json.loads(result.content[0].text) if result.content else None)


def run(coro):
    return asyncio.run(asyncio.wait_for(coro, TIMEOUT * 2))


def test_initialize_and_list_tools(tmp_path) -> None:
    async def go():
        async with stdio_client(params(tmp_path / "bus.db")) as (r, w):
            async with ClientSession(r, w) as session:
                init = await session.initialize()
                tools = await session.list_tools()
                return init, {t.name for t in tools.tools}

    init, names = run(go())
    assert init.serverInfo.name == "eventure"
    assert {"publish", "subscribe", "poll"} <= names


def test_two_server_processes_share_events(tmp_path) -> None:
    """Agent A and agent B each launch their own server; the database joins them."""
    db = tmp_path / "bus.db"

    async def go():
        async with stdio_client(params(db)) as (ra, wa):
            async with ClientSession(ra, wa) as a:
                await a.initialize()
                async with stdio_client(params(db)) as (rb, wb):
                    async with ClientSession(rb, wb) as b:
                        await b.initialize()
                        _, sub = await tool(b, "subscribe", subscriber="b", pattern="task.#")
                        _, pub = await tool(
                            a, "publish", topic="task.done", payload={"id": 7}, publisher="a"
                        )
                        _, got = await tool(b, "poll", subscription_id=sub["id"], wait_seconds=5)
                        return pub, got

    pub, got = run(go())
    assert pub["delivered_to"] == 1
    assert [e["topic"] for e in got["events"]] == ["task.done"]
    assert got["events"][0]["payload"] == {"id": 7}
    assert got["events"][0]["publisher"] == "a"


def test_tool_errors_come_back_as_is_error(tmp_path) -> None:
    async def go():
        async with stdio_client(params(tmp_path / "bus.db")) as (r, w):
            async with ClientSession(r, w) as session:
                await session.initialize()
                result = await session.call_tool("publish", {"topic": "bad topic"})
                return result

    result = run(go())
    assert result.isError
    assert "invalid topic" in result.content[0].text


def test_subscription_survives_server_restart(tmp_path) -> None:
    db = tmp_path / "bus.db"

    async def first():
        async with stdio_client(params(db)) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                _, sub = await tool(s, "subscribe", subscriber="keeper", pattern="#")
                await tool(s, "publish", topic="before.restart")
                return sub

    async def second(sub_id):
        async with stdio_client(params(db)) as (r, w):
            async with ClientSession(r, w) as s:
                await s.initialize()
                _, got = await tool(s, "poll", subscription_id=sub_id)
                return got

    sub = run(first())
    got = run(second(sub["id"]))
    assert [e["topic"] for e in got["events"]] == ["before.restart"]
