"""The MCP server: tool registration, tool behaviour and error reporting."""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest
from mcp.server.fastmcp.exceptions import ToolError

from eventure.bus import EventBus
from eventure.server import create_server


@pytest.fixture
def server():
    bus = EventBus()
    yield create_server(bus)
    bus.close()


def call(server, name: str, args: dict[str, Any] | None = None) -> Any:
    async def go():
        content, structured = await server.call_tool(name, args or {})
        return structured if structured is not None else json.loads(content[0].text)

    return asyncio.run(go())


def test_registers_all_tools(server) -> None:
    names = {t.name for t in asyncio.run(server.list_tools())}
    assert names == {
        "publish",
        "subscribe",
        "poll",
        "unsubscribe",
        "list_subscriptions",
        "history",
        "stats",
    }


def test_tools_have_descriptions_and_schemas(server) -> None:
    for tool in asyncio.run(server.list_tools()):
        assert tool.description and len(tool.description) > 20
        assert tool.inputSchema["type"] == "object"


def test_server_carries_instructions(server) -> None:
    assert "subscribe" in server.instructions


def test_publish_returns_event_and_recipient_count(server) -> None:
    call(server, "subscribe", {"subscriber": "a", "pattern": "jobs.#"})
    out = call(server, "publish", {"topic": "jobs.new", "payload": {"n": 1}, "publisher": "p"})
    assert out["event"]["id"] == 1
    assert out["event"]["publisher"] == "p"
    assert out["event"]["payload"] == {"n": 1}
    assert out["delivered_to"] == 1


def test_publish_without_subscribers_reports_zero(server) -> None:
    assert call(server, "publish", {"topic": "nobody.home"})["delivered_to"] == 0


def test_publish_default_payload_is_empty_object(server) -> None:
    assert call(server, "publish", {"topic": "a"})["event"]["payload"] == {}


def test_publish_invalid_topic_is_a_tool_error(server) -> None:
    with pytest.raises(ToolError, match="invalid topic"):
        call(server, "publish", {"topic": "has space"})


def test_publish_oversized_payload_is_a_tool_error(server) -> None:
    with pytest.raises(ToolError, match="limit"):
        call(server, "publish", {"topic": "a", "payload": "x" * 70_000})


def test_subscribe_then_poll_round_trip(server) -> None:
    sub = call(server, "subscribe", {"subscriber": "worker", "pattern": "jobs.*"})
    call(server, "publish", {"topic": "jobs.a", "payload": 1})
    call(server, "publish", {"topic": "other.b", "payload": 2})
    call(server, "publish", {"topic": "jobs.c", "payload": 3})
    result = call(server, "poll", {"subscription_id": sub["id"]})
    assert [e["topic"] for e in result["events"]] == ["jobs.a", "jobs.c"]
    assert result["has_more"] is False


def test_poll_delivers_each_event_once(server) -> None:
    sub = call(server, "subscribe", {"subscriber": "w", "pattern": "#"})
    call(server, "publish", {"topic": "a"})
    assert len(call(server, "poll", {"subscription_id": sub["id"]})["events"]) == 1
    assert call(server, "poll", {"subscription_id": sub["id"]})["events"] == []


def test_poll_peek_does_not_advance(server) -> None:
    sub = call(server, "subscribe", {"subscriber": "w", "pattern": "#"})
    call(server, "publish", {"topic": "a"})
    assert len(call(server, "poll", {"subscription_id": sub["id"], "peek": True})["events"]) == 1
    assert len(call(server, "poll", {"subscription_id": sub["id"]})["events"]) == 1


def test_poll_limit_and_has_more(server) -> None:
    sub = call(server, "subscribe", {"subscriber": "w", "pattern": "#"})
    for _ in range(5):
        call(server, "publish", {"topic": "t"})
    first = call(server, "poll", {"subscription_id": sub["id"], "limit": 2})
    assert len(first["events"]) == 2 and first["has_more"] is True


def test_poll_unknown_subscription_is_a_tool_error(server) -> None:
    with pytest.raises(ToolError, match="unknown subscription: nope"):
        call(server, "poll", {"subscription_id": "nope"})


def test_poll_rejects_bad_limit(server) -> None:
    sub = call(server, "subscribe", {"subscriber": "w", "pattern": "#"})
    with pytest.raises(ToolError, match="limit"):
        call(server, "poll", {"subscription_id": sub["id"], "limit": 0})


def test_poll_rejects_excessive_wait(server) -> None:
    sub = call(server, "subscribe", {"subscriber": "w", "pattern": "#"})
    with pytest.raises(ToolError, match="wait_seconds"):
        call(server, "poll", {"subscription_id": sub["id"], "wait_seconds": 999})


def test_poll_wait_returns_when_event_arrives(server) -> None:
    sub = call(server, "subscribe", {"subscriber": "w", "pattern": "late.#"})

    async def go():
        async def later():
            await asyncio.sleep(0.2)
            await server.call_tool("publish", {"topic": "late.event"})

        task = asyncio.create_task(later())
        _, out = await server.call_tool(
            "poll", {"subscription_id": sub["id"], "wait_seconds": 5}
        )
        await task
        return out

    out = asyncio.run(go())
    assert [e["topic"] for e in out["events"]] == ["late.event"]


def test_poll_wait_times_out_empty(server) -> None:
    sub = call(server, "subscribe", {"subscriber": "w", "pattern": "#"})
    out = call(server, "poll", {"subscription_id": sub["id"], "wait_seconds": 0.2})
    assert out["events"] == []


def test_subscribe_replay_starts_from_history(server) -> None:
    call(server, "publish", {"topic": "old.one"})
    fresh = call(server, "subscribe", {"subscriber": "n", "pattern": "#"})
    replayed = call(server, "subscribe", {"subscriber": "r", "pattern": "#", "replay": True})
    assert call(server, "poll", {"subscription_id": fresh["id"]})["events"] == []
    assert len(call(server, "poll", {"subscription_id": replayed["id"]})["events"]) == 1


def test_subscribe_twice_resumes_same_subscription(server) -> None:
    a = call(server, "subscribe", {"subscriber": "x", "pattern": "a.#"})
    b = call(server, "subscribe", {"subscriber": "x", "pattern": "a.#"})
    assert a["id"] == b["id"]


def test_subscribe_invalid_pattern_is_a_tool_error(server) -> None:
    with pytest.raises(ToolError, match="invalid pattern"):
        call(server, "subscribe", {"subscriber": "x", "pattern": "a..b"})


def test_unsubscribe_and_list(server) -> None:
    sub = call(server, "subscribe", {"subscriber": "x", "pattern": "#"})
    call(server, "subscribe", {"subscriber": "y", "pattern": "#"})
    assert len(call(server, "list_subscriptions")["subscriptions"]) == 2
    mine = call(server, "list_subscriptions", {"subscriber": "x"})["subscriptions"]
    assert [s["subscriber"] for s in mine] == ["x"]
    assert call(server, "unsubscribe", {"subscription_id": sub["id"]}) == {"removed": True}
    assert call(server, "unsubscribe", {"subscription_id": sub["id"]}) == {"removed": False}
    assert len(call(server, "list_subscriptions")["subscriptions"]) == 1


def test_history_without_subscribing(server) -> None:
    for i in range(4):
        call(server, "publish", {"topic": f"h.{i}"})
    newest = call(server, "history", {"pattern": "h.*", "limit": 2})["events"]
    assert [e["topic"] for e in newest] == ["h.2", "h.3"]
    after = call(server, "history", {"after_id": 1, "limit": 10})["events"]
    assert [e["id"] for e in after] == [2, 3, 4]


def test_stats(server) -> None:
    call(server, "subscribe", {"subscriber": "z", "pattern": "#"})
    call(server, "publish", {"topic": "a"})
    stats = call(server, "stats")
    assert stats["events_retained"] == 1
    assert stats["subscribers"] == ["z"]
