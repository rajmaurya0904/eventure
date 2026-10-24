"""The MCP server: the event bus exposed as tools an agent can call.

Tools: ``publish``, ``subscribe``, ``poll``, ``unsubscribe``, ``list_subscriptions``,
``history`` and ``stats``. Every tool returns plain JSON-friendly dicts. Bad input
(an invalid topic, an unknown subscription handle, an oversized payload) raises, which
the MCP layer reports to the agent as a tool error with the message intact.
"""

from __future__ import annotations

from typing import Any

from mcp.server.fastmcp import FastMCP

from .bus import EventBus
from .store import default_db_path

INSTRUCTIONS = """\
A shared local event bus for coordinating agents.

Topics are dot-separated words such as `build.api.done`. Patterns add `*` (exactly one
word) and `#` (zero or more words): `build.*.done`, `build.#`.

Typical flow: call `subscribe` once with your own name and a pattern, keep the returned
`id`, call `poll` with it whenever you want new events (use wait_seconds to block until
one arrives), and call `publish` to tell other agents something happened. Subscriptions
are durable: subscribing again under the same name and pattern resumes where you stopped.
"""


def create_server(bus: EventBus | None = None, *, name: str = "eventure") -> FastMCP:
    """Build the MCP server around ``bus`` (a bus on the default database if omitted)."""
    bus = bus or EventBus(default_db_path())
    mcp = FastMCP(name, instructions=INSTRUCTIONS, log_level="WARNING")
    mcp.bus = bus  # type: ignore[attr-defined]  # exposed so callers and tests can inspect it

    @mcp.tool()
    def publish(topic: str, payload: Any = None, publisher: str = "") -> dict[str, Any]:
        """Publish an event to a topic such as 'build.api.done'.

        payload is any JSON value (default {}), at most 64 KiB. publisher is an optional
        name recorded on the event. Returns the stored event with its id and the number of
        subscriptions that will receive it.
        """
        event = bus.publish(topic, payload, publisher)
        return {
            "event": event.to_json(),
            "delivered_to": bus.matching_subscriptions(event.topic),
        }

    @mcp.tool()
    def subscribe(subscriber: str, pattern: str, replay: bool = False) -> dict[str, Any]:
        """Subscribe 'subscriber' to a topic pattern ('*' = one word, '#' = any number).

        Returns the subscription; keep its 'id' to poll with. A new subscription only sees
        events published after it was created, unless replay is true (start from the oldest
        retained event). Subscribing again under the same name and pattern returns the
        existing subscription and keeps its place.
        """
        return bus.subscribe(subscriber, pattern, replay=replay).to_json()

    @mcp.tool()
    async def poll(
        subscription_id: str, limit: int = 20, wait_seconds: float = 0.0, peek: bool = False
    ) -> dict[str, Any]:
        """Fetch events that arrived since the last poll of this subscription.

        Returns up to 'limit' events (1-500), oldest first, and advances the cursor so each
        event is delivered once. wait_seconds (0-30) blocks until something arrives or the
        time is up. peek=true reads without advancing. 'has_more' means more are waiting;
        'pruned' counts events retention deleted before you read them.
        """
        result = await bus.poll_wait(
            subscription_id, limit, wait_seconds=wait_seconds, peek=peek
        )
        return result.to_json()

    @mcp.tool()
    def unsubscribe(subscription_id: str) -> dict[str, Any]:
        """Delete a subscription. Returns {'removed': false} if it did not exist."""
        return {"removed": bus.unsubscribe(subscription_id)}

    @mcp.tool()
    def list_subscriptions(subscriber: str | None = None) -> dict[str, Any]:
        """List subscriptions, optionally only those belonging to one subscriber name."""
        return {"subscriptions": [s.to_json() for s in bus.subscriptions(subscriber)]}

    @mcp.tool()
    def history(
        pattern: str = "#", limit: int = 20, after_id: int | None = None
    ) -> dict[str, Any]:
        """Look at retained events without subscribing.

        Returns the newest 'limit' events matching the pattern, oldest first. Pass after_id
        to page forwards from a known event id instead.
        """
        return {"events": [e.to_json() for e in bus.history(pattern, limit, after_id)]}

    @mcp.tool()
    def stats() -> dict[str, Any]:
        """Report the database path, retained event count, latest id and subscribers."""
        return bus.stats()

    return mcp


def run(
    bus: EventBus | None = None,
    *,
    transport: str = "stdio",
    host: str = "127.0.0.1",
    port: int = 8000,
) -> None:
    """Serve until the client disconnects (stdio) or the process is stopped."""
    server = create_server(bus)
    server.settings.host = host
    server.settings.port = port
    server.run(transport=transport)  # type: ignore[arg-type]
