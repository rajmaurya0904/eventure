"""Command line: run the MCP server, or talk to the same bus from a shell."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections.abc import Sequence

from . import __version__
from .bus import MAX_POLL_LIMIT, MAX_WAIT_SECONDS, EventBus
from .models import Event, EventureError
from .store import DB_ENV, default_db_path

EPILOG = f"""\
All commands share one SQLite database: --db PATH, else ${DB_ENV}, else ~/.eventure/bus.db.
With no command, eventure serves MCP over stdio (what MCP clients launch).
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="eventure",
        description="A local publish/subscribe event bus for AI agents, served over MCP.",
        epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--version", action="version", version=f"eventure {__version__}")
    parser.add_argument("--db", metavar="PATH", help="bus database file (default: see below)")
    sub = parser.add_subparsers(dest="command", metavar="command")

    serve = sub.add_parser("serve", help="run the MCP server (default)")
    serve.add_argument(
        "--transport", choices=["stdio", "sse", "streamable-http"], default="stdio"
    )
    serve.add_argument("--host", default="127.0.0.1", help="bind address for http transports")
    serve.add_argument("--port", type=int, default=8000, help="port for http transports")

    pub = sub.add_parser("publish", help="publish one event")
    pub.add_argument("topic", help="e.g. build.api.done")
    pub.add_argument("payload", nargs="?", help="JSON payload (default: {})")
    pub.add_argument("--publisher", default="cli", help="name recorded on the event")

    poll = sub.add_parser("poll", help="print events a named subscriber has not seen yet")
    poll.add_argument("subscriber", help="your name; the place you stopped is remembered")
    poll.add_argument("pattern", help="topic pattern: * is one word, # is any number")
    poll.add_argument("-n", "--limit", type=int, default=20, help=f"1-{MAX_POLL_LIMIT}")
    poll.add_argument(
        "--wait",
        type=float,
        default=0.0,
        metavar="SECONDS",
        help=f"block up to this long for an event (max {MAX_WAIT_SECONDS:g})",
    )
    poll.add_argument("--replay", action="store_true", help="start at the oldest retained event")
    poll.add_argument("--peek", action="store_true", help="do not mark events as read")

    hist = sub.add_parser("history", help="show retained events without subscribing")
    hist.add_argument("pattern", nargs="?", default="#")
    hist.add_argument("-n", "--limit", type=int, default=20)

    tail = sub.add_parser("tail", help="follow new events as they are published (Ctrl-C to stop)")
    tail.add_argument("pattern", nargs="?", default="#")
    tail.add_argument("--interval", type=float, default=0.25, help="seconds between checks")

    sub.add_parser("stats", help="show database path, event count and subscribers")
    sub.add_parser("subscriptions", help="list every subscription and its cursor")
    return parser


def format_event(event: Event) -> str:
    """One line: ``#7 12:04:31 build.api.done [planner] {"ok": true}``."""
    stamp = event.timestamp.astimezone().strftime("%H:%M:%S")
    who = f" [{event.publisher}]" if event.publisher else ""
    return f"#{event.id} {stamp} {event.topic}{who} {json.dumps(event.payload)}"


def _parse_payload(text: str | None) -> object:
    if text is None:
        return {}
    try:
        return json.loads(text)
    except json.JSONDecodeError as exc:
        raise EventureError(f"payload is not valid JSON: {exc.msg} (column {exc.colno})") from exc


def _print_events(events: Sequence[Event]) -> None:
    for event in events:
        print(format_event(event))


def run(args: argparse.Namespace, out=None) -> int:
    command = args.command or "serve"
    path = args.db or default_db_path()
    if command == "serve":
        from .server import run as serve

        bus = EventBus(path)
        transport = getattr(args, "transport", "stdio")
        print(f"eventure {__version__}: serving MCP ({transport}) on {bus.path}", file=sys.stderr)
        serve(
            bus,
            transport=transport,
            host=getattr(args, "host", "127.0.0.1"),
            port=getattr(args, "port", 8000),
        )
        return 0

    with EventBus(path) as bus:
        if command == "publish":
            event = bus.publish(args.topic, _parse_payload(args.payload), args.publisher)
            n = bus.matching_subscriptions(event.topic)
            print(format_event(event))
            print(f"delivered to {n} subscription{'' if n == 1 else 's'}")
        elif command == "poll":
            sub = bus.subscribe(args.subscriber, args.pattern, replay=args.replay)
            result = asyncio.run(
                bus.poll_wait(sub.id, args.limit, wait_seconds=args.wait, peek=args.peek)
            )
            _print_events(result.events)
            if result.pruned:
                print(f"note: retention deleted up to {result.pruned} events before you read them")
            if result.has_more:
                print("more events are waiting; poll again")
            elif not result.events:
                print("no new events")
        elif command == "history":
            events = bus.history(args.pattern, args.limit)
            _print_events(events)
            if not events:
                print("no matching events")
        elif command == "tail":
            cursor = bus.store.max_event_id()
            try:
                while True:
                    fresh = bus.history(args.pattern, MAX_POLL_LIMIT, after_id=cursor)
                    if fresh:
                        _print_events(fresh)
                        sys.stdout.flush()
                    cursor = bus.store.max_event_id()
                    time.sleep(args.interval)
            except KeyboardInterrupt:
                return 0
        elif command == "stats":
            print(json.dumps(bus.stats(), indent=2))
        elif command == "subscriptions":
            subs = bus.subscriptions()
            for s in subs:
                print(f"{s.id}  {s.subscriber:<16} {s.pattern:<24} cursor={s.cursor}")
            if not subs:
                print("no subscriptions")
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except EventureError as exc:
        print(f"eventure: error: {exc}", file=sys.stderr)
        return 1
    except OSError as exc:
        print(f"eventure: cannot use the database: {exc}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    sys.exit(main())
