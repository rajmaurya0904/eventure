"""The event bus: publish, durable subscriptions with cursors, polling and listeners."""

from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .models import (
    Event,
    EventureError,
    InvalidTopic,
    PollResult,
    Subscription,
    UnknownSubscription,
    validate_pattern,
    validate_payload,
    validate_topic,
)
from .patterns import matches
from .store import MEMORY, Store

log = logging.getLogger("eventure")

DEFAULT_RETENTION = 10_000
MAX_POLL_LIMIT = 500
MAX_WAIT_SECONDS = 30.0
MAX_NAME_LENGTH = 100

Listener = Callable[[Event], Any]


@dataclass(frozen=True)
class ListenerError:
    """A listener raised while handling an event. The bus carried on regardless."""

    listener_id: int
    event_id: int
    error: str


def _check_name(name: str, what: str) -> str:
    if not isinstance(name, str) or not name.strip():
        raise EventureError(f"{what} must be a non-empty string")
    name = name.strip()
    if len(name) > MAX_NAME_LENGTH or any(ord(c) < 32 for c in name):
        raise EventureError(
            f"{what} must be at most {MAX_NAME_LENGTH} characters with no control characters"
        )
    return name


def _check_limit(limit: int) -> int:
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= MAX_POLL_LIMIT:
        raise EventureError(f"limit must be an integer between 1 and {MAX_POLL_LIMIT}")
    return limit


class EventBus:
    """A topic-based event bus over a SQLite store.

    Subscriptions are durable and carry a cursor: ``poll`` returns the matching
    events published since the last poll and moves the cursor forward, so an agent
    that restarts and subscribes again under the same name resumes where it left
    off. Several consumers may share one subscription handle to split the work.

    ``add_listener`` registers an in-process callback instead. Listeners run
    synchronously inside ``publish`` for events published through this same
    object; an exception in one is recorded in ``errors`` and never reaches the
    publisher or the other listeners.
    """

    def __init__(self, path: str | Path = MEMORY, *, retention: int = DEFAULT_RETENTION) -> None:
        if retention < 1:
            raise ValueError("retention must be at least 1")
        self.store = Store(path)
        self.retention = retention
        self.errors: deque[ListenerError] = deque(maxlen=100)
        self._listeners: dict[int, tuple[str, Listener]] = {}
        self._next_listener = 1

    @property
    def path(self) -> str:
        return self.store.path

    def close(self) -> None:
        self.store.close()

    def __enter__(self) -> EventBus:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    # ---------------------------------------------------------------- publish

    def publish(self, topic: str, payload: Any = None, publisher: str = "") -> Event:
        """Store an event and notify in-process listeners. Raises on a bad topic or payload."""
        validate_topic(topic)
        payload = validate_payload({} if payload is None else payload)
        event = Event(
            topic=topic,
            payload=payload,
            publisher=publisher.strip() if isinstance(publisher, str) else "",
        )
        stored = self.store.add_event(event, self.retention)
        self._notify(stored)
        return stored

    def matching_subscriptions(self, topic: str) -> int:
        """How many stored subscriptions would receive an event on ``topic``."""
        return sum(1 for p in self.store.subscription_patterns() if matches(p, topic))

    def _notify(self, event: Event) -> None:
        for listener_id, (pattern, fn) in list(self._listeners.items()):
            if not matches(pattern, event.topic):
                continue
            try:
                fn(event)
            except Exception as exc:  # noqa: BLE001 - a listener must never break the bus
                message = f"{type(exc).__name__}: {exc}"
                self.errors.append(ListenerError(listener_id, event.id, message))
                log.warning("listener %s failed on event %s: %s", listener_id, event.id, exc)

    # -------------------------------------------------------------- listeners

    def add_listener(self, pattern: str, fn: Listener) -> int:
        """Call ``fn(event)`` for each matching event published through this bus object."""
        validate_pattern(pattern)
        if not callable(fn):
            raise EventureError("listener must be callable")
        listener_id = self._next_listener
        self._next_listener += 1
        self._listeners[listener_id] = (pattern, fn)
        return listener_id

    def remove_listener(self, listener_id: int) -> bool:
        return self._listeners.pop(listener_id, None) is not None

    # ---------------------------------------------------------- subscriptions

    def subscribe(self, subscriber: str, pattern: str, *, replay: bool = False) -> Subscription:
        """Create (or resume) the subscription ``subscriber`` + ``pattern``.

        A new subscription starts at "now" and only sees later events, unless
        ``replay`` is true, in which case it starts from the oldest retained event.
        Subscribing again with the same name and pattern returns the existing
        subscription with its cursor intact (``replay`` rewinds it).
        """
        subscriber = _check_name(subscriber, "subscriber")
        validate_pattern(pattern)
        sub = Subscription(
            id=uuid.uuid4().hex[:12],
            subscriber=subscriber,
            pattern=pattern,
            cursor=0 if replay else self.store.max_event_id(),
        )
        stored = self.store.add_subscription(sub)
        if replay and stored.cursor != 0:
            self.store.set_cursor(stored.id, 0)
            stored = stored.model_copy(update={"cursor": 0})
        return stored

    def unsubscribe(self, subscription_id: str) -> bool:
        """Remove a subscription. Returns False if it did not exist."""
        return self.store.remove_subscription(subscription_id)

    def subscriptions(self, subscriber: str | None = None) -> list[Subscription]:
        return self.store.list_subscriptions(subscriber)

    # ---------------------------------------------------------------- reading

    def poll(self, subscription_id: str, limit: int = 20, *, peek: bool = False) -> PollResult:
        """Return up to ``limit`` unread events and (unless ``peek``) mark them read."""
        _check_limit(limit)
        return self.store.read(subscription_id, limit, matches, commit=not peek)

    async def poll_wait(
        self,
        subscription_id: str,
        limit: int = 20,
        *,
        wait_seconds: float = 0.0,
        peek: bool = False,
        interval: float = 0.1,
    ) -> PollResult:
        """Like ``poll`` but, if nothing is waiting, retry for up to ``wait_seconds``."""
        if not 0 <= wait_seconds <= MAX_WAIT_SECONDS:
            raise EventureError(f"wait_seconds must be between 0 and {MAX_WAIT_SECONDS:g}")
        deadline = time.monotonic() + wait_seconds
        while True:
            result = self.poll(subscription_id, limit, peek=peek)
            if result.events or time.monotonic() >= deadline:
                return result
            await asyncio.sleep(min(interval, max(0.0, deadline - time.monotonic())))

    def history(
        self, pattern: str = "#", limit: int = 20, after_id: int | None = None
    ) -> list[Event]:
        """Matching events from the retained log, oldest first.

        Without ``after_id`` this is the newest ``limit`` matching events; with it,
        the next ``limit`` matching events after that id (for paging forwards).
        """
        validate_pattern(pattern)
        _check_limit(limit)
        if after_id is not None:
            if after_id < 0:
                raise EventureError("after_id must be zero or greater")
            found: list[Event] = []
            cursor = after_id
            while len(found) < limit:
                rows = self.store.events_after(cursor, 500)
                if not rows:
                    break
                for event in rows:
                    cursor = event.id
                    if matches(pattern, event.topic):
                        found.append(event)
                        if len(found) == limit:
                            break
            return found
        newest: list[Event] = []
        before: int | None = None
        while len(newest) < limit:
            rows = self.store.events_before(before, 500)
            if not rows:
                break
            for event in rows:
                before = event.id
                if matches(pattern, event.topic):
                    newest.append(event)
                    if len(newest) == limit:
                        break
        newest.reverse()
        return newest

    def stats(self) -> dict[str, Any]:
        subs = self.store.list_subscriptions()
        latest = self.store.max_event_id()
        return {
            "database": self.path,
            "events_retained": self.store.event_count(),
            "latest_event_id": latest,
            "retention": self.retention,
            "subscriptions": len(subs),
            "subscribers": sorted({s.subscriber for s in subs}),
        }


__all__ = ["EventBus", "InvalidTopic", "ListenerError", "UnknownSubscription"]
