"""SQLite persistence for events and subscriptions.

SQLite (from the standard library) is what lets several agent processes, each
running its own stdio server, share one bus: they all open the same database
file. ``:memory:`` gives a private, in-process store for tests and single-server
use.
"""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

from .models import Event, PollResult, Subscription, UnknownSubscription

SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id        INTEGER PRIMARY KEY AUTOINCREMENT,
    topic     TEXT NOT NULL,
    payload   TEXT NOT NULL,
    publisher TEXT NOT NULL DEFAULT '',
    ts        REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS subscriptions (
    id         TEXT PRIMARY KEY,
    subscriber TEXT NOT NULL,
    pattern    TEXT NOT NULL,
    cursor     INTEGER NOT NULL DEFAULT 0,
    created    REAL NOT NULL,
    UNIQUE (subscriber, pattern)
);
"""

MEMORY = ":memory:"
DB_ENV = "EVENTURE_DB"


def default_db_path() -> str:
    """The database every process shares: ``$EVENTURE_DB`` or ``~/.eventure/bus.db``."""
    return os.environ.get(DB_ENV) or str(Path.home() / ".eventure" / "bus.db")


def _event_from_row(row: sqlite3.Row | tuple) -> Event:
    return Event(
        id=row[0],
        topic=row[1],
        payload=json.loads(row[2]),
        publisher=row[3],
        timestamp=datetime.fromtimestamp(row[4], UTC),
    )


def _sub_from_row(row: sqlite3.Row | tuple) -> Subscription:
    return Subscription(
        id=row[0],
        subscriber=row[1],
        pattern=row[2],
        cursor=row[3],
        created_at=datetime.fromtimestamp(row[4], UTC),
    )


class Store:
    """A thin, thread-safe wrapper over one SQLite connection."""

    def __init__(self, path: str | Path = MEMORY, *, busy_timeout: float = 10.0) -> None:
        self.path = str(path)
        if self.path != MEMORY:
            Path(self.path).expanduser().parent.mkdir(parents=True, exist_ok=True)
            self.path = str(Path(self.path).expanduser())
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            self.path, timeout=busy_timeout, isolation_level=None, check_same_thread=False
        )
        self._initialise(busy_timeout)

    def _initialise(self, busy_timeout: float) -> None:
        """Switch to WAL and create tables, retrying while another process does the same.

        Two servers started at the same instant on a fresh file can both try to change the
        journal mode; SQLite reports "database is locked" immediately for that rather than
        waiting, so the busy timeout is not enough on its own.
        """
        deadline = time.monotonic() + busy_timeout
        while True:
            try:
                if self.path != MEMORY:
                    self._conn.execute("PRAGMA journal_mode=WAL")
                self._conn.execute("PRAGMA synchronous=NORMAL")
                self._conn.executescript(SCHEMA)
                return
            except sqlite3.OperationalError as exc:
                if "locked" not in str(exc) or time.monotonic() >= deadline:
                    raise
                time.sleep(0.05)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Run a write transaction that takes the database write lock immediately."""
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                yield self._conn
            except BaseException:
                self._conn.execute("ROLLBACK")
                raise
            else:
                self._conn.execute("COMMIT")

    # ----------------------------------------------------------------- events

    def add_event(self, event: Event, keep: int) -> Event:
        """Insert ``event`` and prune to the newest ``keep`` events. Returns it with its id."""
        with self.transaction() as conn:
            cur = conn.execute(
                "INSERT INTO events (topic, payload, publisher, ts) VALUES (?, ?, ?, ?)",
                (
                    event.topic,
                    json.dumps(event.payload),
                    event.publisher,
                    event.timestamp.timestamp(),
                ),
            )
            new_id = cur.lastrowid
            assert new_id is not None
            conn.execute("DELETE FROM events WHERE id <= ?", (new_id - keep,))
        return event.model_copy(update={"id": new_id})

    def events_after(self, after_id: int, limit: int) -> list[Event]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT id, topic, payload, publisher, ts FROM events WHERE id > ? "
                "ORDER BY id LIMIT ?",
                (after_id, limit),
            ).fetchall()
        return [_event_from_row(r) for r in rows]

    def events_before(self, before_id: int | None, limit: int) -> list[Event]:
        """Newest-first events with id < ``before_id`` (or the newest overall)."""
        with self._lock:
            if before_id is None:
                rows = self._conn.execute(
                    "SELECT id, topic, payload, publisher, ts FROM events ORDER BY id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT id, topic, payload, publisher, ts FROM events WHERE id < ? "
                    "ORDER BY id DESC LIMIT ?",
                    (before_id, limit),
                ).fetchall()
        return [_event_from_row(r) for r in rows]

    def max_event_id(self) -> int:
        """The highest id ever assigned (0 if nothing was ever published)."""
        with self._lock:
            row = self._conn.execute("SELECT seq FROM sqlite_sequence WHERE name='events'")
            found = row.fetchone()
        return int(found[0]) if found else 0

    def oldest_event_id(self) -> int:
        with self._lock:
            found = self._conn.execute("SELECT MIN(id) FROM events").fetchone()
        return int(found[0]) if found and found[0] is not None else 0

    def event_count(self) -> int:
        with self._lock:
            return int(self._conn.execute("SELECT COUNT(*) FROM events").fetchone()[0])

    # ---------------------------------------------------------- subscriptions

    def add_subscription(self, sub: Subscription) -> Subscription:
        """Insert ``sub``; if (subscriber, pattern) exists, return the existing row."""
        with self.transaction() as conn:
            existing = conn.execute(
                "SELECT id, subscriber, pattern, cursor, created FROM subscriptions "
                "WHERE subscriber = ? AND pattern = ?",
                (sub.subscriber, sub.pattern),
            ).fetchone()
            if existing:
                return _sub_from_row(existing)
            conn.execute(
                "INSERT INTO subscriptions (id, subscriber, pattern, cursor, created) "
                "VALUES (?, ?, ?, ?, ?)",
                (sub.id, sub.subscriber, sub.pattern, sub.cursor, sub.created_at.timestamp()),
            )
        return sub

    def get_subscription(self, sub_id: str) -> Subscription | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT id, subscriber, pattern, cursor, created FROM subscriptions WHERE id = ?",
                (sub_id,),
            ).fetchone()
        return _sub_from_row(row) if row else None

    def list_subscriptions(self, subscriber: str | None = None) -> list[Subscription]:
        query = "SELECT id, subscriber, pattern, cursor, created FROM subscriptions"
        args: tuple = ()
        if subscriber:
            query += " WHERE subscriber = ?"
            args = (subscriber,)
        with self._lock:
            rows = self._conn.execute(query + " ORDER BY created, id", args).fetchall()
        return [_sub_from_row(r) for r in rows]

    def remove_subscription(self, sub_id: str) -> bool:
        with self.transaction() as conn:
            return conn.execute("DELETE FROM subscriptions WHERE id = ?", (sub_id,)).rowcount > 0

    def set_cursor(self, sub_id: str, cursor: int) -> None:
        with self.transaction() as conn:
            conn.execute("UPDATE subscriptions SET cursor = ? WHERE id = ?", (cursor, sub_id))

    def subscription_patterns(self) -> list[str]:
        with self._lock:
            rows = self._conn.execute("SELECT pattern FROM subscriptions").fetchall()
        return [r[0] for r in rows]

    def read(
        self,
        sub_id: str,
        limit: int,
        matches: Callable[[str, str], bool],
        *,
        commit: bool,
        batch: int = 500,
    ) -> PollResult:
        """Return up to ``limit`` unread events for a subscription.

        With ``commit`` the subscription cursor moves past everything scanned, in the
        same write transaction that read it, so two consumers sharing a handle never
        receive the same event. Without it nothing is changed (a peek).
        """
        if commit:
            ctx = self.transaction()
        else:
            ctx = self._lock  # type: ignore[assignment]
        with ctx:
            sub = self.get_subscription(sub_id)
            if sub is None:
                raise UnknownSubscription(sub_id)
            found: list[Event] = []
            scanned = sub.cursor
            while len(found) < limit:
                rows = self.events_after(scanned, batch)
                if not rows:
                    break
                for event in rows:
                    scanned = event.id
                    if matches(sub.pattern, event.topic):
                        found.append(event)
                        if len(found) == limit:
                            break
            if commit and scanned != sub.cursor:
                self._conn.execute(
                    "UPDATE subscriptions SET cursor = ? WHERE id = ?", (scanned, sub_id)
                )
            oldest = self.oldest_event_id()
            pruned = max(0, oldest - 1 - sub.cursor) if oldest else 0
            return PollResult(
                subscription_id=sub_id,
                events=found,
                cursor=scanned if commit else sub.cursor,
                has_more=scanned < self.max_event_id(),
                pruned=pruned,
            )
