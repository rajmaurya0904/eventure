"""Tests for invalid input and faulty listeners."""

import logging
import sqlite3
from pathlib import Path

import pytest
from pydantic import ValidationError

from eventure.bus import MAX_POLL_LIMIT, EventBus
from eventure.models import (
    MAX_PAYLOAD_BYTES,
    EventureError,
    InvalidTopic,
    PayloadError,
    Subscription,
    UnknownSubscription,
)


@pytest.fixture
def bus():
    with EventBus() as b:
        yield b


@pytest.mark.parametrize("topic", ["", "a..b", "has space", "a.*", "a.#", "x" * 201])
def test_publish_rejects_invalid_topics(bus: EventBus, topic: str) -> None:
    with pytest.raises((InvalidTopic, ValidationError)):
        bus.publish(topic, {})
    assert bus.stats()["events_retained"] == 0


def test_publish_rejects_unserialisable_payload(bus: EventBus) -> None:
    with pytest.raises((PayloadError, ValidationError)):
        bus.publish("a", {"bad": object()})
    assert bus.stats()["events_retained"] == 0


def test_publish_rejects_oversized_payload(bus: EventBus) -> None:
    with pytest.raises((PayloadError, ValidationError)):
        bus.publish("a", "x" * (MAX_PAYLOAD_BYTES + 1))


def test_subscribe_rejects_invalid_pattern(bus: EventBus) -> None:
    with pytest.raises(InvalidTopic):
        bus.subscribe("agent", "a.**")
    assert bus.subscriptions() == []


@pytest.mark.parametrize("name", ["", "   ", "x" * 101, "bad\nname"])
def test_subscribe_rejects_invalid_subscriber_names(bus: EventBus, name: str) -> None:
    with pytest.raises(EventureError):
        bus.subscribe(name, "#")


def test_poll_unknown_subscription(bus: EventBus) -> None:
    with pytest.raises(UnknownSubscription, match="nope"):
        bus.poll("nope")


@pytest.mark.parametrize("limit", [0, -1, MAX_POLL_LIMIT + 1, True, "5", 2.5])
def test_poll_rejects_bad_limits(bus: EventBus, limit) -> None:
    sub = bus.subscribe("a", "#")
    with pytest.raises(EventureError):
        bus.poll(sub.id, limit=limit)


def test_history_validates_inputs(bus: EventBus) -> None:
    with pytest.raises(InvalidTopic):
        bus.history("bad pattern")
    with pytest.raises(EventureError):
        bus.history(limit=0)
    with pytest.raises(EventureError):
        bus.history(after_id=-1)


def test_failing_listener_does_not_break_publish(bus: EventBus) -> None:
    def boom(event) -> None:
        raise RuntimeError("listener exploded")

    bus.add_listener("#", boom)
    event = bus.publish("a", {"ok": True})
    assert event.id == 1
    assert bus.stats()["events_retained"] == 1


def test_failing_listener_is_recorded(bus: EventBus) -> None:
    def boom(event) -> None:
        raise ValueError("bad event")

    handle = bus.add_listener("#", boom)
    bus.publish("a")
    (error,) = bus.errors
    assert (error.listener_id, error.event_id) == (handle, 1)
    assert error.error == "ValueError: bad event"


def test_other_listeners_still_run_after_one_fails(bus: EventBus) -> None:
    seen = []

    def boom(event) -> None:
        raise RuntimeError("x")

    bus.add_listener("#", boom)
    bus.add_listener("#", seen.append)
    bus.publish("a")
    bus.publish("b")
    assert [e.topic for e in seen] == ["a", "b"]
    assert len(bus.errors) == 2


def test_failing_listener_is_logged(bus: EventBus, caplog: pytest.LogCaptureFixture) -> None:
    def boom(event) -> None:
        raise RuntimeError("logged failure")

    bus.add_listener("#", boom)
    with caplog.at_level(logging.WARNING, logger="eventure"):
        bus.publish("a")
    assert "logged failure" in caplog.text


def test_listener_errors_are_capped(bus: EventBus) -> None:
    def boom(event) -> None:
        raise RuntimeError("x")

    bus.add_listener("#", boom)
    for _ in range(150):
        bus.publish("a")
    assert len(bus.errors) == 100


def test_add_listener_validates(bus: EventBus) -> None:
    with pytest.raises(InvalidTopic):
        bus.add_listener("bad pattern", print)
    with pytest.raises(EventureError):
        bus.add_listener("#", "not callable")  # type: ignore[arg-type]


def test_retention_must_be_positive() -> None:
    with pytest.raises(ValueError):
        EventBus(retention=0)


def test_failed_publish_inside_store_rolls_back(tmp_path: Path) -> None:
    with EventBus(tmp_path / "bus.db") as bus:
        bus.publish("a")
        bus.store._conn.execute("DROP TABLE subscriptions")
        with pytest.raises(sqlite3.OperationalError):
            bus.store.add_subscription(Subscription(id="x", subscriber="s", pattern="#"))
        # the failed transaction must not leave the connection stuck inside BEGIN
        assert bus.publish("b").id == 2


def test_database_in_a_new_directory_is_created(tmp_path: Path) -> None:
    path = tmp_path / "nested" / "dir" / "bus.db"
    with EventBus(path) as bus:
        bus.publish("a")
    assert path.is_file()
