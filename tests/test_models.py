"""Tests for the Event and Subscription models and input validation."""

import json
from datetime import UTC, datetime, timedelta, timezone

import pytest
from pydantic import ValidationError

from eventure.models import (
    MAX_PAYLOAD_BYTES,
    Event,
    InvalidTopic,
    PayloadError,
    Subscription,
    validate_pattern,
    validate_payload,
    validate_topic,
)


def test_event_defaults() -> None:
    event = Event(topic="test", payload={})
    assert event.id == 0
    assert event.payload == {}
    assert event.publisher == ""
    assert event.timestamp.tzinfo is not None
    assert abs(datetime.now(UTC) - event.timestamp) < timedelta(seconds=5)


def test_payload_defaults_to_empty_object() -> None:
    assert Event(topic="a").payload == {}


@pytest.mark.parametrize("payload", [{}, [], [1, "a", None], "text", 3, 2.5, True, None])
def test_any_json_value_is_a_valid_payload(payload) -> None:
    assert Event(topic="t", payload=payload).payload == payload


def test_nested_payload_round_trips_through_json() -> None:
    event = Event(topic="a.b", payload={"x": [1, {"y": None}]}, publisher="planner", id=7)
    again = Event.model_validate_json(json.dumps(event.to_json()))
    assert again == event


def test_to_json_is_plain_json_types() -> None:
    data = Event(topic="a", payload={"n": 1}).to_json()
    json.dumps(data)
    assert isinstance(data["timestamp"], str)
    assert set(data) == {"id", "topic", "payload", "publisher", "timestamp"}


def test_naive_timestamps_are_treated_as_utc() -> None:
    event = Event(topic="a", timestamp=datetime(2026, 1, 2, 3, 4, 5))
    assert event.timestamp == datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)


def test_aware_timestamps_are_converted_to_utc() -> None:
    plus_two = timezone(timedelta(hours=2))
    event = Event(topic="a", timestamp=datetime(2026, 1, 1, 12, 0, tzinfo=plus_two))
    assert event.timestamp == datetime(2026, 1, 1, 10, 0, tzinfo=UTC)


def test_unknown_fields_are_rejected() -> None:
    with pytest.raises(ValidationError):
        Event(topic="a", colour="red")  # type: ignore[call-arg]


def test_negative_id_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Event(topic="a", id=-1)


def test_missing_topic_is_rejected() -> None:
    with pytest.raises(ValidationError):
        Event()  # type: ignore[call-arg]


@pytest.mark.parametrize(
    "topic",
    ["", ".", "a..b", ".a", "a.", "has space", "a.b/c", "a.*", "a.#", "é", "x" * 201],
)
def test_invalid_topics_are_rejected(topic: str) -> None:
    with pytest.raises(ValidationError):
        Event(topic=topic)
    with pytest.raises(InvalidTopic):
        validate_topic(topic)


@pytest.mark.parametrize("topic", ["a", "agents.planner", "a-b_c.D9", "x" * 200])
def test_valid_topics(topic: str) -> None:
    assert validate_topic(topic) == topic


def test_too_many_segments_is_rejected() -> None:
    with pytest.raises(InvalidTopic, match="segments"):
        validate_topic(".".join(["a"] * 17))
    assert validate_topic(".".join(["a"] * 16))


@pytest.mark.parametrize("pattern", ["#", "*", "a.*", "a.#", "*.b.#", "a.b"])
def test_valid_patterns(pattern: str) -> None:
    assert validate_pattern(pattern) == pattern


@pytest.mark.parametrize("pattern", ["", "a.**", "a.b*", "a..b", "a b", "*#"])
def test_invalid_patterns(pattern: str) -> None:
    with pytest.raises(InvalidTopic):
        validate_pattern(pattern)


def test_non_string_topic_is_rejected() -> None:
    with pytest.raises(InvalidTopic):
        validate_topic(None)  # type: ignore[arg-type]


def test_non_serialisable_payload_is_rejected() -> None:
    with pytest.raises(ValidationError, match="not JSON-serialisable"):
        Event(topic="a", payload={"f": object()})
    with pytest.raises(PayloadError):
        validate_payload({1, 2})


def test_nan_payload_is_rejected() -> None:
    with pytest.raises(PayloadError):
        validate_payload(float("nan"))


def test_oversized_payload_is_rejected() -> None:
    with pytest.raises(PayloadError, match="limit"):
        validate_payload("x" * (MAX_PAYLOAD_BYTES + 1))
    assert validate_payload("x" * 1000)


def test_subscription_validates_pattern_and_defaults_cursor() -> None:
    sub = Subscription(id="s1", subscriber="planner", pattern="jobs.*")
    assert sub.cursor == 0
    with pytest.raises(ValidationError):
        Subscription(id="s1", subscriber="planner", pattern="bad pattern")


def test_subscription_to_json() -> None:
    data = Subscription(id="s1", subscriber="p", pattern="#", cursor=4).to_json()
    assert data["cursor"] == 4 and data["pattern"] == "#"
    json.dumps(data)
