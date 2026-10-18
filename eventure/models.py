"""Data models and input validation for Eventure."""

from __future__ import annotations

import json
import re
from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

MAX_TOPIC_LENGTH = 200
MAX_SEGMENTS = 16
MAX_PAYLOAD_BYTES = 64 * 1024

_SEGMENT = re.compile(r"^[A-Za-z0-9_-]+$")


class EventureError(Exception):
    """Base class for every error Eventure raises on purpose."""


class InvalidTopic(EventureError, ValueError):
    """A topic or pattern is malformed."""


class PayloadError(EventureError, ValueError):
    """A payload is not JSON-serialisable or is too large."""


class UnknownSubscription(EventureError, KeyError):
    """No subscription exists with the given handle."""

    def __str__(self) -> str:
        return f"unknown subscription: {self.args[0]}"


def _split(value: str, kind: str, wildcards: bool) -> list[str]:
    if not isinstance(value, str) or not value:
        raise InvalidTopic(f"{kind} must be a non-empty string")
    if len(value) > MAX_TOPIC_LENGTH:
        raise InvalidTopic(f"{kind} is longer than {MAX_TOPIC_LENGTH} characters")
    segments = value.split(".")
    if len(segments) > MAX_SEGMENTS:
        raise InvalidTopic(f"{kind} has more than {MAX_SEGMENTS} segments")
    for segment in segments:
        if wildcards and segment in ("*", "#"):
            continue
        if not _SEGMENT.match(segment):
            raise InvalidTopic(
                f"invalid {kind} {value!r}: segments are separated by '.' and may contain "
                "only letters, digits, '_' and '-'"
                + (" (or the wildcards '*' and '#')" if wildcards else "")
            )
    return segments


def validate_topic(topic: str) -> str:
    """Return ``topic`` if it is a valid concrete topic like ``agents.planner.done``."""
    _split(topic, "topic", wildcards=False)
    return topic


def validate_pattern(pattern: str) -> str:
    """Return ``pattern`` if valid. ``*`` matches one segment, ``#`` zero or more."""
    _split(pattern, "pattern", wildcards=True)
    return pattern


def validate_payload(payload: Any) -> Any:
    """Check that ``payload`` is JSON-serialisable and not larger than 64 KiB."""
    try:
        encoded = json.dumps(payload, allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise PayloadError(f"payload is not JSON-serialisable: {exc}") from exc
    size = len(encoded.encode("utf-8"))
    if size > MAX_PAYLOAD_BYTES:
        raise PayloadError(f"payload is {size} bytes; the limit is {MAX_PAYLOAD_BYTES}")
    return payload


def utcnow() -> datetime:
    return datetime.now(UTC)


class Event(BaseModel):
    """One published event. ``id`` is assigned by the bus and increases monotonically."""

    model_config = ConfigDict(extra="forbid")

    id: int = Field(default=0, ge=0)
    topic: str
    payload: Any = Field(default_factory=dict)
    publisher: str = ""
    timestamp: datetime = Field(default_factory=utcnow)

    @field_validator("topic")
    @classmethod
    def _check_topic(cls, value: str) -> str:
        return validate_topic(value)

    @field_validator("payload")
    @classmethod
    def _check_payload(cls, value: Any) -> Any:
        return validate_payload(value)

    @field_validator("timestamp")
    @classmethod
    def _utc(cls, value: datetime) -> datetime:
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)

    def to_json(self) -> dict[str, Any]:
        """A plain dict that can go straight into a JSON tool result."""
        return self.model_dump(mode="json")


class Subscription(BaseModel):
    """A named, durable interest in a topic pattern, with a read cursor."""

    model_config = ConfigDict(extra="forbid")

    id: str
    subscriber: str
    pattern: str
    cursor: int = Field(default=0, ge=0)
    created_at: datetime = Field(default_factory=utcnow)

    @field_validator("pattern")
    @classmethod
    def _check_pattern(cls, value: str) -> str:
        return validate_pattern(value)

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class PollResult(BaseModel):
    """What one poll of a subscription returned."""

    model_config = ConfigDict(extra="forbid")

    subscription_id: str
    events: list[Event]
    cursor: int = Field(ge=0)
    has_more: bool = False
    pruned: int = Field(
        default=0,
        ge=0,
        description="Events deleted by retention before this subscription read them "
        "(an upper bound: it also counts events that would not have matched).",
    )

    def to_json(self) -> dict[str, Any]:
        return self.model_dump(mode="json")
