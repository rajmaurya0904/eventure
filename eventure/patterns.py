"""Topic pattern matching.

Topics are dot-separated segments such as ``agents.planner.done``. A pattern may
use two wildcards, in the style of AMQP topic exchanges:

* ``*`` matches exactly one segment: ``agents.*`` matches ``agents.planner`` but
  not ``agents`` or ``agents.planner.done``.
* ``#`` matches zero or more segments: ``agents.#`` matches ``agents``,
  ``agents.planner`` and ``agents.planner.done``; ``#`` alone matches everything.
"""

from __future__ import annotations

from functools import lru_cache


@lru_cache(maxsize=1024)
def _segments(value: str) -> tuple[str, ...]:
    return tuple(value.split("."))


@lru_cache(maxsize=8192)
def _match(pattern: tuple[str, ...], topic: tuple[str, ...]) -> bool:
    if not pattern:
        return not topic
    head = pattern[0]
    if head == "#":
        rest = pattern[1:]
        if not rest:
            return True
        return any(_match(rest, topic[i:]) for i in range(len(topic) + 1))
    if not topic:
        return False
    if head == "*" or head == topic[0]:
        return _match(pattern[1:], topic[1:])
    return False


def matches(pattern: str, topic: str) -> bool:
    """Return True if ``topic`` is covered by ``pattern``. Inputs are not validated."""
    return _match(_segments(pattern), _segments(topic))


def is_wildcard(pattern: str) -> bool:
    return "*" in pattern or "#" in pattern
