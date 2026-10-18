"""Tests for topic pattern matching."""

import pytest

from eventure.patterns import is_wildcard, matches


@pytest.mark.parametrize(
    ("pattern", "topic", "expected"),
    [
        ("agents.foo", "agents.foo", True),
        ("agents.foo", "agents.bar", False),
        ("agents.foo", "agents", False),
        ("agents", "agents.foo", False),
        ("agents.*", "agents.foo", True),
        ("agents.*", "agents", False),
        ("agents.*", "agents.foo.bar", False),
        ("*", "agents", True),
        ("*", "agents.foo", False),
        ("*.done", "planner.done", True),
        ("*.done", "planner.worker.done", False),
        ("a.*.c", "a.b.c", True),
        ("a.*.c", "a.b.d", False),
        ("agents.#", "agents", True),
        ("agents.#", "agents.foo", True),
        ("agents.#", "agents.foo.bar.baz", True),
        ("agents.#", "other.foo", False),
        ("#", "anything", True),
        ("#", "any.thing.at.all", True),
        ("#.done", "done", True),
        ("#.done", "a.b.done", True),
        ("#.done", "a.b.done.not", False),
        ("a.#.z", "a.z", True),
        ("a.#.z", "a.b.c.z", True),
        ("a.#.z", "a.b.c", False),
        ("#.mid.#", "x.mid.y", True),
        ("#.mid.#", "x.y", False),
    ],
)
def test_matches(pattern: str, topic: str, expected: bool) -> None:
    assert matches(pattern, topic) is expected


def test_matching_is_case_sensitive() -> None:
    assert not matches("Agents.foo", "agents.foo")


def test_literal_segments_do_not_match_by_prefix() -> None:
    assert not matches("agent.*", "agents.foo")
    assert not matches("agents.fo", "agents.foo")


def test_many_hash_wildcards_do_not_blow_up() -> None:
    pattern = ".".join(["#"] * 10 + ["end"])
    topic = ".".join(["a"] * 12)
    assert matches(pattern, topic) is False


def test_is_wildcard() -> None:
    assert is_wildcard("a.*") and is_wildcard("#")
    assert not is_wildcard("a.b")
