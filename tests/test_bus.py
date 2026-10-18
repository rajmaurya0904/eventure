"""Tests for the EventBus: publishing, subscriptions, polling and history."""

import asyncio
import threading
from pathlib import Path

import pytest

from eventure.bus import EventBus
from eventure.models import EventureError, UnknownSubscription


@pytest.fixture
def bus():
    with EventBus() as b:
        yield b


def topics(events) -> list[str]:
    return [e.topic for e in events]


def test_publish_assigns_increasing_ids(bus: EventBus) -> None:
    ids = [bus.publish("a", {"n": i}).id for i in range(3)]
    assert ids == [1, 2, 3]


def test_publish_returns_stored_event(bus: EventBus) -> None:
    event = bus.publish("jobs.done", {"job": 7}, publisher="worker-1")
    assert (event.topic, event.payload, event.publisher) == ("jobs.done", {"job": 7}, "worker-1")
    assert event.id == 1


def test_missing_payload_becomes_empty_object(bus: EventBus) -> None:
    assert bus.publish("a").payload == {}


def test_subscriber_only_sees_events_after_subscribing(bus: EventBus) -> None:
    bus.publish("old")
    sub = bus.subscribe("agent", "#")
    bus.publish("new")
    assert topics(bus.poll(sub.id).events) == ["new"]


def test_replay_starts_from_the_oldest_retained_event(bus: EventBus) -> None:
    bus.publish("one")
    bus.publish("two")
    sub = bus.subscribe("agent", "#", replay=True)
    assert topics(bus.poll(sub.id).events) == ["one", "two"]


def test_poll_marks_events_as_read(bus: EventBus) -> None:
    sub = bus.subscribe("agent", "#")
    bus.publish("a")
    assert len(bus.poll(sub.id).events) == 1
    assert bus.poll(sub.id).events == []


def test_peek_does_not_advance_the_cursor(bus: EventBus) -> None:
    sub = bus.subscribe("agent", "#")
    bus.publish("a")
    assert len(bus.poll(sub.id, peek=True).events) == 1
    assert len(bus.poll(sub.id, peek=True).events) == 1
    assert len(bus.poll(sub.id).events) == 1


def test_poll_respects_limit_and_reports_has_more(bus: EventBus) -> None:
    sub = bus.subscribe("agent", "#")
    for i in range(5):
        bus.publish("t", {"i": i})
    first = bus.poll(sub.id, limit=2)
    assert [e.payload["i"] for e in first.events] == [0, 1]
    assert first.has_more
    rest = bus.poll(sub.id, limit=10)
    assert [e.payload["i"] for e in rest.events] == [2, 3, 4]
    assert not rest.has_more


def test_multi_subscriber_delivery_is_independent(bus: EventBus) -> None:
    a = bus.subscribe("a", "jobs.#")
    b = bus.subscribe("b", "jobs.#")
    bus.publish("jobs.new")
    assert len(bus.poll(a.id).events) == 1
    assert len(bus.poll(b.id).events) == 1
    assert bus.poll(a.id).events == []


def test_subscription_patterns_filter_events(bus: EventBus) -> None:
    planner = bus.subscribe("planner", "tasks.created")
    audit = bus.subscribe("audit", "tasks.*")
    everything = bus.subscribe("logger", "#")
    bus.publish("tasks.created")
    bus.publish("tasks.failed")
    bus.publish("other.thing")
    assert topics(bus.poll(planner.id).events) == ["tasks.created"]
    assert topics(bus.poll(audit.id).events) == ["tasks.created", "tasks.failed"]
    all_topics = ["tasks.created", "tasks.failed", "other.thing"]
    assert topics(bus.poll(everything.id).events) == all_topics


def test_non_matching_events_do_not_cause_rescans(bus: EventBus) -> None:
    sub = bus.subscribe("agent", "wanted")
    for _ in range(3):
        bus.publish("noise")
    result = bus.poll(sub.id)
    assert result.events == [] and result.cursor == 3
    assert bus.subscriptions()[0].cursor == 3


def test_subscribing_twice_returns_the_same_subscription(bus: EventBus) -> None:
    first = bus.subscribe("agent", "a.*")
    again = bus.subscribe("agent", "a.*")
    assert again.id == first.id
    assert len(bus.subscriptions()) == 1


def test_resubscribing_resumes_from_the_saved_cursor(bus: EventBus) -> None:
    sub = bus.subscribe("agent", "#")
    bus.publish("a")
    bus.poll(sub.id)
    bus.publish("b")
    resumed = bus.subscribe("agent", "#")
    assert resumed.id == sub.id
    assert topics(bus.poll(resumed.id).events) == ["b"]


def test_resubscribing_with_replay_rewinds(bus: EventBus) -> None:
    sub = bus.subscribe("agent", "#")
    bus.publish("a")
    bus.poll(sub.id)
    rewound = bus.subscribe("agent", "#", replay=True)
    assert rewound.cursor == 0
    assert topics(bus.poll(sub.id).events) == ["a"]


def test_same_pattern_for_different_subscribers_is_separate(bus: EventBus) -> None:
    assert bus.subscribe("a", "#").id != bus.subscribe("b", "#").id


def test_unsubscribe(bus: EventBus) -> None:
    sub = bus.subscribe("agent", "#")
    assert bus.unsubscribe(sub.id) is True
    assert bus.unsubscribe(sub.id) is False
    with pytest.raises(UnknownSubscription):
        bus.poll(sub.id)


def test_list_subscriptions_by_subscriber(bus: EventBus) -> None:
    bus.subscribe("a", "x")
    bus.subscribe("a", "y")
    bus.subscribe("b", "x")
    assert sorted(s.pattern for s in bus.subscriptions("a")) == ["x", "y"]
    assert len(bus.subscriptions()) == 3


def test_shared_subscription_splits_work_between_consumers(bus: EventBus) -> None:
    sub = bus.subscribe("workers", "jobs")
    for i in range(10):
        bus.publish("jobs", {"i": i})
    got_a = bus.poll(sub.id, limit=4).events
    got_b = bus.poll(sub.id, limit=4).events
    got_c = bus.poll(sub.id, limit=4).events
    seen = [e.payload["i"] for e in got_a + got_b + got_c]
    assert sorted(seen) == list(range(10))
    assert len(set(seen)) == 10


def test_concurrent_consumers_never_receive_the_same_event(tmp_path: Path) -> None:
    path = tmp_path / "bus.db"
    with EventBus(path) as producer:
        sub = producer.subscribe("workers", "jobs")
        for i in range(60):
            producer.publish("jobs", {"i": i})
    received: list[int] = []
    lock = threading.Lock()

    def worker() -> None:
        with EventBus(path) as mine:
            while True:
                result = mine.poll(sub.id, limit=3)
                if not result.events:
                    return
                with lock:
                    received.extend(e.payload["i"] for e in result.events)

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    assert sorted(received) == list(range(60))


def test_two_bus_objects_share_a_database_file(tmp_path: Path) -> None:
    path = tmp_path / "shared.db"
    with EventBus(path) as writer, EventBus(path) as reader:
        sub = reader.subscribe("reader", "news.#")
        writer.publish("news.sports", {"score": "2-1"}, publisher="writer")
        result = reader.poll(sub.id)
        assert [(e.topic, e.publisher) for e in result.events] == [("news.sports", "writer")]


def test_state_survives_reopening_the_database(tmp_path: Path) -> None:
    path = tmp_path / "bus.db"
    with EventBus(path) as first:
        sub = first.subscribe("agent", "#")
        first.publish("a")
    with EventBus(path) as second:
        assert topics(second.poll(sub.id).events) == ["a"]
        assert second.publish("b").id == 2


def test_retention_keeps_only_the_newest_events() -> None:
    with EventBus(retention=3) as small:
        for i in range(10):
            small.publish("t", {"i": i})
        assert [e.payload["i"] for e in small.history(limit=50)] == [7, 8, 9]
        assert small.stats()["events_retained"] == 3
        assert small.stats()["latest_event_id"] == 10


def test_pruned_events_are_reported_to_slow_subscribers() -> None:
    with EventBus(retention=3) as small:
        sub = small.subscribe("slow", "#")
        for i in range(10):
            small.publish("t", {"i": i})
        result = small.poll(sub.id, limit=50)
        assert [e.payload["i"] for e in result.events] == [7, 8, 9]
        assert result.pruned == 7


def test_history_returns_newest_matching_events_oldest_first(bus: EventBus) -> None:
    for i in range(6):
        bus.publish("a.x" if i % 2 == 0 else "b.y", {"i": i})
    recent = bus.history("a.*", limit=2)
    assert [e.payload["i"] for e in recent] == [2, 4]


def test_history_after_id_pages_forward(bus: EventBus) -> None:
    for i in range(5):
        bus.publish("t", {"i": i})
    page = bus.history("#", limit=2, after_id=1)
    assert [e.id for e in page] == [2, 3]
    assert [e.id for e in bus.history("#", limit=10, after_id=page[-1].id)] == [4, 5]


def test_history_on_empty_bus(bus: EventBus) -> None:
    assert bus.history() == []


def test_matching_subscriptions_counts_receivers(bus: EventBus) -> None:
    bus.subscribe("a", "jobs.*")
    bus.subscribe("b", "#")
    bus.subscribe("c", "other")
    assert bus.matching_subscriptions("jobs.new") == 2
    assert bus.matching_subscriptions("nothing") == 1


def test_stats(bus: EventBus) -> None:
    bus.subscribe("b", "#")
    bus.subscribe("a", "x")
    bus.publish("x")
    stats = bus.stats()
    assert stats["events_retained"] == 1 and stats["latest_event_id"] == 1
    assert stats["subscriptions"] == 2 and stats["subscribers"] == ["a", "b"]


def test_listeners_receive_matching_events(bus: EventBus) -> None:
    got = []
    bus.add_listener("jobs.*", got.append)
    bus.publish("jobs.a")
    bus.publish("other")
    bus.publish("jobs.b")
    assert topics(got) == ["jobs.a", "jobs.b"]


def test_removed_listener_stops_receiving(bus: EventBus) -> None:
    got = []
    handle = bus.add_listener("#", got.append)
    bus.publish("a")
    assert bus.remove_listener(handle) is True
    assert bus.remove_listener(handle) is False
    bus.publish("b")
    assert topics(got) == ["a"]


def test_poll_wait_returns_immediately_when_events_are_waiting(bus: EventBus) -> None:
    sub = bus.subscribe("a", "#")
    bus.publish("x")
    result = asyncio.run(bus.poll_wait(sub.id, wait_seconds=5))
    assert topics(result.events) == ["x"]


def test_poll_wait_times_out_with_no_events(bus: EventBus) -> None:
    sub = bus.subscribe("a", "#")
    result = asyncio.run(bus.poll_wait(sub.id, wait_seconds=0.3, interval=0.05))
    assert result.events == []


def test_poll_wait_wakes_up_when_an_event_arrives(bus: EventBus) -> None:
    sub = bus.subscribe("a", "#")

    async def scenario():
        waiter = asyncio.create_task(bus.poll_wait(sub.id, wait_seconds=5, interval=0.02))
        await asyncio.sleep(0.1)
        bus.publish("late")
        return await asyncio.wait_for(waiter, timeout=3)

    assert topics(asyncio.run(scenario()).events) == ["late"]


def test_poll_wait_rejects_out_of_range_waits(bus: EventBus) -> None:
    sub = bus.subscribe("a", "#")
    with pytest.raises(EventureError):
        asyncio.run(bus.poll_wait(sub.id, wait_seconds=31))
    with pytest.raises(EventureError):
        asyncio.run(bus.poll_wait(sub.id, wait_seconds=-1))
