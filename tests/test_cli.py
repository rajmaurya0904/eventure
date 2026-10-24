"""Command line behaviour, run in-process against a temporary database."""

from __future__ import annotations

import json
import subprocess
import sys

import pytest

from eventure import __version__
from eventure.bus import EventBus
from eventure.cli import build_parser, format_event, main
from eventure.models import Event


@pytest.fixture
def db(tmp_path, monkeypatch):
    path = tmp_path / "bus.db"
    monkeypatch.setenv("EVENTURE_DB", str(path))
    return path


def test_help_exits_zero(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--help"])
    assert exc.value.code == 0
    out = capsys.readouterr().out
    for word in ("serve", "publish", "poll", "history", "tail", "stats"):
        assert word in out


def test_version(capsys) -> None:
    with pytest.raises(SystemExit) as exc:
        main(["--version"])
    assert exc.value.code == 0
    assert __version__ in capsys.readouterr().out


def test_unknown_command_is_usage_error() -> None:
    with pytest.raises(SystemExit) as exc:
        main(["frobnicate"])
    assert exc.value.code == 2


def test_publish_prints_event_and_recipients(db, capsys) -> None:
    assert main(["publish", "build.api.done", '{"ok": true}']) == 0
    out = capsys.readouterr().out
    assert "#1" in out and "build.api.done" in out and '{"ok": true}' in out
    assert "delivered to 0 subscriptions" in out


def test_publish_defaults_to_empty_payload(db, capsys) -> None:
    main(["publish", "ping"])
    assert "{}" in capsys.readouterr().out


def test_publish_rejects_bad_json(db, capsys) -> None:
    assert main(["publish", "a", "{nope"]) == 1
    assert "not valid JSON" in capsys.readouterr().err


def test_publish_rejects_bad_topic(db, capsys) -> None:
    assert main(["publish", "has space"]) == 1
    assert "invalid topic" in capsys.readouterr().err


def test_publish_records_publisher(db, capsys) -> None:
    main(["publish", "a", "--publisher", "planner"])
    assert "[planner]" in capsys.readouterr().out


def test_poll_resumes_between_invocations(db, capsys) -> None:
    main(["poll", "bob", "jobs.*"])  # creates the subscription at "now"
    capsys.readouterr()
    main(["publish", "jobs.one"])
    main(["publish", "other.one"])
    main(["publish", "jobs.two"])
    capsys.readouterr()
    main(["poll", "bob", "jobs.*"])
    first = capsys.readouterr().out
    assert "jobs.one" in first and "jobs.two" in first and "other.one" not in first
    main(["poll", "bob", "jobs.*"])
    assert "no new events" in capsys.readouterr().out


def test_poll_replay_sees_history(db, capsys) -> None:
    main(["publish", "old.event"])
    capsys.readouterr()
    main(["poll", "late", "#", "--replay"])
    assert "old.event" in capsys.readouterr().out


def test_poll_peek_leaves_events_unread(db, capsys) -> None:
    main(["poll", "p", "#"])
    main(["publish", "x"])
    capsys.readouterr()
    main(["poll", "p", "#", "--peek"])
    assert "x" in capsys.readouterr().out
    main(["poll", "p", "#"])
    assert "x" in capsys.readouterr().out


def test_poll_limit_reports_more_waiting(db, capsys) -> None:
    main(["poll", "p", "#"])
    for i in range(3):
        main(["publish", f"t{i}"])
    capsys.readouterr()
    main(["poll", "p", "#", "-n", "2"])
    assert "more events are waiting" in capsys.readouterr().out


def test_poll_rejects_bad_pattern(db, capsys) -> None:
    assert main(["poll", "p", "a..b"]) == 1
    assert "invalid pattern" in capsys.readouterr().err


def test_poll_wait_returns_after_timeout(db, capsys) -> None:
    assert main(["poll", "p", "#", "--wait", "0.2"]) == 0
    assert "no new events" in capsys.readouterr().out


def test_poll_wait_out_of_range(db, capsys) -> None:
    assert main(["poll", "p", "#", "--wait", "500"]) == 1
    assert "wait_seconds" in capsys.readouterr().err


def test_history_filters_and_limits(db, capsys) -> None:
    for t in ("a.1", "b.1", "a.2", "a.3"):
        main(["publish", t])
    capsys.readouterr()
    main(["history", "a.*", "-n", "2"])
    out = capsys.readouterr().out
    assert "a.2" in out and "a.3" in out and "a.1" not in out and "b.1" not in out


def test_history_empty(db, capsys) -> None:
    main(["history"])
    assert "no matching events" in capsys.readouterr().out


def test_stats_is_json(db, capsys) -> None:
    main(["publish", "a"])
    capsys.readouterr()
    main(["stats"])
    data = json.loads(capsys.readouterr().out)
    assert data["events_retained"] == 1
    assert data["database"] == str(db)


def test_subscriptions_listing(db, capsys) -> None:
    main(["subscriptions"])
    assert "no subscriptions" in capsys.readouterr().out
    main(["poll", "alice", "x.#"])
    capsys.readouterr()
    main(["subscriptions"])
    out = capsys.readouterr().out
    assert "alice" in out and "x.#" in out and "cursor=" in out


def test_db_flag_overrides_environment(db, tmp_path, capsys) -> None:
    other = tmp_path / "other.db"
    main(["--db", str(other), "publish", "elsewhere"])
    capsys.readouterr()
    with EventBus(other) as bus:
        assert [e.topic for e in bus.history()] == ["elsewhere"]
    with EventBus(db) as bus:
        assert bus.history() == []


def test_unusable_database_path_exits_two(tmp_path, capsys) -> None:
    blocker = tmp_path / "file"
    blocker.write_text("x")
    assert main(["--db", str(blocker / "sub" / "bus.db"), "stats"]) == 2
    assert "cannot use the database" in capsys.readouterr().err


def test_format_event_line() -> None:
    line = format_event(Event(id=7, topic="a.b", payload={"k": 1}, publisher="me"))
    assert line.startswith("#7 ") and line.endswith('a.b [me] {"k": 1}')


def test_parser_defaults_to_no_command() -> None:
    assert build_parser().parse_args([]).command is None


def test_module_entry_point_runs(db) -> None:
    proc = subprocess.run(
        [sys.executable, "-m", "eventure.cli", "--db", str(db), "publish", "via.module"],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0
    assert "via.module" in proc.stdout
