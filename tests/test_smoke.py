"""Smoke test: package imports cleanly. Replace/extend as modules land."""

import eventure


def test_version_is_set() -> None:
    assert eventure.__version__
