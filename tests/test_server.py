"""Unit tests for MCP server initialization."""

import pytest

import eventure.server


def test_main_raises_not_implemented() -> None:
    """main() should raise NotImplementedError as per the stub."""
    with pytest.raises(NotImplementedError):
        eventure.server.main()


def test_server_module_imports() -> None:
    """Server module should be importable without side effects."""
    # Just importing is enough; if it fails we'll get an ImportError.
    assert eventure.server is not None