"""The example script must keep working: it is the README's worked example."""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

DEMO = Path(__file__).resolve().parent.parent / "examples" / "demo.py"


def test_demo_runs_and_prints_results() -> None:
    proc = subprocess.run(
        [sys.executable, str(DEMO)], capture_output=True, text=True, timeout=150
    )
    assert proc.returncode == 0, proc.stderr
    for line in ("3^2 = 9", "4^2 = 16", "5^2 = 25"):
        assert line in proc.stdout
