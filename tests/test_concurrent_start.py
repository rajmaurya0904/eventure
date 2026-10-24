"""Several processes opening a brand new database at the same moment must all succeed."""

from __future__ import annotations

import subprocess
import sys

CODE = (
    "import sys, time; from eventure.bus import EventBus; "
    "t = float(sys.argv[2]); time.sleep(max(0, t - time.time())); "
    "b = EventBus(sys.argv[1]); b.publish('race.start'); print('ok')"
)


def test_simultaneous_first_open(tmp_path) -> None:
    import time

    db = str(tmp_path / "bus.db")
    start = str(time.time() + 3)
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", CODE, db, start],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        for _ in range(6)
    ]
    results = [p.communicate(timeout=60) for p in procs]
    for proc, (out, err) in zip(procs, results, strict=True):
        assert proc.returncode == 0, err
        assert out.strip() == "ok"
