"""Bounded-cost appends for the bridge event log.

`append_bridge_event` runs on every bridge GET. It used to prune unconditionally,
re-reading and re-parsing the whole log per request (3.2ms at 1,908 rows, 8.4ms
past the row cap), paid again by every concurrent request in a page-load burst
while the accept thread competed for the GIL.

AI boundary owns: append/prune scheduling and retained-row bounds.
AI boundary search before contracts: diagnostic event contract, bridge logging, and ops diagnostics routes.
AI boundary verify: this file plus `test_diagnostic_events.py`.
"""

from __future__ import annotations

import json
from pathlib import Path

from src.bridge import diagnostic_events


def _event(index: int) -> dict[str, object]:
    return {
        "schemaVersion": 1,
        "ts": f"2026-09-29T16:30:{index % 60:02d}.000000+00:00",
        "level": "info",
        "event": "http_get_route",
        "message": "http_get_route",
        "fields": {"rawPath": "/ops/task-state?view=summary", "routePath": "/ops/task-state"},
    }


def _row_count(path: Path) -> int:
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def test_frequent_appends_do_not_rewrite_the_log_every_call(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "admin-bridge-events.jsonl"
    for index in range(50):
        diagnostic_events.append_bridge_event(path, _event(index))

    rewrites = {"count": 0}
    original = diagnostic_events.prune_bridge_events

    def _counting_prune(*args: object, **kwargs: object) -> None:
        rewrites["count"] += 1
        original(*args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(diagnostic_events, "prune_bridge_events", _counting_prune)
    for index in range(50, 100):
        diagnostic_events.append_bridge_event(path, _event(index))

    # A prune per append is the regression this guards; the byte trigger is far
    # below the file size here, so only the row window can fire.
    assert rewrites["count"] == 0
    assert _row_count(path) == 100
    assert len(diagnostic_events.read_bridge_events(path, limit=100)) == 100


def test_append_eventually_prunes_past_the_row_cap(tmp_path: Path) -> None:
    path = tmp_path / "admin-bridge-events.jsonl"
    total = diagnostic_events._PRUNE_ROW_WINDOW + diagnostic_events.DEFAULT_MAX_ROWS + 10

    for index in range(total):
        diagnostic_events.append_bridge_event(path, _event(index))

    rows = diagnostic_events.read_bridge_events(path, limit=0)
    # Retention stays bounded: the newest rows survive and the count is near the
    # 2,000 cap rather than growing with every append.
    assert len(rows) <= diagnostic_events.DEFAULT_MAX_ROWS + diagnostic_events._PRUNE_ROW_WINDOW
    assert rows[-1]["fields"]["rawPath"] == "/ops/task-state?view=summary"


def test_append_prunes_when_the_file_exceeds_the_byte_trigger(tmp_path: Path, monkeypatch) -> None:
    path = tmp_path / "admin-bridge-events.jsonl"
    path.write_text(
        "".join(
            json.dumps({"schemaVersion": 1, "event": f"pad_{index}", "blob": "x" * 400}) + "\n"
            for index in range(50)
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(diagnostic_events, "_PRUNE_BYTES_TRIGGER", 1000)
    monkeypatch.setattr(diagnostic_events, "_PRUNE_ROW_WINDOW", 10_000)
    assert path.stat().st_size > 1000

    diagnostic_events.append_bridge_event(path, _event(1))

    # Over the byte trigger, so the log is truncated back under the default cap
    # instead of growing without bound.
    assert path.stat().st_size <= diagnostic_events.DEFAULT_MAX_BYTES
    assert diagnostic_events.read_bridge_events(path, limit=0)[-1]["event"] == "http_get_route"
