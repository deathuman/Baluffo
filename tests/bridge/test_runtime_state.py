from __future__ import annotations

import json
from pathlib import Path

from src.bridge.server import runtime_state


def test_append_startup_metric_persists_browser_created_timestamp(tmp_path: Path) -> None:
    metrics_path = tmp_path / "startup-metrics.jsonl"
    previous_path = runtime_state.STARTUP_METRICS_PATH
    try:
        runtime_state.STARTUP_METRICS_PATH = metrics_path
        runtime_state.append_startup_metric(
            "jobs_first_render",
            {"elapsedMs": 1200, "browserCreatedAtMs": 1_744_880_000_123},
            now_iso=lambda: "2026-04-17T09:00:00+00:00",
        )
    finally:
        runtime_state.STARTUP_METRICS_PATH = previous_path

    rows = [
        json.loads(line)
        for line in metrics_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    assert rows == [
        {
            "schemaVersion": 1,
            "ts": "2026-04-17T09:00:00+00:00",
            "event": "jobs_first_render",
            "category": "page",
            "payload": {"elapsedMs": 1200, "browserCreatedAtMs": 1_744_880_000_123},
            "browserTsMs": 1_744_880_000_123,
        }
    ]


def test_read_startup_metrics_ignores_invalid_jsonl_rows(tmp_path: Path) -> None:
    metrics_path = tmp_path / "startup-metrics.jsonl"
    metrics_path.write_text(
        "\n".join(
            [
                "{bad json",
                json.dumps(
                    {
                        "schemaVersion": 1,
                        "ts": "2026-04-17T09:00:00+00:00",
                        "event": "desktop_launch_start",
                        "category": "launch",
                        "fields": {"elapsedMs": 0},
                    }
                ),
            ]
        ),
        encoding="utf-8",
    )
    previous_path = runtime_state.STARTUP_METRICS_PATH
    try:
        runtime_state.STARTUP_METRICS_PATH = metrics_path
        rows = runtime_state.read_startup_metrics(limit=10)
    finally:
        runtime_state.STARTUP_METRICS_PATH = previous_path

    assert [row["event"] for row in rows] == ["desktop_launch_start"]
    assert rows[0]["schemaVersion"] == 1
    assert rows[0]["category"] == "launch"
