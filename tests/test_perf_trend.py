from __future__ import annotations

import json

from scripts import perf_trend
from tests.helpers.temp_paths import workspace_tmpdir


def test_load_trend_rows_missing_file_returns_empty_list() -> None:
    with workspace_tmpdir("perf-trend-missing") as data_dir:
        assert perf_trend.load_trend_rows(data_dir / "missing.ndjson") == []


def test_load_trend_rows_ignores_malformed_and_incomplete_rows() -> None:
    with workspace_tmpdir("perf-trend-parse") as data_dir:
        path = data_dir / "perf-trend.ndjson"
        path.write_text(
            "\n".join(
                [
                    "{not-json",
                    json.dumps({"mode": "discovery"}),
                    json.dumps({"mode": "", "totalDurationMs": 100}),
                    json.dumps({"mode": "fetch", "totalDurationMs": 2500, "ts": "2026-05-05T10:00:00Z"}),
                ]
            ),
            encoding="utf-8",
        )

        rows = perf_trend.load_trend_rows(path)

        assert len(rows) == 1
        assert rows[0]["mode"] == "fetch"
        assert rows[0]["totalDurationMs"] == 2500


def test_trend_entries_compute_previous_and_baseline_deltas_by_mode() -> None:
    rows = [
        {
            "ts": "2026-05-01T10:00:00Z",
            "mode": "discovery",
            "totalDurationMs": 10000,
            "status": "pass",
            "commitSha": "abcdef123",
        },
        {
            "ts": "2026-05-02T10:00:00Z",
            "mode": "fetch",
            "totalDurationMs": 20000,
            "status": "pass",
            "commitSha": "bbbbbbb",
        },
        {
            "ts": "2026-05-03T10:00:00Z",
            "mode": "discovery",
            "totalDurationMs": 15000,
            "status": "warn",
            "commitSha": "ccccccc",
        },
        {
            "ts": "2026-05-04T10:00:00Z",
            "mode": "discovery",
            "totalDurationMs": 12000,
            "status": "pass",
            "commitSha": "ddddddd",
        },
    ]

    entries = perf_trend.trend_entries(rows)

    assert entries[0]["vsPrev"] == "--"
    assert entries[0]["vsBaseline"] == "--"
    assert entries[2]["vsPrev"] == "+50.0%"
    assert entries[2]["vsBaseline"] == "+50.0%"
    assert entries[3]["vsPrev"] == "-20.0%"
    assert entries[3]["vsBaseline"] == "+20.0%"


def test_format_trend_table_limits_to_recent_rows() -> None:
    rows = [
        {"ts": f"2026-05-{day:02d}T10:00:00Z", "mode": "fetch", "totalDurationMs": day * 1000}
        for day in range(1, 5)
    ]

    table = perf_trend.format_trend_table(rows, limit=2)

    assert "2026-05-01" not in table
    assert "2026-05-02" not in table
    assert "2026-05-03" in table
    assert "2026-05-04" in table
