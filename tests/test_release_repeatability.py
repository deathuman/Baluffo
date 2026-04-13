from pathlib import Path

import pytest

from src import release_repeatability as rr

pytestmark = pytest.mark.release


def _write_report(
    path: Path,
    *,
    output_count: int,
    failed_sources: int,
    wall_clock_ms: int,
    source_rows: list[dict],
) -> None:
    path.write_text(
        __import__("json").dumps(
            {
                "startedAt": "2026-03-20T18:00:00+00:00",
                "finishedAt": "2026-03-20T18:10:00+00:00",
                "summary": {"outputCount": output_count, "failedSources": failed_sources},
                "runtime": {
                    "selectedSourceCount": 91,
                    "incrementalCacheEnabled": False,
                    "forceRefreshAll": True,
                    "socialEnabled": True,
                    "timingSummary": {
                        "wallClockDurationMs": wall_clock_ms,
                        "totalDurationMs": wall_clock_ms * 3,
                    },
                },
                "sources": source_rows,
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def test_build_report_flags_volatile_source_and_release_floor_failure(tmp_path: Path) -> None:
    run1 = tmp_path / "run1.json"
    run2 = tmp_path / "run2.json"
    _write_report(
        run1,
        output_count=34081,
        failed_sources=0,
        wall_clock_ms=100000,
        source_rows=[
            {
                "name": "gracklehq",
                "adapter": "html",
                "status": "ok",
                "keptCount": 1200,
                "fetchedCount": 1200,
                "durationMs": 50000,
            },
            {
                "name": "social_mastodon",
                "adapter": "social",
                "status": "ok",
                "keptCount": 118,
                "fetchedCount": 118,
                "durationMs": 1500,
            },
        ],
    )
    _write_report(
        run2,
        output_count=33230,
        failed_sources=19,
        wall_clock_ms=650000,
        source_rows=[
            {
                "name": "gracklehq",
                "adapter": "html",
                "status": "error",
                "keptCount": 0,
                "fetchedCount": 0,
                "durationMs": 52093,
                "error": "Network error for https://gracklehq.com/jobs",
            },
            {
                "name": "social_mastodon",
                "adapter": "social",
                "status": "ok",
                "keptCount": 118,
                "fetchedCount": 118,
                "durationMs": 1400,
            },
        ],
    )

    report = rr.build_report([run1, run2], release_floor=34131)

    assert int((report.get("totals") or {}).get("outputSwing") or 0) == 851
    assert int((report.get("gate") or {}).get("minOutputCount") or 0) == 33230
    assert bool((report.get("gate") or {}).get("passesReleaseFloor")) is False
    volatile = report.get("volatileSources") or []
    assert str(volatile[0].get("name") or "") == "gracklehq"
    assert int(volatile[0].get("keptCountSwing") or 0) == 1200
    assert int(volatile[0].get("errorRuns") or 0) == 1


def test_render_markdown_includes_volatility_summary(tmp_path: Path) -> None:
    run = tmp_path / "run.json"
    _write_report(
        run,
        output_count=34158,
        failed_sources=0,
        wall_clock_ms=98051,
        source_rows=[
            {
                "name": "social_reddit",
                "adapter": "social",
                "status": "ok",
                "keptCount": 13,
                "fetchedCount": 13,
                "durationMs": 1500,
            },
        ],
    )

    report = rr.build_report([run], release_floor=34131)
    text = rr.render_markdown(report)

    assert "Release Repeatability Report" in text
    assert "Executive summary" in text
    assert "Runs" in text
    assert "Volatile sources" in text
