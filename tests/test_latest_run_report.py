from __future__ import annotations

import importlib.util
import json
import os
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "tools"
    / "measurements"
    / "pipeline"
    / "latest_run_report.py"
)
SPEC = importlib.util.spec_from_file_location("latest_run_report", MODULE_PATH)
assert SPEC is not None and SPEC.loader is not None
latest_run_report = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(latest_run_report)


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _touch(path: Path, timestamp: float) -> None:
    os.utime(path, (timestamp, timestamp))


def _write_report_bundle(
    root: Path,
    *,
    discovery_started: str,
    discovery_finished: str,
    fetch_started: str,
    fetch_finished: str,
    fetch_source_name: str,
    fetch_source_error: str = "HTTP 500",
    listing_url: str | None = "https://example.com/careers",
    write_discovery: bool = True,
) -> None:
    discovery_report_path = root / "source-discovery-report.json"
    fetch_report_path = root / "jobs-fetch-report.json"
    jobs_unified_path = root / "jobs-unified.json"
    parser_queue_path = root / "jobs-parser-regression-queue.json"
    browser_queue_path = root / "jobs-browser-fallback-queue.json"

    if write_discovery:
        _write_json(
            discovery_report_path,
            {
                "startedAt": discovery_started,
                "finishedAt": discovery_finished,
                "summary": {
                    "queuedCandidateCount": 3,
                    "failedProbeCount": 1,
                    "probeMissCount": 0,
                    "discoverableButDeferredCount": 2,
                    "lossAccounting": {"queueFiltered": 1},
                },
                "runtime": {
                    "totalDurationMs": 5000,
                    "stageTop": [{"stage": "probe", "durationMs": 2500}],
                    "adapterTimings": [{"adapter": "static", "durationMs": 3000, "queuedCount": 2}],
                },
                "failures": [
                    {
                        "name": "Bad Studio",
                        "adapter": "static",
                        "stage": "probe_failed",
                        "dropStage": "probe_failed",
                        "error": "timeout",
                    }
                ],
                "topFailures": [{"key": "static:bad.example", "count": 1}],
            },
        )
    _write_json(
        fetch_report_path,
        {
            "startedAt": fetch_started,
            "finishedAt": fetch_finished,
            "summary": {"successfulSources": 1, "failedSources": 1, "outputCount": 2},
            "runtime": {
                "staticDomainGateWaitMs": 2100,
                "staticDetailBatchCount": 7,
                "staticAdaptiveStops": 2,
                "staticListingTimeoutStops": 3,
                "staticListingBrowserFallbacks": 4,
                "timingSummary": {
                    "totalDurationMs": 10000,
                    "wallClockDurationMs": 12000,
                    "stageTop": [{"stage": "fetch", "durationMs": 7000}],
                    "slowestAdapters": [
                        {"adapter": "static", "durationMs": 8000, "sourceCount": 2}
                    ],
                    "highCostLowYieldSources": [
                        {
                            "name": fetch_source_name,
                            "adapter": "static",
                            "durationMs": 9000,
                            "keptCount": 0,
                        }
                    ],
                    "detailHeavySources": [],
                },
            },
            "sources": [
                {
                    "name": fetch_source_name,
                    "adapter": "static",
                    "status": "ok",
                    "failureBucket": "site_changed",
                    "listingUrl": listing_url,
                    "keptCount": 0,
                    "fetchedCount": 10,
                    "durationMs": 1234,
                    "details": [],
                },
                {
                    "name": "Bad Source",
                    "adapter": "static",
                    "status": "error",
                    "keptCount": 0,
                    "fetchedCount": 7,
                    "durationMs": 3456,
                    "error": fetch_source_error,
                },
            ],
            "outputs": {"report": str(fetch_report_path)},
        },
    )
    _write_json(jobs_unified_path, [{"id": 1}, {"id": 2}])
    _write_json(
        parser_queue_path,
        [
            {
                "source": fetch_source_name,
                "oldUrl": listing_url or "",
                "lastStatus": "ok",
                "listingFingerprintChanged": False,
                "adapter": "static",
            }
        ]
        if listing_url
        else [],
    )
    _write_json(browser_queue_path, [])


def test_latest_run_report_picks_newest_report_paths(tmp_path: Path) -> None:
    older_root = tmp_path / "data"
    newer_root = tmp_path / "_out" / "runs" / "20260328_120000"
    _write_report_bundle(
        older_root,
        discovery_started="2026-03-28T10:00:00+00:00",
        discovery_finished="2026-03-28T10:05:00+00:00",
        fetch_started="2026-03-28T10:06:00+00:00",
        fetch_finished="2026-03-28T10:10:00+00:00",
        fetch_source_name="Older Source",
        listing_url="https://older.example.com/careers",
    )
    _write_report_bundle(
        newer_root,
        discovery_started="2026-03-28T11:00:00+00:00",
        discovery_finished="2026-03-28T11:05:00+00:00",
        fetch_started="2026-03-28T11:06:00+00:00",
        fetch_finished="2026-03-28T11:10:00+00:00",
        fetch_source_name="Newer Source",
        listing_url="https://newer.example.com/careers",
    )
    for offset, path in enumerate(
        [
            older_root / "source-discovery-report.json",
            older_root / "jobs-fetch-report.json",
            newer_root / "source-discovery-report.json",
            newer_root / "jobs-fetch-report.json",
        ],
        start=1,
    ):
        _touch(path, 1_000_000.0 + float(offset))

    summary = latest_run_report.build_latest_run_summary(repo_root=tmp_path)

    assert Path(summary["paths"]["discoveryReport"]).parent == newer_root
    assert Path(summary["paths"]["fetchReport"]).parent == newer_root


def test_latest_run_report_summary_includes_key_metrics_and_queue_preview(tmp_path: Path) -> None:
    root = tmp_path / "data"
    _write_report_bundle(
        root,
        discovery_started="2026-03-28T12:00:00+00:00",
        discovery_finished="2026-03-28T12:05:00+00:00",
        fetch_started="2026-03-28T12:06:00+00:00",
        fetch_finished="2026-03-28T12:10:00+00:00",
        fetch_source_name="KoolHaus Games inc.",
        listing_url="https://example.com/careers",
    )

    summary = latest_run_report.build_latest_run_summary(repo_root=tmp_path, limit=3)
    text = latest_run_report.render_text_summary(summary)

    assert (
        int((summary.get("report") or {}).get("fetch", {}).get("siteChangedDiagnosedCount") or 0)
        == 1
    )
    assert (
        int((summary.get("report") or {}).get("fetch", {}).get("parserRegressionQueueCount") or 0)
        == 1
    )
    assert (
        int(
            (summary.get("report") or {}).get("fetch", {}).get("siteChangedMissingOldUrlCount") or 0
        )
        == 0
    )
    assert "site_changed diagnosed: 1" in text
    assert "parser regression queue: 1" in text
    assert "Static domain-gate wait: 2100 ms" in text
    assert "Static detail batches: 7" in text
    assert "Static adaptive stops: 2" in text
    assert "Static listing timeout stops: 3" in text
    assert "Static listing browser fallbacks: 4" in text
    assert "oldUrl=https://example.com/careers" in text
    assert "Top Fetch Failures" in text
    assert "Top Discovery Failures" in text


def test_latest_run_report_supports_fetch_only_report_roots(tmp_path: Path) -> None:
    repo_root = tmp_path / "ship"
    root = repo_root / "data"
    _write_report_bundle(
        root,
        discovery_started="2026-03-28T12:00:00+00:00",
        discovery_finished="2026-03-28T12:05:00+00:00",
        fetch_started="2026-03-28T12:06:00+00:00",
        fetch_finished="2026-03-28T12:10:00+00:00",
        fetch_source_name="Fetch Only Source",
        listing_url="https://example.com/fetch-only",
        write_discovery=False,
    )

    summary = latest_run_report.build_latest_run_summary(repo_root=repo_root, limit=2)
    text = latest_run_report.render_text_summary(summary)

    assert summary["paths"]["discoveryReport"] == ""
    assert int((summary.get("report") or {}).get("fetch", {}).get("outputCount") or 0) == 2
    assert (
        int((summary.get("report") or {}).get("discovery", {}).get("queuedCandidateCount") or 0)
        == 0
    )
    assert "Discovery report: (missing)" in text
    assert "Fetch report:" in text


def test_latest_run_report_fails_cleanly_without_fetch_report(tmp_path: Path) -> None:
    discovery_report_path = tmp_path / "data" / "source-discovery-report.json"
    _write_json(
        discovery_report_path,
        {
            "startedAt": "2026-03-28T12:00:00+00:00",
            "finishedAt": "2026-03-28T12:05:00+00:00",
            "summary": {},
            "runtime": {},
            "failures": [],
        },
    )

    with pytest.raises(FileNotFoundError, match="jobs-fetch-report.json"):
        latest_run_report.build_latest_run_summary(repo_root=tmp_path)


def test_latest_run_report_main_reports_expected_artifact_failures(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    exit_code = latest_run_report.main(["--repo-root", str(tmp_path)])
    captured = capsys.readouterr()

    assert exit_code == 1
    assert "jobs-fetch-report.json" in captured.err


def test_latest_run_report_main_does_not_hide_programming_failures(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def _raise_unexpected_failure(**_kwargs: object) -> dict[str, object]:
        raise AssertionError("unexpected latest-run report bug")

    monkeypatch.setattr(
        latest_run_report,
        "build_latest_run_summary",
        _raise_unexpected_failure,
    )

    with pytest.raises(AssertionError, match="unexpected latest-run report bug"):
        latest_run_report.main(["--repo-root", str(tmp_path)])


def test_latest_run_report_json_output(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = tmp_path / "data"
    _write_report_bundle(
        root,
        discovery_started="2026-03-28T12:00:00+00:00",
        discovery_finished="2026-03-28T12:05:00+00:00",
        fetch_started="2026-03-28T12:06:00+00:00",
        fetch_finished="2026-03-28T12:10:00+00:00",
        fetch_source_name="KoolHaus Games inc.",
        listing_url="https://example.com/careers",
    )

    exit_code = latest_run_report.main(["--repo-root", str(tmp_path), "--json", "--limit", "1"])
    captured = capsys.readouterr()

    assert exit_code == 0
    payload = json.loads(captured.out)
    assert (
        int((payload.get("report") or {}).get("fetch", {}).get("parserRegressionQueueCount") or 0)
        == 1
    )
    assert (payload.get("parserRegressionQueuePreview") or [{}])[0].get(
        "oldUrl"
    ) == "https://example.com/careers"
