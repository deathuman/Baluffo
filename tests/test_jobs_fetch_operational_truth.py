import json
from pathlib import Path

import src.admin_bridge as admin_bridge
from src import jobs_fetcher as jf
from tests.helpers.temp_paths import workspace_tmpdir


def _truth_job(title: str = "Operational Truth Engineer") -> dict[str, object]:
    return {
        "title": title,
        "company": "Truth Studio",
        "city": "Remote",
        "country": "Remote",
        "workType": "Remote",
        "contractType": "Full-time",
        "jobLink": f"https://example.com/jobs/{title.lower().replace(' ', '-')}",
        "sector": "Game",
        "sourceJobId": title.lower().replace(" ", "-"),
        "postedAt": "2026-03-01",
    }


def _assert_completed_fetch_report_truth(report: dict[str, object], output_dir: Path) -> None:
    summary = report.get("summary") or {}
    progress = report.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    sources = report.get("sources") or []
    output_rows = json.loads((output_dir / "jobs-unified.json").read_text(encoding="utf-8"))
    source_count = int(summary.get("sourceCount") or 0)
    failed_sources = int(summary.get("failedSources") or 0)
    output_count = int(summary.get("outputCount") or 0)
    source_health = report.get("sourceHealth") or {}

    assert str(report.get("finishedAt") or "")
    assert progress.get("active") is False
    assert progress.get("phaseKey") == "completed"
    assert progress.get("phaseLabel") == "Completed"
    assert float(progress.get("ratio") or 0) == 1.0
    if source_count:
        assert sources
    assert source_count == len(sources)
    assert failed_sources == sum(1 for row in sources if row.get("status") == "error")
    assert output_count == len(output_rows)
    assert int(counts.get("sourceCount") or 0) == source_count
    assert int(counts.get("failedSources") or 0) == failed_sources
    assert int(counts.get("outputCount") or 0) == output_count
    assert int(source_health.get("totalSources") or 0) == source_count
    assert int(source_health.get("failedSources") or 0) == failed_sources


def test_completed_fetch_report_operational_truth_matches_sources_and_output() -> None:
    def ok_loader(**_: object):
        return [_truth_job()]

    def failing_loader(**_: object):
        raise RuntimeError("boom")

    with workspace_tmpdir("jobs-fetch-operational-truth") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("ok_source", ok_loader), ("failing_source", failing_loader)],
            show_progress=False,
            preserve_previous_on_empty=False,
        )

        _assert_completed_fetch_report_truth(report, out)
        assert int((report.get("summary") or {}).get("sourceCount") or 0) == 2
        assert int((report.get("summary") or {}).get("failedSources") or 0) == 1


def test_completed_fetch_report_zero_output_overwrites_stale_output() -> None:
    def ok_loader(**_: object):
        return [_truth_job("Initial Truth Engineer")]

    def empty_loader(**_: object):
        return []

    with workspace_tmpdir("jobs-fetch-operational-truth-empty") as tmp:
        out = Path(tmp)
        first = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("truth_source", ok_loader)],
            show_progress=False,
            preserve_previous_on_empty=False,
        )
        second = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("truth_source", empty_loader)],
            show_progress=False,
            preserve_previous_on_empty=False,
            force_refresh_all=True,
        )

        assert int((first.get("summary") or {}).get("outputCount") or 0) == 1
        _assert_completed_fetch_report_truth(second, out)
        assert json.loads((out / "jobs-unified.json").read_text(encoding="utf-8")) == []
        assert int((second.get("summary") or {}).get("outputCount") or 0) == 0


def test_completed_fetch_report_includes_operational_skips_not_only_source_nonselections() -> None:
    def ok_loader(**_: object):
        return [_truth_job()]

    with workspace_tmpdir("jobs-fetch-operational-truth-skips") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("ok_source", ok_loader)],
            selection_exclusions=[
                {
                    "name": "cache_skipped_source",
                    "status": "excluded",
                    "adapter": "static",
                    "fetchStrategy": "auto",
                    "studio": "",
                    "fetchedCount": 0,
                    "keptCount": 0,
                    "error": "cache_skip",
                    "exclusionReason": "cache_skip",
                    "durationMs": 0,
                },
                {
                    "name": "non_selected_source",
                    "status": "excluded",
                    "adapter": "static",
                    "fetchStrategy": "auto",
                    "studio": "",
                    "fetchedCount": 0,
                    "keptCount": 0,
                    "error": "only_sources_filter",
                    "exclusionReason": "only_sources_filter",
                    "durationMs": 0,
                },
            ],
            show_progress=False,
            preserve_previous_on_empty=False,
        )

        _assert_completed_fetch_report_truth(report, out)
        source_names = {str(row.get("name") or "") for row in report.get("sources") or []}
        assert "ok_source" in source_names
        assert "cache_skipped_source" in source_names
        assert "non_selected_source" not in source_names
        assert int((report.get("summary") or {}).get("sourceCount") or 0) == 2
        assert int((report.get("summary") or {}).get("excludedSources") or 0) == 1


def test_bridge_normalizer_finished_fetch_report_forces_terminal_progress() -> None:
    payload = admin_bridge.normalize_fetch_report_contract(
        {
            "startedAt": "2026-03-23T16:16:54.905369+00:00",
            "finishedAt": "2026-03-23T16:18:10.053424+00:00",
            "taskProgress": {
                "active": True,
                "phaseKey": "executing_sources",
                "phaseLabel": "Executing sources",
                "mode": "determinate",
                "ratio": 0.25,
                "counts": {"resolvedSources": 1, "sourceCount": 2, "outputCount": 3},
            },
            "summary": {
                "outputCount": 3,
                "failedSources": 1,
                "excludedSources": 0,
                "sourceCount": 2,
                "successfulSources": 1,
            },
            "sources": [
                {"name": "ok_source", "status": "ok", "fetchedCount": 3, "keptCount": 3},
                {"name": "bad_source", "status": "error", "error": "timeout"},
            ],
        }
    )

    progress = payload.get("taskProgress") or {}
    counts = progress.get("counts") or {}
    assert progress.get("active") is False
    assert progress.get("phaseKey") == "completed"
    assert progress.get("phaseLabel") == "Completed"
    assert float(progress.get("ratio") or 0) == 1.0
    assert int(counts.get("sourceCount") or 0) == 2
    assert int(counts.get("failedSources") or 0) == 1
    assert int(counts.get("outputCount") or 0) == 3


def test_bridge_normalizer_unfinished_fetch_report_keeps_active_progress() -> None:
    payload = admin_bridge.normalize_fetch_report_contract(
        {
            "startedAt": "2026-03-23T16:16:54.905369+00:00",
            "finishedAt": "",
            "taskProgress": {
                "active": True,
                "phaseKey": "executing_sources",
                "phaseLabel": "Executing sources",
                "mode": "determinate",
                "ratio": 0.25,
                "counts": {"resolvedSources": 1, "sourceCount": 2, "outputCount": 3},
            },
            "summary": {"outputCount": 3, "failedSources": 0, "sourceCount": 2},
        }
    )

    progress = payload.get("taskProgress") or {}
    assert progress.get("active") is True
    assert progress.get("phaseKey") == "executing_sources"
    assert progress.get("phaseLabel") == "Executing sources"
