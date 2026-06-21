"""Tests for jobs fetcher pipeline error cases."""

import json
from pathlib import Path

from src import jobs_fetcher as jf
from src.shared.json_io import read_json
from tests.helpers.job_fixtures import _fixture_json
from tests.helpers.temp_paths import workspace_tmpdir


def test_run_pipeline_preserves_provider_diagnostics_when_loader_raises() -> None:
    source_name = "greenhouse_boards"
    jf.SOURCE_DIAGNOSTICS.pop(source_name, None)

    def provider_family_loader(**_: object):
        jf.SOURCE_DIAGNOSTICS[source_name] = {
            "adapter": "greenhouse",
            "studio": "multiple",
            "details": [
                {
                    "name": "Board A",
                    "studio": "Board A",
                    "adapter": "greenhouse",
                    "status": "error",
                    "error": "HTTP 401",
                    "fetchedCount": 0,
                    "keptCount": 0,
                    "migrationSourceIdentity": "static:board-a",
                }
            ],
            "partialErrors": ["greenhouse:Board A: HTTP 401"],
        }
        raise RuntimeError("aggregate provider failure")

    with workspace_tmpdir("jobs-fetcher-provider-error-diagnostics") as tmp:
        out = Path(tmp)
        report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[(source_name, provider_family_loader)],
            show_progress=False,
            force_refresh_all=True,
        )
        row = next(item for item in report["sources"] if item["name"] == source_name)
        assert row["status"] == "error"
        assert "HTTP 401" in row["error"]
        assert row["boardCount"] == 1
        assert row["details"][0]["name"] == "Board A"
        assert row["details"][0]["error"] == "HTTP 401"

        state_payload = read_json(out / "jobs-source-state.json", {})
        sources_state = state_payload.get("sources") or {}
        assert sources_state["Board A"]["lastStatus"] == "error"
        assert sources_state["Board A"]["migrationSourceIdentity"] == "static:board-a"


def test_run_pipeline_excludes_quarantined_source_unless_ignored() -> None:
    calls = {"count": 0}

    def ok_loader(**_: object):
        calls["count"] += 1
        return [
            {
                "title": "Gameplay Engineer",
                "company": "Circuit Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/circuit/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "circuit-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        out = Path(tmp)
        blocked_until = (jf.datetime.now(jf.timezone.utc) + jf.timedelta(hours=2)).isoformat()
        state_payload = {
            "updatedAt": jf.now_iso(),
            "sources": {
                "blocked_source": {
                    "consecutiveFailures": 3,
                    "quarantinedUntilAt": blocked_until,
                }
            },
        }
        (out / "jobs-source-state.json").write_text(json.dumps(state_payload), encoding="utf-8")

        blocked_report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("blocked_source", ok_loader)],
            circuit_breaker_failures=3,
            circuit_breaker_cooldown_minutes=180,
            ignore_circuit_breaker=False,
        )
        blocked_rows = [
            row
            for row in (blocked_report.get("sourceFamilies") or [])
            if row.get("name") == "blocked_source"
        ]
        assert calls["count"] == 0
        assert len(blocked_rows) == 1
        assert str(blocked_rows[0].get("status") or "") == "excluded"
        assert "circuit_breaker_active_until" in str(blocked_rows[0].get("error") or "")

        unblocked_report = jf.run_pipeline(
            output_dir=out,
            source_loaders=[("blocked_source", ok_loader)],
            circuit_breaker_failures=3,
            circuit_breaker_cooldown_minutes=180,
            ignore_circuit_breaker=True,
        )
        unblocked_rows = [
            row
            for row in unblocked_report.get("sources", [])
            if row.get("name") == "blocked_source"
        ]
        assert calls["count"] == 1
        assert len(unblocked_rows) == 1
        assert str(unblocked_rows[0].get("status") or "") == "ok"


def test_pipeline_report_snapshot_contract() -> None:
    def ok_loader(**_: object):
        return [
            {
                "title": "Technical Artist",
                "company": "Snapshot Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/snapshot/ta",
                "sector": "Game",
                "sourceJobId": "snap-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher") as tmp:
        report = jf.run_pipeline(output_dir=Path(tmp), source_loaders=[("ok", ok_loader)])
        snapshot = {
            "schemaVersion": report.get("schemaVersion"),
            "summary": {
                "inputCount": int(report["summary"].get("inputCount") or 0),
                "mergedCount": int(report["summary"].get("mergedCount") or 0),
                "outputCount": int(report["summary"].get("outputCount") or 0),
                "rawFetchedCount": int(report["summary"].get("rawFetchedCount") or 0),
                "uniqueOutputCount": int(report["summary"].get("uniqueOutputCount") or 0),
                "sourceCount": int(report["summary"].get("sourceCount") or 0),
                "successfulSources": int(report["summary"].get("successfulSources") or 0),
                "failedSources": int(report["summary"].get("failedSources") or 0),
                "excludedSources": int(report["summary"].get("excludedSources") or 0),
            },
            "outputs": {
                "hasJson": bool(report.get("outputs", {}).get("json")),
                "hasCsv": bool(report.get("outputs", {}).get("csv")),
                "hasLightJson": bool(report.get("outputs", {}).get("lightJson")),
                "hasChangedFlags": isinstance(report.get("outputs", {}).get("changed"), dict),
            },
            "sources": [
                {
                    "name": str(report["sources"][0].get("name")),
                    "status": str(report["sources"][0].get("status")),
                    "fetchedCount": int(report["sources"][0].get("fetchedCount") or 0),
                    "keptCount": int(report["sources"][0].get("keptCount") or 0),
                }
            ],
        }
        assert snapshot == _fixture_json("jobs_fetch_report_snapshot.json")


def test_run_pipeline_records_wall_clock_timing_summary() -> None:
    def ok_loader(**_: object):
        return [
            {
                "title": "Gameplay Engineer",
                "company": "Timing Studio",
                "city": "Remote",
                "country": "Remote",
                "workType": "Remote",
                "contractType": "Full-time",
                "jobLink": "https://example.com/timing/gameplay-engineer",
                "sector": "Game",
                "sourceJobId": "timing-1",
                "postedAt": "2026-03-01",
            }
        ]

    with workspace_tmpdir("jobs-fetcher-wall-clock") as tmp:
        report = jf.run_pipeline(
            output_dir=Path(tmp), source_loaders=[("timing_source", ok_loader)], show_progress=False
        )
        timing = ((report.get("runtime") or {}).get("timingSummary")) or {}
        assert int(timing.get("wallClockDurationMs") or 0) >= 0
