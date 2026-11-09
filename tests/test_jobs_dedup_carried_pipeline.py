import json
from pathlib import Path
from unittest import mock

from src import jobs_fetcher as jf
from src.pipeline_io import write_pipeline_rows_sidecar
from tests.helpers.temp_paths import workspace_tmpdir


def test_seeded_existing_output_marks_dedup_bundle_collisions_as_carried() -> None:
    existing = [
        {
            "title": "Senior Engineer",
            "company": "Studio One",
            "city": "Remote",
            "country": "Remote",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://example.com/jobs/1",
            "sector": "Game",
            "source": "greenhouse:slug:studio-one",
            "sourceJobId": "gh-1",
            "sourceBundleCount": 2,
            "sourceBundle": [
                {
                    "source": "greenhouse:slug:studio-one",
                    "sourceJobId": "gh-1",
                    "jobLink": "https://example.com/jobs/1",
                    "adapter": "greenhouse",
                },
                {
                    "source": "greenhouse:slug:studio-one",
                    "sourceJobId": "gh-1",
                    "jobLink": "https://example.com/jobs/1",
                    "adapter": "greenhouse",
                },
            ],
        }
    ]

    with workspace_tmpdir("jobs-fetcher-carried-dedup-evidence") as tmp:
        out = Path(tmp)
        (out / "jobs-unified.json").write_text(json.dumps(existing), encoding="utf-8")
        write_pipeline_rows_sidecar(out / "jobs-unified.json", existing)
        with mock.patch.dict(
            "os.environ", {"BALUFFO_FETCH_SEED_EXISTING_OUTPUT": "1"}, clear=False
        ):
            report = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("refresh_source", lambda **_: [])],
                show_progress=False,
                preserve_previous_on_empty=False,
                force_refresh_all=True,
            )

        evidence = report["dedupEvidence"]
        assert evidence["currentRunSourceBundleCollisionCount"] == 0
        assert evidence["carriedSourceBundleCollisionCount"] == 1
        assert evidence["dedupAuditGate"]["carriedCollisionLikelyHistoricalCount"] == 1
        assert evidence["dedupAuditGate"]["currentRunHighRiskReviewQueueCount"] == 0


def test_seeded_existing_output_reports_carried_source_bundle_provenance_mismatch() -> None:
    common = {
        "city": "Remote",
        "country": "Remote",
        "workType": "Remote",
        "contractType": "Full-time",
        "sector": "Game",
        "source": "greenhouse:slug:studio-one",
    }
    existing = [
        {
            **common,
            "title": "Level Designer",
            "company": "Studio One",
            "jobLink": "https://example.com/jobs/1",
            "sourceJobId": "gh-1",
            "sourceBundleCount": 1,
            "sourceBundle": [
                {
                    "source": "google_sheets_1er2oaxo",
                    "sourceJobId": "sheet-2",
                    "jobLink": "https://example.com/jobs/2",
                    "adapter": "google_sheets",
                }
            ],
        },
        {
            **common,
            "title": "Senior Engineer",
            "company": "Studio Two",
            "jobLink": "https://example.com/jobs/2",
            "sourceJobId": "gh-2",
            "sourceBundleCount": 0,
            "sourceBundle": [],
        },
    ]

    with workspace_tmpdir("jobs-fetcher-carried-provenance-evidence") as tmp:
        out = Path(tmp)
        (out / "jobs-unified.json").write_text(json.dumps(existing), encoding="utf-8")
        write_pipeline_rows_sidecar(out / "jobs-unified.json", existing)
        with mock.patch.dict(
            "os.environ", {"BALUFFO_FETCH_SEED_EXISTING_OUTPUT": "1"}, clear=False
        ):
            report = jf.run_pipeline(
                output_dir=out,
                source_loaders=[("refresh_source", lambda **_: [])],
                show_progress=False,
                preserve_previous_on_empty=False,
                force_refresh_all=True,
            )

        evidence = report["dedupEvidence"]
        assert evidence["carriedSourceBundleProvenanceMismatchCount"] == 1
        assert evidence["carriedSourceBundleProvenanceMismatchMovableCount"] == 1
        assert (
            evidence["carriedSourceBundleProvenanceMismatchExamples"][0]["targetJobLink"]
            == "https://example.com/jobs/2"
        )
        gate = evidence["dedupAuditGate"]
        assert gate["carriedSourceBundleProvenanceMismatchCount"] == 1
        assert "carried_source_bundle_provenance_mismatch_present" in gate["warnings"]
