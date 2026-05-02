import json
from pathlib import Path
from unittest import mock

from src import jobs_fetcher as jf
from tests.helpers.temp_paths import workspace_tmpdir


def test_seeded_existing_output_marks_dedup_bundle_collisions_as_carried() -> None:
    existing = [
        {
            "title": "Product-management",
            "company": "eBay",
            "city": "Remote",
            "country": "Remote",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://example.com/jobs/1",
            "sector": "Game",
            "source": "google_sheets",
            "sourceJobId": "sheet-1",
            "sourceBundleCount": 2,
            "sourceBundle": [
                {
                    "source": "google_sheets",
                    "sourceJobId": "sheet-1",
                    "jobLink": "https://example.com/jobs/1",
                    "adapter": "google_sheets",
                },
                {
                    "source": "google_sheets",
                    "sourceJobId": "sheet-2",
                    "jobLink": "https://example.com/jobs/2",
                    "adapter": "google_sheets",
                },
            ],
        }
    ]

    with workspace_tmpdir("jobs-fetcher-carried-dedup-evidence") as tmp:
        out = Path(tmp)
        (out / "jobs-unified.json").write_text(json.dumps(existing), encoding="utf-8")
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
        assert evidence["dedupAuditGate"]["carriedHighRiskReviewQueueCount"] == 1
        assert evidence["dedupAuditGate"]["currentRunHighRiskReviewQueueCount"] == 0
