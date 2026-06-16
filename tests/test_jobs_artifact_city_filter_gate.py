from __future__ import annotations

from pathlib import Path

from scripts.jobs_artifact_quality_gate import analyze_jobs_artifact
from tests.test_jobs_artifact_quality_gate import _write_csv


def test_jobs_artifact_quality_gate_blocks_city_filter_pollution(tmp_path: Path) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Technical Artist",
                "company": "Example Games",
                "city": "00:00",
                "country": "Unknown",
                "locations": '[{"city":"sqs","country":"Unknown"},{"city":"Development","country":""}]',
                "locationSummary": "For all applicants",
                "jobLink": "https://example.com/jobs/1",
                "source": "static_source::static:listing_url:https://example.com/careers",
                "sourceJobId": "static:1",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "blocked"
    assert report["ok"] is False
    assert report["counts"]["cityFilterCandidateLeaks"] == 1
    example = report["blocked"]["cityFilterCandidateExamples"][0]
    assert {hit["value"] for hit in example["cityFilterHits"]} >= {
        "00:00",
        "sqs",
        "Development",
        "For all applicants",
    }


def test_jobs_artifact_quality_gate_preserves_real_city_false_positives(tmp_path: Path) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Solution Architect",
                "company": "Salesforce",
                "city": "McLean",
                "country": "US",
                "locations": '[{"city":"Newport News","country":"US"},{"city":"Ciudad Juárez","country":"Mexico"}]',
                "locationSummary": "McLean, US | Newport News, US | Ciudad Juárez, Mexico",
                "jobLink": "https://example.com/jobs/1",
                "source": "google_sheets",
                "sourceJobId": "sheet-1",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "pass"
    assert report["counts"]["cityFilterCandidateLeaks"] == 0


def test_jobs_artifact_quality_gate_warns_on_unsafe_compound_city_candidates(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Gameplay Engineer",
                "company": "Example Games",
                "city": "New York or London",
                "country": "US",
                "locations": '[{"city":"Tokyo or Fukuoka","country":"Japan"},{"city":"New York or London","country":"US"}]',
                "locationSummary": "New York or London, US",
                "jobLink": "https://example.com/jobs/1",
                "source": "greenhouse",
                "sourceJobId": "greenhouse:1",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "warning"
    assert report["ok"] is True
    assert report["counts"]["cityFilterCandidateLeaks"] == 0
    assert report["counts"]["cityFilterCompoundWarnings"] == 1
    example = report["warnings"]["cityFilterCompoundExamples"][0]
    assert {hit["value"] for hit in example["cityFilterHits"]} == {"New York or London"}
