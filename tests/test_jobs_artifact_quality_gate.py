from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.jobs_artifact_quality_gate import analyze_jobs_artifact


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.write_text(json.dumps(rows), encoding="utf-8")


@pytest.mark.parametrize(
    "title", ["Art", "Design", "Animation", "Product-management", "Technical-art"]
)
def test_jobs_artifact_quality_gate_blocks_exact_category_titles(
    tmp_path: Path, title: str
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": title,
                "company": "Example Games",
                "jobLink": "https://example.com/jobs/1",
                "source": "google_sheets",
                "sourceJobId": "sheet-1",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "blocked"
    assert report["counts"]["exactCategoryTitleLeaks"] == 1
    assert report["blocked"]["exactCategoryTitleExamples"][0]["title"] == title


def test_jobs_artifact_quality_gate_blocks_unknown_company_with_structured_link_evidence(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Technical Director",
                "company": "Unknown company",
                "jobLink": "https://jobs.smartrecruiters.com/Ubisoft2/744000108777145-role",
                "source": "google_sheets",
                "sourceJobId": "sheet-1",
                "sourceBundle": '[{"source":"google_sheets","jobLink":"https://gracklehq.com/rd/372393"}]',
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "blocked"
    assert report["counts"]["unknownCompanyStrongEvidenceLeaks"] == 1
    assert report["blocked"]["unknownCompanyExamples"][0]["resolvedCompany"] == "Ubisoft2"


def test_jobs_artifact_quality_gate_blocks_unknown_company_with_linkedin_detail_evidence(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Senior Render Artist",
                "company": "Unknown company",
                "jobLink": "https://es.linkedin.com/jobs/view/senior-render-artist-at-scopely-4371673234",
                "source": "google_sheets",
                "sourceJobId": "sheet-1",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "blocked"
    assert report["counts"]["unknownCompanyStrongEvidenceLeaks"] == 1
    assert report["blocked"]["unknownCompanyExamples"][0]["resolvedCompany"] == "Scopely"


def test_jobs_artifact_quality_gate_blocks_unknown_company_with_first_party_host_evidence(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Technical Artist",
                "company": "Unknown company",
                "jobLink": "https://techland.net/job-offers/technical-artist-41",
                "source": "google_sheets",
                "sourceJobId": "sheet-1",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "blocked"
    assert report["counts"]["unknownCompanyStrongEvidenceLeaks"] == 1
    assert report["blocked"]["unknownCompanyExamples"][0]["resolvedCompany"] == "Techland"


def test_jobs_artifact_quality_gate_warns_on_unknown_company_without_strong_evidence(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Senior UX Designer",
                "company": "Unknown company",
                "jobLink": "https://www.linkedin.com/jobs/view/123",
                "source": "google_sheets",
                "sourceJobId": "sheet-1",
                "sourceBundle": '[{"source":"google_sheets","jobLink":"https://gracklehq.com/rd/372393"}]',
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "warning"
    assert report["ok"] is True
    assert report["counts"]["unknownCompanyStrongEvidenceLeaks"] == 0
    assert report["counts"]["unknownCompanyWeakEvidenceWarnings"] == 1
    assert report["warnings"]["unknownCompanyHostCounts"]["www.linkedin.com"] == 1


@pytest.mark.parametrize(
    (
        "title",
        "company",
        "job_link",
        "source",
        "source_job_id",
        "expected_status",
        "counter_key",
        "expected_count",
    ),
    [
        pytest.param(
            "Animator",
            "Example Games",
            "https://example.com/jobs/1",
            "google_sheets",
            "sheet-1",
            "pass",
            "exactCategoryTitleLeaks",
            0,
            id="real-animator-title",
        ),
        pytest.param(
            "Graphics Engineer",
            "PlayStation Global",
            "https://job-boards.greenhouse.io/sonyinteractiveentertainmentglobal/jobs/5837065004",
            "greenhouse_boards",
            "greenhouse:sonyinteractiveentertainmentglobal:5837065004",
            "pass",
            "exactCategoryTitleLeaks",
            0,
            id="exact-role-shaped-title",
        ),
        pytest.param(
            "VFX",
            "Digital Confectioners",
            "https://www.digitalconfectioners.com/jobs/vfx",
            "scrapy_static_sources",
            "static:vfx",
            "blocked",
            "exactCategoryTitleLeaks",
            1,
            id="static-exact-category-title-with-container-evidence",
        ),
        pytest.param(
            "Jobs",
            "Example Games",
            "https://example.com/jobs",
            "google_sheets",
            "sheet-2",
            "blocked",
            "staticContainerTitleLeaks",
            1,
            id="sheet-container-word-with-container-url",
        ),
        pytest.param(
            "Creative Producer",
            "Example Games",
            "https://example.com/careers/creative-producer",
            "static_source::static:listing_url:https://example.com/careers",
            "static:creative-producer",
            "pass",
            "staticContainerTitleLeaks",
            0,
            id="real-container-word-role",
        ),
        pytest.param(
            "Design",
            "Example Provider",
            "https://job-boards.greenhouse.io/example/jobs/123",
            "greenhouse_boards",
            "greenhouse:example:123",
            "pass",
            "exactCategoryTitleLeaks",
            0,
            id="exact-category-term-without-static-or-sheet-evidence",
        ),
    ],
)
def test_jobs_artifact_quality_gate_title_evidence_outcomes(
    tmp_path: Path,
    title: str,
    company: str,
    job_link: str,
    source: str,
    source_job_id: str,
    expected_status: str,
    counter_key: str,
    expected_count: int,
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": title,
                "company": company,
                "jobLink": job_link,
                "source": source,
                "sourceJobId": source_job_id,
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == expected_status
    assert report["counts"][counter_key] == expected_count


@pytest.mark.parametrize(
    ("title", "job_link"),
    [
        ("Creative", "https://example.com/careers/creative"),
        ("Analytics", "https://example.com/careers/analytics"),
        ("3D", "https://example.com/careers/function-3d"),
        ("9", "https://example.com/careers?page-is-9"),
        ("한국어 ( Koreanisch )", "https://example.com/careers?lang=ko"),
        ("English ( Inglese )", "https://example.com/careers"),
        ("en", "https://example.com/career"),
        ("All categories : new", "https://example.com/vacancies"),
        ("...", "https://example.com/vacancies/filter/page-is-6/apply"),
        ("Finance & Legal", "https://example.com/careers/finance-legal"),
        ("Data Ai", "https://example.com/careers/data-ai"),
    ],
)
def test_jobs_artifact_quality_gate_blocks_static_container_artifact_titles(
    tmp_path: Path, title: str, job_link: str
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": title,
                "company": "Example Games",
                "jobLink": job_link,
                "source": "static_source::static:listing_url:https://example.com/careers",
                "sourceJobId": "static:container",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "blocked"
    assert report["counts"]["staticContainerTitleLeaks"] == 1
    assert report["blocked"]["staticContainerTitleExamples"][0]["title"] == title


def test_jobs_artifact_quality_gate_does_not_block_sheet_container_word_on_detail_url(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Creative",
                "company": "Vidsy",
                "city": "London",
                "country": "United Kingdom",
                "jobLink": "https://jobs.lever.co/vidsy/37b557aa-3225-4f05-b068-77440a9f60d7",
                "source": "google_sheets",
                "sourceJobId": "sheet-16452",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "pass"
    assert report["counts"]["staticContainerTitleLeaks"] == 0


def test_jobs_artifact_quality_gate_blocks_container_artifact_from_static_bundle(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "All categories : new",
                "company": "Example Games",
                "jobLink": "https://example.com/jobs",
                "source": "provider_feed",
                "sourceJobId": "provider:1",
                "sourceBundle": '[{"source":"static_source::example","jobLink":"https://example.com/careers?function-all"}]',
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "blocked"
    assert report["counts"]["staticContainerTitleLeaks"] == 1
