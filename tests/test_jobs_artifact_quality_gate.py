from __future__ import annotations

from pathlib import Path

import pytest

from scripts.jobs_artifact_quality_gate import analyze_jobs_artifact


def _write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    lines = [
        "id,title,company,city,country,workType,contractType,jobLink,sector,profession,companyType,description,source,sourceJobId,fetchedAt,postedAt,status,firstSeenAt,lastSeenAt,removedAt,lifecycleEvent,lifecycleReason,dedupKey,qualityScore,focusScore,sourceBundleCount,sourceBundle,locations,locationSummary"
    ]
    for row in rows:
        values = [str(row.get(key, "")) for key in lines[0].split(",")]
        escaped = [
            '"' + value.replace('"', '""') + '"' if "," in value or '"' in value else value
            for value in values
        ]
        lines.append(",".join(escaped))
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


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


def test_jobs_artifact_quality_gate_does_not_block_real_animator_title(tmp_path: Path) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Animator",
                "company": "Example Games",
                "jobLink": "https://example.com/jobs/1",
                "source": "google_sheets",
                "sourceJobId": "sheet-1",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "pass"
    assert report["counts"]["exactCategoryTitleLeaks"] == 0


def test_jobs_artifact_quality_gate_does_not_block_exact_role_shaped_title(tmp_path: Path) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Graphics Engineer",
                "company": "PlayStation Global",
                "jobLink": (
                    "https://job-boards.greenhouse.io/sonyinteractiveentertainmentglobal/jobs/5837065004"
                ),
                "source": "greenhouse_boards",
                "sourceJobId": "greenhouse:sonyinteractiveentertainmentglobal:5837065004",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "pass"
    assert report["counts"]["exactCategoryTitleLeaks"] == 0


def test_jobs_artifact_quality_gate_blocks_static_exact_category_title_with_container_evidence(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "VFX",
                "company": "Digital Confectioners",
                "jobLink": "https://www.digitalconfectioners.com/jobs/vfx",
                "source": "scrapy_static_sources",
                "sourceJobId": "static:vfx",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "blocked"
    assert report["counts"]["exactCategoryTitleLeaks"] == 1


def test_jobs_artifact_quality_gate_does_not_block_exact_category_term_without_static_or_sheet_evidence(
    tmp_path: Path,
) -> None:
    csv_path = tmp_path / "jobs-unified.csv"
    _write_csv(
        csv_path,
        [
            {
                "id": "1",
                "title": "Design",
                "company": "Example Provider",
                "jobLink": "https://job-boards.greenhouse.io/example/jobs/123",
                "source": "greenhouse_boards",
                "sourceJobId": "greenhouse:example:123",
            }
        ],
    )

    report = analyze_jobs_artifact(str(csv_path))

    assert report["status"] == "pass"
    assert report["counts"]["exactCategoryTitleLeaks"] == 0
