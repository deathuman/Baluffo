"""Provider identity from Epic and Greenhouse job-detail links."""

from __future__ import annotations

from typing import cast

from src import jobs_fetcher as jf
from src.jobs.dedup_identity import _provider_job_identities
from src.jobs.models import CanonicalJob

_FETCHED_AT = "2026-11-09T00:00:00Z"


def _job(
    *,
    title: str,
    company: str,
    city: str,
    country: str,
    link: str,
    source: str,
    source_job_id: str,
) -> CanonicalJob:
    job = jf.canonicalize_job(
        {
            "title": title,
            "company": company,
            "city": city,
            "country": country,
            "locations": [{"city": city, "country": country}],
            "jobLink": link,
            "sector": "Game",
            "sourceJobId": source_job_id,
            "workType": "Onsite",
            "contractType": "Unknown",
        },
        source=source,
        fetched_at=_FETCHED_AT,
    )
    assert job is not None
    return cast(CanonicalJob, job)


def test_epic_detail_link_yields_provider_identity() -> None:
    assert _provider_job_identities(
        {
            "source": "google_sheets_1er2oaxo",
            "sourceJobId": "sheet-1",
            "jobLink": (
                "https://store.epicgames.com/en-US/careers/jobs/6148799004?gh_jid=6148799004"
            ),
        }
    ) == frozenset({"epic:6148799004"})
    assert _provider_job_identities(
        {
            "source": "epic_games_careers",
            "sourceJobId": "epic:6148799004",
            "jobLink": "",
        }
    ) == frozenset({"epic:6148799004"})


def test_greenhouse_detail_link_hosts_share_provider_identity() -> None:
    expected = frozenset({"greenhouse:studio-one:123"})
    for host in (
        "boards.greenhouse.io",
        "boards.eu.greenhouse.io",
        "job-boards.greenhouse.io",
        "job-boards.eu.greenhouse.io",
    ):
        assert (
            _provider_job_identities(
                {
                    "source": "google_sheets_1er2oaxo",
                    "sourceJobId": "sheet-1",
                    "jobLink": f"https://{host}/studio-one/jobs/123",
                }
            )
            == expected
        )
    assert (
        _provider_job_identities(
            {
                "source": "greenhouse_boards",
                "sourceJobId": "greenhouse:studio-one:123",
                "jobLink": "",
            }
        )
        == expected
    )


def test_greenhouse_board_page_is_not_a_provider_job_identity() -> None:
    assert not _provider_job_identities({"jobLink": "https://job-boards.greenhouse.io/studio-one"})


def test_distinct_greenhouse_links_block_secondary_merge_without_provider_source() -> None:
    first = _job(
        title="Live Ops Manager - MARVEL Strike Force",
        company="Scopely",
        city="Mexico City",
        country="Mexico",
        link="https://job-boards.greenhouse.io/scopely/jobs/5430224008?gh_jid=5430224008",
        source="google_sheets_1er2oaxo",
        source_job_id="sheet-3154",
    )
    second = _job(
        title="Live Ops Manager - MARVEL Strike Force",
        company="Scopely",
        city="Mexico City",
        country="Mexico",
        link="https://job-boards.greenhouse.io/scopely/jobs/5471621008?gh_jid=5471621008",
        source="google_sheets_1er2oaxo",
        source_job_id="sheet-303",
    )

    rows, stats = jf.deduplicate_jobs([first, second])

    assert len(rows) == 2
    assert stats["mergedCount"] == 0
    assert stats["mergedBySecondaryKey"] == 0


def test_distinct_epic_links_block_secondary_merge_without_epic_source() -> None:
    first = _job(
        title="Level Design Intern",
        company="Epic Games",
        city="Cary",
        country="US",
        link="https://epicgames.com/careers/jobs/6148799004?gh_jid=6148799004",
        source="google_sheets_1er2oaxo",
        source_job_id="sheet-1",
    )
    second = _job(
        title="Level Design Intern",
        company="Epic Games",
        city="Cary",
        country="US",
        link="https://epicgames.com/careers/jobs/6173450004?gh_jid=6173450004",
        source="google_sheets_1er2oaxo",
        source_job_id="sheet-2",
    )

    rows, stats = jf.deduplicate_jobs([first, second])

    assert len(rows) == 2
    assert stats["mergedCount"] == 0
    assert stats["mergedBySecondaryKey"] == 0
