"""Regression guards for false-positive cross-board merges."""

from __future__ import annotations

from typing import cast

from src import jobs_fetcher as jf
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
    work_type: str = "Onsite",
    contract_type: str = "Unknown",
    adapter: str = "",
) -> CanonicalJob:
    job = jf.canonicalize_job(
        {
            "title": title,
            "company": company,
            "city": city,
            "country": country,
            "locations": [{"city": city, "country": country}] if city or country else [],
            "jobLink": link,
            "sector": "Game",
            "sourceJobId": source_job_id,
            "adapter": adapter,
            "workType": work_type,
            "contractType": contract_type,
        },
        source=source,
        fetched_at=_FETCHED_AT,
    )
    assert job is not None
    return cast(CanonicalJob, job)


def test_epic_distinct_requisitions_do_not_merge_on_secondary_key() -> None:
    first = _job(
        title="Level Design Intern",
        company="Epic Games",
        city="Cary",
        country="US",
        link="https://epicgames.com/careers/jobs/6148799004?gh_jid=6148799004",
        source="epic_games_careers",
        source_job_id="epic:6148799004",
    )
    second = _job(
        title="Level Design Intern",
        company="Epic Games",
        city="Cary",
        country="US",
        link="https://epicgames.com/careers/jobs/6173450004?gh_jid=6173450004",
        source="epic_games_careers",
        source_job_id="epic:6173450004",
    )

    rows, stats = jf.deduplicate_jobs([first, second])

    assert len(rows) == 2
    assert stats["mergedCount"] == 0
    assert stats["mergedBySecondaryKey"] == 0


def test_riot_staff_specialisms_do_not_merge_on_sparse_identity() -> None:
    gameplay = _job(
        title="Staff Software Engineer, Gameplay - Unpublished R&D Product",
        company="Riot Games",
        city="",
        country="",
        link="https://www.riotgames.com/en/j/7922898",
        source="static_source::static:listing_url:https://www.riotgames.com/en/work-with-us",
        source_job_id=(
            "static:static:listing_url:https://www.riotgames.com/en/work-with-us:76951ad756"
        ),
        adapter="static",
    )
    tools = _job(
        title="Staff Software Engineer, Tools (Full-Stack) - Central Creative",
        company="Riot Games",
        city="Los Angeles",
        country="US",
        link="https://www.riotgames.com/en/j/8221973",
        source="google_sheets_1er2oaxo",
        source_job_id="sheet-2614",
    )

    rows, stats = jf.deduplicate_jobs([gameplay, tools])

    assert len(rows) == 2
    assert stats["mergedCount"] == 0
    assert stats["mergedBySparseIdentity"] == 0


def test_riot_blank_location_does_not_merge_distinct_producer_requisitions() -> None:
    guangzhou = _job(
        title="Senior Game Producer 高级游戏项目管理经理 - Unpublished R&D Product",
        company="Riot Games",
        city="",
        country="",
        link="https://www.riotgames.com/en/j/7704828",
        source="static_source::static:listing_url:https://www.riotgames.com/en/work-with-us",
        source_job_id=(
            "static:static:listing_url:https://www.riotgames.com/en/work-with-us:0025c7ff16"
        ),
        work_type="Onsite",
        adapter="static",
    )
    shanghai = _job(
        title="Senior Game Producer 高级游戏项目管理经理 - Unpublished R&D Product",
        company="Riot Games",
        city="Shanghai",
        country="China",
        link="https://www.riotgames.com/en/j/8279501",
        source="google_sheets_1er2oaxo",
        source_job_id="sheet-802",
        work_type="Hybrid",
    )

    rows, stats = jf.deduplicate_jobs([guangzhou, shanghai])

    assert len(rows) == 2
    assert stats["mergedCount"] == 0
    assert stats["mergedBySparseIdentity"] == 0


def test_scopely_regional_requisitions_do_not_merge() -> None:
    canada = _job(
        title="Live Ops Manager - MARVEL Strike Force",
        company="Scopely",
        city="Mexico City",
        country="Mexico",
        link="https://job-boards.greenhouse.io/scopely/jobs/5430224008?gh_jid=5430224008",
        source="google_sheets_1er2oaxo",
        source_job_id="sheet-3154",
        work_type="Hybrid",
    )
    united_states = _job(
        title="Live Ops Manager - MARVEL Strike Force",
        company="Scopely",
        city="Mexico City",
        country="Mexico",
        link="https://job-boards.greenhouse.io/scopely/jobs/5471621008?gh_jid=5471621008",
        source="google_sheets_1er2oaxo",
        source_job_id="sheet-303",
        work_type="Remote",
    )

    rows, stats = jf.deduplicate_jobs([canada, united_states])

    assert len(rows) == 2
    assert stats["mergedCount"] == 0
    assert stats["mergedBySecondaryKey"] == 0


def test_epic_same_requisition_variants_still_merge() -> None:
    cary = _job(
        title="Sr. Product Manager (Epic Online Services)",
        company="Epic Games",
        city="Cary",
        country="US",
        link="https://epicgames.com/careers/jobs/6218273004?gh_jid=6218273004",
        source="epic_games_careers",
        source_job_id="epic:6218273004",
    )
    multiple_locations = _job(
        title="Sr. Product Manager (Epic Online Services)",
        company="Epic Games",
        city="",
        country="",
        link="https://epicgames.com/careers/jobs/6218275004?gh_jid=6218275004",
        source="epic_games_careers",
        source_job_id="epic:6218275004",
    )

    rows, stats = jf.deduplicate_jobs([cary, multiple_locations])

    assert len(rows) == 1
    assert stats["mergedCount"] == 1
    assert stats["mergedBySparseIdentity"] == 1
    assert rows[0].sourceBundleCount == 2
    assert {item["sourceJobId"] for item in rows[0].sourceBundle} == {
        cary.sourceJobId,
        multiple_locations.sourceJobId,
    }
