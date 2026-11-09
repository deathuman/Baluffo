"""Cross-board duplicate representations that must collapse without loss."""

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
    adapter: str = "static",
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
        },
        source=source,
        fetched_at=_FETCHED_AT,
    )
    assert job is not None
    return cast(CanonicalJob, job)


def _bundle_source_ids(row: CanonicalJob) -> set[str]:
    return {str(item.get("sourceJobId") or "") for item in row.sourceBundle}


def test_kojima_bilingual_board_pair_merges_and_keeps_both_admissions() -> None:
    hrmos = _job(
        title="ライティングアーティスト / Lighting Artist",
        company="KOJIMA PRODUCTIONS",
        city="",
        country="",
        link="https://hrmos.co/pages/2052996596448374784/jobs/0000021",
        source=(
            "static_source::static:listing_url:https://hrmos.co/pages/2052996596448374784/jobs"
        ),
        source_job_id=(
            "static:static:listing_url:https://hrmos.co/pages/2052996596448374784/jobs:7152c2ee3b"
        ),
    )
    official = _job(
        title="Lighting Artist",
        company="Kojimaproductions",
        city="Tokyo",
        country="Japan",
        link="https://www.kojimaproductions.jp/en/lighting-artist",
        source=("static_source::static:listing_url:https://www.kojimaproductions.jp/en/careers"),
        source_job_id=(
            "static:static:listing_url:https://www.kojimaproductions.jp/en/careers:lighting-artist"
        ),
    )

    for inputs in ([hrmos, official], [official, hrmos]):
        rows, stats = jf.deduplicate_jobs(inputs)

        assert len(rows) == 1
        assert stats["mergedCount"] == 1
        assert stats["mergedBySparseIdentity"] == 1
        assert rows[0].sourceBundleCount == 2
        assert _bundle_source_ids(rows[0]) == {hrmos.sourceJobId, official.sourceJobId}


def test_activision_raven_cross_board_pair_merges_and_keeps_both_admissions() -> None:
    activision = _job(
        title="Lead Lighting Artist - Raven - Middleton, WI",
        company="Activision",
        city="Middleton",
        country="US",
        link=(
            "https://careers.activision.com/job/ACPUUSR028052EXTERNAL/"
            "Lead-Lighting-Artist-Raven-Middleton-WI"
        ),
        source="static_source::static:listing_url:https://careers.activision.com/",
        source_job_id="phapp:ACPUUSR028052EXTERNAL",
    )
    raven = _job(
        title="Lead Lighting Artist - Raven - Middleton, WI",
        company="Raven Software",
        city="Middleton",
        country="US",
        link=(
            "https://careers.ravensoftware.com/job/R028052/Lead-Lighting-Artist-Raven-Middleton-WI"
        ),
        source="static_source::static:listing_url:https://careers.ravensoftware.com/",
        source_job_id="phapp:R028052",
    )

    for inputs in ([activision, raven], [raven, activision]):
        rows, stats = jf.deduplicate_jobs(inputs)

        assert len(rows) == 1
        assert stats["mergedCount"] == 1
        assert stats["mergedBySecondaryKey"] == 1
        assert rows[0].company == "Raven Software"
        assert rows[0].sourceBundleCount == 2
        assert _bundle_source_ids(rows[0]) == {activision.sourceJobId, raven.sourceJobId}


def test_eidos_montreal_accent_variant_merges_and_keeps_both_admissions() -> None:
    direct = _job(
        title="Cinematic Lighting Artist",
        company="Eidos Montreal",
        city="Montréal",
        country="CA",
        link="https://jobs.dayforcehcm.com/en-CA/eic/CANDIDATEPORTAL/jobs/2342",
        source=("static_source::static:listing_url:https://www.eidosmontreal.com/careers/"),
        source_job_id=(
            "static:static:listing_url:https://www.eidosmontreal.com/careers/:243ca9a19b"
        ),
    )
    grackle = _job(
        title="Cinematic Lighting Artist",
        company="Eidos Montreal",
        city="Montreal",
        country="CA",
        link="https://gracklehq.com/rd/383180",
        source="gracklehq",
        source_job_id="gracklehq:https://gracklehq.com/rd/383180",
        adapter="gracklehq",
    )

    for inputs in ([direct, grackle], [grackle, direct]):
        rows, stats = jf.deduplicate_jobs(inputs)

        assert len(rows) == 1
        assert stats["mergedCount"] == 1
        assert stats["mergedBySecondaryKey"] == 1
        assert rows[0].sourceBundleCount == 2
        assert _bundle_source_ids(rows[0]) == {direct.sourceJobId, grackle.sourceJobId}
