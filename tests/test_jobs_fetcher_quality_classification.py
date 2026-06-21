"""Tests for jobs fetcher quality classification behavior."""

from src import jobs_fetcher as jf


def test_map_profession_recognizes_focus_synonyms() -> None:
    assert jf.map_profession("Senior Tech Artist") == "technical-artist"
    assert jf.map_profession("Material Artist") == "technical-artist"
    assert jf.map_profession("World Artist") == "environment-artist"
    assert jf.map_profession("Terrain Artist") == "environment-artist"
    assert jf.map_profession("Technical Director") == "technical-director"
    assert jf.map_profession("Associate Technical Director") == "technical-director"
    assert jf.map_profession("Senior Animation TD") == "technical-director"
    assert jf.map_profession("Pipeline TD") == "technical-director"
    assert jf.map_profession("TDengine Programmer") == "engine"


def test_compute_focus_score_prioritizes_target_nl_and_remote() -> None:
    ta_nl = jf.canonicalize_job(
        {
            "title": "Technical Artist",
            "company": "Studio NL",
            "city": "Amsterdam",
            "country": "NL",
            "workType": "Hybrid",
            "contractType": "Full-time",
            "jobLink": "https://example.com/ta-nl",
            "sector": "Game",
            "postedAt": "2026-03-01",
        },
        source="x",
        fetched_at=jf.now_iso(),
    )
    ta_remote = jf.canonicalize_job(
        {
            "title": "Technical Artist",
            "company": "Studio Remote",
            "city": "Remote",
            "country": "Remote",
            "workType": "Remote",
            "contractType": "Full-time",
            "jobLink": "https://example.com/ta-remote",
            "sector": "Game",
            "postedAt": "2026-03-01",
        },
        source="x",
        fetched_at=jf.now_iso(),
    )
    non_target = jf.canonicalize_job(
        {
            "title": "Gameplay Programmer",
            "company": "Studio Other",
            "city": "Amsterdam",
            "country": "NL",
            "workType": "Hybrid",
            "contractType": "Full-time",
            "jobLink": "https://example.com/gameplay",
            "sector": "Game",
            "postedAt": "2026-03-01",
        },
        source="x",
        fetched_at=jf.now_iso(),
    )
    assert ta_nl
    assert ta_remote
    assert non_target
    assert ta_nl.focusScore > ta_remote.focusScore
    assert ta_remote.focusScore > non_target.focusScore


def test_dedup_primary_key_prefers_richer_latest_record() -> None:
    first = jf.canonicalize_job(
        {
            "title": "Gameplay Programmer",
            "company": "Pixel Forge",
            "city": "Amsterdam",
            "country": "NL",
            "workType": "Hybrid",
            "contractType": "Full-time",
            "jobLink": "https://pixelforge.dev/jobs/123?utm_source=x",
            "sector": "Game",
            "postedAt": "2026-01-01",
        },
        source="a",
        fetched_at=jf.now_iso(),
    )
    second = jf.canonicalize_job(
        {
            "title": "Gameplay Programmer",
            "company": "Pixel Forge",
            "city": "Amsterdam",
            "country": "Netherlands",
            "workType": "Hybrid",
            "contractType": "Permanent",
            "jobLink": "https://pixelforge.dev/jobs/123",
            "sector": "Gaming",
            "postedAt": "2026-02-10",
            "sourceJobId": "r-2",
        },
        source="b",
        fetched_at=jf.now_iso(),
    )
    assert first is not None
    assert second is not None
    rows, stats = jf.deduplicate_jobs([first, second])
    assert stats["outputCount"] == 1
    assert int(stats.get("mergedByPrimaryUrl") or 0) == 1
    assert int(stats.get("mergedBySecondaryKey") or 0) == 0
    assert int(stats.get("mergedBySocialKey") or 0) == 0
    assert rows[0].sourceJobId == "r-2"
    assert rows[0].dedupKey.startswith("url:")


def test_canonicalize_job_rejects_linkless_rows_before_dedup() -> None:
    first = jf.canonicalize_job(
        {
            "title": "Technical Artist",
            "company": "Orion Labs",
            "city": "Remote",
            "country": "Remote",
            "workType": "Remote",
            "contractType": "Contract",
            "jobLink": "",
            "sector": "Game",
            "postedAt": "2026-02-01",
        },
        source="a",
        fetched_at=jf.now_iso(),
    )
    second = jf.canonicalize_job(
        {
            "title": "Technical Artist",
            "company": "Orion Labs",
            "city": "Remote",
            "country": "Remote",
            "workType": "Remote",
            "contractType": "Contract",
            "jobLink": "",
            "sector": "Game",
            "postedAt": "2026-02-05",
        },
        source="b",
        fetched_at=jf.now_iso(),
    )
    assert first is None
    assert second is None


def test_canonicalize_job_with_reason_accounts_drop_reasons() -> None:
    dropped_title, reason_title = jf.canonicalize_job_with_reason(
        {"company": "Studio A", "jobLink": "https://example.com/jobs/1"},
        source="x",
        fetched_at=jf.now_iso(),
    )
    dropped_company, reason_company = jf.canonicalize_job_with_reason(
        {"title": "Gameplay Engineer", "jobLink": "https://example.com/jobs/2"},
        source="x",
        fetched_at=jf.now_iso(),
    )
    dropped_payload, reason_payload = jf.canonicalize_job_with_reason(
        "not-a-dict",
        source="x",
        fetched_at=jf.now_iso(),
    )
    assert dropped_title is None
    assert dropped_company is None
    assert dropped_payload is None
    assert reason_title == "missing_title"
    assert reason_company == "missing_company"
    assert reason_payload == "invalid_payload"


def test_canonicalize_job_with_reason_requires_job_link() -> None:
    dropped_link, reason_link = jf.canonicalize_job_with_reason(
        {"title": "Gameplay Engineer", "company": "Studio A", "jobLink": ""},
        source="x",
        fetched_at=jf.now_iso(),
    )
    assert dropped_link is None
    assert reason_link == "missing_job_link"
