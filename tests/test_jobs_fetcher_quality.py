# ruff: noqa: F403,F405
from tests.jobs_fetcher_helpers import *

patch_jobs_fetcher_aliases()


def test_public_text_sanitizer_cleans_html_contaminated_fields() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": '<div class="title">Technical Artist</div>',
            "company": "Kojimaproductions",
            "city": '<div class="location">Tokyo',
            "country": "Japan</div>",
            "contractType": "<span>Full-time</span>",
            "jobLink": "https://www.kojimaproductions.jp/en/technical-artist",
            "sector": "<div>Game</div>",
        },
        source="static_source::kojima",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["title"] == "Technical Artist"
    assert payload["city"] == "Tokyo"
    assert payload["country"] == "Japan"
    assert payload["contractType"] == "Full-time"
    assert payload["sector"] == "Game"


def test_contamination_audit_reports_public_field_examples() -> None:
    report = build_contamination_report(
        [
            {
                "title": "Clean",
                "company": "Studio",
                "city": "Paris",
                "country": "France",
                "jobLink": "https://example.com/1",
            },
            {
                "title": '<div class="title">Artist</div>',
                "company": "Studio",
                "city": '<div class="location">Tokyo',
                "country": "Japan</div>",
                "source": "static",
                "jobLink": "https://example.com/2",
            },
        ]
    )
    assert int(report["contaminatedRows"]) == 1
    assert int(report["fieldCounts"]["title"]) == 1
    assert int(report["fieldCounts"]["city"]) == 1
    assert int(report["fieldCounts"]["country"]) == 1
    assert str(report["examples"][0]["fields"]["city"]) == '<div class="location">Tokyo'


def test_canonicalize_job_with_reason_blanks_semantic_location_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Growth Marketing Intern",
            "company": "Sleeper",
            "city": "Remote, United States; San Francisco Area, United States Remote; New York City; Los Angeles",
            "country": "Unknown",
            "jobLink": "https://jobs.ashbyhq.com/sleeper/example",
            "sector": "Game",
        },
        source="ashby_sources",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == ""
    assert payload["country"] == "Unknown"


def test_location_quality_audit_reports_semantic_location_examples() -> None:
    report = build_location_quality_report(
        [
            {
                "title": "Clean",
                "company": "Studio",
                "city": "Paris",
                "country": "France",
                "jobLink": "https://example.com/1",
            },
            {
                "title": "Growth Marketing Intern",
                "company": "Sleeper",
                "city": "Remote, United States; San Francisco Area, United States Remote; New York City; Los Angeles",
                "country": "Unknown",
                "source": "ashby_sources",
                "jobLink": "https://example.com/2",
            },
        ]
    )
    assert int(report["invalidLocationFieldCount"]) == 1
    assert int(report["fieldCounts"]["city"]) == 1
    assert (
        str(report["examples"][0]["fields"]["city"]["reason"])
        == "invalid_city_semantic_multi_location_blob"
    )


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


def test_normalize_work_type_derives_remote_from_title_when_field_empty() -> None:
    from src.jobs.normalizers import normalize_work_type

    assert normalize_work_type("", "Technical Artist (Malta/Remote)") == "Remote"
    assert normalize_work_type("", "Gameplay Programmer (Malta/Remote)") == "Remote"
    assert normalize_work_type("", "Senior Engineer - Remote") == "Remote"
    assert normalize_work_type("", "Ui Programmer (Remote)") == "Remote"
    assert normalize_work_type("", "Ai Programmer (Malta/Remote)") == "Remote"

    assert normalize_work_type("", "Senior Engineer - Onsite") == "Onsite"
    assert normalize_work_type("", "Office Assistant (Malta)") == "Onsite"
    assert normalize_work_type("", "Project Manager (Malta)") == "Onsite"

    assert normalize_work_type("Remote", "Some Onsite Job") == "Remote"
    assert normalize_work_type("Hybrid", "Onsite Engineer") == "Hybrid"
    assert normalize_work_type("", "Engineer - Hybrid") == "Hybrid"
    assert normalize_work_type("", "Mixed Mode Artist") == "Hybrid"
