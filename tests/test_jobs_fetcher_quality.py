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


def test_canonicalize_job_with_reason_blanks_metric_and_css_location_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "6,559 followers",
            "country": '--grid-gutter: calc(var(--sqs-mobile-site-gutter, 6vw) - 0.0px);',
            "jobLink": "https://example.com/metric-noise",
            "sector": "Game",
        },
        source="static_source::noise",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == ""
    assert payload["country"] == ""


def test_canonicalize_job_with_reason_normalizes_sector_from_game_evidence() -> None:
    jobs_canonicalize.reset_sector_quality_audit()
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Assembler/Bike Builder",
            "company": "Trek",
            "city": "Bennetts Green",
            "country": "Australia",
            "jobLink": "https://trekbikes.wd1.myworkdayjobs.com/en-US/TREK/job/Bennetts-Green-NSW-Australia/Assembler-Bike-Builder_Trek113973-1",
            "sector": "Game",
        },
        source="google_sheets",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["sector"] == "Tech"
    audit = jobs_canonicalize.snapshot_sector_quality_audit(total_rows=1)
    assert int(audit["downgradedGameSectorCount"]) == 1
    assert audit["examples"][0]["rawSector"] == "Game"
    normalized_report = jobs_reporting.normalize_fetch_report_payload(
        {
            "runId": "sector-audit",
            "sectorQualityAudit": audit,
        }
    )
    assert int(normalized_report["sectorQualityAudit"]["downgradedGameSectorCount"]) == 1

    game_row, game_reason = jf.canonicalize_job_with_reason(
        {
            "title": "Gameplay Programmer",
            "company": "Studio Other",
            "city": "Amsterdam",
            "country": "Netherlands",
            "jobLink": "https://example.com/gameplay",
            "sector": "Tech",
        },
        source="google_sheets",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert game_reason == ""
    assert game_row is not None
    game_payload = game_row if isinstance(game_row, dict) else game_row.to_dict()
    assert game_payload["sector"] == "Game"
    assert int(jobs_canonicalize.snapshot_sector_quality_audit(total_rows=2)["downgradedGameSectorCount"]) == 1

    zynga_row, zynga_reason = jf.canonicalize_job_with_reason(
        {
            "title": "Marketing Artist",
            "company": "Zynga",
            "city": "San Francisco",
            "country": "US",
            "jobLink": "https://job-boards.greenhouse.io/zyngacareers/jobs/5835998004",
            "sector": "Tech",
            "sourceBundle": [
                {
                    "source": "greenhouse_boards",
                    "sourceJobId": "greenhouse:zyngacareers:5835998004",
                    "jobLink": "https://job-boards.greenhouse.io/zyngacareers/jobs/5835998004",
                    "postedAt": "2026-03-27T11:09:42+00:00",
                    "adapter": "greenhouse",
                    "studio": "Zynga",
                }
            ],
        },
        source="greenhouse_boards",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert zynga_reason == ""
    assert zynga_row is not None
    zynga_payload = zynga_row if isinstance(zynga_row, dict) else zynga_row.to_dict()
    assert zynga_payload["sector"] == "Game"
    assert zynga_payload["companyType"] == "Game"

    gameloft_row, gameloft_reason = jf.canonicalize_job_with_reason(
        {
            "title": "[Dungeons & Dragons PC-Console] Artiste d'éclairage de niveaux - Lighter level artist",
            "company": "Gameloft",
            "city": "Montreal",
            "country": "Canada",
            "jobLink": "https://jobs.smartrecruiters.com/Gameloft/744000115751281",
            "sector": "Tech",
        },
        source="smartrecruiters_sources",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert gameloft_reason == ""
    assert gameloft_row is not None
    gameloft_payload = gameloft_row if isinstance(gameloft_row, dict) else gameloft_row.to_dict()
    assert gameloft_payload["sector"] == "Game"
    assert gameloft_payload["companyType"] == "Game"

    cloud_row, cloud_reason = jf.canonicalize_job_with_reason(
        {
            "title": "Senior Gameplay Programmer",
            "company": "Cloud Chamber",
            "city": "Montreal",
            "country": "Canada",
            "jobLink": "https://example.com/cloud-chamber/senior-gameplay-programmer",
            "sector": "Tech",
            "sourceBundle": [
                {
                    "source": "greenhouse_boards",
                    "sourceJobId": "greenhouse:cloudchamberen:7655929003",
                    "jobLink": "https://job-boards.greenhouse.io/cloudchamberen/jobs/7655929003",
                    "postedAt": "2026-03-16T15:18:09+00:00",
                    "adapter": "greenhouse",
                    "studio": "Cloud Chamber",
                }
            ],
        },
        source="google_sheets",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert cloud_reason == ""
    assert cloud_row is not None
    cloud_payload = cloud_row if isinstance(cloud_row, dict) else cloud_row.to_dict()
    assert cloud_payload["sector"] == "Game"
    assert cloud_payload["companyType"] == "Game"


def test_canonicalize_job_with_reason_blanks_title_like_city_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "2D Artist, Bombergrounds",
            "country": "31-621 Kraków, Poland",
            "jobLink": "https://example.com/title-noise",
            "sector": "Game",
        },
        source="static_source::noise",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == ""
    assert payload["country"] == ""


def test_canonicalize_job_with_reason_blanks_composite_and_script_city_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "Berlin / Hamburg",
            "country": "document.addEventListener(\"DOMContentLoaded\", function () {",
            "jobLink": "https://example.com/composite-noise",
            "sector": "Game",
        },
        source="static_source::noise",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == ""
    assert payload["country"] == ""


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
            {
                "title": "Artist",
                "company": "Studio",
                "city": "6,559 followers",
                "country": '--grid-gutter: calc(var(--sqs-mobile-site-gutter, 6vw) - 0.0px);',
                "source": "static_source::noise",
                "jobLink": "https://example.com/3",
            },
            {
                "title": "Artist",
                "company": "Studio",
                "city": "2D Artist, Bombergrounds",
                "country": "31-621 Kraków, Poland",
                "source": "static_source::noise",
                "jobLink": "https://example.com/4",
            },
            {
                "title": "Artist",
                "company": "Studio",
                "city": "Berlin / Hamburg",
                "country": "document.addEventListener(\"DOMContentLoaded\", function () {",
                "source": "static_source::noise",
                "jobLink": "https://example.com/5",
            },
        ]
    )
    assert int(report["invalidLocationFieldCount"]) == 7
    assert int(report["fieldCounts"]["city"]) == 4
    assert int(report["fieldCounts"]["country"]) == 3
    assert (
        str(report["examples"][0]["fields"]["city"]["reason"])
        == "invalid_city_semantic_multi_location_blob"
    )
    assert (
        str(report["examples"][1]["fields"]["city"]["reason"])
        == "invalid_city_semantic_noise"
    )
    assert (
        str(report["examples"][1]["fields"]["country"]["reason"])
        == "invalid_country_semantic_noise"
    )
    assert (
        str(report["examples"][2]["fields"]["city"]["reason"])
        == "invalid_city_semantic_noise"
    )
    assert (
        str(report["examples"][2]["fields"]["country"]["reason"])
        == "invalid_country_semantic_noise"
    )
    assert (
        str(report["examples"][3]["fields"]["city"]["reason"])
        == "invalid_city_semantic_noise"
    )
    assert (
        str(report["examples"][3]["fields"]["country"]["reason"])
        == "invalid_country_semantic_noise"
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
