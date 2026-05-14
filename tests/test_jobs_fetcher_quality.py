import json

import pytest

import src.jobs.text_utils as jobs_text_utils
from src import jobs_fetcher as jf
from src.jobs import canonicalize as jobs_canonicalize
from src.jobs.contamination_audit import (
    build_city_garbage_report,
    build_contamination_report,
    build_location_quality_report,
    build_public_text_quality_report,
)
from src.jobs.text_utils import load_city_noise_contract
from tests.helpers import jobs_reporting


def _clear_contract_loader_caches() -> None:
    jobs_text_utils.load_city_noise_contract.cache_clear()
    jobs_text_utils.load_country_acceptance_contract.cache_clear()


def _make_packaged_text_utils_path(tmp_path) -> str:
    versioned_module_path = (
        tmp_path / "ship" / "app" / "versions" / "1.2.3" / "src" / "jobs" / "text_utils.py"
    )
    versioned_module_path.parent.mkdir(parents=True, exist_ok=True)
    versioned_module_path.write_text("# test stub\n", encoding="utf-8")
    return str(versioned_module_path)


@pytest.mark.parametrize(
    ("loader_name", "contract_name", "shared_payload", "expected_contract"),
    [
        (
            "load_city_noise_contract",
            "city_noise_contract.json",
            {
                "version": 1,
                "proseFragments": ["Bachelor's Degree"],
                "sentencePrefixes": ["Learn"],
                "placeholderFragments": ["%label_"],
                "knownJunkTokens": ["????"],
            },
            {
                "version": 1,
                "proseFragments": ["bachelor's degree"],
                "sentencePrefixes": ["learn"],
                "placeholderFragments": ["%label_"],
                "knownJunkTokens": ["????"],
            },
        ),
        (
            "load_country_acceptance_contract",
            "country_acceptance.json",
            {
                "version": 1,
                "acceptedExactLabels": ["United States"],
                "normalizeAliasesToValue": {"usa": "United States"},
            },
            {
                "version": 1,
                "exactLabelMap": {"unitedstates": "United States"},
                "aliasToCanonical": {"usa": "United States"},
            },
        ),
    ],
)
def test_contract_loaders_fall_back_to_packaged_ship_data(
    tmp_path, monkeypatch, loader_name, contract_name, shared_payload, expected_contract
) -> None:
    contract_path = tmp_path / "ship" / "data" / "contracts" / contract_name
    contract_path.parent.mkdir(parents=True, exist_ok=True)
    contract_path.write_text(json.dumps(shared_payload), encoding="utf-8")

    monkeypatch.setattr(jobs_text_utils, "__file__", _make_packaged_text_utils_path(tmp_path))
    _clear_contract_loader_caches()
    try:
        contract = getattr(jobs_text_utils, loader_name)()
    finally:
        _clear_contract_loader_caches()

    assert contract == expected_contract


@pytest.mark.parametrize(
    ("loader_name", "contract_name", "shared_payload", "version_payload", "expected_contract"),
    [
        (
            "load_city_noise_contract",
            "city_noise_contract.json",
            {
                "version": 1,
                "knownJunkTokens": ["shared"],
            },
            {
                "version": 2,
                "knownJunkTokens": ["version-local"],
            },
            {
                "version": 2,
                "proseFragments": [],
                "sentencePrefixes": [],
                "placeholderFragments": [],
                "knownJunkTokens": ["version-local"],
            },
        ),
        (
            "load_country_acceptance_contract",
            "country_acceptance.json",
            {
                "version": 1,
                "acceptedExactLabels": ["Shared Country"],
                "normalizeAliasesToValue": {"shared": "Shared Country"},
            },
            {
                "version": 2,
                "acceptedExactLabels": ["Version Local Country"],
                "normalizeAliasesToValue": {"vlc": "Version Local Country"},
            },
            {
                "version": 2,
                "exactLabelMap": {"versionlocalcountry": "Version Local Country"},
                "aliasToCanonical": {"vlc": "Version Local Country"},
            },
        ),
    ],
)
def test_contract_loaders_prefer_version_local_packaged_contracts(
    tmp_path,
    monkeypatch,
    loader_name,
    contract_name,
    shared_payload,
    version_payload,
    expected_contract,
) -> None:
    shared_contract_path = tmp_path / "ship" / "data" / "contracts" / contract_name
    shared_contract_path.parent.mkdir(parents=True, exist_ok=True)
    shared_contract_path.write_text(json.dumps(shared_payload), encoding="utf-8")
    version_contract_path = (
        tmp_path / "ship" / "app" / "versions" / "1.2.3" / "data" / "contracts" / contract_name
    )
    version_contract_path.parent.mkdir(parents=True, exist_ok=True)
    version_contract_path.write_text(json.dumps(version_payload), encoding="utf-8")

    monkeypatch.setattr(jobs_text_utils, "__file__", _make_packaged_text_utils_path(tmp_path))
    _clear_contract_loader_caches()
    try:
        contract = getattr(jobs_text_utils, loader_name)()
    finally:
        _clear_contract_loader_caches()

    assert contract == expected_contract


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


def test_city_garbage_audit_reports_obvious_garbage_examples() -> None:
    report = build_city_garbage_report(
        [
            {
                "title": "Gameplay Engineer",
                "company": "Studio",
                "city": "We're sorry",
                "country": "US",
                "locationSummary": "Winston-Salem, US | Clear search results",
                "locations": [
                    {"city": "We're sorry", "country": "US"},
                    {"city": "Berlin", "country": "DE"},
                ],
                "jobLink": "https://example.com/1",
            },
            {
                "title": "Gameplay Engineer",
                "company": "Studio",
                "city": "AI Enablement",
                "country": "US",
                "locationSummary": "AI Enablement | Regensburg, DE",
                "locations": [{"city": "AI Enablement", "country": "US"}],
                "jobLink": "https://example.com/2",
            },
            {
                "title": "Gameplay Engineer",
                "company": "Studio",
                "city": "Tokyo",
                "country": "JP",
                "locationSummary": "Tokyo, JP",
                "locations": [{"city": "Tokyo", "country": "JP"}],
                "jobLink": "https://example.com/3",
            },
        ]
    )
    assert int(report["totalRows"]) == 3
    assert int(report["garbageRows"]) == 2
    assert int(report["fieldCounts"]["city"]) == 2
    assert int(report["fieldCounts"]["locationSummary"]) == 2
    assert int(report["fieldCounts"]["locations.city"]) == 2
    assert int(report["categoryCounts"]["site_chrome"]) == 3
    assert int(report["categoryCounts"]["role_category"]) == 3
    assert str(report["examples"][0]["fields"]["city"]["category"]) == "site_chrome"
    assert str(report["examples"][1]["fields"]["city"]["category"]) == "role_category"


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
    assert payload["country"] == ""


def test_canonicalize_job_with_reason_normalizes_raw_city_blob_without_locations() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Technical Artist",
            "company": "Riot Games",
            "city": "Los Angeles, USA",
            "country": "Unknown",
            "jobLink": "https://example.com/riot",
            "sector": "Game",
        },
        source="static_source::static:listing_url:https://www.riotgames.com/en/work-with-us/jobs",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Los Angeles"
    assert payload["country"] == "US"
    assert payload["locations"] == [{"city": "Los Angeles", "country": "US"}]


def test_canonicalize_job_with_reason_promotes_country_only_raw_city_value() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Localization Producer",
            "company": "PlayStation Global",
            "city": "Japan",
            "country": "Unknown",
            "jobLink": "https://example.com/japan",
            "sector": "Game",
        },
        source="greenhouse_boards",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == ""
    assert payload["country"] == "Japan"
    assert payload["locations"] == [{"city": "", "country": "Japan"}]


def test_canonicalize_job_with_reason_drops_static_page_noise_in_city_field() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Studio Operations",
            "company": "Warner Bros. Games",
            "city": "Content & Editorial",
            "country": "Unknown",
            "jobLink": "https://careers.wbd.com/global/en/c/studio-operations-jobs",
            "sector": "Game",
        },
        source="static_source::static:listing_url:https://careers.wbd.com/global/en/wb-games-jobs",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert row is None
    assert reason == "non_job_static_page"


@pytest.mark.parametrize(
    "raw, source",
    [
        (
            {
                "title": "Software",
                "company": "Stardock",
                "city": "Increase productivity, design intelligent controls and reinforce branding with our enterprise products.",
                "country": "Unknown",
                "jobLink": "https://www.stardock.com/products",
                "sector": "Game",
            },
            "static_source::static:listing_url:https://www.stardock.com/careers",
        ),
        (
            {
                "title": "Purpose-built for gaming.",
                "company": "Immutable",
                "city": "Know which channels are driving players likely to purchase, and which are driving empty wishlists.",
                "country": "Unknown",
                "jobLink": "https://www.immutable.com/chain",
                "sector": "Game",
            },
            "static_source::static:listing_url:https://www.immutable.com/jobs",
        ),
        (
            {
                "title": "JOB OFFERS",
                "company": "GS Studio",
                "city": "Full-time",
                "country": "Unknown",
                "jobLink": "https://www.gs-studio.eu/career/no-open-positions",
                "sector": "Game",
            },
            "static_source::static:listing_url:https://www.gs-studio.eu/career",
        ),
        (
            {
                "title": "Speculative Application - Art UK Remote / West Midlands",
                "company": "Flix Interactive",
                "city": "Don’t see an Art role available at Flix right now? We still want to hear from you",
                "country": "UK",
                "jobLink": "https://www.flixinteractive.com/vacancies/speculative-application-art",
                "sector": "Game",
            },
            "static_source::static:listing_url:https://www.flixinteractive.com/",
        ),
    ],
)
def test_canonicalize_job_with_reason_drops_high_confidence_non_job_static_pages(
    raw: dict[str, str],
    source: str,
) -> None:
    row, reason = jf.canonicalize_job_with_reason(
        raw,
        source=source,
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert row is None
    assert reason == "non_job_static_page"


def test_public_text_quality_report_includes_city_garbage_audit() -> None:
    report = build_public_text_quality_report(
        [
            {
                "title": "Gameplay Engineer",
                "company": "Studio",
                "city": "We're sorry",
                "country": "US",
                "locationSummary": "Winston-Salem, US | Clear search results",
                "locations": [{"city": "We're sorry", "country": "US"}],
                "jobLink": "https://example.com/report",
            }
        ]
    )
    assert "contaminatedRows" in report
    assert "locationQualityAudit" in report
    assert "cityGarbageAudit" in report
    assert int(report["cityGarbageAudit"]["garbageRows"]) == 1


def test_normalize_fetch_report_payload_preserves_city_garbage_audit() -> None:
    normalized_report = jobs_reporting.normalize_fetch_report_payload(
        {
            "schemaVersion": 1,
            "runId": "run-1",
            "startedAt": "2026-03-30T00:00:00Z",
            "finishedAt": "2026-03-30T00:05:00Z",
            "runtime": {},
            "contaminationAudit": {"totalRows": 1, "contaminatedRows": 0},
            "cityGarbageAudit": {
                "totalRows": 1,
                "garbageRows": 1,
                "fieldCounts": {"city": 1},
                "categoryCounts": {"site_chrome": 1},
                "examples": [],
            },
            "locationQualityAudit": {"totalRows": 1, "invalidLocationFieldCount": 0},
            "sectorQualityAudit": {"totalRows": 1, "downgradedGameSectorCount": 0},
        }
    )
    assert int(normalized_report["cityGarbageAudit"]["garbageRows"]) == 1
    assert int(normalized_report["cityGarbageAudit"]["fieldCounts"]["city"]) == 1
    assert int(normalized_report["cityGarbageAudit"]["categoryCounts"]["site_chrome"]) == 1


def test_canonicalize_job_with_reason_blanks_shared_city_noise_contract_fragments() -> None:
    contract = load_city_noise_contract()
    assert "bachelor's degree" in contract["proseFragments"]
    assert "learn" in contract["sentencePrefixes"]
    assert "%label_" in contract["placeholderFragments"]
    assert "????" in contract["knownJunkTokens"]
    assert "ai solutions pm" in contract["knownJunkTokens"]
    for token in [
        "Any",
        "Apps for kids",
        "CET +- 4",
        "CET +- 2",
        "COME FLY WITH US",
        "Chief Human Resource Officer (CHRO)",
        "Come work with us!",
        "Chronos: Before the Ashes",
        "Community",
        "Contact",
        "Create amazing characters that are efficient",
        "Create",
        "Cybersecurity",
        "Culture & Values",
        "Data & Engineering",
        "Data & Research",
        "Department",
        "Departments",
        "Do Not Sell My Information",
        "Do Not Share My Personal",
        "EU & NA",
        "Endless Legend is a 4X turn",
        "Ensure brand message is consistent",
        "Entertain the world",
        "Filter by",
        "Filter roles by",
        "Filters",
        "Finance",
        "Finance & Accounting",
        "Find us on Facebook",
        "From Concept to Console: Meet Winslow",
        "Full",
        "Full or part",
        "Games FQA Warsaw",
        "HUMANKIND is a turn",
        "Head of IP Licensing BD",
        "Head of Recruiting",
        "Help create video scripts",
        "In this role",
        "Imprint",
        "Internal Tools & Player Insights",
        "Interviews",
        "Junior",
        "Join our crew",
        "Join the community",
        "Join us",
        "Legal",
        "Ltd. )",
        "Mastery social platforms: Facebook",
        "Office",
        "Organization",
        "People & Culture",
        "Senior Production Accountant (Feature) : 2026",
        "Sega of America",
        "Sign in",
        "Spontaneous application",
        "Startup Directory Founder Directory Launch YC",
        "Student",
        "Student & Recent Graduates",
        "Studio",
        "Studios",
        "Titan Quest II Announced",
        "To be clear",
        "To be considered",
        "UNAVAILABLE",
        "UK",
        "Web Build Purple Imp",
        "Work & Innovation",
    ]:
        assert token.lower() in contract["knownJunkTokens"]

    cases = [
        "A bachelor's degree in digital communications",
        "If you are looking for Tokyo",
        "%LABEL_POSITION_TYPE_REMOTE_ANY%",
        "????",
    ]
    for city in cases:
        row, reason = jf.canonicalize_job_with_reason(
            {
                "title": "Artist",
                "company": "Studio",
                "city": city,
                "country": "Japan",
                "jobLink": "https://example.com/city-contract",
                "sector": "Game",
            },
            source="static_source::noise",
            fetched_at="2026-03-20T00:00:00Z",
        )
        assert reason == ""
        assert row is not None
        payload = row if isinstance(row, dict) else row.to_dict()
        assert payload["city"] == ""
        assert payload["country"] == "Japan"


def test_canonicalize_job_with_reason_blanks_structural_city_noise_values() -> None:
    for city in ["2026", "3"]:
        row, reason = jf.canonicalize_job_with_reason(
            {
                "title": "Artist",
                "company": "Studio",
                "city": city,
                "country": "Japan",
                "jobLink": "https://example.com/city-structural-noise",
                "sector": "Game",
            },
            source="static_source::noise",
            fetched_at="2026-03-20T00:00:00Z",
        )
        assert reason == ""
        assert row is not None
        payload = row if isinstance(row, dict) else row.to_dict()
        assert payload["city"] == ""
        assert payload["country"] == "Japan"


def test_canonicalize_job_with_reason_blanks_metric_and_css_location_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "6,559 followers",
            "country": "--grid-gutter: calc(var(--sqs-mobile-site-gutter, 6vw) - 0.0px);",
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


def test_canonicalize_job_with_reason_rejects_country_work_type_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "Tokyo",
            "country": "Hybrid",
            "jobLink": "https://example.com/country-noise",
            "sector": "Game",
        },
        source="static_source::noise",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Tokyo"
    assert payload["country"] == "Japan"


def test_canonicalize_job_with_reason_preserves_region_country_names() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Artist",
            "company": "Studio",
            "city": "Skopje",
            "country": "North Macedonia",
            "jobLink": "https://example.com/region-country",
            "sector": "Game",
        },
        source="static_source::region-country",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Skopje"
    assert payload["country"] == "North Macedonia"


def test_canonicalize_job_with_reason_promotes_first_meaningful_multi_location_entry() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Rendering Engineer",
            "company": "Stellar Entertainment",
            "locations": [
                {"city": "", "country": "Unknown"},
                {"city": "Guildford", "country": "England"},
                {"city": "Utrecht", "country": "NL"},
            ],
            "jobLink": "https://jobs.ashbyhq.com/stellarentertainment/8615ea53-96d1-4923-9d48-a920639c9fbe",
            "sector": "Game",
        },
        source="ashby_sources",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Guildford"
    assert payload["country"] == "England"
    assert payload["locationSummary"] == "Guildford, England | Utrecht, NL"
    assert payload["locations"][0] == {"city": "Guildford", "country": "England"}


def test_canonicalize_job_with_reason_rebuilds_location_summary_from_surviving_entries() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Rendering Engineer",
            "company": "Stellar Entertainment",
            "city": "AI Solutions PM",
            "country": "Unknown",
            "locations": [
                {"city": "AI Solutions PM", "country": "Unknown"},
                {"city": "Guildford", "country": "UK"},
            ],
            "jobLink": "https://jobs.ashbyhq.com/stellarentertainment/5e067256-96d1-4923-9d48-a920639c9fbe",
            "sector": "Tech",
        },
        source="static_source::listing_url:https://stellarentertainment.software/join-us/",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Guildford"
    assert payload["country"] == "UK"
    assert payload["locationSummary"] == "Guildford, UK"
    assert payload["locations"] == [{"city": "Guildford", "country": "UK"}]


def test_deduplicate_jobs_merges_sparse_variant_into_richer_multi_location_row() -> None:
    sparse = jf.canonicalize_job(
        {
            "title": "Rendering Engineer Engineering Guildford, UK | Utrecht, NL United Kingdom",
            "company": "Stellar Entertainment Software",
            "city": "",
            "country": "Unknown",
            "locations": [{"city": "", "country": "Unknown"}],
            "jobLink": "https://jobs.ashbyhq.com/stellarentertainment/5e067256-96d1-4923-9d48-a920639c9fbe",
            "sector": "Tech",
        },
        source="static_source::static:listing_url:https://stellarentertainment.software/join-us/",
        fetched_at="2026-03-20T00:00:00Z",
    )
    rich = jf.canonicalize_job(
        {
            "title": "Rendering Engineer",
            "company": "Stellar Entertainment",
            "city": "Guildford",
            "country": "England",
            "locations": [
                {"city": "", "country": "Unknown"},
                {"city": "Guildford", "country": "England"},
                {"city": "Utrecht", "country": "NL"},
            ],
            "jobLink": "https://jobs.ashbyhq.com/stellarentertainment/8615ea53-9992-489f-b2cd-38ede3434679",
            "sector": "Game",
        },
        source="google_sheets_1er2oaxo",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert sparse is not None
    assert rich is not None
    rows, stats = jf.deduplicate_jobs([sparse, rich])
    assert int(stats["outputCount"]) == 1
    assert len(rows) == 1
    payload = rows[0].to_dict()
    assert payload["title"] == "Rendering Engineer"
    assert payload["company"] == "Stellar Entertainment"
    assert payload["city"] == "Guildford"
    assert payload["country"] == "England"
    assert payload["locations"] == [
        {"city": "Guildford", "country": "England"},
        {"city": "Utrecht", "country": "NL"},
    ]
    assert payload["locationSummary"] == "Guildford, England | Utrecht, NL"


def test_deduplicate_jobs_merges_sparse_stellar_technical_artist_variant_into_richer_row() -> None:
    sparse = jf.canonicalize_job(
        {
            "title": "Technical Artist Art Guildford, UK | Utrecht, NL United Kingdom",
            "company": "Stellar Entertainment Software",
            "city": "",
            "country": "Unknown",
            "locations": [{"city": "", "country": "Unknown"}],
            "jobLink": "https://jobs.ashbyhq.com/stellarentertainment/4526ffd2-860e-4e2d-8743-4e637ca0ced6",
            "sector": "Game",
        },
        source="static_source::static:listing_url:https://stellarentertainment.software/join-us/",
        fetched_at="2026-03-20T00:00:00Z",
    )
    rich = jf.canonicalize_job(
        {
            "title": "Technical Artist",
            "company": "Stellar Entertainment",
            "city": "Guildford",
            "country": "England",
            "locations": [
                {"city": "", "country": "Unknown"},
                {"city": "Guildford", "country": "England"},
                {"city": "Utrecht", "country": "NL"},
            ],
            "jobLink": "https://jobs.ashbyhq.com/stellarentertainment/8615ea53-9992-489f-b2cd-38ede3434679",
            "sector": "Game",
        },
        source="google_sheets_1er2oaxo",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert sparse is not None
    assert rich is not None
    rows, stats = jf.deduplicate_jobs([sparse, rich])
    assert int(stats["outputCount"]) == 1
    assert len(rows) == 1
    payload = rows[0].to_dict()
    assert payload["title"] == "Technical Artist"
    assert payload["company"] == "Stellar Entertainment"
    assert payload["city"] == "Guildford"
    assert payload["country"] == "England"
    assert payload["locations"] == [
        {"city": "Guildford", "country": "England"},
        {"city": "Utrecht", "country": "NL"},
    ]
    assert payload["locationSummary"] == "Guildford, England | Utrecht, NL"


def test_canonicalize_job_with_reason_blanks_label_placeholder_location_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Associate QA Coordinator United States",
            "company": "IllFonic",
            "city": "%LABEL_POSITION_TYPE_REMOTE_ANY%",
            "country": "Unknown",
            "locations": [
                {"city": "%LABEL_POSITION_TYPE_REMOTE_ANY%", "country": "Unknown"},
                {"city": "", "country": "US"},
            ],
            "jobLink": "https://illfonic.breezy.hr/p/06c96306a484-associate-qa-coordinator",
            "sector": "Tech",
        },
        source="static_source::static",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == ""
    assert payload["country"] == "US"
    assert payload["locationSummary"] == "US"
    assert payload["locations"] == [{"city": "", "country": "US"}]


def test_canonicalize_job_with_reason_blanks_role_blob_location_noise() -> None:
    row, reason = jf.canonicalize_job_with_reason(
        {
            "title": "Lead Level Scripter Montréal CDI",
            "company": "Don't Nod",
            "city": "Administratif, Assistant, Gestion, RH...",
            "country": "Unknown",
            "locations": [
                {"city": "Administratif, Assistant, Gestion, RH...", "country": "Unknown"},
                {"city": "Paris", "country": "FR"},
            ],
            "jobLink": "https://jobs.smartrecruiters.com/DONTNOD/744000104833006",
            "sector": "Game",
        },
        source="static_source::static",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert reason == ""
    assert row is not None
    payload = row if isinstance(row, dict) else row.to_dict()
    assert payload["city"] == "Paris"
    assert payload["country"] == "FR"
    assert payload["locationSummary"] == "Paris, FR"
    assert payload["locations"] == [{"city": "Paris", "country": "FR"}]


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
    assert (
        int(
            jobs_canonicalize.snapshot_sector_quality_audit(total_rows=2)[
                "downgradedGameSectorCount"
            ]
        )
        == 1
    )

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

    sega_me_row, sega_me_reason = jf.canonicalize_job_with_reason(
        {
            "title": "Projects /After sales services Engineer",
            "company": "SEGA",
            "city": "10th of Ramadan",
            "country": "Unknown",
            "jobLink": "https://eg.linkedin.com/jobs/view/projects-after-sales-services-engineer-at-%E2%80%8F%E2%80%8Esega-m-electrical-products%E2%80%8E-4399033334",
            "sector": "Tech",
            "sourceBundle": [
                {
                    "source": "static_source::static:listing_url:https://www.linkedin.com/jobs/search/?currentjobid=4148163061&geoid=92000000&keywords=sega",
                    "sourceJobId": "static:static:listing_url:https://www.linkedin.com/jobs/search/?currentjobid=4148163061&geoid=92000000&keywords=sega:55d919920a",
                    "jobLink": "https://eg.linkedin.com/jobs/view/projects-after-sales-services-engineer-at-%E2%80%8F%E2%80%8Esega-m-electrical-products%E2%80%8E-4399033334",
                    "postedAt": "",
                    "adapter": "static",
                    "studio": "SEGA",
                }
            ],
        },
        source="static_source::static:listing_url:https://www.linkedin.com/jobs/search/?currentjobid=4148163061&geoid=92000000&keywords=sega",
        fetched_at="2026-03-20T00:00:00Z",
    )
    assert sega_me_reason == ""
    assert sega_me_row is not None
    sega_me_payload = sega_me_row if isinstance(sega_me_row, dict) else sega_me_row.to_dict()
    assert sega_me_payload["sector"] == "Tech"
    assert sega_me_payload["companyType"] == "Tech"


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
            "country": 'document.addEventListener("DOMContentLoaded", function () {',
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
                "country": "--grid-gutter: calc(var(--sqs-mobile-site-gutter, 6vw) - 0.0px);",
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
                "country": 'document.addEventListener("DOMContentLoaded", function () {',
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
    assert str(report["examples"][1]["fields"]["city"]["reason"]) == "invalid_city_semantic_noise"
    assert (
        str(report["examples"][1]["fields"]["country"]["reason"])
        == "invalid_country_semantic_noise"
    )
    assert str(report["examples"][2]["fields"]["city"]["reason"]) == "invalid_city_semantic_noise"
    assert (
        str(report["examples"][2]["fields"]["country"]["reason"])
        == "invalid_country_semantic_noise"
    )
    assert str(report["examples"][3]["fields"]["city"]["reason"]) == "invalid_city_semantic_noise"
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
