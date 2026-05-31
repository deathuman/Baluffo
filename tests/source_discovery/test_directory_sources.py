# ruff: noqa: F401
import pytest

from src.source_discovery import gamesmap_parsing

from ._helpers import (
    FIXTURES_DIR,
    DiscoveryReportSummarySchema,
    Path,
    _fixture_json,
    _fixture_text,
    _gamesmap_next_payload_html,
    async_fetch_text_httpx,
    asyncio,
    classify_probe_failure_stage,
    discovery_config_module,
    discovery_orchestrator,
    discovery_url_patches,
    gamesmap_adapter,
    importlib,
    json,
    mock,
    os,
    override_discovery_config,
    override_discovery_runtime,
    sd,
    sr,
    sys,
    threading,
    time,
    workspace_tmpdir,
)


def test_discover_game_studio_sheet_candidates_reports_parse_failure_when_csv_empty_parse() -> None:
    """When CSV is non-empty but no rows are parsed, discovery returns a directory_parse failure."""
    with workspace_tmpdir("sheet-directory-public-parse-failure") as root:
        csv_with_wrong_header = "Column A,Column B,Column C\nx,y,z\n"
        payloads = {
            sd.game_studios_sheet_candidate_urls(
                sd.GAME_STUDIOS_SHEET_ID, sd.GAME_STUDIOS_SHEET_GID
            )[0]: csv_with_wrong_header,
        }

        def fake_fetch(url: str, _: int) -> str:
            if url not in payloads:
                raise RuntimeError(f"unexpected URL: {url}")
            return payloads[url]

        provider, static, failures = sd.discover_game_studio_sheet_candidates(
            5,
            config={"sheetDirectory": {"activeAuditPath": str(root / "sheet-audit.json")}},
            fetcher=fake_fetch,
        )
        assert provider == []
        assert static == []
        assert len(failures) == 1
        assert str(failures[0].get("adapter")) == "sheet_directory"
        assert str(failures[0].get("stage")) == "directory_parse"
        assert "no rows parsed" in str(failures[0].get("error"))


@pytest.mark.parametrize(
    (
        "teams_json",
        "payloads",
        "failing_urls",
        "expected_failures",
        "expect_provider",
        "expected_static_url",
        "expected_manual_only",
    ),
    [
        pytest.param(
            """[
                {"name": "Studio With Careers", "url": "https://example-studio.com/", "place": "Rome"},
                {"name": "Studio Website Only", "url": "https://website-only.it/", "place": "Milan"}
            ]""",
            {
                "https://example-studio.com/": """<!DOCTYPE html>
                <html><body>
                <a href="https://boards.greenhouse.io/example">Jobs</a>
                </body></html>""",
                "https://website-only.it/": """<!DOCTYPE html>
                <html><body><a href="/careers">Careers</a></body></html>""",
            },
            set(),
            0,
            True,
            "https://website-only.it/careers",
            False,
            id="provider-and-static-website-fallback",
        ),
        pytest.param(
            """[{"name": "Test Studio", "url": "https://example.com/", "place": "Rome"}]""",
            {},
            {"https://example.com/"},
            1,
            False,
            "",
            True,
            id="fetch-failure-manual-fallback",
        ),
        pytest.param(
            """[
                {"name": "Studio Website Only", "url": "https://website-only.it/", "place": "Milan"}
            ]""",
            {
                "https://website-only.it/": (
                    "<!DOCTYPE html><html><body><h1>Welcome</h1></body></html>"
                ),
            },
            set(),
            0,
            False,
            "https://website-only.it/careers",
            False,
            id="guessed-careers-fallback",
        ),
    ],
)
def test_discover_gameprog_candidates_directory_scenarios(
    teams_json: str,
    payloads: dict[str, str],
    failing_urls: set[str],
    expected_failures: int,
    expect_provider: bool,
    expected_static_url: str,
    expected_manual_only: bool,
) -> None:
    config = {
        "gameprog": {
            "enabled": True,
            "activeAuditTtlMinutes": 0,
            "teamsUrl": "https://gameprog.it/teams.json",
            "websiteOnlyFallback": True,
            "maxStudios": 10,
        }
    }
    all_payloads = {"https://gameprog.it/teams.json": teams_json, **payloads}

    def fake_fetch(url: str, _: int) -> str:
        if url in failing_urls:
            raise RuntimeError("fetch failed")
        if url not in all_payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return all_payloads[url]

    provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(failures) == expected_failures
    if expect_provider:
        assert len(provider_rows) >= 1
        assert str(provider_rows[0].get("adapter") or "") == "greenhouse"
        assert str(provider_rows[0].get("discoveryMethod") or "") == "gameprog"
        assert str(provider_rows[0].get("sourceDirectory") or "") == "gameprog"
    else:
        assert provider_rows == []
    assert len(static_rows) >= 1
    assert str(static_rows[0].get("adapter") or "") == "static"
    if expected_static_url:
        assert str(static_rows[0].get("careersUrl") or "") == expected_static_url
        assert "gameprog_careers_url" in (static_rows[0].get("evidenceTypes") or [])
    assert bool(static_rows[0].get("manualOnly")) is expected_manual_only


def test_discover_gameprog_candidates_reuses_fresh_cache() -> None:
    with workspace_tmpdir("gameprog-cache") as root:
        audit_path = root / "gameprog-audit.json"
        config = {
            "gameprog": {
                "enabled": True,
                "activeAuditPath": str(audit_path),
                "activeAuditTtlMinutes": 60,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 10,
            }
        }
        teams_json = """[
            {"name": "Studio With Careers", "url": "https://example-studio.com/", "place": "Rome"},
            {"name": "Studio Website Only", "url": "https://website-only.it/", "place": "Milan"}
        ]"""
        payloads = {
            "https://gameprog.it/teams.json": teams_json,
            "https://example-studio.com/": """<!DOCTYPE html><html><body><a href="https://boards.greenhouse.io/example">Jobs</a></body></html>""",
            "https://website-only.it/": """<!DOCTYPE html><html><body><h1>Welcome</h1></body></html>""",
        }
        calls: list[str] = []

        def fake_fetch(url: str, _: int) -> str:
            calls.append(url)
            if url not in payloads:
                raise RuntimeError(f"unexpected URL: {url}")
            return payloads[url]

        provider_rows_1, static_rows_1, failures_1 = sd.discover_gameprog_candidates(
            5, config=config, fetcher=fake_fetch
        )
        assert len(calls) > 0
        first_call_count = len(calls)

        with mock.patch(
            "src.source_discovery.gameprog.fetch_directory_pages",
            side_effect=AssertionError("directory fetch helper should be bypassed on cache hit"),
        ):
            provider_rows_2, static_rows_2, failures_2 = sd.discover_gameprog_candidates(
                5, config=config, fetcher=fake_fetch
            )

        assert len(calls) == first_call_count
        assert provider_rows_1 == provider_rows_2
        assert static_rows_1 == static_rows_2
        assert failures_1 == failures_2


@pytest.mark.parametrize(
    (
        "case_config",
        "payloads",
        "expected_provider_count",
        "expected_static_count",
        "expected_static_url",
        "expected_static_evidence",
        "expected_weak_signal",
        "expected_manual_only",
    ),
    [
        pytest.param(
            {
                "websiteOnlyFallback": True,
                "allowedCategoryTokens": ["developer", "publisher", "mobile", "pc", "console"],
                "blockedCategoryTokens": ["association", "education"],
                "activeAuditPath": str(Path(".tmp") / "gamesmap-direct-provider-audit.json"),
            },
            {
                "https://www.gamesmap.de/en": _fixture_text("gamesmap_index_next_payload.html"),
                "https://homepage-provider.example.com": _fixture_text(
                    "gamedevmap_homepage_provider.html"
                ),
                "https://homepage-website-only.example.com": _fixture_text(
                    "gamedevmap_homepage_no_jobs.html"
                ),
            },
            2,
            1,
            "",
            "gamesmap_website_fetch",
            True,
            False,
            id="direct-provider-homepage-provider-and-static",
        ),
        pytest.param(
            {
                "websiteOnlyFallback": False,
                "allowedCategoryTokens": ["developer", "publisher", "mobile", "pc", "console"],
                "blockedCategoryTokens": ["association", "education"],
                "activeAuditPath": str(Path(".tmp") / "gamesmap-explicit-careers-audit.json"),
            },
            {
                "https://www.gamesmap.de/en": """
                <!DOCTYPE html>
                <html lang="en">
                  <body>
                    <script>
                      self.__next_f.push([1,"payload-start \\"companies\\":[{\\"id\\":\\"1\\",\\"name\\":\\"Explicit Careers Studio\\",\\"slug\\":\\"explicit-careers-studio\\",\\"categories\\":[{\\"name\\":\\"Developer\\"}],\\"address\\":{\\"city\\":\\"Berlin\\",\\"state\\":\\"Berlin\\",\\"country\\":\\"DE\\"},\\"websites\\":[\\"https://homepage-careers.example.com\\"]}],\\"regions\\":[] payload-end"]);
                    </script>
                  </body>
                </html>
                """,
                "https://homepage-careers.example.com": """
                <!doctype html>
                <html><body><a href="/careers">Careers</a></body></html>
                """,
            },
            0,
            1,
            "https://homepage-careers.example.com/careers",
            "gamesmap_careers_url",
            False,
            False,
            id="explicit-careers-without-website-fallback",
        ),
        pytest.param(
            {
                "websiteOnlyFallback": True,
                "websiteOnlyManualOnly": True,
                "allowedCategoryTokens": ["publisher"],
                "blockedCategoryTokens": ["association", "education"],
                "activeAuditPath": str(Path(".tmp") / "gamesmap-manual-website-audit.json"),
            },
            {
                "https://www.gamesmap.de/en": _fixture_text("gamesmap_index_next_payload.html"),
                "https://homepage-website-only.example.com": _fixture_text(
                    "gamedevmap_homepage_no_jobs.html"
                ),
            },
            0,
            1,
            "",
            "gamesmap_manual_website_only",
            True,
            True,
            id="manual-website-only-static",
        ),
    ],
)
def test_discover_gamesmap_candidates_directory_scenarios(
    case_config: dict[str, object],
    payloads: dict[str, str],
    expected_provider_count: int,
    expected_static_count: int,
    expected_static_url: str,
    expected_static_evidence: str,
    expected_weak_signal: bool,
    expected_manual_only: bool,
) -> None:
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "maxDetailPages": 10,
            "activeAuditTtlMinutes": 0,
            **case_config,
        }
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(failures) == 0
    assert len(provider_rows) == expected_provider_count
    assert len(static_rows) == expected_static_count
    if expected_provider_count:
        direct_provider = next(
            row
            for row in provider_rows
            if str(row.get("careersUrl") or "") == "https://boards.greenhouse.io/examplestudio"
        )
        homepage_provider = next(
            row
            for row in provider_rows
            if str(row.get("careersUrl") or "") == "https://homepage-provider.example.com"
        )
        assert str(direct_provider.get("adapter") or "") == "greenhouse"
        assert str(direct_provider.get("sourceDirectory") or "") == "gamesmap"
        assert "gamesmap_website" in (direct_provider.get("evidenceTypes") or [])
        assert "gamesmap_website_fetch" in (homepage_provider.get("evidenceTypes") or [])
    else:
        assert provider_rows == []
    if expected_static_url:
        assert str(static_rows[0].get("careersUrl") or "") == expected_static_url
    assert str(static_rows[0].get("adapter") or "") == "static"
    assert bool(static_rows[0].get("weakSignal")) is expected_weak_signal
    assert bool(static_rows[0].get("manualOnly")) is expected_manual_only
    assert expected_static_evidence in (static_rows[0].get("evidenceTypes") or [])
    assert "gamesmap_website_fetch" in (static_rows[0].get("evidenceTypes") or [])


def test_discover_gamesmap_candidates_reports_parse_failure_when_index_shape_is_unknown() -> None:
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": True,
            "maxDetailPages": 10,
            "allowedCategoryTokens": ["developer"],
            "blockedCategoryTokens": [],
            "activeAuditPath": str(Path(".tmp") / "gamesmap-parse-failure-audit.json"),
            "activeAuditTtlMinutes": 0,
        }
    }

    def fake_fetch(url: str, _: int) -> str:
        if url != "https://www.gamesmap.de/en":
            raise RuntimeError(f"unexpected URL: {url}")
        return "<html><body><h1>No embedded company payload</h1></body></html>"

    provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
        5, config=config, fetcher=fake_fetch
    )
    assert len(provider_rows) == 0
    assert len(static_rows) == 0
    assert len(failures) == 1
    assert str(failures[0].get("stage") or "") == "directory_index_parse"


def test_discover_gamesmap_candidates_reuses_fresh_cache() -> None:
    with workspace_tmpdir("gamesmap-cache") as root:
        audit_path = root / "gamesmap-audit.json"
        config = {
            "gamesmap": {
                "enabled": True,
                "baseUrl": "https://www.gamesmap.de",
                "indexUrls": ["https://www.gamesmap.de/en"],
                "websiteOnlyFallback": True,
                "maxDetailPages": 10,
                "allowedCategoryTokens": ["developer", "publisher", "mobile", "pc", "console"],
                "blockedCategoryTokens": ["association", "education"],
                "activeAuditPath": str(audit_path),
                "activeAuditTtlMinutes": 60,
            }
        }
        payloads = {
            "https://www.gamesmap.de/en": _fixture_text("gamesmap_index_next_payload.html"),
            "https://homepage-provider.example.com": _fixture_text(
                "gamedevmap_homepage_provider.html"
            ),
            "https://homepage-website-only.example.com": _fixture_text(
                "gamedevmap_homepage_no_jobs.html"
            ),
        }
        calls: list[str] = []

        def fake_fetch(url: str, _: int) -> str:
            calls.append(url)
            if url not in payloads:
                raise RuntimeError(f"unexpected URL: {url}")
            return payloads[url]

        provider_rows_1, static_rows_1, failures_1 = sd.discover_gamesmap_candidates(
            5, config=config, fetcher=fake_fetch
        )
        assert len(calls) > 0
        first_call_count = len(calls)

        with mock.patch(
            "src.source_discovery.gamesmap.fetch_directory_pages",
            side_effect=AssertionError("directory fetch helper should be bypassed on cache hit"),
        ):
            provider_rows_2, static_rows_2, failures_2 = sd.discover_gamesmap_candidates(
                5, config=config, fetcher=fake_fetch
            )
        assert len(calls) == first_call_count
        assert provider_rows_1 == provider_rows_2
        assert static_rows_1 == static_rows_2
        assert failures_1 == failures_2
        assert audit_path.exists()


def test_gamesmap_category_filter_rejects_blocked_entries() -> None:
    row = sd.parse_gamesmap_detail_page(
        "https://www.gamesmap.de/detail/industry/tooling-association",
        _fixture_text("gamesmap_detail_blocked.html"),
    )
    assert row is not None
    config = {
        "gamesmap": {
            "allowedCategoryTokens": ["developer", "publisher"],
            "blockedCategoryTokens": ["association", "education"],
        }
    }
    assert not sd.gamesmap_matches_category(
        row.get("categories") or [],
        config["gamesmap"]["allowedCategoryTokens"],
        config["gamesmap"]["blockedCategoryTokens"],
    )


def test_gamesmap_strip_html_tags_strips_malformed_script_and_style_end_tags() -> None:
    html = """
        <script>Script Studio Developer</script
            ignored>
        <style>Style Publisher</style
            ignored>
        <span>Visible Studio</span>
    """

    assert gamesmap_parsing._strip_html_tags(html) == "Visible Studio"


def test_gamesmap_matches_category_uses_token_aware_rules() -> None:
    allowed = [
        "developer",
        "developer and publisher",
        "publisher",
        "console",
        "pc",
        "mobile",
        "browser",
        "online",
        "vr",
        "ar",
        "serious games",
    ]
    blocked = ["public institution", "service provider"]

    assert sd.gamesmap_matches_category(["Developer"], allowed, blocked)
    assert sd.gamesmap_matches_category(
        ["Developer", "Publisher"], ["developer and publisher"], blocked
    )
    assert sd.gamesmap_matches_category(["Console / PC"], allowed, blocked)
    assert sd.gamesmap_matches_category(["VR / AR"], allowed, blocked)
    assert sd.gamesmap_matches_category(["Serious games"], allowed, blocked)
    assert not sd.gamesmap_matches_category(["Research"], ["ar"], [])
    assert not sd.gamesmap_matches_category(["Market research"], ["ar"], [])
    assert not sd.gamesmap_matches_category(["PR/marketing agency"], ["ar"], [])
    assert not sd.gamesmap_matches_category(["Public institutions"], allowed, blocked)
    assert not sd.gamesmap_matches_category(["Service provider"], allowed, blocked)


def test_parse_gamedevmap_csv_returns_normalized_rows() -> None:
    rows = sd.parse_gamedevmap_csv(_fixture_text("gamedevmap_data.csv"))
    assert len(rows) == 6
    assert rows[0]["studio"] == "Provider Feed Studio"
    assert rows[0]["url"] == "https://boards.greenhouse.io/provider-feed-studio"
    assert rows[0]["country"] == "Sweden"


def test_parse_game_studio_sheet_csv_handles_metadata_rows_and_openings_flag() -> None:
    csv_text = """,,,,
,Studios Hiring now,,,Last update: 18 Feb 2026
,, ,,
,Studio,Hiring Location,Roles open (as of 18 Feb),Link
,Example Studio,Remote,yes,https://boards.greenhouse.io/example
,Example Studio 2,Remote,no,https://jobs.lever.co/example2
"""
    rows = sd.parse_game_studio_sheet_csv(csv_text)
    assert len(rows) == 2
    assert rows[0]["studio"] == "Example Studio"
    assert rows[0]["careersUrl"] == "https://boards.greenhouse.io/example"
    assert rows[0]["openingsFlag"] == "yes"
    assert rows[1]["openingsFlag"] == "no"


def test_parse_game_studio_sheet_csv_returns_expected_keys() -> None:
    """Health check: parsed rows must have studio, careersUrl, openingsFlag (game-studios-sheet contract)."""
    csv_text = """,,,,
,Studio,Hiring Location,Roles open,Link
,Acme Games,Remote,yes,https://example.com/careers
"""
    rows = sd.parse_game_studio_sheet_csv(csv_text)
    assert len(rows) >= 1
    for row in rows:
        assert "studio" in row
        assert "careersUrl" in row
        assert "openingsFlag" in row
        assert row["careersUrl"].startswith("http")


def test_parse_gameprog_teams_json_handles_missing_fields() -> None:
    json_text = """[
        {"name": "Valid Studio", "url": "https://valid.com/", "place": "Turin"},
        {"name": "No URL"},
        {"url": "https://no-name.com/"},
        {"name": "", "url": ""}
    ]"""
    rows = sd.parse_gameprog_teams_json(json_text)
    assert len(rows) == 1
    assert rows[0]["studio"] == "Valid Studio"


def test_parse_gameprog_teams_json_returns_studios() -> None:
    json_text = """[
        {"name": "Test Studio", "url": "https://test-studio.com/", "place": "Rome"},
        {"name": "Another Studio", "url": "https://another.it/", "place": "Milan"}
    ]"""
    rows = sd.parse_gameprog_teams_json(json_text)
    assert len(rows) == 2
    assert rows[0]["studio"] == "Test Studio"
    assert rows[0]["url"] == "https://test-studio.com/"
    assert rows[0]["place"] == "Rome"


def test_parse_gamesmap_detail_page_extracts_careers_and_provenance() -> None:
    row = sd.parse_gamesmap_detail_page(
        "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh",
        _fixture_text("gamesmap_detail_careers.html"),
    )
    assert row is not None
    assert str(row.get("studio") or "") == "Example Studio GmbH"
    assert str(row.get("careersUrl") or "") == "https://boards.greenhouse.io/examplestudio"
    assert str(row.get("websiteUrl") or "") == "https://www.example-studio.com"
    assert "Developer and Publisher" in (row.get("categories") or [])


def test_parse_gamesmap_detail_page_ignores_directory_and_social_links_for_website_fallback() -> (
    None
):
    html = """
    <html><body>
      <h1>Example Studio</h1>
      <h3>Categories</h3>
      <div><span class="view-detail-category">Developer</span></div>
      <a href="https://www.game.de/datenschutz/">Data protection</a>
      <a href="https://www.facebook.com/example">Facebook</a>
      <a href="https://example-studio.com/">https://example-studio.com/</a>
    </body></html>
    """
    row = sd.parse_gamesmap_detail_page(
        "https://www.gamesmap.de/en/detail/industry/example-studio",
        html,
    )
    assert row is not None
    assert str(row.get("websiteUrl") or "") == "https://example-studio.com/"
    assert str(row.get("careersUrl") or "") == ""


def test_parse_gamesmap_detail_page_supports_website_only_entries() -> None:
    row = sd.parse_gamesmap_detail_page(
        "https://www.gamesmap.de/en/detail/industry/example-publisher",
        _fixture_text("gamesmap_detail_website_only.html"),
    )
    assert row is not None
    assert str(row.get("careersUrl") or "") == ""
    assert str(row.get("websiteUrl") or "") == "https://www.example-publisher.com"
    assert "Publisher" in (row.get("categories") or [])


def test_parse_gamesmap_index_entries_extracts_company_rows_from_next_payload() -> None:
    rows = sd.parse_gamesmap_index_entries(
        _fixture_text("gamesmap_index_next_payload.html"),
        "https://www.gamesmap.de",
        prefer_english=True,
    )
    assert len(rows) == 5
    direct_provider = next(
        row for row in rows if str(row.get("studio") or "") == "Provider Direct Studio"
    )
    assert (
        str(direct_provider.get("detailUrl") or "")
        == "https://www.gamesmap.de/en/company/provider-direct-studio"
    )
    assert (
        str(direct_provider.get("websiteUrl") or "") == "https://boards.greenhouse.io/examplestudio"
    )
    assert direct_provider.get("categories") == ["Developer"]
    assert str(direct_provider.get("location") or "") == "Hamburg"
    homepage_provider = next(
        row for row in rows if str(row.get("studio") or "") == "Homepage Provider Studio"
    )
    assert homepage_provider.get("categories") == ["Console / PC", "Developer"]
    missing_website = next(
        row for row in rows if str(row.get("studio") or "") == "Missing Website Studio"
    )
    assert str(missing_website.get("websiteUrl") or "") == ""
    assert missing_website.get("categories") == ["Developer", "Publisher"]


def test_parse_gamesmap_index_entries_resolves_category_references_and_drops_bad_ones() -> None:
    category_ref = "$1b:props:children:props:children:props:children:props:companies:{company}:categories:{category}"
    companies = [
        {
            "id": "1",
            "name": "Anchor Developer",
            "slug": "anchor-developer",
            "categories": [{"name": "Developer"}],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://anchor.example.com"],
        },
        {
            "id": "2",
            "name": "Recursive Reference",
            "slug": "recursive-reference",
            "categories": [category_ref.format(company=0, category=0)],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://recursive.example.com"],
        },
        {
            "id": "3",
            "name": "Bad Reference",
            "slug": "bad-reference",
            "categories": [
                category_ref.format(company=99, category=0),
                {"name": "Publisher"},
            ],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://bad.example.com"],
        },
        {
            "id": "4",
            "name": "Cyclic Reference",
            "slug": "cyclic-reference",
            "categories": [category_ref.format(company=3, category=0)],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://cycle.example.com"],
        },
    ]

    rows, diagnostics = gamesmap_adapter._parse_gamesmap_index_entries_with_diagnostics(
        _gamesmap_next_payload_html(companies),
        "https://www.gamesmap.de",
        prefer_english=True,
    )

    by_studio = {str(row.get("studio") or ""): row for row in rows}
    assert by_studio["Recursive Reference"]["categories"] == ["Developer"]
    assert by_studio["Bad Reference"]["categories"] == ["Publisher"]
    assert by_studio["Cyclic Reference"]["categories"] == []
    assert int(diagnostics.get("unresolvedReferenceCount") or 0) == 2
    assert all(
        "$1b:" not in category for row in rows for category in list(row.get("categories") or [])
    )


def test_parse_gamesmap_live_style_reference_payload_preserves_many_eligible_rows() -> None:
    category_ref = "$1b:props:children:props:children:props:children:props:companies:{company}:categories:{category}"
    companies = [
        {
            "id": "1",
            "name": "Direct Developer",
            "slug": "direct-developer",
            "categories": [{"name": "Developer"}],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://direct.example.com"],
        },
        {
            "id": "2",
            "name": "Resolved Developer",
            "slug": "resolved-developer",
            "categories": [category_ref.format(company=0, category=0)],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://resolved.example.com"],
        },
        {
            "id": "3",
            "name": "Recursive Developer",
            "slug": "recursive-developer",
            "categories": [category_ref.format(company=1, category=0)],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://recursive.example.com"],
        },
        {
            "id": "4",
            "name": "Developer Publisher",
            "slug": "developer-publisher",
            "categories": [
                category_ref.format(company=0, category=0),
                {"name": "Publisher"},
            ],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://publisher.example.com"],
        },
        {
            "id": "5",
            "name": "Research Agency",
            "slug": "research-agency",
            "categories": [{"name": "Market research"}],
            "address": {"city": "Berlin", "state": "Berlin", "country": "DE"},
            "websites": ["https://research.example.com"],
        },
    ]

    rows = sd.parse_gamesmap_index_entries(
        _gamesmap_next_payload_html(companies),
        "https://www.gamesmap.de",
        prefer_english=True,
    )
    eligible_rows = [
        row
        for row in rows
        if str(row.get("websiteUrl") or "").strip()
        and sd.gamesmap_matches_category(
            list(row.get("categories") or []),
            ["developer", "publisher", "console", "pc", "mobile", "vr", "ar"],
            ["association", "education", "service provider"],
        )
    ]
    assert len(eligible_rows) == 4
    assert {str(row.get("studio") or "") for row in eligible_rows} == {
        "Direct Developer",
        "Resolved Developer",
        "Recursive Developer",
        "Developer Publisher",
    }


def test_select_gamedevmap_representative_rows_filters_and_dedupes() -> None:
    rows = sd.parse_gamedevmap_csv(_fixture_text("gamedevmap_data.csv"))
    selected = sd.select_gamedevmap_representative_rows(
        rows,
        allowed_categories=[
            "Developer",
            "Developer and Publisher",
            "Publisher",
            "Mobile",
        ],
        blocked_categories=["Organization"],
        index_url="https://www.gamedevmap.com/index.php",
    )
    assert len(selected) == 4
    duplicate = next(
        row for row in selected if str(row.get("url") or "") == "https://duplicate.example.com"
    )
    assert str(duplicate.get("studio") or "") == "Duplicate Direct A"
    assert int(duplicate.get("duplicateCount") or 0) == 2
    assert duplicate.get("categories") == ["Developer", "Mobile"]
    assert "query=Duplicate+Direct+A" in str(duplicate.get("sourceDirectoryEntryUrl") or "")
    assert "exact=1" in str(duplicate.get("sourceDirectoryEntryUrl") or "")
    assert "type=Developer" in str(duplicate.get("sourceDirectoryEntryUrl") or "")
