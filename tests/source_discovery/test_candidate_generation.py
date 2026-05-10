# ruff: noqa: F401
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
    web_audit_rows,
)


def test_analyze_fetched_page_falls_back_to_generic_static_without_explicit_links() -> None:
    html = """
    <script type="application/ld+json">{"@type":"JobPosting","title":"Gameplay Engineer"}</script>
    """
    analyzed = sd.analyze_fetched_page(
        "https://studio.example.com/careers",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="web_search",
    )
    assert analyzed["provider_candidates"] == []
    assert str(analyzed.get("explicit_careers_url") or "") == ""
    assert analyzed["generic_static_candidate"] is not None


def test_analyze_fetched_page_prefers_provider_candidates_over_other_outcomes() -> None:
    html = """
    <a href="/careers">Careers</a>
    <a href="https://boards.greenhouse.io/example-studio/jobs/123">Rendering Engineer</a>
    <script type="application/ld+json">{"@type":"JobPosting","title":"Gameplay Engineer"}</script>
    """
    analyzed = sd.analyze_fetched_page(
        "https://studio.example.com/",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="gamedevmap",
    )
    assert len(analyzed["provider_candidates"]) == 1
    assert str(analyzed.get("explicit_careers_url") or "") == ""
    assert analyzed["generic_static_candidate"] is None


def test_async_fetch_text_httpx_enables_redirect_following() -> None:
    calls = []

    class _Response:
        def __init__(self):
            self.encoding = None
            self.text = "ok"

        def raise_for_status(self) -> None:
            return None

    class _Client:
        async def get(self, url: str, **kwargs):
            calls.append((url, kwargs))
            return _Response()

    result = asyncio.run(async_fetch_text_httpx(_Client(), "https://example.com/jobs", timeout_s=5))
    assert result == "ok"
    assert calls == [
        (
            "https://example.com/jobs",
            {
                "headers": mock.ANY,
                "follow_redirects": True,
            },
        )
    ]


def test_async_probe_candidate_mirrors_sync_probe_count() -> None:
    greenhouse = {
        "adapter": "greenhouse",
        "slug": "example",
        "api_url": "https://boards-api.greenhouse.io/v1/boards/example/jobs?content=true",
    }

    async def fake_async_fetch(url: str, _timeout: int) -> str:
        assert "boards-api.greenhouse.io" in url
        return '{"jobs":[{"id":1,"title":"Gameplay Role 1","absolute_url":"https://job-boards.greenhouse.io/example/jobs/1"},{"id":2,"title":"Gameplay Role 2","absolute_url":"https://job-boards.greenhouse.io/example/jobs/2"},{"id":3,"title":"Gameplay Role 3","absolute_url":"https://job-boards.greenhouse.io/example/jobs/3"}]}'

    ok, count, error = asyncio.run(
        sd.async_probe_candidate(greenhouse, timeout_s=5, fetcher=fake_async_fetch)
    )
    assert ok
    assert count == 3
    assert error == ""


def test_build_known_careers_url_candidate_preserves_requested_fields() -> None:
    row = sd.build_known_careers_url_candidate(
        "https://example.com/careers",
        studio="Example Studio",
        name_suffix="Gameprog",
        nl_priority=False,
        discovery_method="gameprog",
        evidence_source="gameprog",
        evidence_types=["gameprog_directory", "gameprog_careers_url"],
        evidence_score=40,
        enabled_by_default=False,
        extra_fields={
            "sourceDirectory": "gameprog",
            "sourceDirectoryEntryUrl": "https://example.com/",
            "manualOnly": False,
        },
    )
    assert str(row.get("name") or "") == "Example Studio (Gameprog)"
    assert str(row.get("careersUrl") or "") == "https://example.com/careers"
    assert int(row.get("evidenceScore") or 0) == 40
    assert str(row.get("sourceDirectory") or "") == "gameprog"
    assert "gameprog_careers_url" in (row.get("evidenceTypes") or [])


def test_merge_candidate_streams_preserves_gameprog_no_openings_evidence() -> None:
    rows = sd.merge_candidate_streams(
        [
            (
                "generic_static",
                [
                    {
                        "name": "Fallback Studio (Gameprog)",
                        "studio": "Fallback Studio",
                        "adapter": "static",
                        "listing_url": "https://fallback.example.com/",
                        "evidenceTypes": [
                            "gameprog_directory",
                            "gameprog_no_current_openings",
                        ],
                    }
                ],
            )
        ]
    )

    assert rows[0]["evidenceTypes"] == [
        "gameprog_directory",
        "gameprog_no_current_openings",
    ]


def test_build_m5_strategic_backlog_applies_frozen_lanes_and_identity_rules() -> None:
    backlog = sd.build_m5_strategic_backlog(
        report_candidates=[
            {
                "sourceId": "source-1",
                "name": "TiMi Studio Group",
                "studio": "TiMi Studio Group",
                "adapter": "static",
                "rankScore": 88,
                "rankReasons": ["live_jobs_detected"],
                "jobsFound": 4,
                "hqRegion": "Asia",
            },
            {
                "id": "custom-row",
                "name": "Custom Studio",
                "studio": "Custom Studio",
                "adapter": "static",
                "score": 55,
                "rankReasons": [],
                "jobsFound": 0,
                "region": "North America",
            },
            {
                "id": "workday-row",
                "name": "Wolcen Studios",
                "studio": "Wolcen Studios",
                "adapter": "workday",
                "rankScore": 80,
                "rankReasons": ["live_jobs_detected"],
                "jobsFound": 2,
            },
        ],
        failures=[
            {
                "name": "Blocked Static",
                "adapter": "static",
                "dropReason": "blocked_domain",
                "dropStage": "suppressed_static",
            },
            {
                "name": "Existing Source",
                "adapter": "static",
                "dropReason": "existing_id",
            },
        ],
        active_rows=[],
        source_state_rows={
            "Custom Studio": {
                "lastStatus": "ok",
                "lastKeptCount": 4,
            },
            "Wolcen Studios": {
                "structuredMigrationBaselineCapturedAt": "2026-03-26T09:00:00Z",
                "structuredMigrationBaselineDurationMs": 9100,
                "structuredMigrationBaselineStatus": "error",
                "structuredMigrationBaselineError": "static timeout",
                "structuredMigrationBaselineFailureBucket": "static_listing",
                "structuredMigrationBaselineKeptCount": 1,
                "lastDurationMs": 5400,
                "lastStatus": "ok",
                "lastError": "",
                "lastFailureBucket": "structured_listing",
                "lastKeptCount": 2,
                "structuredMigrationShadowRunCount": 4,
                "structuredMigrationHealthyRunCount": 3,
                "structuredMigrationPromotedAt": "2026-03-26T10:00:00Z",
            },
        },
    )

    assert [row["coverageLane"] for row in backlog] == [
        "lane_c_asia_custom",
        "lane_b_custom",
        "lane_a_m4_followup",
        "lane_d_defer",
        "lane_d_defer",
    ]
    assert {row["coverageLane"] for row in backlog}.issubset(
        {
            "lane_a_m4_followup",
            "lane_b_custom",
            "lane_c_asia_custom",
            "lane_d_defer",
        }
    )

    asia_row = backlog[0]
    assert asia_row["candidateIdentityKey"] == "source-1"
    assert asia_row["coveragePriority"] > backlog[1]["coveragePriority"]
    assert "asia_hq" in asia_row["rankReasons"]
    assert "open_role_evidence" in asia_row["rankReasons"]
    assert "weak_regional_coverage" in asia_row["rankReasons"]

    custom_row = backlog[1]
    assert custom_row["candidateIdentityKey"] == sr.source_identity(
        {"id": "custom-row", "name": "Custom Studio", "adapter": "static"}
    )
    assert custom_row["firstRunOutcome"] == "healthy_keep"
    assert custom_row["firstRunKeptCount"] == 4

    workday_row = backlog[2]
    assert workday_row["coverageLane"] == "lane_a_m4_followup"
    assert workday_row["exclusionStatus"] == "excluded"
    assert workday_row["exclusionReason"] == "m4_family_followup"
    assert workday_row["migrationComparison"] == {
        "before": {
            "durationMs": 9100,
            "status": "error",
            "error": "static timeout",
            "failureBucket": "static_listing",
            "keptCount": 1,
        },
        "after": {
            "durationMs": 5400,
            "status": "ok",
            "error": "",
            "failureBucket": "structured_listing",
            "keptCount": 2,
        },
        "runtimeDeltaMs": -3700,
        "keptCountDelta": 1,
        "shadowRunCount": 4,
        "healthyRunCount": 3,
        "promotedAt": "2026-03-26T10:00:00Z",
        "demotedAt": "",
        "rollbackChecklist": [
            "Re-enable the static twin in the registry.",
            "Keep structured shadow mode until 3 consecutive healthy runs complete.",
            "Demote the structured source if kept count drops to zero or duplicate rate regresses.",
        ],
    }

    blocked_row = backlog[3]
    assert blocked_row["exclusionStatus"] == "excluded"
    assert blocked_row["exclusionReason"] == "blocked_domain"

    existing_row = backlog[4]
    assert existing_row["exclusionStatus"] == "excluded"
    assert existing_row["exclusionReason"] == "existing_id"


def test_build_pattern_candidates_adds_reinforcement_for_provider_matching_careers_url() -> None:
    with override_discovery_config(
        studio_seeds=[
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "likelyProviders": ["greenhouse"],
                "careersUrl": "https://boards.greenhouse.io/example-studio",
            }
        ]
    ):
        rows = sd.build_pattern_candidates()
    assert len(rows) >= 1
    assert all(int(row.get("evidenceScore") or 0) >= 42 for row in rows)
    assert all("seed_provider_reinforced" in (row.get("evidenceTypes") or []) for row in rows)


def test_build_pattern_candidates_generates_root_ashby_board_urls() -> None:
    with override_discovery_config(
        studio_seeds=[
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "likelyProviders": ["ashby"],
            }
        ]
    ):
        rows = sd.build_pattern_candidates()
    assert len(rows) == 1
    assert rows[0]["adapter"] == "ashby"
    assert rows[0]["board_url"] == "https://jobs.ashbyhq.com/example-studio"


def test_build_pattern_candidates_respects_likely_providers() -> None:
    with override_discovery_config(
        studio_seeds=[
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": True,
                "likelyProviders": ["greenhouse", "teamtailor"],
            }
        ]
    ):
        rows = sd.build_pattern_candidates()

    adapters = {str(row.get("adapter")) for row in rows}
    assert adapters == {"greenhouse", "teamtailor"}


def test_build_pattern_candidates_supports_recruitee_and_pinpoint_providers() -> None:
    with override_discovery_config(
        studio_seeds=[
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "likelyProviders": ["recruitee", "pinpoint"],
            }
        ]
    ):
        rows = sd.build_pattern_candidates()

    adapters = {str(row.get("adapter")) for row in rows}
    assert adapters == {"recruitee", "pinpoint"}
    assert any(str(row.get("api_url") or "").endswith("/api/offers/") for row in rows)
    assert any(str(row.get("api_url") or "").endswith("/postings.json") for row in rows)


def test_build_static_candidate_from_page_blocks_linkedin_like_domains() -> None:
    row = sd.build_static_candidate_from_page(
        "https://www.linkedin.com/company/example/jobs/",
        '<a href="/jobs/test">Test</a>',
        studio="Example Studio",
        nl_priority=False,
        discovery_method="web_search",
    )
    assert row is None


def test_build_static_candidate_from_page_records_evidence() -> None:
    html = """
    <a href="/jobs/rendering-engineer">Rendering Engineer</a>
    <script type="application/ld+json">{"@type":"JobPosting","title":"Gameplay Engineer"}</script>
    """
    row = sd.build_static_candidate_from_page(
        "https://example.com/careers",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="web_search",
    )
    assert row is not None
    assert str(row.get("adapter") or "") == "static"
    assert int(row.get("evidenceScore") or 0) >= sd.MIN_STATIC_EVIDENCE_TO_QUEUE
    assert "jobposting_jsonld" in (row.get("evidenceTypes") or [])


def test_classify_probe_failure_stage_treats_httpx_404_as_probe_miss() -> None:
    assert (
        classify_probe_failure_stage(
            "https://example.com/jobs: Client error '404 Not Found' for url 'https://example.com/jobs'"
        )
        == "probe_miss"
    )


def test_classify_static_suppression_preserves_strong_or_previously_productive_static_candidate() -> (
    None
):
    strong_reason = sd.classify_static_suppression(
        {
            "name": "Strong Static (Manual Website)",
            "studio": "Strong Static",
            "adapter": "static",
            "discoveryStage": "generic_static",
            "weakSignal": True,
            "manualOnly": True,
            "evidenceScore": 26,
            "evidenceTypes": ["careers_keyword", "structured_job_links"],
        },
        source_state_rows={},
        thresholds=sd.DEFAULT_DISCOVERY_THRESHOLDS,
    )
    productive_reason = sd.classify_static_suppression(
        {
            "name": "Previously Productive (Manual Website)",
            "studio": "Previously Productive",
            "adapter": "static",
            "discoveryStage": "generic_static",
            "weakSignal": True,
            "manualOnly": True,
            "evidenceScore": 24,
            "evidenceTypes": ["careers_keyword"],
        },
        source_state_rows={
            "Previously Productive (Manual Website)": {
                "lastDurationMs": 22000,
                "lastKeptCount": 3,
                "lastDetailPagesVisited": 6,
                "lastDetailYieldPct": 25,
            }
        },
        thresholds=sd.DEFAULT_DISCOVERY_THRESHOLDS,
    )
    assert strong_reason == ""
    assert productive_reason == ""


def test_classify_static_suppression_suppresses_weak_repeat_low_yield_static_candidate() -> None:
    reason = sd.classify_static_suppression(
        {
            "name": "Weak Static (Manual Website)",
            "studio": "Weak Static",
            "adapter": "static",
            "discoveryStage": "generic_static",
            "weakSignal": True,
            "manualOnly": True,
            "evidenceScore": 26,
            "evidenceTypes": ["careers_keyword"],
        },
        source_state_rows={
            "Weak Static (Manual Website)": {
                "lastDurationMs": 32000,
                "lastKeptCount": 0,
                "lastDetailPagesVisited": 14,
                "lastDetailYieldPct": 0,
                "lastCandidateLinksFound": 12,
            }
        },
        thresholds=sd.DEFAULT_DISCOVERY_THRESHOLDS,
    )
    assert reason == "manual_only_repeat_low_yield"


def test_web_audit_seed_careers_builds_static_candidate_without_web_search() -> None:
    with override_discovery_config(
        studio_seeds=[
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "careersUrl": "https://example.com/careers",
            }
        ]
    ):
        providers, static_rows, failures = web_audit_rows(
            name="candidate-generation-seed-static",
            studio_seeds=discovery_config_module.STUDIO_SEEDS,
            fetcher=lambda *_: (
                """
            <a href="/jobs/rendering-engineer">Rendering Engineer</a>
            <a href="/jobs/gameplay-engineer">Gameplay Engineer</a>
            """
            ),
            include_seed_careers=True,
            include_web_search=False,
        )

    assert len(failures) == 0
    assert len(providers) == 0
    assert len(static_rows) == 1
    assert str(static_rows[0].get("adapter") or "") == "static"
    assert str(static_rows[0].get("discoveryMethod") or "") == "seed_careers_page"


def test_web_audit_seed_careers_infers_provider_without_web_search() -> None:
    with override_discovery_config(
        studio_seeds=[
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "careersUrl": "https://example.com/careers",
            }
        ]
    ):
        providers, static_rows, failures = web_audit_rows(
            name="candidate-generation-seed-provider",
            studio_seeds=discovery_config_module.STUDIO_SEEDS,
            fetcher=lambda *_: (
                '<a href="https://boards.greenhouse.io/example-studio/jobs/123">Job</a>'
            ),
            include_seed_careers=True,
            include_web_search=False,
        )

    assert len(failures) == 0
    assert len(static_rows) == 0
    assert len(providers) == 1
    assert str(providers[0].get("adapter") or "") == "greenhouse"
    assert str(providers[0].get("discoveryMethod") or "") == "seed_careers_page"


def test_web_audit_seed_careers_prefers_explicit_careers_links() -> None:
    with override_discovery_config(
        studio_seeds=[
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "careersUrl": "https://example.com/",
            }
        ]
    ):
        providers, static_rows, failures = web_audit_rows(
            name="candidate-generation-seed-explicit-careers",
            studio_seeds=discovery_config_module.STUDIO_SEEDS,
            fetcher=lambda *_: (
                """
            <a href="/careers">Careers</a>
            <a href="/jobs/rendering-engineer">Rendering Engineer</a>
            """
            ),
            include_seed_careers=True,
            include_web_search=False,
        )

    assert len(failures) == 0
    assert providers == []
    assert len(static_rows) == 1
    assert str(static_rows[0].get("careersUrl") or "") == "https://example.com/careers"
    assert str(static_rows[0].get("name") or "") == "Example Studio (Manual Website)"


def test_web_audit_seed_careers_prefers_personio_provider_over_static() -> None:
    with override_discovery_config(
        studio_seeds=[
            {
                "studio": "Example Studio",
                "aliases": ["example-studio"],
                "nlPriority": False,
                "careersUrl": "https://example.jobs.personio.de/",
            }
        ]
    ):

        def should_not_fetch(*_: object) -> str:
            raise AssertionError("direct provider seed URLs should not be fetched")

        providers, static_rows, failures = web_audit_rows(
            name="candidate-generation-seed-personio",
            studio_seeds=discovery_config_module.STUDIO_SEEDS,
            fetcher=should_not_fetch,
            include_seed_careers=True,
            include_web_search=False,
        )

    assert len(failures) == 0
    assert len(providers) == 1
    assert len(static_rows) == 0
    assert str(providers[0].get("adapter") or "") == "personio"


def test_web_audit_web_search_prefers_explicit_careers_links_from_result_pages() -> None:
    studio_seeds = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
        }
    ]

    def fake_fetch(url: str, _: int) -> str:
        if "duckduckgo.com" in url:
            return '<a href="https://example.com/jobs">Example Studio</a>'
        if url == "https://example.com/jobs":
            return """
            <a href="/careers">Careers</a>
            <a href="/jobs/rendering-engineer">Rendering Engineer</a>
            """
        raise RuntimeError(f"unexpected URL: {url}")

    providers, static_rows, failures = web_audit_rows(
        name="candidate-generation-web-explicit-careers",
        studio_seeds=studio_seeds,
        fetcher=fake_fetch,
        include_seed_careers=False,
        include_web_search=True,
        max_queries=1,
    )
    assert failures == []
    assert providers == []
    assert len(static_rows) == 1
    assert str(static_rows[0].get("careersUrl") or "") == "https://example.com/careers"
    assert str(static_rows[0].get("discoveryMethod") or "") == "web_search"


def test_extract_explicit_careers_url_from_page_prefers_landing_page_over_job_detail_links() -> (
    None
):
    html = """
    <a href="/jobs/rendering-engineer">Rendering Engineer</a>
    <a href="/careers">Careers</a>
    """
    careers_url = sd.extract_explicit_careers_url_from_page(
        "https://studio.example.com/",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="gamesmap",
    )
    assert careers_url == "https://studio.example.com/careers"


def test_extract_explicit_careers_url_from_page_skips_provider_and_offsite_links() -> None:
    html = """
    <a href="https://boards.greenhouse.io/example-studio">Greenhouse</a>
    <a href="https://external.example.net/careers">External Careers</a>
    <a href="/careers">Careers</a>
    """
    careers_url = sd.extract_explicit_careers_url_from_page(
        "https://studio.example.com/",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="gamesmap",
    )
    assert careers_url == "https://studio.example.com/careers"


def test_infer_provider_candidates_from_html_collapses_competing_seed_page_variants() -> None:
    html = """
    <a href="https://boards.greenhouse.io/first-board/jobs/123">Job A</a>
    <a href="https://boards.greenhouse.io/second-board/jobs/456">Job B</a>
    """
    rows = sd.infer_provider_candidates_from_html(
        "https://example.com/careers",
        html,
        studio="Example Studio",
        nl_priority=False,
        discovery_method="seed_careers_page",
    )
    assert len(rows) == 1
    assert str(rows[0].get("adapter") or "") == "greenhouse"


def test_infer_provider_candidates_from_html_detects_embedded_urls() -> None:
    html = """
    <a href="https://boards.greenhouse.io/example/jobs/123">Job</a>
    <script>const api='https://api.lever.co/v0/postings/example?mode=json';</script>
    """
    rows = sd.infer_provider_candidates_from_html(
        "https://example.com/careers",
        html,
        studio="Example Studio",
        nl_priority=False,
    )
    adapters = {str(row.get("adapter") or "") for row in rows}
    assert "greenhouse" in adapters
    assert "lever" in adapters


def test_infer_provider_candidates_from_html_detects_pinpoint_provider_from_page_url() -> None:
    rows = sd.infer_provider_candidates_from_html(
        "https://example.pinpointhq.com/",
        "<html><body>Careers</body></html>",
        studio="Example Studio",
        nl_priority=False,
        discovery_method="seed_careers_page",
    )
    assert len(rows) == 1
    assert str(rows[0].get("adapter") or "") == "pinpoint"
    assert str(rows[0].get("evidenceSource") or "") == "page_url"


def test_infer_provider_candidates_from_html_detects_provider_from_page_url() -> None:
    rows = sd.infer_provider_candidates_from_html(
        "https://example.jobs.personio.de/",
        "<html><body>Careers</body></html>",
        studio="Example Studio",
        nl_priority=False,
        discovery_method="seed_careers_page",
    )
    assert len(rows) == 1
    assert str(rows[0].get("adapter") or "") == "personio"
    assert str(rows[0].get("evidenceSource") or "") == "page_url"


def test_probe_candidate_maps_jobs_found_for_greenhouse_and_teamtailor() -> None:
    greenhouse = {
        "adapter": "greenhouse",
        "slug": "example",
        "api_url": "https://boards-api.greenhouse.io/v1/boards/example/jobs?content=true",
    }
    ok, count, error = sd.probe_candidate(
        greenhouse,
        timeout_s=5,
        fetcher=lambda *_: (
            '{"jobs":[{"id":1,"title":"Gameplay Role 1","absolute_url":"https://job-boards.greenhouse.io/example/jobs/1"},{"id":2,"title":"Gameplay Role 2","absolute_url":"https://job-boards.greenhouse.io/example/jobs/2"}]}'
        ),
    )
    assert ok
    assert count == 2
    assert error == ""
    teamtailor = {"adapter": "teamtailor", "listing_url": "https://example.teamtailor.com/jobs"}
    html = """
    <a href="https://example.teamtailor.com/jobs/123-role-a">A</a>
    <a href="https://example.teamtailor.com/jobs/456-role-b">B</a>
    """
    ok, count, error = sd.probe_candidate(teamtailor, timeout_s=5, fetcher=lambda *_: html)
    assert ok
    assert count == 2
    assert error == ""


def test_probe_candidate_maps_jobs_found_for_recruitee_and_pinpoint() -> None:
    recruitee = {
        "adapter": "recruitee",
        "subdomain": "example",
        "api_url": "https://example.recruitee.com/api/offers/",
    }
    ok, count, error = sd.probe_candidate(
        recruitee,
        timeout_s=5,
        fetcher=lambda *_: json.dumps({"offers": [{}, {}]}),
    )
    assert ok
    assert count == 2
    assert error == ""

    pinpoint = {
        "adapter": "pinpoint",
        "subdomain": "gameplaygalaxy",
        "api_url": "https://gameplaygalaxy.pinpointhq.com/postings.json",
    }
    ok, count, error = sd.probe_candidate(
        pinpoint,
        timeout_s=5,
        fetcher=lambda *_: json.dumps({"data": [{}, {}, {}]}),
    )
    assert ok
    assert count == 3
    assert error == ""


def test_probe_candidate_uses_fallback_when_primary_fails() -> None:
    greenhouse = {
        "adapter": "greenhouse",
        "slug": "example",
        "api_url": "https://boards-api.greenhouse.io/v1/boards/example/jobs?content=true",
    }

    def fake_fetch(url: str, _: int) -> str:
        if "boards-api.greenhouse.io" in url:
            raise RuntimeError("HTTP Error 404: Not Found")
        if "boards.greenhouse.io/example" in url:
            return '<a href="https://boards.greenhouse.io/example/jobs/123">Role</a>'
        raise RuntimeError(f"unexpected URL: {url}")

    ok, count, error = sd.probe_candidate(greenhouse, timeout_s=5, fetcher=fake_fetch)
    assert ok
    assert count == 1
    assert error == ""


def test_validate_candidate_for_probe_rejects_invalid_identity() -> None:
    valid, reason = sd.validate_candidate_for_probe({"adapter": "lever", "account": "12"})
    assert not valid
    assert "invalid" in reason
