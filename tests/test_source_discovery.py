import json
import sys
from pathlib import Path
from unittest import mock

from src import source_discovery as sd
from tests.helpers.temp_paths import workspace_tmpdir


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _fixture_json(name: str):
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def _fixture_text(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


def test_build_pattern_candidates_respects_likely_providers() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": True,
            "likelyProviders": ["greenhouse", "teamtailor"],
        }
    ]
    try:
        rows = sd.build_pattern_candidates()
    finally:
        sd.STUDIO_SEEDS = previous

    adapters = {str(row.get("adapter")) for row in rows}
    assert adapters == {"greenhouse", "teamtailor"}


def test_build_pattern_candidates_adds_reinforcement_for_provider_matching_careers_url() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "likelyProviders": ["greenhouse"],
            "careersUrl": "https://boards.greenhouse.io/example-studio",
        }
    ]
    try:
        rows = sd.build_pattern_candidates()
    finally:
        sd.STUDIO_SEEDS = previous
    assert len(rows) >= 1
    assert all(int(row.get("evidenceScore") or 0) >= 42 for row in rows)
    assert all("provider_reinforced" in (row.get("evidenceTypes") or []) for row in rows)


def test_probe_candidate_maps_jobs_found_for_greenhouse_and_teamtailor() -> None:
    greenhouse = {
        "adapter": "greenhouse",
        "slug": "example",
        "api_url": "https://boards-api.greenhouse.io/v1/boards/example/jobs?content=true",
    }
    ok, count, error = sd.probe_candidate(greenhouse, timeout_s=5, fetcher=lambda *_: json.dumps({"jobs": [{}, {}]}))
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


def test_discover_seed_careers_page_candidates_infers_provider_without_web_search() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "careersUrl": "https://example.com/careers",
        }
    ]
    try:
        providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
            5,
            fetcher=lambda *_: '<a href="https://boards.greenhouse.io/example-studio/jobs/123">Job</a>',
        )
    finally:
        sd.STUDIO_SEEDS = previous

    assert len(failures) == 0
    assert len(static_rows) == 0
    assert len(providers) == 1
    assert str(providers[0].get("adapter") or "") == "greenhouse"
    assert str(providers[0].get("discoveryMethod") or "") == "seed_careers_page"

def test_discover_seed_careers_page_candidates_prefers_personio_provider_over_static() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "careersUrl": "https://example.jobs.personio.de/",
        }
    ]
    try:
        providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
            5,
            fetcher=lambda *_: '<a href="/position/artist">Artist</a>',
        )
    finally:
        sd.STUDIO_SEEDS = previous

    assert len(failures) == 0
    assert len(providers) == 1
    assert len(static_rows) == 0
    assert str(providers[0].get("adapter") or "") == "personio"

def test_discover_seed_careers_page_candidates_builds_static_candidate_without_web_search() -> None:
    previous = list(sd.STUDIO_SEEDS)
    sd.STUDIO_SEEDS = [
        {
            "studio": "Example Studio",
            "aliases": ["example-studio"],
            "nlPriority": False,
            "careersUrl": "https://example.com/careers",
        }
    ]
    try:
        providers, static_rows, failures = sd.discover_seed_careers_page_candidates(
            5,
            fetcher=lambda *_: """
            <a href="/jobs/rendering-engineer">Rendering Engineer</a>
            <a href="/jobs/gameplay-engineer">Gameplay Engineer</a>
            """,
        )
    finally:
        sd.STUDIO_SEEDS = previous

    assert len(failures) == 0
    assert len(providers) == 0
    assert len(static_rows) == 1
    assert str(static_rows[0].get("adapter") or "") == "static"
    assert str(static_rows[0].get("discoveryMethod") or "") == "seed_careers_page"

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

def test_build_static_candidate_from_page_blocks_linkedin_like_domains() -> None:
    row = sd.build_static_candidate_from_page(
        "https://www.linkedin.com/company/example/jobs/",
        '<a href="/jobs/test">Test</a>',
        studio="Example Studio",
        nl_priority=False,
        discovery_method="web_search",
    )
    assert row is None


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

def test_parse_gamesmap_detail_page_supports_website_only_entries() -> None:
    row = sd.parse_gamesmap_detail_page(
        "https://www.gamesmap.de/en/detail/industry/example-publisher",
        _fixture_text("gamesmap_detail_website_only.html"),
    )
    assert row is not None
    assert str(row.get("careersUrl") or "") == ""
    assert str(row.get("websiteUrl") or "") == "https://www.example-publisher.com"
    assert "Publisher" in (row.get("categories") or [])

def test_parse_gamesmap_detail_page_ignores_directory_and_social_links_for_website_fallback() -> None:
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

def test_parse_gamesmap_index_entries_extracts_industry_rows_from_js_payload() -> None:
    rows = sd.parse_gamesmap_index_entries(
        _fixture_text("gamesmap_index.html"),
        "https://www.gamesmap.de",
        prefer_english=True,
    )
    assert len(rows) == 3
    assert str(rows[0].get("detailUrl") or "") == "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh"
    assert str(rows[0].get("studio") or "") == "Example Studio GmbH"
    assert str(rows[0].get("location") or "") == "Hamburg"

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

def test_discover_gamesmap_candidates_emits_provider_and_static_rows() -> None:
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": True,
            "maxDetailPages": 10,
            "allowedCategoryTokens": ["developer", "publisher", "mobile", "pc", "console"],
            "blockedCategoryTokens": ["association", "education"],
        }
    }

    payloads = {
        "https://www.gamesmap.de/en": _fixture_text("gamesmap_index.html"),
        "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh": _fixture_text("gamesmap_detail_careers.html"),
        "https://www.gamesmap.de/en/detail/industry/tooling-association": _fixture_text("gamesmap_detail_blocked.html"),
        "https://www.gamesmap.de/en/detail/industry/example-publisher": _fixture_text("gamesmap_detail_website_only.html"),
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(5, config=config, fetcher=fake_fetch)
    assert len(failures) == 0
    assert len(provider_rows) == 1
    assert str(provider_rows[0].get("adapter") or "") == "greenhouse"
    assert str(provider_rows[0].get("discoveryMethod") or "") == "gamesmap"
    assert str(provider_rows[0].get("sourceDirectory") or "") == "gamesmap"
    assert len(static_rows) == 1
    assert str(static_rows[0].get("adapter") or "") == "static"
    assert bool(static_rows[0].get("weakSignal"))
    assert str(static_rows[0].get("sourceDirectoryEntryUrl") or "") == "https://www.gamesmap.de/en/detail/industry/example-publisher"
    assert not (bool(static_rows[0].get("manualOnly")))

def test_discover_gamesmap_candidates_marks_manual_website_only_rows() -> None:
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": True,
            "websiteOnlyManualOnly": True,
            "maxDetailPages": 10,
            "allowedCategoryTokens": ["publisher"],
            "blockedCategoryTokens": ["association", "education"],
        }
    }
    payloads = {
        "https://www.gamesmap.de/en": _fixture_text("gamesmap_index.html"),
        "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh": _fixture_text("gamesmap_detail_careers.html"),
        "https://www.gamesmap.de/en/detail/industry/tooling-association": _fixture_text("gamesmap_detail_blocked.html"),
        "https://www.gamesmap.de/en/detail/industry/example-publisher": _fixture_text("gamesmap_detail_website_only.html"),
    }

    def fake_fetch(url: str, _: int) -> str:
        if url not in payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return payloads[url]

    _provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(5, config=config, fetcher=fake_fetch)
    assert len(failures) == 0
    assert len(static_rows) == 1
    assert bool(static_rows[0].get("weakSignal"))
    assert bool(static_rows[0].get("manualOnly"))
    assert "gamesmap_manual_website_only" in (static_rows[0].get("evidenceTypes") or [])

def test_discover_gamesmap_candidates_dedupes_repeated_directory_entries() -> None:
    html = """
    <a href="/en/detail/industry/example-studio-gmbh">Example Studio</a>
    <a href="/detail/industry/example-studio-gmbh">Example Studio duplicate</a>
    """
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": False,
            "maxDetailPages": 10,
            "allowedCategoryTokens": ["developer"],
            "blockedCategoryTokens": [],
        }
    }

    def fake_fetch(url: str, _: int) -> str:
        if url == "https://www.gamesmap.de/en":
            return html
        return _fixture_text("gamesmap_detail_careers.html")

    provider_rows, static_rows, _failures = sd.discover_gamesmap_candidates(5, config=config, fetcher=fake_fetch)
    assert len(provider_rows) == 1
    assert len(static_rows) == 0

def test_run_discovery_gamesmap_candidates_flow_into_report_and_queue() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []

            config = {
                "gamesmap": {
                    "enabled": True,
                    "baseUrl": "https://www.gamesmap.de",
                    "indexUrls": ["https://www.gamesmap.de/en"],
                    "websiteOnlyFallback": False,
                    "maxDetailPages": 10,
                    "allowedCategoryTokens": ["developer", "publisher", "pc", "console"],
                    "blockedCategoryTokens": ["association", "education"],
                }
            }
            payloads = {
                "https://www.gamesmap.de/en": _fixture_text("gamesmap_index.html"),
                "https://www.gamesmap.de/en/detail/industry/example-studio-gmbh": _fixture_text("gamesmap_detail_careers.html"),
                "https://www.gamesmap.de/en/detail/industry/tooling-association": _fixture_text("gamesmap_detail_blocked.html"),
                "https://www.gamesmap.de/en/detail/industry/example-publisher": _fixture_text("gamesmap_detail_website_only.html"),
                "https://boards-api.greenhouse.io/v1/boards/examplestudio/jobs?content=true": json.dumps({"jobs": [{}, {}]}),
            }

            def fake_fetch(url: str, _: int) -> str:
                if url not in payloads:
                    raise RuntimeError(f"unexpected URL: {url}")
                return payloads[url]

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config=config,
                fetcher=fake_fetch,
            )
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert int((report["summary"].get("generatedCountByStage") or {}).get("web_provider") or 0) == 1
            queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "gamesmap"
            assert str(queued[0].get("sourceDirectory") or "") == "gamesmap"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds

def test_run_discovery_dynamic_tracks_stage_metrics_and_queue_contract() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {
                    "name": "Demo Lever",
                    "studio": "Demo",
                    "adapter": "lever",
                    "account": "demo",
                    "api_url": "https://api.lever.co/v0/postings/demo?mode=json",
                    "nlPriority": True,
                },
                {
                    "name": "Demo Greenhouse",
                    "studio": "Demo",
                    "adapter": "greenhouse",
                    "slug": "demo",
                    "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true",
                    "nlPriority": True,
                },
            ]

            def fake_fetch(url: str, _: int) -> str:
                if "api.lever.co" in url:
                    return json.dumps([{"id": 1}, {"id": 2}, {"id": 3}])
                if "boards-api.greenhouse.io" in url:
                    return json.dumps({"jobs": [{}, {}]})
                raise RuntimeError(f"unexpected URL: {url}")

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}},
                fetcher=fake_fetch,
            )
            summary = report["summary"]
            assert int(summary.get("foundEndpointCount") or 0) == 2
            assert int(summary.get("probedCandidateCount") or 0) == 2
            assert int(summary.get("queuedCandidateCount") or 0) == 2
            assert "generatedCountByStage" in summary
            assert "queuedCountByStage" in summary
            assert "lossAccounting" in summary
            assert int((summary.get("lossAccounting") or {}).get("generated") or 0) == 2
            assert int((summary.get("lossAccounting") or {}).get("queued") or 0) == 2
            assert int((summary.get("queuedCountByStage") or {}).get("curated_seed") or 0) == 2

            queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
            assert len(queued) == 2
            for row in queued:
                assert "evidenceScore" in row
                assert "evidenceTypes" in row
                assert "discoveryStage" in row
                assert not (bool(row.get("deferred")))
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds

def test_run_discovery_emits_phase_logs_for_candidate_generation() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = []

            with mock.patch.object(sd, "emit_log") as emit_log_mock:
                report = sd.run_discovery(
                    timeout_s=5,
                    top_n=0,
                    mode="dynamic",
                    include_web_search=False,
                    discovery_config={"gamesmap": {"enabled": False}},
                    fetcher=lambda *_: "",
                )

            messages = [str(call.args[0]) for call in emit_log_mock.call_args_list if call.args]
            assert any("Generating curated seed candidates" in message for message in messages)
            assert any("Generating provider-pattern candidates" in message for message in messages)
            assert any("Scanning known careers pages" in message for message in messages)
            assert any("Starting probe phase" in message for message in messages)
            assert str((report.get("summary") or {}).get("phase") or "") == "completed"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds

def test_run_discovery_skips_duplicate_endpoint_fingerprints() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {"name": "Demo Lever A", "studio": "Demo", "adapter": "lever", "account": "demo", "api_url": "https://api.lever.co/v0/postings/demo?mode=json"},
                {"name": "Demo Lever A Duplicate", "studio": "Demo", "adapter": "lever", "account": "demo2", "api_url": "https://api.lever.co/v0/postings/demo?mode=json", "discoveryMethod": "pattern"},
            ]
            report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=lambda *_: json.dumps([{"id": 1}]))
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert int(report["summary"].get("skippedDuplicateCount") or 0) == 1
            assert "duplicateReasons" in report["summary"]
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds

def test_run_discovery_balances_queue_with_deferrals() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        prev_caps = dict(sd.ADAPTER_QUEUE_CAPS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STUDIO_SEEDS = []
            sd.ADAPTER_QUEUE_CAPS["lever"] = 1
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {"name": "Demo Lever A", "studio": "Demo A", "adapter": "lever", "account": "demoa", "api_url": "https://api.lever.co/v0/postings/demoa?mode=json"},
                {"name": "Demo Lever B", "studio": "Demo B", "adapter": "lever", "account": "demob", "api_url": "https://api.lever.co/v0/postings/demob?mode=json"},
            ]

            report = sd.run_discovery(timeout_s=5, top_n=0, mode="dynamic", include_web_search=False, fetcher=lambda *_: json.dumps([{"id": 1}, {"id": 2}]))
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert int(report["summary"].get("discoverableButDeferredCount") or 0) == 1
            assert int((report["summary"].get("lossAccounting") or {}).get("deferredByCap") or 0) == 1
            deferred = [row for row in (report.get("candidates") or []) if bool(row.get("deferred"))]
            assert len(deferred) == 1
            assert str(deferred[0].get("deferReason") or "") == "adapter_cap"
            assert str(deferred[0].get("dropStage") or "") == "deferred_by_cap"
            assert str(deferred[0].get("dropReason") or "") == "adapter_cap"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds
            sd.ADAPTER_QUEUE_CAPS.clear()
            sd.ADAPTER_QUEUE_CAPS.update(prev_caps)

def test_run_discovery_pattern_candidates_below_reinforced_threshold_are_skipped() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STATIC_DISCOVERY_CANDIDATES = []
            sd.STUDIO_SEEDS = [
                {
                    "studio": "Example Studio",
                    "aliases": ["example-studio"],
                    "nlPriority": False,
                    "likelyProviders": ["teamtailor"],
                    "careersUrl": "https://example.com/careers",
                }
            ]
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"thresholds": {"patternProviderProbeThreshold": 32}},
                fetcher=lambda *_: json.dumps({"jobs": [{}]}),
            )
            assert int(report["summary"].get("probedCandidateCount") or 0) == 0
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 0
            assert int((report["summary"].get("lossAccounting") or {}).get("lowEvidenceSkipped") or 0) == 1
            stages = [str(row.get("stage") or "") for row in (report.get("failures") or [])]
            assert "probe_skipped" in stages
            dropped = [row for row in (report.get("failures") or []) if str(row.get("dropStage") or "") == "low_evidence_skipped"]
            assert dropped
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds

def test_run_discovery_tracks_probe_miss_separately_from_failures() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {"name": "Demo Lever", "studio": "Demo", "adapter": "lever", "account": "demo", "api_url": "https://api.lever.co/v0/postings/demo?mode=json"}
            ]
            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}},
                fetcher=lambda *_a, **_k: (_ for _ in ()).throw(RuntimeError("HTTP Error 404: Not Found")),
            )
            assert int(report["summary"].get("probedCandidateCount") or 0) == 1
            assert int(report["summary"].get("failedProbeCount") or 0) == 0
            assert int(report["summary"].get("probeMissCount") or 0) == 1
            assert str((report.get("failures") or [])[0].get("stage") or "") == "probe_miss"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds

def test_run_discovery_uses_seed_careers_pages_without_web_search() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STATIC_DISCOVERY_CANDIDATES = []
            sd.STUDIO_SEEDS = [
                {
                    "studio": "Example Studio",
                    "aliases": ["example-studio"],
                    "nlPriority": False,
                    "likelyProviders": ["teamtailor"],
                    "careersUrl": "https://example.com/careers",
                }
            ]

            def fake_fetch(url: str, _: int) -> str:
                if url == "https://example.com/careers":
                    return '<a href="https://boards.greenhouse.io/example-studio/jobs/123">Job</a>'
                if "boards-api.greenhouse.io" in url:
                    return json.dumps({"jobs": [{}, {}]})
                raise RuntimeError(f"unexpected URL: {url}")

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}},
                fetcher=fake_fetch,
            )
            assert int(report["summary"].get("queuedCandidateCount") or 0) == 1
            assert int((report["summary"].get("queuedCountByStage") or {}).get("web_provider") or 0) == 1
            assert int((report["summary"].get("generatedCountByStage") or {}).get("web_provider") or 0) == 1
            assert int((report["summary"].get("generatedCountByStage") or {}).get("generic_static") or 0) == 0
            queued = json.loads(sd.DISCOVERY_CANDIDATES_PATH.read_text(encoding="utf-8"))
            assert len(queued) == 1
            assert str(queued[0].get("discoveryMethod") or "") == "seed_careers_page"
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds

def test_discovery_report_snapshot_contract() -> None:
    with workspace_tmpdir("source-discovery") as root:
        prev_paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
        )
        prev_static = list(sd.STATIC_DISCOVERY_CANDIDATES)
        prev_seeds = list(sd.STUDIO_SEEDS)
        try:
            sd.ACTIVE_PATH = root / "active.json"
            sd.PENDING_PATH = root / "pending.json"
            sd.REJECTED_PATH = root / "rejected.json"
            sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
            sd.DISCOVERY_REPORT_PATH = root / "report.json"
            sd.STUDIO_SEEDS = []
            sd.STATIC_DISCOVERY_CANDIDATES = [
                {"name": "Demo Lever", "studio": "Demo", "adapter": "lever", "account": "demo", "api_url": "https://api.lever.co/v0/postings/demo?mode=json"},
                {"name": "Demo Greenhouse", "studio": "Demo", "adapter": "greenhouse", "slug": "demo", "api_url": "https://boards-api.greenhouse.io/v1/boards/demo/jobs?content=true"},
            ]

            def fake_fetch(url: str, _: int) -> str:
                if "api.lever.co" in url:
                    return json.dumps([{"id": 1}, {"id": 2}])
                if "boards-api.greenhouse.io" in url:
                    return json.dumps({"jobs": [{}]})
                raise RuntimeError(f"unexpected URL: {url}")

            report = sd.run_discovery(
                timeout_s=5,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}},
                fetcher=fake_fetch,
            )
            snapshot = {
                "schemaVersion": report.get("schemaVersion"),
                "mode": str(report.get("mode")),
                "summary": {
                    "foundEndpointCount": int(report["summary"].get("foundEndpointCount") or 0),
                    "probedCandidateCount": int(report["summary"].get("probedCandidateCount") or 0),
                    "queuedCandidateCount": int(report["summary"].get("queuedCandidateCount") or 0),
                    "discoverableButDeferredCount": int(report["summary"].get("discoverableButDeferredCount") or 0),
                    "failedProbeCount": int(report["summary"].get("failedProbeCount") or 0),
                },
                "counts": {
                    "candidates": len(report.get("candidates") or []),
                    "failures": len(report.get("failures") or []),
                },
                "adapterCounts": report["summary"].get("adapterCounts") or {},
                "methodCounts": report["summary"].get("methodCounts") or {},
                "generatedCountByStage": report["summary"].get("generatedCountByStage") or {},
            }
            assert snapshot == _fixture_json("source_discovery_report_snapshot.json")
        finally:
            (
                sd.ACTIVE_PATH,
                sd.PENDING_PATH,
                sd.REJECTED_PATH,
                sd.DISCOVERY_CANDIDATES_PATH,
                sd.DISCOVERY_REPORT_PATH,
            ) = prev_paths
            sd.STATIC_DISCOVERY_CANDIDATES = prev_static
            sd.STUDIO_SEEDS = prev_seeds

def test_parse_args_supports_manual_gamesmap_mode() -> None:
    prev_argv = list(sys.argv)
    try:
        sys.argv = [
            "source_discovery.py",
            "--gamesmap-website-only-fallback",
            "--gamesmap-max-detail-pages",
            "25",
        ]
        args = sd.parse_args()
    finally:
        sys.argv = prev_argv
    assert bool(args.gamesmap_website_only_fallback)
    assert int(args.gamesmap_max_detail_pages or 0) == 25

def test_load_discovery_config_uses_configured_path() -> None:
    with workspace_tmpdir("source-discovery") as root:
        previous_path = sd.DISCOVERY_CONFIG_PATH
        try:
            sd.DISCOVERY_CONFIG_PATH = root / "nested" / "discovery.json"
            sd.DISCOVERY_CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
            sd.DISCOVERY_CONFIG_PATH.write_text(
                json.dumps({"gamesmap": {"enabled": True, "maxDetailPages": 25}}),
                encoding="utf-8",
            )
            cfg = sd.load_discovery_config()
        finally:
            sd.DISCOVERY_CONFIG_PATH = previous_path
        assert bool((cfg.get("gamesmap") or {}).get("enabled"))
        assert int((cfg.get("gamesmap") or {}).get("maxDetailPages") or 0) == 25

def test_resolve_discovery_thresholds_overrides_defaults() -> None:
    thresholds = sd.resolve_discovery_thresholds(
        {
            "thresholds": {
                "minProviderEvidenceToProbe": 7,
                "patternProviderQueueThreshold": 55,
            }
        }
    )
    assert int(thresholds.get("minProviderEvidenceToProbe") or 0) == 7
    assert int(thresholds.get("patternProviderQueueThreshold") or 0) == 55
    assert int(thresholds.get("minStaticEvidenceToQueue") or 0) == int(
        sd.DEFAULT_DISCOVERY_THRESHOLDS["minStaticEvidenceToQueue"]
    )



