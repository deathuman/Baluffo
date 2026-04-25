import argparse
import json
from pathlib import Path

import pytest

from src.source_discovery import probe, stage_control, url_patches


def test_stage_control_applies_all_cli_overrides_and_stage_defaults() -> None:
    args = argparse.Namespace(
        gamesmap_website_only_fallback=True,
        gamesmap_max_detail_pages=12,
        gamedevmap_enabled=True,
        gamedevmap_max_rows=25,
        gamedevmap_max_homepage_fetches=7,
        only_gamedevmap=False,
        gameprog_enabled=True,
        gameprog_max_studios=9,
        gameprog_website_only_fallback=True,
    )

    cfg = stage_control.apply_discovery_cli_args_to_config(
        {"stageToggles": {"webSearch": False}}, args
    )

    assert cfg["gamesmap"] == {
        "websiteOnlyFallback": True,
        "websiteOnlyManualOnly": True,
        "maxDetailPages": 12,
    }
    assert cfg["gamedevmap"] == {"enabled": True, "maxRows": 25, "maxHomepageFetches": 7}
    assert cfg["gameprog"] == {
        "enabled": True,
        "maxStudios": 9,
        "websiteOnlyFallback": True,
    }
    assert stage_control.discovery_stage_enabled(cfg, "webSearch") is False
    assert stage_control.discovery_stage_enabled(cfg, "missing", default=False) is False
    assert stage_control.discovery_stage_enabled(None, "missing", default=True) is True
    assert (
        stage_control.discovery_stage_enabled({"stageToggles": []}, "missing", default=False)
        is False
    )


def test_stage_control_only_gamedevmap_replaces_stage_toggles() -> None:
    args = argparse.Namespace(
        gamesmap_website_only_fallback=False,
        gamesmap_max_detail_pages=0,
        gamedevmap_enabled=False,
        gamedevmap_max_rows=0,
        gamedevmap_max_homepage_fetches=0,
        only_gamedevmap=True,
        gameprog_enabled=False,
        gameprog_max_studios=0,
        gameprog_website_only_fallback=False,
    )

    cfg = stage_control.apply_discovery_cli_args_to_config({"gamedevmap": {"maxRows": 3}}, args)

    assert cfg["stageToggles"] == {
        "curatedSeed": False,
        "sheetDirectory": False,
        "providerPatterns": False,
        "seedCareersScan": False,
        "gamesmap": False,
        "gameprog": False,
        "gamedevmap": True,
        "webSearch": False,
    }
    assert cfg["gamedevmap"] == {"maxRows": 3, "enabled": True}


def test_probe_validation_and_fallback_urls_cover_provider_branches() -> None:
    validation_cases = [
        ({"adapter": "smartrecruiters", "company_id": "123"}, False, "invalid company"),
        ({"adapter": "smartrecruiters", "company_id": "studio123"}, True, ""),
        (
            {"adapter": "personio", "feed_url": "https://example.com/jobs.xml"},
            False,
            "invalid personio",
        ),
        (
            {"adapter": "personio", "feed_url": "https://demo.jobs.personio.de/xml"},
            True,
            "",
        ),
        ({"adapter": "teamtailor", "listing_url": "https://example.com/jobs"}, True, ""),
        ({"adapter": "ashby", "board_url": "https://example.com/jobs"}, False, "invalid ashby"),
        ({"adapter": "static", "pages": ["https://example.com/careers"]}, True, ""),
        ({"adapter": "static", "pages": []}, False, "invalid static"),
    ]

    for candidate, expected_valid, expected_reason in validation_cases:
        valid, reason = probe.validate_candidate_for_probe(candidate)
        assert valid is expected_valid
        assert expected_reason in reason

    assert probe.fallback_probe_urls({"adapter": "lever", "account": "demo"}) == [
        "https://jobs.lever.co/demo"
    ]
    assert probe.fallback_probe_urls({"adapter": "smartrecruiters", "company_id": "Demo"}) == [
        "https://jobs.smartrecruiters.com/Demo"
    ]
    assert probe.fallback_probe_urls({"adapter": "workable", "account": "demo"}) == [
        "https://apply.workable.com/demo"
    ]
    assert probe.fallback_probe_urls(
        {"adapter": "recruitee", "api_url": "https://demo.recruitee.com/api/offers/"}
    ) == ["https://demo.recruitee.com/"]
    assert probe.fallback_probe_urls(
        {"adapter": "pinpoint", "api_url": "https://demo.pinpointhq.com/postings.json"}
    ) == ["https://demo.pinpointhq.com/"]
    assert probe.fallback_probe_urls(
        {"adapter": "personio", "feed_url": "https://demo.jobs.personio.de/xml"}
    ) == ["https://demo.jobs.personio.de/"]


def test_parse_probe_count_covers_provider_payload_shapes() -> None:
    cases = [
        ("lever", json.dumps([{"id": 1}, {"id": 2}]), 2),
        ("lever", json.dumps({"data": [{"id": 1}]}), 1),
        (
            "greenhouse",
            '<a href="https://boards.greenhouse.io/demo/jobs/123">A</a>'
            '<a href="https://boards.greenhouse.io/demo/jobs/123">A duplicate</a>',
            1,
        ),
        (
            "smartrecruiters",
            '<a href="https://jobs.smartrecruiters.com/Demo/job/743999-game">A</a>',
            1,
        ),
        ("smartrecruiters", json.dumps({"content": [{}, {}]}), 2),
        ("workable", '<a href="https://apply.workable.com/demo/j/ABCDEF">A</a>', 1),
        ("workable", json.dumps({"jobs": [{}, {}, {}]}), 3),
        ("recruitee", '<a href="https://demo.recruitee.com/o/designer">A</a>', 1),
        ("pinpoint", '<a href="https://demo.pinpointhq.com/postings/123">A</a>', 1),
        ("personio", "<workzag-jobs><position/><position/></workzag-jobs>", 2),
        ("ashby", '<a href="https://jobs.ashbyhq.com/demo/job/123">A</a>', 1),
        ("static", '<a href="/jobs/rendering-engineer">Rendering Engineer</a>', 1),
    ]

    for adapter, payload, expected in cases:
        assert probe.parse_probe_count(adapter, payload) == expected

    with pytest.raises(ValueError, match="unsupported adapter"):
        probe.parse_probe_count("unknown", "")


def test_probe_candidate_reports_missing_invalid_and_failed_fallbacks() -> None:
    assert probe.probe_candidate({"adapter": "lever"}, timeout_s=5) == (
        False,
        0,
        "missing adapter or URL",
    )
    assert probe.probe_candidate(
        {"adapter": "greenhouse", "slug": "12", "api_url": "https://example.com/jobs"},
        timeout_s=5,
    ) == (False, 0, "invalid board slug")

    def failing_fetch(url: str, _timeout: int) -> str:
        raise RuntimeError(f"HTTP Error 404 for {url}")

    ok, count, error = probe.probe_candidate(
        {
            "adapter": "workable",
            "account": "demo",
            "api_url": "https://api.workable.com/spi/v3/accounts/demo/jobs",
        },
        timeout_s=5,
        fetcher=failing_fetch,
    )
    assert not ok
    assert count == 0
    assert "https://apply.workable.com/demo" in error


def test_static_probe_uses_sync_and_async_playwright_fallbacks() -> None:
    candidate = {"adapter": "static", "listing_url": "https://example.com/careers"}

    def blocked_fetch(_url: str, _timeout: int) -> str:
        raise RuntimeError("HTTP Error 403: challenge")

    ok, count, error = probe.probe_candidate(
        candidate,
        timeout_s=5,
        fetcher=blocked_fetch,
        try_playwright=lambda _url, _timeout: (
            '<a href="https://example.com/jobs/rendering-engineer">Role</a>',
            "",
        ),
    )
    assert (ok, count, error) == (True, 1, "")

    async def async_blocked_fetch(_url: str, _timeout: int) -> str:
        raise RuntimeError("timed out")

    async def run_async_probe() -> tuple[bool, int, str]:
        return await probe.async_probe_candidate(
            candidate,
            timeout_s=5,
            fetcher=async_blocked_fetch,
            try_playwright=lambda _url, _timeout: (
                '<a href="https://example.com/jobs/gameplay-engineer">Role</a>',
                "",
            ),
        )

    import asyncio

    assert asyncio.run(run_async_probe()) == (True, 1, "")


def test_url_patch_manifest_merge_apply_and_runtime_stats(tmp_path: Path) -> None:
    manifest_path = tmp_path / "url-patches.json"
    manifest_path.write_text(
        json.dumps(
            {
                "patches": {
                    " https://old.example/jobs/ ": " https://new.example/jobs ",
                    "": "https://ignored.example/jobs",
                    "https://bad.example/jobs": "",
                }
            }
        ),
        encoding="utf-8",
    )

    manifest = url_patches.load_url_patch_manifest(manifest_path)
    assert manifest["patches"] == {"https://old.example/jobs": "https://new.example/jobs"}
    assert manifest["_stats"]["total_patches"] == 1
    assert url_patches.load_url_patches(manifest_path) == manifest["patches"]

    merged, added, updated = url_patches.merge_url_patches(
        {"https://old.example/jobs": "https://new.example/jobs"},
        {
            "https://old.example/jobs": "https://updated.example/jobs",
            "https://added.example/jobs": "https://target.example/jobs",
            "": "https://ignored.example/jobs",
        },
    )
    assert merged == {
        "https://old.example/jobs": "https://updated.example/jobs",
        "https://added.example/jobs": "https://target.example/jobs",
    }
    assert (added, updated) == (1, 1)

    saved = url_patches.save_url_patch_manifest(
        merged, path=manifest_path, added=-1, updated=2, reprobed=3
    )
    assert saved["_stats"] == {"total_patches": 2, "added": 0, "updated": 2, "reprobed": 3}

    candidate, changed = url_patches.apply_url_patches_to_candidate(
        {
            "listing_url": "https://old.example/jobs",
            "pages": ["https://old.example/jobs", "https://untouched.example/jobs"],
        },
        {"https://old.example/jobs": "https://updated.example/jobs"},
    )
    assert changed
    assert candidate["listing_url"] == "https://updated.example/jobs"
    assert candidate["pages"] == ["https://updated.example/jobs", "https://untouched.example/jobs"]
    assert candidate["urlPatchApplied"] is True
    assert url_patches.apply_url_patches_to_candidate({"listing_url": "https://x.example"}, {}) == (
        {"listing_url": "https://x.example"},
        False,
    )

    assert url_patches.summarize_url_patch_runtime(loaded=-1, added=1, updated=-2, reprobed=4) == {
        "loaded": 0,
        "added": 1,
        "updated": 0,
        "reprobed": 4,
    }


def test_url_patch_recovery_target_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    assert url_patches.should_attempt_patch_recovery("HTTP Error 410 gone")
    assert not url_patches.should_attempt_patch_recovery("connection refused")
    assert (
        url_patches.extract_url_from_error("failed at https://old.example/jobs'")
        == "https://old.example/jobs"
    )
    assert (
        url_patches.extract_redirect_location("Redirect location: 'https://new.example/jobs'")
        == "https://new.example/jobs"
    )
    assert url_patches.resolve_greenhouse_known("Larian Studios") == "https://larian.com/careers"

    assert (
        url_patches.resolve_patch_target(
            candidate={},
            error_text="Redirect location: 'https://direct.example/jobs'",
            timeout_s=5,
        )
        == "https://direct.example/jobs"
    )
    assert (
        url_patches.resolve_patch_target(
            candidate={"studio": "Guerrilla", "careersUrl": "https://boards.greenhouse.io/bad"},
            error_text="HTTP Error 404 for https://boards.greenhouse.io/bad",
            timeout_s=5,
        )
        == "https://job-boards.greenhouse.io/guerrilla-games"
    )

    monkeypatch.setattr(
        url_patches,
        "suggest_alternate_career_urls",
        lambda _url: ["https://suggested.example/careers"],
    )
    assert (
        url_patches.resolve_patch_target(
            candidate={"listing_url": "https://old.example/jobs"},
            error_text="HTTP Error 404",
            timeout_s=5,
        )
        == "https://suggested.example/careers"
    )

    monkeypatch.setattr(url_patches, "suggest_alternate_career_urls", lambda _url: [])
    monkeypatch.setattr(
        url_patches,
        "discover_redirect_career_candidates",
        lambda _url, _timeout: ["https://redirected.example/careers"],
    )
    assert (
        url_patches.resolve_patch_target(
            candidate={"api_url": "https://old.example/api"},
            error_text="HTTP Error 404",
            timeout_s=5,
        )
        == "https://redirected.example/careers"
    )
    assert url_patches.resolve_patch_target(candidate={}, error_text="", timeout_s=5) == ""


def test_extract_redirect_failures_and_resolve_url(monkeypatch: pytest.MonkeyPatch) -> None:
    failures = url_patches.extract_redirect_failures(
        {
            "failures": [
                {
                    "name": "Recoverable",
                    "adapter": "static",
                    "error": "HTTP Error 404 for https://old.example/jobs",
                },
                {"name": "Ignored", "adapter": "static", "error": "connection refused"},
                {"name": "Missing URL", "adapter": "static", "error": "HTTP Error 410 gone"},
            ]
        }
    )
    assert failures == [
        {
            "name": "Recoverable",
            "url": "https://old.example/jobs",
            "adapter": "static",
            "original_error": "HTTP Error 404 for https://old.example/jobs",
        }
    ]
    assert url_patches.extract_redirect_failures({"failures": {}}) == []

    class _FakeUrl:
        def __init__(self, url: str):
            self.url = url

    class _FakeResponse:
        url = "https://final.example/jobs"
        status_code = 200
        history = [_FakeUrl("https://old.example/jobs")]

    class _FakeClient:
        def __init__(self, **kwargs):
            self.kwargs = kwargs

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        async def get(self, url: str):
            assert url == "https://old.example/jobs"
            assert self.kwargs["follow_redirects"] is True
            return _FakeResponse()

    monkeypatch.setattr(url_patches.httpx, "AsyncClient", _FakeClient)

    import asyncio

    assert asyncio.run(url_patches.resolve_url("https://old.example/jobs")) == (
        "https://final.example/jobs",
        200,
        ["https://old.example/jobs", "https://final.example/jobs"],
    )

    class _FailingClient(_FakeClient):
        async def get(self, url: str):
            raise RuntimeError("boom")

    monkeypatch.setattr(url_patches.httpx, "AsyncClient", _FailingClient)
    assert asyncio.run(url_patches.resolve_url("https://old.example/jobs")) == ("", 0, [])
