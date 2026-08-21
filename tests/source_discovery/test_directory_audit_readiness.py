from __future__ import annotations

from ._helpers import Path, gamesmap_adapter, mock, sd, workspace_tmpdir


def test_gameprog_audit_readiness_caps_entries_before_website_fetch_jobs() -> None:
    config = {
        "gameprog": {
            "enabled": True,
            "activeAuditPath": str(Path(".tmp") / "gameprog-readiness-caps-audit.json"),
            "activeAuditTtlMinutes": 0,
            "teamsUrl": "https://gameprog.it/teams.json",
            "websiteOnlyFallback": True,
            "maxStudios": 1,
        }
    }
    teams_json = """[
        {"name": "First Studio", "url": "https://first.example.com/", "place": "Rome"},
        {"name": "Second Studio", "url": "https://second.example.com/", "place": "Milan"}
    ]"""
    captured_jobs: list[dict[str, object]] = []

    def fake_fetch(url: str, _: int) -> str:
        if url != "https://gameprog.it/teams.json":
            raise RuntimeError(f"unexpected URL before fetch job capture: {url}")
        return teams_json

    def fake_fetch_pages(_timeout_s, page_jobs, **_kwargs):
        captured_jobs.extend(page_jobs)
        return []

    with mock.patch("src.source_discovery.gameprog.fetch_directory_pages", fake_fetch_pages):
        provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
            5, config=config, fetcher=fake_fetch
        )

    assert provider_rows == []
    assert static_rows == []
    assert failures == []
    assert [job["url"] for job in captured_jobs] == ["https://first.example.com/"]
    assert captured_jobs[0]["adapter"] == "gameprog"
    assert captured_jobs[0]["failureStage"] == "website_fetch"
    assert captured_jobs[0]["name"] == "https://first.example.com/"
    assert captured_jobs[0]["payload"] == {
        "studio": "First Studio",
        "url": "https://first.example.com/",
        "place": "Rome",
    }


def test_gamesmap_audit_readiness_caps_entries_before_website_fetch_jobs() -> None:
    config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": True,
            "maxDetailPages": 2,
            "allowedCategoryTokens": ["developer"],
            "blockedCategoryTokens": [],
            "activeAuditPath": str(Path(".tmp") / "gamesmap-readiness-caps-audit.json"),
            "activeAuditTtlMinutes": 0,
        }
    }
    parsed_entries = [
        {
            "detailUrl": f"https://www.gamesmap.de/en/company/studio-{index}",
            "studio": f"Studio {index}",
            "categories": ["Developer"],
            "websiteUrl": f"https://studio-{index}.example.com",
            "location": "Berlin",
        }
        for index in range(3)
    ]
    captured_jobs: list[dict[str, object]] = []

    def fake_fetch(url: str, _: int) -> str:
        if url != "https://www.gamesmap.de/en":
            raise RuntimeError(f"unexpected URL before fetch job capture: {url}")
        return "<html></html>"

    def fake_fetch_pages(_timeout_s, page_jobs, **_kwargs):
        captured_jobs.extend(page_jobs)
        return []

    with (
        mock.patch.object(
            gamesmap_adapter,
            "_parse_gamesmap_index_entries_with_diagnostics",
            return_value=(parsed_entries, {"unresolvedReferenceCount": 0}),
        ),
        mock.patch.object(gamesmap_adapter, "infer_web_candidate", return_value=None),
        mock.patch.object(gamesmap_adapter, "fetch_directory_pages", fake_fetch_pages),
    ):
        provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
            5, config=config, fetcher=fake_fetch
        )

    assert provider_rows == []
    assert static_rows == []
    assert failures == []
    assert [job["url"] for job in captured_jobs] == [
        "https://studio-0.example.com",
        "https://studio-1.example.com",
    ]
    assert all(job["adapter"] == "gamesmap" for job in captured_jobs)
    assert all(job["failureStage"] == "website_fetch" for job in captured_jobs)
    assert [job["name"] for job in captured_jobs] == [
        "https://studio-0.example.com",
        "https://studio-1.example.com",
    ]
    assert captured_jobs[0]["payload"] == parsed_entries[0]


def test_directory_audit_readiness_website_fetch_failures_stay_in_failure_channel() -> None:
    gameprog_config = {
        "gameprog": {
            "enabled": True,
            "activeAuditTtlMinutes": 0,
            "activeAuditPath": str(Path(".tmp") / "gameprog-readiness-failure-audit.json"),
            "teamsUrl": "https://gameprog.it/teams.json",
            "websiteOnlyFallback": False,
            "maxStudios": 1,
        }
    }
    gameprog_payloads = {
        "https://gameprog.it/teams.json": """[
            {"name": "Broken Studio", "url": "https://broken-gameprog.example.com/", "place": "Rome"}
        ]""",
    }

    def fake_gameprog_fetch(url: str, _: int) -> str:
        if url not in gameprog_payloads:
            raise RuntimeError(f"fetch failed: {url}")
        return gameprog_payloads[url]

    provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
        5, config=gameprog_config, fetcher=fake_gameprog_fetch
    )

    assert provider_rows == []
    assert static_rows == []
    assert len(failures) == 1
    assert failures[0]["adapter"] == "gameprog"
    assert failures[0]["stage"] == "website_fetch"

    gamesmap_config = {
        "gamesmap": {
            "enabled": True,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": True,
            "maxDetailPages": 1,
            "allowedCategoryTokens": ["developer"],
            "blockedCategoryTokens": [],
            "activeAuditPath": str(Path(".tmp") / "gamesmap-readiness-failure-audit.json"),
            "activeAuditTtlMinutes": 0,
        }
    }
    gamesmap_entries = [
        {
            "detailUrl": "https://www.gamesmap.de/en/company/broken-studio",
            "studio": "Broken Studio",
            "categories": ["Developer"],
            "websiteUrl": "https://broken-gamesmap.example.com",
            "location": "Berlin",
        }
    ]

    def fake_gamesmap_fetch(url: str, _: int) -> str:
        if url == "https://www.gamesmap.de/en":
            return "<html></html>"
        raise RuntimeError(f"fetch failed: {url}")

    with (
        mock.patch.object(
            gamesmap_adapter,
            "_parse_gamesmap_index_entries_with_diagnostics",
            return_value=(gamesmap_entries, {"unresolvedReferenceCount": 0}),
        ),
        mock.patch.object(gamesmap_adapter, "infer_web_candidate", return_value=None),
    ):
        provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
            5, config=gamesmap_config, fetcher=fake_gamesmap_fetch
        )

    assert provider_rows == []
    assert static_rows == []
    assert len(failures) == 1
    assert failures[0]["adapter"] == "gamesmap"
    assert failures[0]["stage"] == "website_fetch"


def test_directory_audit_contract_cache_hit_bypasses_boundary_work() -> None:
    with workspace_tmpdir("directory-audit-cache-contract") as root:
        gameprog_audit_path = root / "gameprog-audit.json"
        gameprog_config = {
            "gameprog": {
                "enabled": True,
                "activeAuditPath": str(gameprog_audit_path),
                "activeAuditTtlMinutes": 60,
                "teamsUrl": "https://gameprog.it/teams.json",
                "websiteOnlyFallback": True,
                "maxStudios": 1,
            }
        }
        gameprog_payloads = {
            "https://gameprog.it/teams.json": """[
                {"name": "Cached Studio", "url": "https://cached-gameprog.example.com/", "place": "Rome"}
            ]""",
            "https://cached-gameprog.example.com/": """
                <!doctype html><html><body><a href="/careers">Careers</a></body></html>
            """,
        }

        def fake_gameprog_fetch(url: str, _: int) -> str:
            if url not in gameprog_payloads:
                raise RuntimeError(f"unexpected URL: {url}")
            return gameprog_payloads[url]

        first_gameprog = sd.discover_gameprog_candidates(
            5, config=gameprog_config, fetcher=fake_gameprog_fetch
        )

        with mock.patch(
            "src.source_discovery.gameprog.fetch_directory_pages",
            side_effect=AssertionError("cache hit should bypass website boundary work"),
        ):
            second_gameprog = sd.discover_gameprog_candidates(
                5,
                config=gameprog_config,
                fetcher=lambda *_args: (_ for _ in ()).throw(
                    AssertionError("cache hit should bypass teams fetch")
                ),
            )

        assert first_gameprog == second_gameprog

        gamesmap_audit_path = root / "gamesmap-audit.json"
        gamesmap_config = {
            "gamesmap": {
                "enabled": True,
                "baseUrl": "https://www.gamesmap.de",
                "indexUrls": ["https://www.gamesmap.de/en"],
                "websiteOnlyFallback": True,
                "maxDetailPages": 1,
                "allowedCategoryTokens": ["developer"],
                "blockedCategoryTokens": [],
                "activeAuditPath": str(gamesmap_audit_path),
                "activeAuditTtlMinutes": 60,
            }
        }
        gamesmap_entries = [
            {
                "detailUrl": "https://www.gamesmap.de/en/company/cached-studio",
                "studio": "Cached Studio",
                "categories": ["Developer"],
                "websiteUrl": "https://cached-gamesmap.example.com",
                "location": "Berlin",
            }
        ]
        gamesmap_payloads = {
            "https://www.gamesmap.de/en": "<html></html>",
            "https://cached-gamesmap.example.com": """
                <!doctype html><html><body><a href="/careers">Careers</a></body></html>
            """,
        }

        def fake_gamesmap_fetch(url: str, _: int) -> str:
            if url not in gamesmap_payloads:
                raise RuntimeError(f"unexpected URL: {url}")
            return gamesmap_payloads[url]

        with (
            mock.patch.object(
                gamesmap_adapter,
                "_parse_gamesmap_index_entries_with_diagnostics",
                return_value=(gamesmap_entries, {"unresolvedReferenceCount": 0}),
            ),
            mock.patch.object(gamesmap_adapter, "infer_web_candidate", return_value=None),
        ):
            first_gamesmap = sd.discover_gamesmap_candidates(
                5, config=gamesmap_config, fetcher=fake_gamesmap_fetch
            )

        with mock.patch(
            "src.source_discovery.gamesmap.fetch_directory_pages",
            side_effect=AssertionError("cache hit should bypass website boundary work"),
        ):
            second_gamesmap = sd.discover_gamesmap_candidates(
                5,
                config=gamesmap_config,
                fetcher=lambda *_args: (_ for _ in ()).throw(
                    AssertionError("cache hit should bypass index fetch")
                ),
            )

        assert first_gamesmap == second_gamesmap
        assert gamesmap_audit_path.exists()


def test_directory_audit_contract_candidate_outputs_keep_boundary_provenance() -> None:
    gameprog_config = {
        "gameprog": {
            "enabled": True,
            "activeAuditPath": str(Path(".tmp") / "gameprog-boundary-audit.json"),
            "activeAuditTtlMinutes": 0,
            "teamsUrl": "https://gameprog.it/teams.json",
            "websiteOnlyFallback": True,
            "maxStudios": 1,
        }
    }
    gameprog_payloads = {
        "https://gameprog.it/teams.json": """[
            {"name": "Boundary Studio", "url": "https://boundary-gameprog.example.com/", "place": "Rome"}
        ]""",
        "https://boundary-gameprog.example.com/": """
            <!doctype html><html><body><a href="/careers">Careers</a></body></html>
        """,
    }

    def fake_gameprog_fetch(url: str, _: int) -> str:
        if url not in gameprog_payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return gameprog_payloads[url]

    provider_rows, static_rows, failures = sd.discover_gameprog_candidates(
        5, config=gameprog_config, fetcher=fake_gameprog_fetch
    )

    assert provider_rows == []
    assert failures == []
    assert len(static_rows) == 1
    assert static_rows[0]["sourceDirectory"] == "gameprog"
    assert static_rows[0]["sourceDirectoryEntryUrl"] == "https://boundary-gameprog.example.com/"
    assert static_rows[0]["sourceDirectoryLocation"] == "Rome"
    assert "gameprog_directory" in static_rows[0]["evidenceTypes"]

    gamesmap_config = {
        "gamesmap": {
            "enabled": True,
            "activeAuditPath": str(Path(".tmp") / "gamesmap-boundary-audit.json"),
            "activeAuditTtlMinutes": 0,
            "baseUrl": "https://www.gamesmap.de",
            "indexUrls": ["https://www.gamesmap.de/en"],
            "websiteOnlyFallback": True,
            "maxDetailPages": 1,
            "allowedCategoryTokens": ["developer"],
            "blockedCategoryTokens": [],
        }
    }
    gamesmap_entries = [
        {
            "detailUrl": "https://www.gamesmap.de/en/company/boundary-studio",
            "studio": "Boundary Studio",
            "categories": ["Developer"],
            "websiteUrl": "https://boundary-gamesmap.example.com",
            "location": "Berlin",
        }
    ]
    gamesmap_payloads = {
        "https://www.gamesmap.de/en": "<html></html>",
        "https://boundary-gamesmap.example.com": """
            <!doctype html><html><body><a href="/careers">Careers</a></body></html>
        """,
    }

    def fake_gamesmap_fetch(url: str, _: int) -> str:
        if url not in gamesmap_payloads:
            raise RuntimeError(f"unexpected URL: {url}")
        return gamesmap_payloads[url]

    with (
        mock.patch.object(
            gamesmap_adapter,
            "_parse_gamesmap_index_entries_with_diagnostics",
            return_value=(gamesmap_entries, {"unresolvedReferenceCount": 0}),
        ),
        mock.patch.object(gamesmap_adapter, "infer_web_candidate", return_value=None),
    ):
        provider_rows, static_rows, failures = sd.discover_gamesmap_candidates(
            5, config=gamesmap_config, fetcher=fake_gamesmap_fetch
        )

    assert provider_rows == []
    assert failures == []
    assert len(static_rows) == 1
    assert static_rows[0]["sourceDirectory"] == "gamesmap"
    assert (
        static_rows[0]["sourceDirectoryEntryUrl"]
        == "https://www.gamesmap.de/en/company/boundary-studio"
    )
    assert static_rows[0]["sourceDirectoryLocation"] == "Berlin"
    assert static_rows[0]["sourceDirectoryCategories"] == ["Developer"]
    assert "gamesmap_directory" in static_rows[0]["evidenceTypes"]
