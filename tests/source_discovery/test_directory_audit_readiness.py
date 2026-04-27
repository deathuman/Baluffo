from __future__ import annotations

from ._helpers import gamesmap_adapter, mock, sd


def test_gameprog_audit_readiness_caps_entries_before_website_fetch_jobs() -> None:
    config = {
        "gameprog": {
            "enabled": True,
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


def test_directory_audit_readiness_website_fetch_failures_stay_in_failure_channel() -> None:
    gameprog_config = {
        "gameprog": {
            "enabled": True,
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
