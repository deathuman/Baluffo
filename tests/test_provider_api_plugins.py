from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest

from src.jobs import common
from src.jobs.adapters import _runtime, provider_api, provider_parsers

FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


class _FakeDeps:
    def __init__(self) -> None:
        self._registry: dict[str, list[dict[str, Any]]] = {}
        self._responses: dict[str, str] = {}
        self.SOURCE_DIAGNOSTICS: dict[str, dict[str, Any]] = {}

    def registry_entries(self, key: str) -> list[dict[str, Any]]:
        return list(self._registry.get(key, []))

    def set_registry_entries(self, key: str, rows: list[dict[str, Any]]) -> None:
        self._registry[key] = [dict(row) for row in rows]

    def set_response(self, url: str, payload: Any) -> None:
        self._responses[url] = json.dumps(payload, ensure_ascii=False)

    def set_text_response(self, url: str, payload: str) -> None:
        self._responses[url] = payload

    def fetch_with_retries(
        self,
        url: str,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
    ) -> str:
        _ = fetch_text, timeout_s, retries, backoff_s
        if url not in self._responses:
            raise RuntimeError(f"missing fixture for url: {url}")
        return self._responses[url]

    def set_source_diagnostics(
        self, name: str, *, adapter: str, studio: str, details: list, partial_errors: list
    ) -> None:
        self.SOURCE_DIAGNOSTICS[name] = {
            "adapter": adapter,
            "studio": studio,
            "details": details,
            "partialErrors": partial_errors,
            "lowConfidenceDropped": 0,
        }

    # Parsing helpers used by provider_api
    def parse_teamtailor_listing_links(self, listing_html: str, *, base_url: str) -> list[str]:
        _ = listing_html
        return [f"{base_url.rstrip('/')}/jobs/1", f"{base_url.rstrip('/')}/jobs/2"]

    def parse_jobpostings_from_html(
        self, html: str, *, base_url: str, fallback_company: str, fallback_source_id_prefix: str
    ):
        _ = html, fallback_source_id_prefix
        # Only return a parsed row for the first detail URL.
        if base_url.endswith("/jobs/1"):
            return [
                {
                    "sourceJobId": "tt:1",
                    "title": "Engineer",
                    "company": fallback_company or "Unknown",
                    "city": "",
                    "country": "Unknown",
                    "workType": "",
                    "contractType": "",
                    "jobLink": base_url,
                    "sector": "Game",
                    "postedAt": "",
                    "adapter": "",
                    "studio": "",
                    "sourceBundle": [],
                }
            ]
        return []


@pytest.fixture()
def fake_deps(monkeypatch: pytest.MonkeyPatch) -> _FakeDeps:
    deps = _FakeDeps()
    monkeypatch.setattr(_runtime, "facade", lambda: deps)
    return deps


def test_provider_api_greenhouse_dispatch_extracts_registry_backed_jobs(
    fake_deps: _FakeDeps,
) -> None:
    fake_deps.set_registry_entries(
        "greenhouse",
        [
            {"slug": "studio-a", "studio": "Studio A", "name": "Studio A"},
        ],
    )
    url = common.GREENHOUSE_JOBS_URL_TEMPLATE.format(slug="studio-a")
    fake_deps.set_response(
        url,
        {
            "jobs": [
                {
                    "id": 1,
                    "text": "Engineer",
                    "title": "Engineer",
                    "location": {"name": "Remote"},
                    "absolute_url": "https://example/jobs/1",
                }
            ]
        },
    )

    dispatched = provider_api.run_greenhouse_boards_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=1,
        backoff_s=0.0,
    )
    assert len(dispatched) == 1
    assert dispatched[0]["adapter"] == "greenhouse"
    assert dispatched[0]["studio"] == "Studio A"
    assert dispatched[0]["sourceJobId"].startswith("greenhouse:studio-a:")


def test_provider_api_teamtailor_dispatch_extracts_registry_backed_jobs(
    fake_deps: _FakeDeps,
) -> None:
    fake_deps.set_registry_entries(
        "teamtailor",
        [
            {"name": "TT", "listing_url": "https://tt/listing", "base_url": "https://tt"},
        ],
    )
    fake_deps.set_response("https://tt/listing", "<html>listing</html>")
    fake_deps.set_response("https://tt/jobs/1", "<html>detail 1</html>")
    fake_deps.set_response("https://tt/jobs/2", "<html>detail 2</html>")

    dispatched = provider_api.run_teamtailor_sources_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=1,
        backoff_s=0.0,
    )
    assert len(dispatched) == 2
    assert all(row["adapter"] == "teamtailor" for row in dispatched)
    assert all(row["studio"] == "TT" for row in dispatched)


def test_provider_api_breezy_dispatch_extracts_registry_backed_jobs(fake_deps: _FakeDeps) -> None:
    fake_deps.set_registry_entries(
        "breezy",
        [
            {
                "name": "YallaPlay (Breezy)",
                "studio": "YallaPlay",
                "board_url": "https://yallaplay.breezy.hr/",
            }
        ],
    )
    fake_deps.set_text_response("https://yallaplay.breezy.hr/", _fixture("breezy_jobs.html"))

    rows = provider_api.run_breezy_sources_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=1,
        backoff_s=0.0,
    )

    assert len(rows) == 2
    assert all(row["adapter"] == "breezy" for row in rows)
    assert all(row["studio"] == "YallaPlay" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def test_provider_api_jazzhr_dispatch_extracts_registry_backed_jobs(fake_deps: _FakeDeps) -> None:
    fake_deps.set_registry_entries(
        "jazzhr",
        [
            {
                "name": "Lost Boys Interactive (JazzHR)",
                "studio": "Lost Boys Interactive",
                "board_url": "https://lostboysinteractive.applytojob.com/apply",
            }
        ],
    )
    fake_deps.set_text_response(
        "https://lostboysinteractive.applytojob.com/apply",
        _fixture("jazzhr_jobs.html"),
    )

    rows = provider_api.run_jazzhr_sources_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=1,
        backoff_s=0.0,
    )

    assert len(rows) == 2
    assert all(row["adapter"] == "jazzhr" for row in rows)
    assert all(row["studio"] == "Lost Boys Interactive" for row in rows)
    assert any(row["contractType"] == "Full Time" for row in rows)


def test_provider_api_recruitee_dispatch_extracts_registry_backed_jobs(
    fake_deps: _FakeDeps,
) -> None:
    fake_deps.set_registry_entries(
        "recruitee",
        [
            {
                "name": "CrazyGames (Recruitee)",
                "studio": "CrazyGames",
                "subdomain": "jobs.crazygames.com",
                "api_url": "https://jobs.crazygames.com/api/offers/",
            }
        ],
    )
    fake_deps.set_response(
        "https://jobs.crazygames.com/api/offers/",
        json.loads(_fixture("recruitee_jobs.json")),
    )

    rows = provider_api.run_recruitee_sources_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=1,
        backoff_s=0.0,
    )

    assert len(rows) == 2
    assert all(row["adapter"] == "recruitee" for row in rows)
    assert all(row["studio"] == "CrazyGames" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)


def test_provider_api_pinpoint_dispatch_extracts_registry_backed_jobs(fake_deps: _FakeDeps) -> None:
    fake_deps.set_registry_entries(
        "pinpoint",
        [
            {
                "name": "Gameplay Galaxy (Pinpoint)",
                "studio": "Gameplay Galaxy",
                "subdomain": "gameplaygalaxy",
                "api_url": "https://gameplaygalaxy.pinpointhq.com/postings.json",
            }
        ],
    )
    fake_deps.set_response(
        "https://gameplaygalaxy.pinpointhq.com/postings.json",
        json.loads(_fixture("pinpoint_jobs.json")),
    )

    rows = provider_api.run_pinpoint_sources_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=1,
        backoff_s=0.0,
    )

    assert len(rows) == 2
    assert all(row["adapter"] == "pinpoint" for row in rows)
    assert all(row["studio"] == "Gameplay Galaxy" for row in rows)
    assert any(row["workType"] == "Remote" for row in rows)
