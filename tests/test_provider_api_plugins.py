from __future__ import annotations

import json
from typing import Any, Callable, Dict, List
from pathlib import Path

import pytest

from src.jobs import common
from src.jobs.adapters import provider_api, provider_parsers, _runtime


FIXTURES_DIR = Path(__file__).resolve().parent / "fixtures"


def _fixture(name: str) -> str:
    return (FIXTURES_DIR / name).read_text(encoding="utf-8")


class _FakeDeps:
    def __init__(self) -> None:
        self._registry: Dict[str, List[Dict[str, Any]]] = {}
        self._responses: Dict[str, str] = {}
        self.SOURCE_DIAGNOSTICS: Dict[str, Dict[str, Any]] = {}

    def registry_entries(self, key: str) -> List[Dict[str, Any]]:
        return list(self._registry.get(key, []))

    def set_registry_entries(self, key: str, rows: List[Dict[str, Any]]) -> None:
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

    def set_source_diagnostics(self, name: str, *, adapter: str, studio: str, details: list, partial_errors: list) -> None:
        self.SOURCE_DIAGNOSTICS[name] = {
            "adapter": adapter,
            "studio": studio,
            "details": details,
            "partialErrors": partial_errors,
            "lowConfidenceDropped": 0,
        }

    # Parsing helpers used by provider_api
    def parse_teamtailor_listing_links(self, listing_html: str, *, base_url: str) -> List[str]:
        _ = listing_html
        return [f"{base_url.rstrip('/')}/jobs/1", f"{base_url.rstrip('/')}/jobs/2"]

    def parse_jobpostings_from_html(self, html: str, *, base_url: str, fallback_company: str, fallback_source_id_prefix: str):
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


def _legacy_greenhouse(fake: _FakeDeps, *, fetch_text, timeout_s: int, retries: int, backoff_s: float):
    jobs: list = []
    errors: list[str] = []
    details: list[dict] = []
    for board in fake.registry_entries("greenhouse"):
        slug = common.clean_text(board.get("slug"))
        if not slug:
            continue
        label = common.clean_text(board.get("name")) or common.clean_text(board.get("studio")) or slug
        url = common.GREENHOUSE_JOBS_URL_TEMPLATE.format(slug=slug)
        entry_report = {
            "adapter": "greenhouse",
            "studio": common.clean_text(board.get("studio")) or label,
            "name": common.clean_text(board.get("name")) or slug,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        try:
            text = fake.fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = provider_parsers.parse_greenhouse_jobs_payload(payload, slug, fallback_company=label)
            for row in parsed:
                row["adapter"] = "greenhouse"
                row["studio"] = common.clean_text(board.get("studio")) or label
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"greenhouse:{slug}: {exc}")
        details.append(entry_report)
    fake.set_source_diagnostics("greenhouse_boards", adapter="greenhouse", studio="multiple", details=details, partial_errors=errors)
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def _legacy_teamtailor(fake: _FakeDeps, *, fetch_text, timeout_s: int, retries: int, backoff_s: float):
    jobs: list = []
    errors: list[str] = []
    seen_links = set()
    details: list[dict] = []
    for source in fake.registry_entries("teamtailor"):
        source_name = common.clean_text(source.get("name")) or "teamtailor_source"
        listing_url = common.clean_text(source.get("listing_url"))
        base_url = common.clean_text(source.get("base_url")) or listing_url
        fallback_company = common.clean_text(source.get("company"))
        entry_report = {
            "adapter": "teamtailor",
            "studio": common.clean_text(source.get("studio")) or fallback_company or source_name,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        if not listing_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing listing_url"
            details.append(entry_report)
            continue
        try:
            listing_html = fake.fetch_with_retries(listing_url, fetch_text, timeout_s, retries, backoff_s)
            job_links = fake.parse_teamtailor_listing_links(listing_html, base_url=base_url)
            entry_report["fetchedCount"] = len(job_links)
            kept_before = len(jobs)
            for idx, job_link in enumerate(job_links, start=1):
                if job_link in seen_links:
                    continue
                seen_links.add(job_link)
                try:
                    detail_html = fake.fetch_with_retries(job_link, fetch_text, timeout_s, retries, backoff_s)
                    parsed = fake.parse_jobpostings_from_html(
                        detail_html,
                        base_url=job_link,
                        fallback_company=fallback_company,
                        fallback_source_id_prefix=f"teamtailor:{source_name}:{idx}",
                    )
                    if parsed:
                        for row in parsed:
                            row["adapter"] = "teamtailor"
                            row["studio"] = common.clean_text(source.get("studio")) or fallback_company or source_name
                        jobs.extend(parsed)
                    else:
                        # fallback path produces a synthesized job
                        slug = job_link.rstrip("/").split("/")[-1]
                        title = slug.replace("-", " ").strip()
                        if title:
                            jobs.append(
                                {
                                    "sourceJobId": f"teamtailor:{source_name}:{slug}",
                                    "title": title,
                                    "company": fallback_company or "Unknown",
                                    "city": "",
                                    "country": "Unknown",
                                    "workType": "",
                                    "contractType": "",
                                    "jobLink": job_link,
                                    "sector": "Game",
                                    "postedAt": "",
                                    "adapter": "teamtailor",
                                    "studio": common.clean_text(source.get("studio")) or fallback_company or source_name,
                                }
                            )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"teamtailor:{source_name}:{job_link}: {exc}")
            entry_report["keptCount"] = max(0, len(jobs) - kept_before)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"teamtailor:{source_name}:{listing_url}: {exc}")
        details.append(entry_report)
    fake.set_source_diagnostics("teamtailor_sources", adapter="teamtailor", studio="multiple", details=details, partial_errors=errors)
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


@pytest.fixture()
def fake_deps(monkeypatch: pytest.MonkeyPatch) -> _FakeDeps:
    deps = _FakeDeps()
    monkeypatch.setattr(_runtime, "facade", lambda: deps)
    return deps


def test_provider_api_greenhouse_dispatch_matches_legacy(fake_deps: _FakeDeps) -> None:
    fake_deps.set_registry_entries(
        "greenhouse",
        [
            {"slug": "studio-a", "studio": "Studio A", "name": "Studio A"},
        ],
    )
    url = common.GREENHOUSE_JOBS_URL_TEMPLATE.format(slug="studio-a")
    fake_deps.set_response(
        url,
        {"jobs": [{"id": 1, "text": "Engineer", "title": "Engineer", "location": {"name": "Remote"}, "absolute_url": "https://example/jobs/1"}]},
    )

    fetch_text = lambda _url, _timeout: ""  # should not be used
    legacy = _legacy_greenhouse(fake_deps, fetch_text=fetch_text, timeout_s=5, retries=1, backoff_s=0.0)
    dispatched = provider_api.run_greenhouse_boards_source(fetch_text=fetch_text, timeout_s=5, retries=1, backoff_s=0.0)
    assert dispatched == legacy


def test_provider_api_teamtailor_dispatch_matches_legacy(fake_deps: _FakeDeps) -> None:
    fake_deps.set_registry_entries(
        "teamtailor",
        [
            {"name": "TT", "listing_url": "https://tt/listing", "base_url": "https://tt"},
        ],
    )
    fake_deps.set_response("https://tt/listing", "<html>listing</html>")
    fake_deps.set_response("https://tt/jobs/1", "<html>detail 1</html>")
    fake_deps.set_response("https://tt/jobs/2", "<html>detail 2</html>")

    fetch_text = lambda _url, _timeout: ""  # should not be used
    legacy = _legacy_teamtailor(fake_deps, fetch_text=fetch_text, timeout_s=5, retries=1, backoff_s=0.0)
    dispatched = provider_api.run_teamtailor_sources_source(fetch_text=fetch_text, timeout_s=5, retries=1, backoff_s=0.0)
    assert dispatched == legacy


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


def test_provider_api_recruitee_dispatch_extracts_registry_backed_jobs(fake_deps: _FakeDeps) -> None:
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

