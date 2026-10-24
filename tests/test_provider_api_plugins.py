from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from src.jobs.adapters import provider_api
from src.jobs.adapters.plugins.provider_api import greenhouse_runner, teamtailor_runner
from src.jobs.adapters.plugins.provider_api import html_board as html_board_runner
from src.jobs.adapters.plugins.provider_api import json_feed as json_feed_runner
from src.jobs.adapters.plugins.provider_api import oracle_hcm as oracle_hcm_runner
from src.jobs.adapters.plugins.provider_api.lifecycle import provider_revalidate_not_modified
from src.jobs.common.config import GREENHOUSE_JOBS_URL_TEMPLATE
from src.jobs.state_incremental import get_incremental_cache_decision
from src.jobs.state_source_state import update_source_state_rows
from tests.helpers.concurrency import BlockingActiveCounter
from tests.helpers.job_fixtures import _fixture
from tests.helpers.mutation import call_and_return


class _FakeDeps:
    def __init__(self) -> None:
        self._registry: dict[str, list[dict[str, Any]]] = {}
        self._responses: dict[str, str] = {}
        self._response_headers: dict[str, dict[str, str]] = {}
        self.SOURCE_DIAGNOSTICS: dict[str, dict[str, Any]] = {}

    def registry_entries(self, key: str) -> list[dict[str, Any]]:
        return list(self._registry.get(key, []))

    def set_registry_entries(self, key: str, rows: list[dict[str, Any]]) -> None:
        self._registry[key] = [dict(row) for row in rows]

    def set_response(self, url: str, payload: Any) -> None:
        self._responses[url] = json.dumps(payload, ensure_ascii=False)

    def set_text_response(self, url: str, payload: str) -> None:
        self._responses[url] = payload

    def set_response_headers(self, url: str, headers: dict[str, str]) -> None:
        self._response_headers[url] = dict(headers)

    def fetch_with_retries(
        self,
        url: str,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
        capture_headers: dict[str, str] | None = None,
    ) -> str:
        _ = fetch_text, timeout_s, retries, backoff_s
        if url not in self._responses:
            raise RuntimeError(f"missing fixture for url: {url}")
        if capture_headers is not None:
            capture_headers.update({"status": "200", **self._response_headers.get(url, {})})
        return self._responses[url]

    def set_source_diagnostics(
        self,
        name: str,
        *,
        adapter: str,
        studio: str,
        provider_url: str = "",
        details: list,
        partial_errors: list,
    ) -> None:
        self.SOURCE_DIAGNOSTICS[name] = {
            "adapter": adapter,
            "studio": studio,
            "providerUrl": provider_url,
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
                    "title": "Gameplay Engineer",
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
    for module in (
        greenhouse_runner,
        html_board_runner,
        json_feed_runner,
        oracle_hcm_runner,
        teamtailor_runner,
    ):
        monkeypatch.setattr(module, "registry_entries", deps.registry_entries)
        monkeypatch.setattr(module, "fetch_with_retries", deps.fetch_with_retries)
        monkeypatch.setattr(module, "set_source_diagnostics", deps.set_source_diagnostics)
    monkeypatch.setattr(
        teamtailor_runner,
        "parse_teamtailor_listing_links",
        deps.parse_teamtailor_listing_links,
    )
    monkeypatch.setattr(
        teamtailor_runner,
        "parse_jobpostings_from_html",
        deps.parse_jobpostings_from_html,
    )
    return deps


@dataclass(frozen=True)
class _DispatchCase:
    name: str
    setup: Callable[[_FakeDeps], Any]
    run: Callable[[], list[dict[str, Any]]]
    expected_len: int
    expected_adapter: str
    expected_studio: str
    extra_check: Callable[[list[dict[str, Any]]], None] = lambda rows: None


def _assert_source_job_id_prefix(rows: list[dict[str, Any]]) -> None:
    assert rows[0]["sourceJobId"].startswith("greenhouse:studio-a:")


def _assert_remote_work_type(rows: list[dict[str, Any]]) -> None:
    assert any(row["workType"] == "Remote" for row in rows)


def _assert_full_time_contract(rows: list[dict[str, Any]]) -> None:
    assert any(row["contractType"] == "Full Time" for row in rows)


DISPATCH_CASES = [
    pytest.param(
        _DispatchCase(
            name="greenhouse",
            setup=lambda deps: (
                call_and_return(
                    deps.set_registry_entries,
                    "greenhouse",
                    [{"slug": "studio-a", "studio": "Studio A", "name": "Studio A"}],
                    value=None,
                ),
                call_and_return(
                    deps.set_response,
                    GREENHOUSE_JOBS_URL_TEMPLATE.format(slug="studio-a"),
                    {
                        "jobs": [
                            {
                                "id": 1,
                                "text": "Gameplay Engineer",
                                "title": "Gameplay Engineer",
                                "location": {"name": "Remote"},
                                "absolute_url": "https://example/jobs/1",
                            }
                        ]
                    },
                    value=None,
                ),
            ),
            run=lambda: provider_api.run_greenhouse_boards_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=1,
                backoff_s=0.0,
            ),
            expected_len=1,
            expected_adapter="greenhouse",
            expected_studio="Studio A",
            extra_check=_assert_source_job_id_prefix,
        ),
        id="greenhouse",
    ),
    pytest.param(
        _DispatchCase(
            name="teamtailor",
            setup=lambda deps: (
                call_and_return(
                    deps.set_registry_entries,
                    "teamtailor",
                    [{"name": "TT", "listing_url": "https://tt/listing", "base_url": "https://tt"}],
                    value=None,
                ),
                call_and_return(
                    deps.set_response, "https://tt/listing", "<html>listing</html>", value=None
                ),
                call_and_return(
                    deps.set_response, "https://tt/jobs/1", "<html>detail 1</html>", value=None
                ),
                call_and_return(
                    deps.set_response, "https://tt/jobs/2", "<html>detail 2</html>", value=None
                ),
            ),
            run=lambda: provider_api.run_teamtailor_sources_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=1,
                backoff_s=0.0,
            ),
            expected_len=2,
            expected_adapter="teamtailor",
            expected_studio="TT",
        ),
        id="teamtailor",
    ),
    pytest.param(
        _DispatchCase(
            name="breezy",
            setup=lambda deps: (
                call_and_return(
                    deps.set_registry_entries,
                    "breezy",
                    [
                        {
                            "name": "YallaPlay (Breezy)",
                            "studio": "YallaPlay",
                            "board_url": "https://yallaplay.breezy.hr/",
                        }
                    ],
                    value=None,
                ),
                call_and_return(
                    deps.set_text_response,
                    "https://yallaplay.breezy.hr/",
                    _fixture("breezy_jobs.html"),
                    value=None,
                ),
            ),
            run=lambda: provider_api.run_breezy_sources_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=1,
                backoff_s=0.0,
            ),
            expected_len=2,
            expected_adapter="breezy",
            expected_studio="YallaPlay",
            extra_check=_assert_remote_work_type,
        ),
        id="breezy",
    ),
    pytest.param(
        _DispatchCase(
            name="jazzhr",
            setup=lambda deps: (
                call_and_return(
                    deps.set_registry_entries,
                    "jazzhr",
                    [
                        {
                            "name": "Lost Boys Interactive (JazzHR)",
                            "studio": "Lost Boys Interactive",
                            "board_url": "https://lostboysinteractive.applytojob.com/apply",
                        }
                    ],
                    value=None,
                ),
                call_and_return(
                    deps.set_text_response,
                    "https://lostboysinteractive.applytojob.com/apply",
                    _fixture("jazzhr_jobs.html"),
                    value=None,
                ),
            ),
            run=lambda: provider_api.run_jazzhr_sources_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=1,
                backoff_s=0.0,
            ),
            expected_len=1,
            expected_adapter="jazzhr",
            expected_studio="Lost Boys Interactive",
            extra_check=_assert_full_time_contract,
        ),
        id="jazzhr",
    ),
    pytest.param(
        _DispatchCase(
            name="oracle_hcm",
            setup=lambda deps: (
                call_and_return(
                    deps.set_registry_entries,
                    "oracle_hcm",
                    [
                        {
                            "name": "Corsair (Oracle HCM)",
                            "studio": "Corsair",
                            "listing_url": "https://edix.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/jobs",
                            "base_url": "https://edix.fa.us2.oraclecloud.com",
                            "site_path": "/hcmUI/CandidateExperience/en/sites/CX_1/jobs",
                        }
                    ],
                    value=None,
                ),
                call_and_return(
                    deps.set_response,
                    "https://edix.fa.us2.oraclecloud.com/hcmRestApi/resources/11.13.18.05/recruitingCEJobRequisitions?expand=requisitionList&onlyData=true&limit=200",
                    json.loads(_fixture("oracle_hcm_requisitions.json")),
                    value=None,
                ),
            ),
            run=lambda: provider_api.run_oracle_hcm_sources_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=1,
                backoff_s=0.0,
            ),
            expected_len=1,
            expected_adapter="oracle_hcm",
            expected_studio="Corsair",
        ),
        id="oracle_hcm",
    ),
    pytest.param(
        _DispatchCase(
            name="recruitee",
            setup=lambda deps: (
                call_and_return(
                    deps.set_registry_entries,
                    "recruitee",
                    [
                        {
                            "name": "CrazyGames (Recruitee)",
                            "studio": "CrazyGames",
                            "subdomain": "jobs.crazygames.com",
                            "api_url": "https://jobs.crazygames.com/api/offers/",
                        }
                    ],
                    value=None,
                ),
                call_and_return(
                    deps.set_response,
                    "https://jobs.crazygames.com/api/offers/",
                    json.loads(_fixture("recruitee_jobs.json")),
                    value=None,
                ),
            ),
            run=lambda: provider_api.run_recruitee_sources_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=1,
                backoff_s=0.0,
            ),
            expected_len=2,
            expected_adapter="recruitee",
            expected_studio="CrazyGames",
            extra_check=_assert_remote_work_type,
        ),
        id="recruitee",
    ),
    pytest.param(
        _DispatchCase(
            name="pinpoint",
            setup=lambda deps: (
                call_and_return(
                    deps.set_registry_entries,
                    "pinpoint",
                    [
                        {
                            "name": "Gameplay Galaxy (Pinpoint)",
                            "studio": "Gameplay Galaxy",
                            "subdomain": "gameplaygalaxy",
                            "api_url": "https://gameplaygalaxy.pinpointhq.com/postings.json",
                        }
                    ],
                    value=None,
                ),
                call_and_return(
                    deps.set_response,
                    "https://gameplaygalaxy.pinpointhq.com/postings.json",
                    json.loads(_fixture("pinpoint_jobs.json")),
                    value=None,
                ),
            ),
            run=lambda: provider_api.run_pinpoint_sources_source(
                fetch_text=lambda _url, _timeout: "",
                timeout_s=5,
                retries=1,
                backoff_s=0.0,
            ),
            expected_len=2,
            expected_adapter="pinpoint",
            expected_studio="Gameplay Galaxy",
            extra_check=_assert_remote_work_type,
        ),
        id="pinpoint",
    ),
]


@pytest.mark.parametrize("case", DISPATCH_CASES, ids=lambda case: case.name)
def test_provider_api_dispatch_extracts_registry_backed_jobs(
    fake_deps: _FakeDeps,
    case: _DispatchCase,
) -> None:
    case.setup(fake_deps)
    rows = case.run()

    assert len(rows) == case.expected_len
    assert all(row["adapter"] == case.expected_adapter for row in rows)
    assert all(row["studio"] == case.expected_studio for row in rows)
    case.extra_check(rows)


def test_greenhouse_boards_fetch_in_parallel_preserving_output_order(
    fake_deps: _FakeDeps,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_deps.set_registry_entries(
        "greenhouse",
        [
            {"slug": "studio-a", "studio": "Studio A", "name": "Studio A"},
            {"slug": "studio-b", "studio": "Studio B", "name": "Studio B"},
            {"slug": "studio-c", "studio": "Studio C", "name": "Studio C"},
        ],
    )
    for slug in ("studio-a", "studio-b", "studio-c"):
        fake_deps.set_response(
            GREENHOUSE_JOBS_URL_TEMPLATE.format(slug=slug),
            {
                "jobs": [
                    {
                        "id": slug,
                        "title": f"{slug} Gameplay Engineer",
                        "location": {"name": "Remote"},
                        "absolute_url": f"https://example/{slug}/jobs/1",
                    }
                ]
            },
        )

    fetches = BlockingActiveCounter(auto_release_at=2)

    def delayed_fetch_with_retries(
        url: str,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
        capture_headers: dict[str, str] | None = None,
    ) -> str:
        fetches.enter()
        try:
            fetches.wait_released()
            return fake_deps.fetch_with_retries(
                url, fetch_text, timeout_s, retries, backoff_s, capture_headers
            )
        finally:
            fetches.exit()

    monkeypatch.setattr(greenhouse_runner, "fetch_with_retries", delayed_fetch_with_retries)

    rows = provider_api.run_greenhouse_boards_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=1,
        backoff_s=0.0,
    )

    assert fetches.peak > 1
    assert [row["studio"] for row in rows] == ["Studio A", "Studio B", "Studio C"]
    details = fake_deps.SOURCE_DIAGNOSTICS["greenhouse_boards"]["details"]
    assert [detail["slug"] for detail in details] == ["studio-a", "studio-b", "studio-c"]
    assert {detail["boardFetchConcurrency"] for detail in details} == {3}


def test_lever_json_feed_sources_fetch_in_parallel_preserving_output_order(
    fake_deps: _FakeDeps,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_deps.set_registry_entries(
        "lever",
        [
            {"account": "studio-a", "studio": "Studio A", "name": "Studio A"},
            {"account": "studio-b", "studio": "Studio B", "name": "Studio B"},
            {"account": "studio-c", "studio": "Studio C", "name": "Studio C"},
        ],
    )
    for account in ("studio-a", "studio-b", "studio-c"):
        fake_deps.set_response(
            f"https://api.lever.co/v0/postings/{account}?mode=json",
            [
                {
                    "id": account,
                    "text": f"{account} Gameplay Engineer",
                    "hostedUrl": f"https://jobs.lever.co/{account}/jobs/1",
                    "categories": {
                        "location": "Remote",
                        "team": "Game Engineering",
                    },
                    "descriptionPlain": "Build gameplay systems for our game team.",
                }
            ],
        )

    fetches = BlockingActiveCounter(auto_release_at=2)

    def delayed_fetch_with_retries(
        url: str,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
        capture_headers: dict[str, str] | None = None,
    ) -> str:
        fetches.enter()
        try:
            fetches.wait_released()
            return fake_deps.fetch_with_retries(
                url, fetch_text, timeout_s, retries, backoff_s, capture_headers
            )
        finally:
            fetches.exit()

    monkeypatch.setattr(json_feed_runner, "fetch_with_retries", delayed_fetch_with_retries)

    rows = provider_api.run_lever_sources_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=1,
        backoff_s=0.0,
    )

    assert fetches.peak > 1
    assert [row["studio"] for row in rows] == ["Studio A", "Studio B", "Studio C"]
    details = fake_deps.SOURCE_DIAGNOSTICS["lever_sources"]["details"]
    assert [detail["name"] for detail in details] == ["Studio A", "Studio B", "Studio C"]
    assert {detail["sourceFetchConcurrency"] for detail in details} == {3}


def _provider_probe(result: dict[str, Any]) -> tuple[dict[str, object], bool, dict[str, Any]]:
    sent: dict[str, Any] = {}
    report: dict[str, object] = {"cacheDecision": "revalidate_only"}

    def fake_revalidate(url: str, timeout_s: int, **kwargs: Any) -> dict[str, Any]:
        sent["url"] = url
        sent["timeout_s"] = timeout_s
        sent.update(kwargs)
        return result

    handled = provider_revalidate_not_modified(
        entry_report=report,
        url="https://example.com/jobs",
        timeout_s=5,
        source_name="Probe Board",
        source_state_rows={"Probe Board": {"lastHttpEtag": "etag-1"}},
        revalidate_url=fake_revalidate,
    )
    return report, handled, sent


def test_provider_probe_retires_a_tag_the_feed_stopped_publishing() -> None:
    """A 200 without ETag/Last-Modified is a changed copy, not a reason to keep asking."""
    report, handled, sent = _provider_probe(
        {
            "supported": True,
            "notModified": False,
            "statusCode": 200,
            "etag": "",
            "lastModified": "",
        }
    )

    assert handled is False, "a changed feed must still run the full read"
    assert sent["etag"] == "etag-1", "the probe asks with the stored tag"
    assert report["httpStatus"] == 200
    assert "httpEtag" not in report
    assert report["clearHttpEtag"] is True
    assert report["clearHttpLastModified"] is True


def test_provider_probe_stores_a_new_tag_without_retiring_it() -> None:
    report, handled, _ = _provider_probe(
        {
            "supported": True,
            "notModified": False,
            "statusCode": 200,
            "etag": "etag-2",
            "lastModified": "",
        }
    )

    assert handled is False
    assert report["httpEtag"] == "etag-2"
    assert "clearHttpEtag" not in report
    assert report["clearHttpLastModified"] is True


@pytest.mark.parametrize(
    ("result", "handled"),
    [
        (
            {
                "supported": True,
                "notModified": True,
                "statusCode": 304,
                "etag": "",
                "lastModified": "",
            },
            True,
        ),
        (
            {
                "supported": True,
                "notModified": False,
                "statusCode": 500,
                "etag": "",
                "lastModified": "",
            },
            False,
        ),
    ],
)
def test_provider_probe_does_not_retire_tags_on_a_304_or_an_error(
    result: dict[str, Any], handled: bool
) -> None:
    """A 304 may echo no headers, and an error says nothing about the feed's validators."""
    report, outcome, _ = _provider_probe(result)

    assert outcome is handled
    assert "clearHttpEtag" not in report
    assert "clearHttpLastModified" not in report


def _greenhouse_detail(fake_deps: _FakeDeps, *, etag: str = "", last_modified: str = "") -> dict:
    url = GREENHOUSE_JOBS_URL_TEMPLATE.format(slug="studio-a")
    fake_deps.set_registry_entries(
        "greenhouse", [{"slug": "studio-a", "studio": "Studio A", "name": "Studio A"}]
    )
    fake_deps.set_response(
        url,
        {
            "jobs": [
                {
                    "id": 1,
                    "text": "Gameplay Engineer",
                    "title": "Gameplay Engineer",
                    "location": {"name": "Remote"},
                    "absolute_url": "https://example/jobs/1",
                }
            ]
        },
    )
    headers = {
        name: value for name, value in (("etag", etag), ("last-modified", last_modified)) if value
    }
    if headers:
        fake_deps.set_response_headers(url, headers)
    provider_api.run_greenhouse_boards_source(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=1,
        backoff_s=0.0,
    )
    detail: dict[str, Any] = fake_deps.SOURCE_DIAGNOSTICS["greenhouse_boards"]["details"][0]
    return detail


def _apply_provider_report(
    detail: dict[str, object],
    rows: dict[str, dict[str, object]] | None = None,
    *,
    finished_minutes_ago: int = 0,
) -> dict[str, dict[str, object]]:
    finished_at = datetime.now(UTC) - timedelta(minutes=finished_minutes_ago)
    return update_source_state_rows(
        source_state_rows=rows or {},
        source_reports=[{**detail, "cacheDecision": "run_now", "cacheDecisionReason": "run_now"}],
        canonical_rows=[],
        finished_at=finished_at.isoformat(),
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=720,
    )


def test_provider_full_read_stores_the_validator_its_get_published(
    fake_deps: _FakeDeps,
) -> None:
    """A feed whose ETag only rides the GET can start revalidating off that response."""
    detail = _greenhouse_detail(
        fake_deps, etag='"v1"', last_modified="Wed, 21 Oct 2026 07:28:00 GMT"
    )

    assert detail["httpEtag"] == '"v1"'
    assert detail["httpLastModified"] == "Wed, 21 Oct 2026 07:28:00 GMT"
    assert "clearHttpEtag" not in detail

    # Past the run freshness window but inside the provider's stable window: the
    # stored GET validator is what turns the next run into a cheap HEAD.
    rows = _apply_provider_report(detail, finished_minutes_ago=30)
    assert rows["Studio A"]["lastHttpEtag"] == '"v1"'
    decision = get_incremental_cache_decision("Studio A", rows, adapter="greenhouse")
    assert decision["cacheDecision"] == "revalidate_only"
    assert decision["cacheDecisionReason"] == "provider_stable_revalidate"


def test_provider_full_read_retires_a_validator_the_get_stopped_publishing(
    fake_deps: _FakeDeps,
) -> None:
    detail = _greenhouse_detail(fake_deps)

    assert "httpEtag" not in detail
    assert detail["clearHttpEtag"] is True

    rows = _apply_provider_report(detail, {"Studio A": {"lastHttpEtag": '"retired"'}})
    assert "lastHttpEtag" not in rows["Studio A"]
