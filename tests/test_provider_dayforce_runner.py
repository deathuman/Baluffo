"""Dayforce provider adapter runner tests: end-to-end over registry rows.

Runner contract (mirrors the phenom/oracle runners): iterate registry rows,
run the CSRF two-step per source, paginate until maxCount, classify empties,
surface transport errors as AdapterValidationError with classified text.
"""

from __future__ import annotations

import json
import sys
import urllib.error

import pytest

sys.path.insert(0, ".")

from src.exceptions import AdapterValidationError  # noqa: E402
from src.jobs.adapters.plugins.provider_api import dayforce as dayforce_runner  # noqa: E402
from tests.helpers.dayforce_fixtures import (  # noqa: E402
    FakeResponse,
    ScriptedOpener,
    bind_runner,
    fixture_payload,
    registry_rows,
)

_REF_ROW = {
    "id": "dayforce:client_namespace:ref",
    "name": "Reflector Entertainment (Dayforce)",
    "adapter": "dayforce",
    "studio": "Reflector Entertainment",
    "client_namespace": "ref",
    "listing_url": "https://jobs.dayforcehcm.com/en-CA/ref/CANDIDATEPORTAL",
    "enabledByDefault": True,
}


def test_runner_single_page_end_to_end(monkeypatch) -> None:
    captured = bind_runner(monkeypatch, registry_rows([_REF_ROW]))
    csrf_body = json.dumps({"csrfToken": "t" * 64}).encode()
    search_body = json.dumps(fixture_payload("dayforce_ref_search.json")).encode()

    class _RunnerOpener(ScriptedOpener):
        def open(self, request, timeout):  # noqa: ANN001, ANN202
            if "api/auth/csrf" in request.full_url:
                return FakeResponse(200, csrf_body)
            if "jobposting/search" in request.full_url:
                sent = json.loads((request.data or b"{}").decode())
                assert sent["paginationStart"] == 0
                return FakeResponse(200, search_body)
            raise AssertionError(request.full_url)

    monkeypatch.setattr(
        dayforce_runner.urllib.request, "build_opener", lambda *_a, **_k: _RunnerOpener({})
    )
    jobs = dayforce_runner.run_dayforce_sources_source(
        fetch_text=lambda _u, _t: (_ for _ in ()).throw(AssertionError("fetch_text used")),
        timeout_s=10,
        retries=2,
        backoff_s=0.0,
    )
    assert len(jobs) == 2
    by_id = {job["sourceJobId"]: job for job in jobs}
    assert set(by_id) == {"dayforce:ref:161", "dayforce:ref:149"}
    assert all(job["adapter"] == "dayforce" for job in jobs)
    assert all(job["studio"] == "Reflector Entertainment" for job in jobs)
    # diagnostics carry the adapter surface (provider_url is kwarg-captured per runner)
    assert captured[0]["adapter"] == "dayforce"


def test_runner_paginates_until_max_count(monkeypatch) -> None:
    bind_runner(monkeypatch, registry_rows([_REF_ROW]))
    page1 = {
        "jobPostings": [
            {"jobPostingId": 1, "jobTitle": "One", "postingLocations": []},
            {"jobPostingId": 2, "jobTitle": "Two", "postingLocations": []},
        ],
        "maxCount": 3,
        "offset": 0,
        "count": 2,
    }
    page2 = {
        "jobPostings": [{"jobPostingId": 3, "jobTitle": "Three", "postingLocations": []}],
        "maxCount": 3,
        "offset": 2,
        "count": 1,
    }
    csrf_body = json.dumps({"csrfToken": "t" * 64}).encode()
    seen_starts: list[int] = []

    class _PagedOpener(ScriptedOpener):
        def open(self, request, timeout):  # noqa: ANN001, ANN202
            if "api/auth/csrf" in request.full_url:
                return FakeResponse(200, csrf_body)
            sent = json.loads((request.data or b"{}").decode())
            seen_starts.append(sent["paginationStart"])
            body = page1 if sent["paginationStart"] == 0 else page2
            return FakeResponse(200, json.dumps(body).encode())

    monkeypatch.setattr(
        dayforce_runner.urllib.request, "build_opener", lambda *_a, **_k: _PagedOpener({})
    )
    jobs = dayforce_runner.run_dayforce_sources_source(
        fetch_text=lambda _u, _t: "",
        timeout_s=10,
        retries=2,
        backoff_s=0.0,
    )
    assert seen_starts == [0, 2]
    assert {job["sourceJobId"] for job in jobs} == {
        "dayforce:ref:1",
        "dayforce:ref:2",
        "dayforce:ref:3",
    }


def test_runner_empty_board_is_legit_empty(monkeypatch) -> None:
    row = dict(_REF_ROW, id="dayforce:client_namespace:empty", client_namespace="empty")
    captured = bind_runner(monkeypatch, registry_rows([row]))
    csrf_body = json.dumps({"csrfToken": "t" * 64}).encode()
    empty_body = json.dumps({"jobPostings": [], "maxCount": 0, "offset": 0, "count": 0}).encode()

    class _EmptyOpener(ScriptedOpener):
        def open(self, request, timeout):  # noqa: ANN001, ANN202
            if "api/auth/csrf" in request.full_url:
                return FakeResponse(200, csrf_body)
            return FakeResponse(200, empty_body)

    monkeypatch.setattr(
        dayforce_runner.urllib.request, "build_opener", lambda *_a, **_k: _EmptyOpener({})
    )
    jobs = dayforce_runner.run_dayforce_sources_source(
        fetch_text=lambda _u, _t: "",
        timeout_s=10,
        retries=2,
        backoff_s=0.0,
    )
    assert jobs == []
    # diagnostics carry the legit-empty classification
    details = captured[0]["details"]
    assert details[0]["classification"] == "dayforce_no_public_jobs"
    assert details[0]["emptyConfirmed"] is True


def test_runner_search_403_surfaces_expected_source_error(monkeypatch) -> None:
    bind_runner(monkeypatch, registry_rows([_REF_ROW]))
    csrf_body = json.dumps({"csrfToken": "t" * 64}).encode()

    class _403Opener(ScriptedOpener):
        def open(self, request, timeout):  # noqa: ANN001, ANN202
            if "api/auth/csrf" in request.full_url:
                return FakeResponse(200, csrf_body)
            raise urllib.error.HTTPError(
                request.full_url,
                403,
                "Forbidden",
                {},
                None,  # type: ignore[arg-type]
            )

    monkeypatch.setattr(
        dayforce_runner.urllib.request, "build_opener", lambda *_a, **_k: _403Opener({})
    )
    with pytest.raises(AdapterValidationError) as excinfo:
        dayforce_runner.run_dayforce_sources_source(
            fetch_text=lambda _u, _t: "",
            timeout_s=10,
            retries=2,
            backoff_s=0.0,
        )
    # the error text is classified (HTTP 4xx shape), not a bare raise
    assert "403" in str(excinfo.value)


def test_runner_missing_namespace_is_source_error_not_crash(monkeypatch) -> None:
    broken = dict(_REF_ROW, id="dayforce:listing_url:broken", client_namespace="", listing_url="")
    bind_runner(monkeypatch, registry_rows([broken]))
    with pytest.raises(AdapterValidationError, match="missing client_namespace"):
        dayforce_runner.run_dayforce_sources_source(
            fetch_text=lambda _u, _t: "",
            timeout_s=10,
            retries=2,
            backoff_s=0.0,
        )


# ── Registration ──────────────────────────────────────────────────────────────


def test_dayforce_plugin_registered() -> None:
    from src.jobs.adapters.plugins import default_registry
    from src.jobs.adapters.plugins.provider_api import ensure_registered
    from src.jobs.adapters.plugins.types import AdapterPluginContext

    ensure_registered()
    plugin, _selection = default_registry.select(
        AdapterPluginContext(family="provider_api", adapter_key="dayforce_sources")
    )
    assert plugin.name == "dayforce_sources"


def test_dayforce_registry_adapter_in_provider_set() -> None:
    from src.jobs.registry import PROVIDER_REGISTRY_ADAPTERS

    assert "dayforce" in PROVIDER_REGISTRY_ADAPTERS
