"""Dayforce (CANDIDATEPORTAL) provider adapter tests: URL shapes, normalization, client.

Contract captured live in tmp/holdtail-wave2-20260910/dayforce-findings.md:
GET /api/auth/csrf (cookie jar) -> csrfToken; POST /api/geo/{ns}/jobposting/search
with x-csrf-token + the SAME client's cookies (cookieless replay 403s).
Runner end-to-end tests: tests/test_provider_dayforce_runner.py.
"""

from __future__ import annotations

import json
import sys
import urllib.error

import pytest

sys.path.insert(0, ".")

from src.jobs.adapters.plugins.provider_api import dayforce as dayforce_runner  # noqa: E402
from tests.helpers.dayforce_fixtures import (  # noqa: E402
    FakeResponse,
    ScriptedOpener,
    fixture_payload,
)

# ── URL / namespace derivation ────────────────────────────────────────────────


def test_client_namespace_from_explicit_field() -> None:
    assert dayforce_runner._client_namespace_for_source({"client_namespace": "ref"}) == "ref"
    assert (
        dayforce_runner._client_namespace_for_source({"clientNamespace": "AcmeCorp"}) == "AcmeCorp"
    )


def test_client_namespace_from_portal_listing_url() -> None:
    # The canonical row shape stores the SPA URL; namespace resolves from
    # the dedicated field in practice, but a dayforcehcm portal URL also works.
    assert dayforce_runner._client_namespace_for_source({}) == ""
    assert (
        dayforce_runner._client_namespace_for_source(
            {"listing_url": "https://jobs.dayforcehcm.com/en-CA/ref/CANDIDATEPORTAL"}
        )
        == "ref"
    )


def test_culture_from_url_shapes() -> None:
    assert (
        dayforce_runner._culture_from_url("https://jobs.dayforcehcm.com/en-CA/ref/CANDIDATEPORTAL")
        == "en-CA"
    )
    assert (
        dayforce_runner._culture_from_url("https://jobs.dayforcehcm.com/fr-FR/acme/CANDIDATEPORTAL")
        == "fr-FR"
    )
    assert dayforce_runner._culture_from_url("https://jobs.dayforcehcm.com/") == "en-CA"


def test_search_url_and_payload_shape() -> None:
    assert (
        dayforce_runner._search_url("ref")
        == "https://jobs.dayforcehcm.com/api/geo/ref/jobposting/search"
    )
    payload = dayforce_runner._search_payload(
        client_namespace="ref", culture_code="en-CA", pagination_start=25
    )
    assert payload == {
        "clientNamespace": "ref",
        "jobBoardCode": "CANDIDATEPORTAL",
        "cultureCode": "en-CA",
        "distanceUnit": 1,
        "paginationStart": 25,
    }


# ── Row normalization ─────────────────────────────────────────────────────────


def test_dayforce_api_job_row_normalizes_capture_shape() -> None:
    payload = fixture_payload("dayforce_ref_search.json")
    posting = payload["jobPostings"][0]
    row = dayforce_runner.dayforce_api_job_row(
        posting,
        client_namespace="ref",
        culture_code="en-CA",
        fallback_company="Reflector Entertainment",
    )
    assert row is not None
    assert row["sourceJobId"] == "dayforce:ref:161"
    assert row["title"] == "Programmer Engine and Tools"
    assert row["company"] == "Reflector Entertainment"
    # HTML entities in jobDescription decoded
    assert "You’ll build" in row["description"]
    assert "&rsquo;" not in row["description"]
    assert "&amp;" not in row["description"]
    assert row["city"] == "Montréal"
    assert row["country"] == "CA"
    assert "Montréal" in row["locationSummary"]
    assert row["jobLink"] == "https://jobs.dayforcehcm.com/en-CA/ref/CANDIDATEPORTAL/jobs/161"
    assert row["sector"] == "Game"
    assert row["postedAt"] == "2026-09-10T04:00:00+00:00"
    # the search payload IS the body — detail fetch skipped
    assert row["_skipDetailFetch"] is True


def test_dayforce_api_job_row_handles_virtual_and_missing_location() -> None:
    payload = fixture_payload("dayforce_ref_search.json")
    row = dayforce_runner.dayforce_api_job_row(
        payload["jobPostings"][1],
        client_namespace="ref",
        culture_code="en-CA",
        fallback_company="Reflector Entertainment",
    )
    assert row is not None
    assert row["city"] == ""
    assert row["locations"] == []
    assert row["jobLink"].endswith("/jobs/149")


def test_dayforce_api_job_row_requires_id_and_title() -> None:
    assert (
        dayforce_runner.dayforce_api_job_row(
            {"jobPostingId": "", "jobTitle": "X"},
            client_namespace="ref",
            culture_code="en-CA",
            fallback_company="c",
        )
        is None
    )
    assert (
        dayforce_runner.dayforce_api_job_row(
            {"jobPostingId": "5", "jobTitle": ""},
            client_namespace="ref",
            culture_code="en-CA",
            fallback_company="c",
        )
        is None
    )


# ── Empty classification ──────────────────────────────────────────────────────


def test_mark_empty_dayforce_payload_classifications() -> None:
    report: dict = {}
    dayforce_runner._mark_empty_dayforce_payload(report, fetched_count=0)
    assert report["emptyConfirmed"] is True
    assert report["zeroKeptClassification"] == "legit_empty"
    report2: dict = {}
    dayforce_runner._mark_empty_dayforce_payload(report2, fetched_count=7)
    assert "emptyConfirmed" not in report2
    assert report2["classification"] == "dayforce_no_supported_game_jobs"


# ── The CSRF two-step client ──────────────────────────────────────────────────


def test_client_two_step_sends_csrf_token_and_payload(monkeypatch) -> None:
    csrf_body = json.dumps({"csrfToken": "tok" * 8}).encode()
    opener = ScriptedOpener(
        {
            "https://jobs.dayforcehcm.com/api/auth/csrf": (200, csrf_body),
            "https://jobs.dayforcehcm.com/api/geo/ref/jobposting/search": (
                200,
                json.dumps(fixture_payload("dayforce_ref_search.json")).encode(),
            ),
        }
    )
    monkeypatch.setattr(dayforce_runner.urllib.request, "build_opener", lambda *_a, **_k: opener)
    client = dayforce_runner._DayforceClient(timeout_s=10)
    token = client.fetch_csrf_token()
    assert token == "tok" * 8
    payload = client.search(
        client_namespace="ref",
        culture_code="en-CA",
        pagination_start=0,
        csrf_token=token,
    )
    assert payload["jobPostings"][0]["jobPostingId"] == 161

    assert len(opener.requests) == 2
    csrf_url, csrf_data, _ = opener.requests[0]
    assert csrf_url.endswith("/api/auth/csrf")
    assert csrf_data is None
    search_url, search_data, search_headers = opener.requests[1]
    assert "/api/geo/ref/jobposting/search" in search_url
    sent = json.loads(search_data.decode())
    assert sent["clientNamespace"] == "ref"
    assert sent["jobBoardCode"] == "CANDIDATEPORTAL"
    assert sent["cultureCode"] == "en-CA"
    assert sent["paginationStart"] == 0
    assert sent["distanceUnit"] == 1
    assert search_headers.get("X-csrf-token") == token
    assert search_headers.get("Content-type") == "application/json"


def test_client_search_403_raises_expected_transport_error(monkeypatch) -> None:
    csrf_body = json.dumps({"csrfToken": "t" * 64}).encode()

    class _403Opener(ScriptedOpener):
        def open(self, request, timeout):  # noqa: ANN001, ANN202
            if "jobposting/search" in request.full_url:
                raise urllib.error.HTTPError(
                    request.full_url,
                    403,
                    "Forbidden",
                    {},
                    None,  # type: ignore[arg-type]
                )
            return FakeResponse(200, csrf_body)

    monkeypatch.setattr(
        dayforce_runner.urllib.request, "build_opener", lambda *_a, **_k: _403Opener({})
    )
    client = dayforce_runner._DayforceClient(timeout_s=10)
    token = client.fetch_csrf_token()
    with pytest.raises(dayforce_runner._DayforceHttpError) as excinfo:
        client.search(
            client_namespace="ref",
            culture_code="en-CA",
            pagination_start=0,
            csrf_token=token,
        )
    assert "403" in str(excinfo.value)
    assert "csrf" in str(excinfo.value)


def test_client_csrf_missing_token_raises(monkeypatch) -> None:
    ScriptedOpener({"https://jobs.dayforcehcm.com/api/auth/csrf": (200, b'{"nope": 1}')})
    monkeypatch.setattr(
        dayforce_runner.urllib.request,
        "build_opener",
        lambda *_a, **_k: ScriptedOpener(
            {"https://jobs.dayforcehcm.com/api/auth/csrf": (200, b'{"nope": 1}')}
        ),
    )
    client = dayforce_runner._DayforceClient(timeout_s=10)
    with pytest.raises(dayforce_runner._DayforceHttpError, match="no csrfToken"):
        client.fetch_csrf_token()
