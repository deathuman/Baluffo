"""Workday CXS TLS-context gate and maintenance-redirect classification (T5/WP18).

Verified TLS previously hard-failed *.myworkdayjobs.com with "certificate has expired"
even though the served chain is valid: the OS cert store can hold a stale Workday-chain
intermediate that poisons chain building. The CXS fetcher therefore anchors verification
on certifi (CERT_REQUIRED, hostname-checked) for that host family only, and surfaces
Workday maintenance 303s as classified HttpStatusError instead of JSON decode noise.
"""

from __future__ import annotations

import json
import ssl
from typing import Any
from urllib.error import HTTPError

import pytest

from src.jobs.adapters import provider_structured_listing as runner
from src.jobs.common.http import HttpStatusError

_ENDPOINT = "https://example.wd5.myworkdayjobs.com/wday/cxs/example/Site/jobs"
_PAYLOAD = {"appliedFacets": {}, "limit": 20, "offset": 0, "searchText": ""}


class _JsonResponse:
    def __init__(self, payload: dict[str, Any]) -> None:
        self._body = json.dumps(payload).encode("utf-8")

    def __enter__(self) -> _JsonResponse:
        return self

    def __exit__(self, *_args: object) -> None:
        return None

    def read(self) -> bytes:
        return self._body


class _FakeOpener:
    def __init__(
        self, *, response: _JsonResponse | None = None, error: Exception | None = None
    ) -> None:
        self._response = response
        self._error = error
        self.requests: list[Any] = []
        self.timeouts: list[int] = []

    def open(self, request: Any, timeout: int) -> _JsonResponse:
        self.requests.append(request)
        self.timeouts.append(timeout)
        if self._error is not None:
            raise self._error
        assert self._response is not None
        return self._response


def test_workday_tls_context_is_fully_verified() -> None:
    context = runner._workday_tls_context()
    assert context is not None
    assert context.verify_mode == ssl.CERT_REQUIRED
    assert context.check_hostname is True
    assert context.get_ca_certs(), "certifi anchors must be loaded"


def test_workday_tls_host_gate_matches_only_workday_hosts() -> None:
    assert runner._is_workday_tls_host(
        "https://xboxgaming.wd1.myworkdayjobs.com/wday/cxs/xboxgaming/External/jobs"
    )
    assert runner._is_workday_tls_host("https://Example.WD5.MyWorkdayJobs.com/jobs")
    assert not runner._is_workday_tls_host("https://beamdog.bamboohr.com/careers/list")
    assert not runner._is_workday_tls_host("https://jobs.example.com/careers")
    assert not runner._is_workday_tls_host("")


def test_fetch_passes_certifi_context_to_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_build_opener(tls_context: ssl.SSLContext | None) -> _FakeOpener:
        captured["context"] = tls_context
        return _FakeOpener(response=_JsonResponse({"total": 0, "jobPostings": []}))

    monkeypatch.setattr(runner, "_build_cxs_opener", fake_build_opener)
    page = runner._fetch_workday_cxs_page(
        endpoint=_ENDPOINT,
        payload=_PAYLOAD,
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )
    assert page == {"total": 0, "jobPostings": []}
    context = captured["context"]
    assert isinstance(context, ssl.SSLContext)
    assert context.verify_mode == ssl.CERT_REQUIRED, "no verification downgrade"
    assert context.check_hostname is True


def test_non_workday_host_keeps_default_transport(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, Any] = {}

    def fake_build_opener(tls_context: ssl.SSLContext | None) -> _FakeOpener:
        captured["context"] = tls_context
        return _FakeOpener(response=_JsonResponse({}))

    monkeypatch.setattr(runner, "_build_cxs_opener", fake_build_opener)
    runner._fetch_workday_cxs_page(
        endpoint="https://example.jobs.example.com/wday/cxs/x/y/jobs",
        payload={},
        timeout_s=5,
        retries=0,
        backoff_s=0.0,
    )
    assert captured["context"] is None


def test_maintenance_redirect_raises_classified_http_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Workday 303 -> maintenance page must surface as HttpStatusError, not JSON noise."""
    error = HTTPError(
        _ENDPOINT,
        303,
        "See Other",
        {"Location": "https://community.workday.com/maintenance-page"},
        None,
    )

    def fake_build_opener(tls_context: ssl.SSLContext | None) -> _FakeOpener:
        assert tls_context is not None
        return _FakeOpener(error=error)

    monkeypatch.setattr(runner, "_build_cxs_opener", fake_build_opener)
    with pytest.raises(HttpStatusError) as excinfo:
        runner._fetch_workday_cxs_page(
            endpoint="https://xboxgaming.wd1.myworkdayjobs.com/wday/cxs/xboxgaming/CentralTech/jobs",
            payload=_PAYLOAD,
            timeout_s=5,
            retries=2,
            backoff_s=0.0,
        )
    assert excinfo.value.code == 303
    assert "maintenance" in (excinfo.value.location or "")


def test_maintenance_redirect_is_not_retried(monkeypatch: pytest.MonkeyPatch) -> None:
    error = HTTPError(
        _ENDPOINT,
        303,
        "See Other",
        {"Location": "https://community.workday.com/maintenance-page"},
        None,
    )
    opener = _FakeOpener(error=error)

    monkeypatch.setattr(runner, "_build_cxs_opener", lambda _ctx: opener)
    with pytest.raises(HttpStatusError):
        runner._fetch_workday_cxs_page(
            endpoint="https://xboxgaming.wd1.myworkdayjobs.com/wday/cxs/xboxgaming/External/jobs",
            payload=_PAYLOAD,
            timeout_s=5,
            retries=3,
            backoff_s=0.0,
        )
    assert len(opener.requests) == 1


def test_other_http_errors_still_retry_and_raise_last(monkeypatch: pytest.MonkeyPatch) -> None:
    opener = _FakeOpener(error=HTTPError(_ENDPOINT, 503, "Service Unavailable", {}, None))

    monkeypatch.setattr(runner, "_build_cxs_opener", lambda _ctx: opener)
    with pytest.raises(HTTPError):
        runner._fetch_workday_cxs_page(
            endpoint=_ENDPOINT,
            payload=_PAYLOAD,
            timeout_s=5,
            retries=1,
            backoff_s=0.0,
        )
    assert len(opener.requests) == 2
