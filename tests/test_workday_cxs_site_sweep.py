"""Focused tests for scripts/workday_cxs_site_sweep.py (no network)."""

from __future__ import annotations

import io
import json
import sys
import urllib.error
import urllib.request
from contextlib import contextmanager
from pathlib import Path
from typing import Any, cast
from urllib.parse import urlparse

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

import workday_cxs_site_sweep as sweep  # noqa: E402


class _FakeResponse:
    def __init__(self, status: int, body: bytes) -> None:
        self.status = status
        self._body = body

    def read(self) -> bytes:
        return self._body

    def geturl(self) -> str:
        return "https://tencent.wd1.myworkdayjobs.com/x"

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *args: object) -> None:
        return None


class _FakeOpener:
    """Stdlib-opener shim: either returns a canned response or raises HTTPError."""

    def __init__(self, response: _FakeResponse | None, url_for_error: str = "") -> None:
        self._response = response
        self._url_for_error = url_for_error
        self.requests: list[urllib.request.Request] = []

    def open(self, request: urllib.request.Request, timeout: int | None = None) -> _FakeResponse:
        self.requests.append(request)
        if self._response is not None:
            return self._response
        raise urllib.error.HTTPError(
            self._url_for_error,
            404,
            "Not Found",
            cast("Any", None),
            fp=io.BytesIO(b"not found"),
        )


@contextmanager
def _patched_opener(monkeypatch: pytest.MonkeyPatch, opener: _FakeOpener):
    captured: dict[str, object] = {}

    def _fake_build_post_opener(endpoint: str) -> _FakeOpener:
        captured["endpoint"] = endpoint
        return opener

    monkeypatch.setattr(sweep, "_build_post_opener", _fake_build_post_opener)
    yield captured


def test_endpoint_shape_mirrors_adapter() -> None:
    endpoint = sweep.cxs_endpoint("tencent.wd1.myworkdayjobs.com", "timi_careers")
    assert endpoint == "https://tencent.wd1.myworkdayjobs.com/wday/cxs/tencent/timi_careers/jobs"


def test_classify_cxs_json_populated_vs_empty() -> None:
    assert sweep.classify_cxs_json({"total": 43, "jobs": [{"x": 1}]}) == "populated"
    assert sweep.classify_cxs_json({"total": 0, "jobs": []}) == "configured_empty"
    assert sweep.classify_cxs_json({"count": 2}) == "populated"
    assert sweep.classify_cxs_json({"unexpected": "shape"}) == "unexpected_json"
    assert sweep.classify_cxs_json(None) == "unexpected_json"


def test_classify_http_status_buckets() -> None:
    assert sweep.classify_http_status(200) == "json_response"
    assert sweep.classify_http_status(303) == "redirect"
    assert sweep.classify_http_status(403) == "blocked_or_challenge"
    assert sweep.classify_http_status(429) == "blocked_or_challenge"
    assert sweep.classify_http_status(404) == "not_found"
    assert sweep.classify_http_status(500) == "http_error"


def test_probe_site_populated_board(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {"total": 43, "jobs": [{"title": "Engineer"}]}
    opener = _FakeOpener(_FakeResponse(200, json.dumps(payload).encode("utf-8")))
    with _patched_opener(monkeypatch, opener) as captured:
        row = sweep.probe_site("tencent.wd1.myworkdayjobs.com", "lightspeed")
    assert row["classification"] == "populated"
    assert row["total"] == 43
    assert row["httpStatus"] == 200
    assert captured["endpoint"] == row["endpoint"]
    # The POST must carry the CXS JSON payload with the right headers.
    assert opener.requests[0].get_method() == "POST"
    body = cast("Any", opener.requests[0].data)
    assert json.loads(body.decode("utf-8")) == {"limit": 20, "offset": 0}


def test_probe_site_configured_empty_board(monkeypatch: pytest.MonkeyPatch) -> None:
    opener = _FakeOpener(_FakeResponse(200, json.dumps({"total": 0, "jobs": []}).encode("utf-8")))
    with _patched_opener(monkeypatch, opener):
        row = sweep.probe_site("tencent.wd1.myworkdayjobs.com", "timi_careers")
    assert row["classification"] == "configured_empty"
    assert row["total"] == 0


def test_probe_site_not_found(monkeypatch: pytest.MonkeyPatch) -> None:
    opener = _FakeOpener(None, url_for_error="https://tencent.wd1.myworkdayjobs.com/wday/cxs")
    with _patched_opener(monkeypatch, opener):
        row = sweep.probe_site("tencent.wd1.myworkdayjobs.com", "does_not_exist")
    assert row["classification"] == "not_found"
    assert row["httpStatus"] == 404


def test_probe_site_non_json_200(monkeypatch: pytest.MonkeyPatch) -> None:
    opener = _FakeOpener(_FakeResponse(200, b"<html>not json</html>"))
    with _patched_opener(monkeypatch, opener):
        row = sweep.probe_site("tencent.wd1.myworkdayjobs.com", "odd_site")
    assert row["classification"] == "unexpected_json"


def test_extract_site_title_both_attr_orders() -> None:
    html_a = '<meta property="og:title" content="Timi Studio Group Careers"/>'
    html_b = '<meta content="Timi Studio Group Careers" property="og:title"/>'
    assert sweep.extract_site_title(html_a) == "Timi Studio Group Careers"
    assert sweep.extract_site_title(html_b) == "Timi Studio Group Careers"
    assert sweep.extract_site_title("<html></html>") == ""


def test_extract_site_title_does_not_span_tags() -> None:
    # A meta tag missing its content must not swallow the rest of the document
    # (regression: DOTALL made the match cross many tags on real Workday pages).
    html = '<meta charset="utf-8"/><meta http-equiv="X-UA-Compatible" content="chrome=1;IE=EDGE"/>'
    assert sweep.extract_site_title(html) == ""
    document = (
        '<head><meta http-equiv="X-UA-Compatible" content="chrome=1;IE=EDGE"/>'
        '<meta property="og:title" content="Timi Studio Group Careers"/></head>'
    )
    assert sweep.extract_site_title(document) == "Timi Studio Group Careers"


def test_extract_sample_titles_caps_and_skips_non_dicts() -> None:
    payload = {
        "total": 3,
        "jobPostings": [
            {"title": "Game Mathematician"},
            "not-a-dict",
            {},
            {"title": "  Senior Software Engineer (Java)  "},
            {"title": "Third"},
        ],
    }
    assert sweep.extract_sample_titles(payload, cap=5) == [
        "Game Mathematician",
        "Senior Software Engineer (Java)",
        "Third",
    ]
    assert sweep.extract_sample_titles(payload, cap=2) == [
        "Game Mathematician",
        "Senior Software Engineer (Java)",
    ]
    assert sweep.extract_sample_titles({"total": 1}) == []
    assert sweep.extract_sample_titles(None) == []


def test_probe_site_captures_sample_titles(monkeypatch: pytest.MonkeyPatch) -> None:
    payload = {
        "total": 2,
        "jobPostings": [{"title": "Game Illustrator"}, {"title": "Slot Artist"}],
    }
    opener = _FakeOpener(_FakeResponse(200, json.dumps(payload).encode("utf-8")))
    with _patched_opener(monkeypatch, opener):
        row = sweep.probe_site("lnw.wd5.myworkdayjobs.com", "GroverGamingExternalCareerSite")
    assert row["classification"] == "populated"
    assert row["total"] == 2
    assert row["sampleTitles"] == ["Game Illustrator", "Slot Artist"]


def test_tls_host_gate() -> None:
    assert sweep.is_workday_tls_host("https://xboxgaming.wd1.myworkdayjobs.com/wday/cxs/x/y/jobs")
    assert not sweep.is_workday_tls_host("https://example.jobs.example.com/wday/cxs/x/y/jobs")


def test_no_redirect_handler_refuses_to_follow() -> None:
    handler = sweep._NoRedirectHandler()
    assert handler.redirect_request(None, None, 303, "See Other", None, "https://other") is None


def test_endpoint_url_path_shape() -> None:
    parsed = urlparse(sweep.cxs_endpoint("tencent.wd1.myworkdayjobs.com", "TiMi_Careers"))
    assert parsed.path == "/wday/cxs/tencent/TiMi_Careers/jobs"
