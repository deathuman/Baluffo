from __future__ import annotations

import json
from urllib.error import HTTPError

import pytest

from scripts import check_published_version as cpv


class _FakeResponse:
    def __init__(self, payload: bytes = b"", headers: dict[str, str] | None = None) -> None:
        self._payload = payload
        self.headers = headers or {}

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> _FakeResponse:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


def _token_response() -> _FakeResponse:
    return _FakeResponse(json.dumps({"token": "anon-token"}).encode("utf-8"))


def test_published_version_reports_digest(monkeypatch) -> None:
    digest = "sha256:" + "a" * 64
    responses = iter([_token_response(), _FakeResponse(headers={"Docker-Content-Digest": digest})])

    def fake_urlopen(request, timeout=None):
        return next(responses)

    monkeypatch.setattr(cpv.urllib.request, "urlopen", fake_urlopen)

    state = cpv.check_version_published("1.2.3")

    assert state.published is True
    assert state.digest == digest
    assert "ALREADY published" in state.detail
    assert "bump the version instead" in state.detail


def test_unpublished_version_is_not_a_failure(monkeypatch) -> None:
    def fake_urlopen(request, timeout=None):
        if "token" in getattr(request, "full_url", str(request)):
            return _token_response()
        raise HTTPError(str(request), 404, "not found", {}, None)

    monkeypatch.setattr(cpv.urllib.request, "urlopen", fake_urlopen)

    state = cpv.check_version_published("9.9.999")

    assert state.published is False
    assert state.digest is None
    assert "not published yet" in state.detail


def test_network_failure_degrades_to_skip_rather_than_crashing(monkeypatch) -> None:
    """A flaky registry must never break the release preflight."""

    def fake_urlopen(request, timeout=None):
        raise OSError("network unreachable")

    monkeypatch.setattr(cpv.urllib.request, "urlopen", fake_urlopen)

    state = cpv.check_version_published("1.2.3")

    assert state.published is False
    assert "registry lookup skipped" in state.detail
    assert "verify manually" in state.detail


def test_token_response_without_token_field_is_a_skip(monkeypatch) -> None:
    def fake_urlopen(request, timeout=None):
        return _FakeResponse(json.dumps({"nope": 1}).encode("utf-8"))

    monkeypatch.setattr(cpv.urllib.request, "urlopen", fake_urlopen)

    state = cpv.check_version_published("1.2.3")

    assert state.published is False
    assert "registry lookup skipped" in state.detail


def test_strict_mode_fails_only_when_published(monkeypatch, capsys) -> None:
    published = cpv.PublishedState("1.2.3", True, "sha256:x", "already there")
    monkeypatch.setattr(cpv, "check_version_published", lambda v: published)
    monkeypatch.setattr(cpv.sys, "argv", ["check_published_version.py", "--strict"])

    assert cpv.main() == 1

    fresh = cpv.PublishedState("1.2.4", False, None, "not published yet")
    monkeypatch.setattr(cpv, "check_version_published", lambda v: fresh)

    assert cpv.main() == 0
    assert "not published yet" in capsys.readouterr().out


def test_default_mode_warns_without_failing(monkeypatch) -> None:
    published = cpv.PublishedState("1.2.3", True, "sha256:x", "already there")
    monkeypatch.setattr(cpv, "check_version_published", lambda v: published)
    monkeypatch.setattr(cpv.sys, "argv", ["check_published_version.py"])

    assert cpv.main() == 0


def test_version_defaults_to_app_version(monkeypatch) -> None:
    from src.app_version import APP_VERSION

    captured: list[str] = []

    def fake_check(version: str) -> cpv.PublishedState:
        captured.append(version)
        return cpv.PublishedState(version, False, None, "not published yet")

    monkeypatch.setattr(cpv, "check_version_published", fake_check)
    monkeypatch.setattr(cpv.sys, "argv", ["check_published_version.py"])

    assert cpv.main() == 0
    assert captured == [APP_VERSION]


@pytest.mark.parametrize("code", [404, 403])
def test_absent_and_unauthorized_tags_both_read_as_unpublished(monkeypatch, code: int) -> None:
    def fake_urlopen(request, timeout=None):
        if "token" in getattr(request, "full_url", str(request)):
            return _token_response()
        raise HTTPError(str(request), code, "nope", {}, None)

    monkeypatch.setattr(cpv.urllib.request, "urlopen", fake_urlopen)

    state = cpv.check_version_published("1.2.3")

    assert state.published is False
    assert "not published yet" in state.detail
