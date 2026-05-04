from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from urllib.parse import quote

import pytest

import src.source_sync_runtime as runtime


class _SyncOperationError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _root(now: datetime | None = None) -> SimpleNamespace:
    current = now or datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    return SimpleNamespace(
        MACHINE_SCOPE="baluffo-test",
        RUNTIME_STATE_RATE_LIMITED="rate_limited",
        INSTALLATION_TOKEN_REFRESH_SKEW_SECONDS=300,
        DEFAULT_TIMEOUT_S=20,
        RATE_LIMIT_BACKOFF_BASE_S=30,
        RATE_LIMIT_BACKOFF_MAX_S=300,
        RATE_LIMIT_WINDOW_S=60,
        RATE_LIMIT_MAX_REQUESTS=2,
        SYNC_ALLOWED_REPO_ENV="BALUFFO_SYNC_ALLOWED_REPO",
        SYNC_ALLOWED_BRANCH_ENV="BALUFFO_SYNC_ALLOWED_BRANCH",
        SYNC_ALLOWED_PATH_PREFIX_ENV="BALUFFO_SYNC_ALLOWED_PATH_PREFIX",
        SyncOperationError=_SyncOperationError,
        quote=quote,
        now_utc=lambda: current,
        now_iso=lambda: current.isoformat(),
        _github_api_base=lambda: "https://api.github.test",
        _github_json_headers=lambda token: {"Authorization": token},
        build_app_jwt=lambda app_id, private_key_pem: f"jwt:{app_id}:{private_key_pem}",
    )


@pytest.fixture(autouse=True)
def _reset_runtime_globals() -> None:
    runtime._RUNTIME_STATE.update({"code": "", "message": "", "until": "", "updatedAt": ""})
    runtime._RATE_LIMIT_STATE.update(
        {
            "calls": [],
            "strike": 0,
            "until": None,
            "remaining": None,
            "limit": None,
            "resetAt": None,
        }
    )
    runtime._AUTH_MANAGER.clear()


def test_runtime_state_payload_clears_expired_rate_limit() -> None:
    now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    root = _root(now)
    runtime.set_runtime_state(
        root,
        root.RUNTIME_STATE_RATE_LIMITED,
        "limited",
        until=now - timedelta(seconds=1),
    )

    assert runtime.runtime_state_payload(root) == {
        "code": "",
        "message": "",
        "until": "",
        "updatedAt": "",
        "lastRateLimitRemaining": "",
        "lastRateLimitResetAt": "",
    }


def test_rate_limit_note_response_tracks_quota_telemetry_and_warns(caplog) -> None:
    now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    root = _root(now)
    reset_at = now + timedelta(seconds=120)

    with caplog.at_level(logging.WARNING):
        runtime.rate_limit_note_response(
            root,
            200,
            {
                "x-ratelimit-limit": "50",
                "x-ratelimit-remaining": "4",
                "x-ratelimit-reset": str(int(reset_at.timestamp())),
            },
            {},
        )

    payload = runtime.runtime_state_payload(root)
    assert payload["lastRateLimitRemaining"] == "4"
    assert payload["lastRateLimitResetAt"] == reset_at.isoformat()
    rate_limit = runtime.rate_limit_payload(root)
    assert rate_limit == {
        "remaining": 4,
        "limit": 50,
        "remainingPercent": 8.0,
        "resetAt": reset_at.isoformat(),
        "until": "",
        "strike": 0,
        "low": True,
    }
    assert "rate limit low" in caplog.text.lower()


def test_local_wrapped_key_cache_handles_invalid_mismatch_and_dpapi_round_trip(
    tmp_path, monkeypatch
) -> None:
    root = _root()
    config_path = tmp_path / "sync-config.json"
    wrapped_calls: list[bytes] = []

    monkeypatch.setattr(runtime.os, "name", "nt")
    monkeypatch.setattr(
        runtime,
        "dpapi_protect",
        lambda _root, raw: wrapped_calls.append(raw) or "wrapped-private-key",
    )
    monkeypatch.setattr(
        runtime,
        "dpapi_unprotect",
        lambda _root, encoded: b"private-key" if encoded == "wrapped-private-key" else b"",
    )

    assert runtime.read_local_wrapped_key(root, config_path, "machine-a") == ""
    runtime.local_key_cache_path(config_path).write_text("not-json", encoding="utf-8")
    assert runtime.read_local_wrapped_key(root, config_path, "machine-a") == ""
    runtime.local_key_cache_path(config_path).write_text(
        '{"fingerprint": "other", "dpapi": "wrapped-private-key"}',
        encoding="utf-8",
    )
    assert runtime.read_local_wrapped_key(root, config_path, "machine-a") == ""

    runtime.write_local_wrapped_key(root, config_path, "machine-a", "private-key")

    assert wrapped_calls == [b"private-key"]
    assert runtime.read_local_wrapped_key(root, config_path, "machine-a") == "private-key"


def test_allowlist_error_applies_env_precedence_and_each_guard() -> None:
    root = _root()
    normalized = {
        "allowedRepo": "owner/repo",
        "allowedBranch": "main",
        "allowedPathPrefix": "baluffo/",
    }

    assert (
        runtime.allowlist_error(
            root,
            repo="wrong/repo",
            branch="main",
            path="baluffo/source-sync.json",
            normalized=normalized,
            env_map={},
        )
        == "Blocked by allowlist: repo must be owner/repo."
    )
    assert (
        runtime.allowlist_error(
            root,
            repo="env/repo",
            branch="dev",
            path="baluffo/source-sync.json",
            normalized=normalized,
            env_map={root.SYNC_ALLOWED_REPO_ENV: "env/repo"},
        )
        == "Blocked by allowlist: branch must be main."
    )
    assert (
        runtime.allowlist_error(
            root,
            repo="owner/repo",
            branch="main",
            path="other/source-sync.json",
            normalized=normalized,
            env_map={},
        )
        == "Blocked by allowlist: path must start with baluffo/."
    )
    assert (
        runtime.allowlist_error(
            root,
            repo="env/repo",
            branch="release",
            path="sync/source.json",
            normalized=normalized,
            env_map={
                root.SYNC_ALLOWED_REPO_ENV: "env/repo",
                root.SYNC_ALLOWED_BRANCH_ENV: "release",
                root.SYNC_ALLOWED_PATH_PREFIX_ENV: "sync/",
            },
        )
        == ""
    )


def test_github_app_auth_refresh_reports_http_and_malformed_token_errors() -> None:
    root = _root()
    auth = SimpleNamespace(
        packaged_config=SimpleNamespace(
            app_id="123",
            installation_id="456",
            private_key_pem="pem",
        )
    )
    responses = iter(
        [
            (500, {"message": "denied"}, {}),
            (201, {"token": "", "expires_at": "2099-01-01T00:00:00Z"}, {}),
        ]
    )
    calls: list[dict[str, object]] = []

    def fake_request(**kwargs):
        calls.append(kwargs)
        return next(responses)

    root._request_raw_json = fake_request

    with pytest.raises(RuntimeError, match="denied"):
        runtime.github_app_auth_refresh_installation_token(root, auth, opener=object())
    with pytest.raises(RuntimeError, match="missing token or expires_at"):
        runtime.github_app_auth_refresh_installation_token(root, auth, opener=object())
    assert calls[0]["method"] == "POST"
    assert calls[0]["url"] == "https://api.github.test/app/installations/456/access_tokens"


def test_rate_limit_retry_after_covers_header_reset_secondary_and_default(monkeypatch) -> None:
    now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    root = _root(now)

    assert runtime.rate_limit_retry_after_seconds(root, {"retry-after": "0"}, {}) == 1
    assert (
        runtime.rate_limit_retry_after_seconds(
            root,
            {"x-ratelimit-reset": str(int((now + timedelta(seconds=90)).timestamp()))},
            {},
        )
        == 90
    )
    assert (
        runtime.rate_limit_retry_after_seconds(
            root, {}, {"message": "You have exceeded a secondary rate limit"}
        )
        == 150
    )
    assert runtime.rate_limit_retry_after_seconds(root, {}, {}) == 30


def test_rate_limit_preflight_throttles_and_note_response_clears_cooldown() -> None:
    now = datetime(2026, 4, 25, 12, 0, tzinfo=UTC)
    root = _root(now)
    runtime._RATE_LIMIT_STATE.update(
        {"calls": [now, now - timedelta(seconds=1)], "strike": 0, "until": None}
    )

    with pytest.raises(_SyncOperationError) as ctx:
        runtime.rate_limit_preflight(root)

    assert ctx.value.code == root.RUNTIME_STATE_RATE_LIMITED
    assert runtime._RATE_LIMIT_STATE["strike"] == 1

    runtime._RATE_LIMIT_STATE.update(
        {"calls": [], "strike": 2, "until": now - timedelta(seconds=1)}
    )
    runtime.set_runtime_state(root, root.RUNTIME_STATE_RATE_LIMITED, "limited", until=now)
    runtime.rate_limit_note_response(root, 200, {}, {})

    assert runtime._RATE_LIMIT_STATE["strike"] == 1
    assert runtime._RATE_LIMIT_STATE["until"] is None
    assert runtime.runtime_state_payload(root)["code"] == ""


def test_request_json_does_not_retry_401_when_retry_disabled(monkeypatch) -> None:
    root = _root()
    manager_calls: list[dict[str, object]] = []
    raw_calls: list[dict[str, object]] = []
    opener = object()

    class _Manager:
        def get_installation_token(self, **kwargs):
            manager_calls.append(kwargs)
            return "installation-token"

    monkeypatch.setattr(runtime, "get_auth_manager", lambda _root, _config: _Manager())

    def fake_raw_request(**kwargs):
        raw_calls.append(kwargs)
        return 401, {"message": "bad credentials"}, {}

    root._request_raw_json = fake_raw_request

    status, body, _headers = runtime.request_json(
        root,
        method="GET",
        url="https://api.github.test/resource",
        config=object(),
        timeout_s=5,
        opener=opener,
        allow_retry_401=False,
    )

    assert status == 401
    assert body["message"] == "bad credentials"
    assert manager_calls == [{"opener": opener}]
    assert len(raw_calls) == 1
