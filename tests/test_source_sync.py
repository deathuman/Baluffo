import base64
import json
import ssl
import threading
from datetime import timedelta
from urllib.error import HTTPError, URLError

import pytest

from src import source_sync as sync
from src import source_sync_crypto
from src.shared import github_https
from tests.source_sync_helpers import source_sync_test_root  # noqa: F401


class _FakeResponse:
    def __init__(self, status: int, payload: dict, headers: dict | None = None):
        self._status = int(status)
        self._payload = dict(payload)
        self.headers = dict(headers or {})

    def getcode(self):
        return self._status

    def read(self):
        return json.dumps(self._payload).encode("utf-8")

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Recorder:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self._uploaded_shards: dict[str, str] = {}

    def __call__(self, req, timeout=20):  # noqa: ANN001
        url = req.full_url
        method = req.get_method()
        body = req.data.decode("utf-8") if isinstance(req.data, bytes) else ""
        self.calls.append(
            {
                "url": url,
                "method": method,
                "headers": dict(req.header_items()),
                "body": body,
                "timeout": timeout,
            }
        )
        if method == "GET" and url.split("?ref=", 1)[0].endswith("/source-sync/shards"):
            return _FakeResponse(404, {"message": "Not Found"})
        if "/source-sync/shards/" in url:
            key = url.split("?ref=", 1)[0]
            if method == "PUT":
                self._uploaded_shards[key] = str(json.loads(body).get("content") or "")
                return _FakeResponse(201, {"content": {"sha": "shard-sha"}})
            if method == "GET" and key in self._uploaded_shards:
                return _FakeResponse(200, {"content": self._uploaded_shards[key]})
        if not self.responses:
            raise AssertionError("No fake responses left")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def _write_packaged_sync_config(path, *, repo: str = "owner/repo") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "schemaVersion": 1,
                "appId": "123456",
                "installationId": "999999",
                "repo": repo,
                "branch": "main",
                "path": "baluffo/source-sync.json",
                "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
            }
        ),
        encoding="utf-8",
    )


def test_packaged_sync_config_env_path_wins(monkeypatch, tmp_path):
    default_path = tmp_path / "default" / "github-app-sync-config.json"
    env_path = tmp_path / "env" / "github-app-sync-config.json"
    _write_packaged_sync_config(default_path, repo="default/repo")
    _write_packaged_sync_config(env_path, repo="env/repo")
    monkeypatch.setattr(sync, "DEFAULT_PACKAGED_SYNC_CONFIG_PATH", default_path)

    cfg = sync.load_packaged_sync_config(env={sync.PACKAGED_SYNC_CONFIG_ENV: str(env_path)})

    assert cfg is not None
    assert cfg.repo == "env/repo"
    assert cfg.config_path == str(env_path.resolve())


def test_packaged_sync_config_default_path_is_used(monkeypatch, tmp_path):
    default_path = tmp_path / "packaging" / "github-app-sync-config.json"
    _write_packaged_sync_config(default_path, repo="bundled/repo")
    monkeypatch.setattr(sync, "DEFAULT_PACKAGED_SYNC_CONFIG_PATH", default_path)

    cfg = sync.load_packaged_sync_config(env={})

    assert cfg is not None
    assert cfg.repo == "bundled/repo"


def test_packaged_sync_config_falls_back_to_user_locations(monkeypatch, tmp_path):
    missing_default = tmp_path / "missing" / "github-app-sync-config.json"
    build_config = tmp_path / "build" / "github-app-sync-config.json"
    appdata_config = tmp_path / "appdata" / "Baluffo" / "github-app-sync-config.json"
    local_appdata_config = tmp_path / "localappdata" / "Baluffo" / "github-app-sync-config.json"
    home_config = tmp_path / "home" / ".baluffo" / "github-app-sync-config.json"
    monkeypatch.setattr(sync, "DEFAULT_PACKAGED_SYNC_CONFIG_PATH", missing_default)

    cases = [
        (
            build_config,
            {sync.PACKAGED_SYNC_BUILD_CONFIG_ENV: str(build_config)},
            "build/repo",
        ),
        (
            appdata_config,
            {"APPDATA": str(tmp_path / "appdata")},
            "appdata/repo",
        ),
        (
            local_appdata_config,
            {"LOCALAPPDATA": str(tmp_path / "localappdata")},
            "localappdata/repo",
        ),
        (
            home_config,
            {"HOME": str(tmp_path / "home")},
            "home/repo",
        ),
    ]

    for path, env, repo in cases:
        _write_packaged_sync_config(path, repo=repo)
        cfg = sync.load_packaged_sync_config(env=env)
        assert cfg is not None
        assert cfg.repo == repo
        path.unlink()


def test_missing_packaged_sync_status_lists_search_paths(monkeypatch, tmp_path):
    missing_default = tmp_path / "packaging" / "github-app-sync-config.json"
    monkeypatch.setattr(sync, "DEFAULT_PACKAGED_SYNC_CONFIG_PATH", missing_default)
    monkeypatch.setenv("APPDATA", str(tmp_path / "appdata"))
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "localappdata"))
    monkeypatch.setenv("HOME", str(tmp_path / "home"))
    monkeypatch.setenv("USERPROFILE", str(tmp_path / "home"))

    env = {
        "APPDATA": str(tmp_path / "appdata"),
        "LOCALAPPDATA": str(tmp_path / "localappdata"),
        "HOME": str(tmp_path / "home"),
        "USERPROFILE": str(tmp_path / "home"),
    }
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=env)
    status = sync.config_status(cfg)

    assert "packaged_github_app_config" in status["missing"]
    assert str(missing_default.resolve()) in status["configSearchPaths"]
    assert (
        str((tmp_path / "home" / ".baluffo" / "github-app-sync-config.json").resolve())
        in status["configSearchPaths"]
    )
    assert "Searched:" in status["message"]


def test_encrypt_and_decrypt_private_key_round_trip():
    salt_b64 = sync._base64url_encode(b"unit-test-salt-123")  # noqa: SLF001
    private_key = "-----BEGIN RSA PRIVATE KEY-----\nabc123\n-----END RSA PRIVATE KEY-----"
    encrypted = sync.encrypt_private_key_pem(
        private_key, salt_b64=salt_b64, app_id="1", installation_id="2"
    )
    assert encrypted.startswith("v2.")
    decrypted = sync.decrypt_private_key_pem(
        encrypted, salt_b64=salt_b64, app_id="1", installation_id="2"
    )
    assert decrypted == private_key


def test_passphrase_encrypt_and_decrypt_private_key_round_trip():
    salt_b64 = sync._base64url_encode(b"unit-test-salt-456")  # noqa: SLF001
    private_key = "-----BEGIN RSA PRIVATE KEY-----\nxyz789\n-----END RSA PRIVATE KEY-----"
    encrypted = sync.encrypt_private_key_pem_with_passphrase(
        private_key,
        salt_b64=salt_b64,
        app_id="1",
        installation_id="2",
        passphrase="unit-passphrase",
    )
    assert encrypted.startswith("v2.")
    decrypted = sync.decrypt_private_key_pem_with_passphrase(
        encrypted,
        salt_b64=salt_b64,
        app_id="1",
        installation_id="2",
        passphrase="unit-passphrase",
    )
    assert decrypted == private_key


def test_config_status_covers_supported_states(source_sync_test_root):
    salt_passphrase = sync._base64url_encode(b"unit-test-salt-789")  # noqa: SLF001
    salt_embedded = sync._base64url_encode(b"unit-test-salt-013")  # noqa: SLF001
    private_key = "-----BEGIN RSA PRIVATE KEY-----\nabc123\n-----END RSA PRIVATE KEY-----"
    passphrase_encrypted = sync.encrypt_private_key_pem_with_passphrase(
        private_key,
        salt_b64=salt_passphrase,
        app_id="123456",
        installation_id="999999",
        passphrase="shared-secret",
    )
    embedded_key_encrypted = source_sync_crypto.encrypt_private_key_pem_for_embedded(
        private_key,
        salt_b64=salt_embedded,
        app_id="123456",
        installation_id="999999",
        hint="embedded-hint-01",
        version="v1",
    )

    cases = [
        {
            "name": "packaged config missing",
            "settings": {"enabled": True},
            "packaged_config": None,
            "expected": {
                "enabled": True,
                "ready": False,
                "state": "misconfigured",
                "missing_contains": "packaged_github_app_config",
            },
        },
        {
            "name": "locally disabled",
            "settings": {"enabled": False},
            "packaged_config": "__default__",
            "expected": {"enabled": False, "ready": False, "state": "disabled"},
        },
        {
            "name": "root feature gate default",
            "settings": {},
            "packaged_config": "__default__",
            "patch_defaults": True,
            "expected": {"enabled": False, "ready": False, "state": "disabled"},
        },
        {
            "name": "passphrase missing",
            "settings": {"enabled": True},
            "packaged_config": {
                "keyDerivation": "passphrase",
                "keySalt": salt_passphrase,
                "privateKeyPemEnc": passphrase_encrypted,
                "privateKeyPem": "",
            },
            "expected": {
                "ready": False,
                "state": "misconfigured",
                "missing_contains": "privateKeyPemEnc",
                "message_contains": sync.PACKAGED_SYNC_PASSPHRASE_ENV,
            },
        },
        {
            "name": "passphrase provided",
            "settings": {"enabled": True},
            "packaged_config": {
                "keyDerivation": "passphrase",
                "keySalt": salt_passphrase,
                "privateKeyPemEnc": passphrase_encrypted,
                "privateKeyPem": "",
            },
            "env": {sync.PACKAGED_SYNC_PASSPHRASE_ENV: "shared-secret"},
            "expected": {"ready": True, "state": "ready"},
        },
        {
            "name": "embedded derivation ready",
            "settings": {"enabled": True},
            "packaged_config": {
                "keyDerivation": "embedded",
                "embeddedKeyHint": "embedded-hint-01",
                "embeddedKeyVersion": "v1",
                "keySalt": salt_embedded,
                "privateKeyPemEnc": embedded_key_encrypted,
                "privateKeyPem": "",
            },
            "expected": {"ready": True, "state": "ready"},
        },
        {
            "name": "allowlist mismatch",
            "settings": {"enabled": True},
            "packaged_config": {
                "allowedRepo": "other/repo",
                "allowedBranch": "main",
                "allowedPathPrefix": "baluffo/source-sync.json",
            },
            "expected": {
                "ready": False,
                "state": "misconfigured",
                "missing_contains": "allowlist",
            },
        },
        {
            "name": "sync disable env",
            "settings": {"enabled": True},
            "packaged_config": "__default__",
            "env": {sync.SYNC_DISABLE_ENV: "1"},
            "expected": {
                "ready": False,
                "state": "disabled",
                "message_contains": sync.SYNC_DISABLE_ENV,
            },
        },
    ]

    original_security = dict(sync._SECURITY_DEFAULTS)  # noqa: SLF001
    original_sync = dict(sync._SYNC_DEFAULTS)  # noqa: SLF001
    try:
        for case in cases:
            if source_sync_test_root.config_path.exists():
                source_sync_test_root.config_path.unlink()
            if case.get("packaged_config") == "__default__":
                source_sync_test_root.write_packaged_config()
            elif case.get("packaged_config") is not None:
                source_sync_test_root.write_packaged_config(case["packaged_config"])
            if case.get("patch_defaults"):
                sync._SECURITY_DEFAULTS["github_app_enabled_default"] = False  # noqa: SLF001
                sync._SYNC_DEFAULTS["local_enabled_default"] = True  # noqa: SLF001
            env = dict(source_sync_test_root.env)
            env.update(case.get("env", {}))
            cfg = sync.resolve_sync_config(settings=case["settings"], env=env)
            status = sync.config_status(cfg)
            expected = case["expected"]
            assert status["enabled"] == expected.get("enabled", status["enabled"]), case["name"]
            assert status["ready"] == expected["ready"], case["name"]
            assert status["state"] == expected["state"], case["name"]
            if "missing_contains" in expected:
                assert expected["missing_contains"] in status["missing"], case["name"]
            if "message_contains" in expected:
                assert expected["message_contains"] in status["message"], case["name"]
            if expected["state"] == "ready":
                assert status["ready"], case["name"]
    finally:
        sync._SECURITY_DEFAULTS = original_security  # type: ignore[assignment] # noqa: SLF001
        sync._SYNC_DEFAULTS = original_sync  # type: ignore[assignment] # noqa: SLF001


def test_config_status_reports_machine_bound_packaged_key_error(source_sync_test_root, monkeypatch):
    source_sync_test_root.write_packaged_config(
        {
            "keyDerivation": sync.KEY_DERIVATION_MACHINE,
            "keySalt": sync._base64url_encode(b"unit-test-salt-999"),  # noqa: SLF001
            "privateKeyPemEnc": "ciphertext",
            "privateKeyPem": "",
        }
    )

    def fail_machine_decrypt(*args, **kwargs):  # noqa: ANN002,ANN003
        raise UnicodeDecodeError("utf-8", b"\xb4\x00", 0, 1, "invalid start byte")

    monkeypatch.setattr(sync, "decrypt_private_key_pem", fail_machine_decrypt)

    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    status = sync.config_status(cfg)

    assert status["ready"] is False
    assert status["state"] == "misconfigured"
    assert "privateKeyPemEnc" in status["missing"]
    assert "machine-bound" in status["message"]


def test_build_app_jwt_has_rs256_shape(source_sync_test_root):
    original_sign = sync._rsa_pkcs1_sign_sha256  # noqa: SLF001
    try:
        sync._rsa_pkcs1_sign_sha256 = lambda _msg, _pem: b"sig-bytes"  # type: ignore[assignment]
        token = sync.build_app_jwt("123456", "pem", issued_at=sync.now_utc())
    finally:
        sync._rsa_pkcs1_sign_sha256 = original_sign  # type: ignore[assignment]
    parts = token.split(".")
    assert len(parts) == 3
    header = json.loads(base64.urlsafe_b64decode(parts[0] + "==").decode("utf-8"))
    payload = json.loads(base64.urlsafe_b64decode(parts[1] + "==").decode("utf-8"))
    assert header["alg"] == "RS256"
    assert payload["iss"] == "123456"
    assert int(payload["exp"]) > int(payload["iat"])


def test_github_app_auth_reuses_cached_installation_token(source_sync_test_root):
    packaged = sync.PackagedGitHubAppConfig(
        app_id="123",
        installation_id="456",
        repo="owner/repo",
        branch="main",
        path="baluffo/source-sync.json",
        private_key_pem="pem",
        config_path=str(source_sync_test_root.config_path),
    )
    auth = sync.GitHubAppAuth(packaged)
    calls = {"count": 0}

    def fake_refresh(*, opener=sync.urlopen):  # noqa: ARG001
        calls["count"] += 1
        auth._token = "inst_token"  # noqa: SLF001
        auth._token_expires_at = sync.now_utc() + timedelta(hours=1)  # noqa: SLF001
        return "inst_token"

    original_refresh = auth._refresh_installation_token
    try:
        auth._refresh_installation_token = fake_refresh  # type: ignore[assignment]
        assert auth.get_installation_token() == "inst_token"
        assert auth.get_installation_token() == "inst_token"
    finally:
        auth._refresh_installation_token = original_refresh  # type: ignore[assignment]
    assert calls["count"] == 1


def test_github_app_auth_concurrent_access_refreshes_once(source_sync_test_root):
    packaged = sync.PackagedGitHubAppConfig(
        app_id="123",
        installation_id="456",
        repo="owner/repo",
        branch="main",
        path="baluffo/source-sync.json",
        private_key_pem="pem",
        config_path=str(source_sync_test_root.config_path),
    )
    auth = sync.GitHubAppAuth(packaged)
    calls = {"count": 0}
    gate = threading.Event()

    def fake_refresh(*, opener=sync.urlopen):  # noqa: ARG001
        calls["count"] += 1
        gate.set()
        auth._token = "shared_token"  # noqa: SLF001
        auth._token_expires_at = sync.now_utc() + timedelta(hours=1)  # noqa: SLF001
        return "shared_token"

    original_refresh = auth._refresh_installation_token
    try:
        auth._refresh_installation_token = fake_refresh  # type: ignore[assignment]
        results = []

        def worker():  # noqa: ANN202
            gate.wait(0.2)
            results.append(auth.get_installation_token())

        threads = [threading.Thread(target=worker) for _ in range(4)]
        for thread in threads:
            thread.start()
        # Prime the first refresh path.
        results.append(auth.get_installation_token())
        for thread in threads:
            thread.join()
    finally:
        auth._refresh_installation_token = original_refresh  # type: ignore[assignment]
    assert calls["count"] == 1
    assert all(item == "shared_token" for item in results)


def test_read_remote_snapshot_normalizes_legacy_rows_and_warns_on_extra_keys(
    source_sync_test_root, caplog
):
    source_sync_test_root.write_packaged_config()
    legacy_row = {
        "adapter": "static",
        "listing_url": "https://legacy.example/jobs",
        "name": "Legacy Jobs",
    }
    snapshot = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T10:00:00+00:00",
        "source": {"name": "admin_bridge"},
        "legacyTag": "compatibility-check",
        "active": [legacy_row],
        "pending": [],
        "rejected": [],
    }
    encoded = base64.b64encode(json.dumps(snapshot).encode("utf-8")).decode("ascii")
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            _FakeResponse(200, {"sha": "abc123", "content": encoded}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        with caplog.at_level("WARNING"):
            result = sync.read_remote_snapshot(cfg, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["exists"]
    assert result["sha"] == "abc123"
    active = result["snapshot"]["active"][0]
    assert active["id"] == sync.source_identity(legacy_row)
    assert active["stateChangedAt"] == snapshot["generatedAt"]
    assert any("unexpected top-level keys" in message for message in caplog.messages)
    assert not any("legacyTag" in message for message in caplog.messages)


def test_read_remote_snapshot_rejects_non_object_rows(source_sync_test_root, caplog):
    source_sync_test_root.write_packaged_config()
    snapshot = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T10:00:00+00:00",
        "source": {"name": "admin_bridge"},
        "active": [
            {
                "adapter": "teamtailor",
                "company": "A",
                "id": "teamtailor:name:a",
            }
        ],
        "pending": ["broken-row"],
        "rejected": [],
    }
    encoded = base64.b64encode(json.dumps(snapshot).encode("utf-8")).decode("ascii")
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            _FakeResponse(200, {"sha": "abc123", "content": encoded}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        with caplog.at_level("ERROR"):
            with pytest.raises(RuntimeError, match=r"pending\[0\] must be an object"):
                sync.read_remote_snapshot(cfg, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert any("Invalid remote sync snapshot payload" in message for message in caplog.messages)


def test_read_remote_snapshot_uses_github_api_base_override(source_sync_test_root, monkeypatch):
    source_sync_test_root.write_packaged_config()
    snapshot = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T10:00:00+00:00",
        "source": {"name": "admin_bridge"},
        "active": [],
        "pending": [],
        "rejected": [],
    }
    encoded = base64.b64encode(json.dumps(snapshot).encode("utf-8")).decode("ascii")
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            _FakeResponse(200, {"sha": "abc123", "content": encoded}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    monkeypatch.setenv(sync.GITHUB_API_BASE_ENV, "http://127.0.0.1:8765")
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.read_remote_snapshot(cfg, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["exists"] is True
    assert opener.calls[0]["url"] == "http://127.0.0.1:8765/app/installations/999999/access_tokens"
    assert (
        opener.calls[1]["url"]
        == "http://127.0.0.1:8765/repos/owner/repo/contents/baluffo/source-sync.json?ref=main"
    )


def test_merge_registry_state_keeps_newer_local_legacy_row():
    local = {
        "active": [
            {
                "adapter": "static",
                "listing_url": "https://legacy.example/jobs",
                "local_note": "newer-local-edit",
            }
        ],
        "pending": [],
        "rejected": [],
    }
    remote = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T11:00:00+00:00",
        "active": [
            {
                "adapter": "static",
                "listing_url": "https://legacy.example/jobs",
                "remote_note": "stale-remote-copy",
            }
        ],
        "pending": [],
        "rejected": [],
    }

    merged = sync.merge_registry_state(local, remote)

    assert len(merged["active"]) == 1
    assert merged["active"][0]["local_note"] == "newer-local-edit"
    assert "remote_note" not in merged["active"][0]


def test_push_sources_snapshot_serializes_expected_payload(source_sync_test_root):
    source_sync_test_root.write_packaged_config()
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync/manifest.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(201, {"content": {"sha": "newsha"}}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    local = {
        "active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}],
        "pending": [{"adapter": "teamtailor", "name": "Foo"}],
        "rejected": [],
    }
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, local, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["pushed"]
    assert result["remoteSha"] == "newsha"
    assert result["snapshotFormat"] == "sharded-v3"
    assert not any(
        call["method"] == "PUT" and call["url"].endswith("baluffo/source-sync.json")
        for call in opener.calls
    )
    decoded = result["snapshot"]
    assert decoded["schemaVersion"] == 2
    assert "active" in decoded
    assert "pending" in decoded
    assert "rejected" not in decoded
    assert decoded["active"][0]["stateChangedAt"] == decoded["generatedAt"]
    assert decoded["active"][0]["stateChangedBy"] == sync.REGISTRY_MIGRATION_V2
    assert decoded["active"][0]["lastPromotedAt"] == decoded["generatedAt"]
    assert decoded["active"][0]["approvedAt"] == decoded["generatedAt"]
    assert decoded["active"][0]["liveAt"] == decoded["generatedAt"]
    assert decoded["pending"][0]["stateChangedAt"] == decoded["generatedAt"]
    assert decoded["pending"][0]["stateChangedBy"] == sync.REGISTRY_MIGRATION_V2
    assert decoded["pending"][0]["lastDemotedAt"] == decoded["generatedAt"]
    assert decoded["pending"][0]["pendingReason"] == sync.REGISTRY_REASON_PENDING_DEFAULT


def test_push_sources_snapshot_preserves_remote_active_and_pending(source_sync_test_root):
    source_sync_test_root.write_packaged_config()
    remote_snapshot = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T10:00:00+00:00",
        "active": [{"adapter": "static", "listing_url": "https://remote-active.example/jobs"}],
        "pending": [{"adapter": "teamtailor", "name": "Remote Pending"}],
        "rejected": [],
    }
    encoded = base64.b64encode(json.dumps(remote_snapshot).encode("utf-8")).decode("ascii")
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync/manifest.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(200, {"sha": "s1", "content": encoded}),
            _FakeResponse(201, {"content": {"sha": "newsha"}}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    local = {"active": [], "pending": [], "rejected": []}
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, local, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["pushed"]
    decoded = result["snapshot"]
    assert len(decoded["active"]) == 1
    assert len(decoded["pending"]) == 1
    assert decoded["schemaVersion"] == 2


def test_build_snapshot_is_idempotent_for_legacy_rows(monkeypatch):
    local = {
        "active": [
            {
                "adapter": "static",
                "listing_url": "https://legacy.example/jobs",
            }
        ],
        "pending": [
            {
                "adapter": "teamtailor",
                "name": "Legacy Pending",
            }
        ],
        "rejected": [],
    }
    monkeypatch.setattr(sync, "now_iso", lambda: "2026-04-09T20:55:07.978053+00:00")
    first = sync.build_snapshot(local, source_label="admin_bridge")
    second = sync.build_snapshot(local, source_label="admin_bridge")
    assert first == second
    assert first["active"][0]["stateChangedAt"] == first["generatedAt"]
    assert first["active"][0]["stateChangedBy"] == sync.REGISTRY_MIGRATION_V2
    assert first["active"][0]["lastPromotedAt"] == first["generatedAt"]
    assert first["pending"][0]["stateChangedAt"] == first["generatedAt"]
    assert first["pending"][0]["stateChangedBy"] == sync.REGISTRY_MIGRATION_V2
    assert first["pending"][0]["lastDemotedAt"] == first["generatedAt"]
    assert first["pending"][0]["pendingReason"] == sync.REGISTRY_REASON_PENDING_DEFAULT


def test_push_sources_snapshot_allows_local_rejected_to_remove_remote_source(source_sync_test_root):
    source_sync_test_root.write_packaged_config()
    remote_snapshot = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T10:00:00+00:00",
        "active": [{"adapter": "static", "listing_url": "https://remove-me.example/jobs"}],
        "pending": [],
        "rejected": [],
    }
    encoded = base64.b64encode(json.dumps(remote_snapshot).encode("utf-8")).decode("ascii")
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync/manifest.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(200, {"sha": "s1", "content": encoded}),
            _FakeResponse(201, {"content": {"sha": "newsha"}}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    local = {
        "active": [],
        "pending": [],
        "rejected": [{"adapter": "static", "listing_url": "https://remove-me.example/jobs"}],
    }
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.push_sources_snapshot(cfg, local, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["pushed"]
    decoded = result["snapshot"]
    assert len(decoded["active"]) == 0
    assert "rejected" not in decoded


def test_401_triggers_installation_token_refresh(source_sync_test_root):
    source_sync_test_root.write_packaged_config()
    snapshot = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T10:00:00+00:00",
        "source": {},
        "active": [],
        "pending": [],
        "rejected": [],
    }
    encoded = base64.b64encode(json.dumps(snapshot).encode("utf-8")).decode("ascii")
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "token_a", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/test", code=401, msg="Unauthorized", hdrs={}, fp=None
            ),
            _FakeResponse(201, {"token": "token_b", "expires_at": "2099-03-10T11:00:00Z"}),
            _FakeResponse(200, {"sha": "abc", "content": encoded}),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.read_remote_snapshot(cfg, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["exists"]
    post_calls = [call for call in opener.calls if call["method"] == "POST"]
    assert len(post_calls) == 2


def test_rate_limited_error_sets_runtime_state(source_sync_test_root):
    source_sync_test_root.write_packaged_config()
    reset_at = sync.now_utc() + timedelta(seconds=120)
    opener = _Recorder(
        [
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/test",
                code=429,
                msg="Too Many Requests",
                hdrs={
                    "x-ratelimit-limit": "50",
                    "x-ratelimit-remaining": "4",
                    "x-ratelimit-reset": str(int(reset_at.timestamp())),
                },
                fp=None,
            ),
        ]
    )
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        with pytest.raises(sync.SyncOperationError) as ctx:
            sync.read_remote_snapshot(cfg, opener=opener)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert ctx.value.code == sync.RUNTIME_STATE_RATE_LIMITED
    status = sync.config_status(cfg)
    assert status["state"] == sync.RUNTIME_STATE_RATE_LIMITED
    assert status["runtimeState"]["lastRateLimitRemaining"] == "4"
    assert (
        status["runtimeState"]["lastRateLimitResetAt"]
        == reset_at.replace(microsecond=0).isoformat()
    )


def test_request_raw_json_uses_ssl_context_for_default_urlopen(monkeypatch):
    seen = {}

    def fake_urlopen(req, timeout=20, context=None):  # noqa: ANN001
        seen["timeout"] = timeout
        seen["context"] = context
        return _FakeResponse(200, {"ok": True})

    monkeypatch.setattr(sync, "urlopen", fake_urlopen)
    status, payload, _headers = sync._request_raw_json(  # noqa: SLF001
        method="GET",
        url="https://api.github.com/test",
        headers={"Accept": "application/json"},
        timeout_s=12,
    )
    assert status == 200
    assert payload["ok"] is True
    assert seen["timeout"] == 12
    assert isinstance(seen["context"], ssl.SSLContext)


def test_request_raw_json_wraps_certificate_verify_failures():
    def failing_opener(_req, timeout=20):  # noqa: ANN001,ARG001
        raise URLError(ssl.SSLError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed"))

    with pytest.raises(
        RuntimeError, match="SSL certificate verification failed while connecting to GitHub"
    ):
        sync._request_raw_json(  # noqa: SLF001
            method="GET",
            url="https://api.github.com/test",
            headers={"Accept": "application/json"},
            timeout_s=12,
            opener=failing_opener,
        )


def test_request_raw_json_loads_sync_specific_ca_bundle(monkeypatch, tmp_path):
    cafile = tmp_path / "custom-ca.pem"
    cafile.write_text("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
    seen = {"default_certs": False, "cafiles": []}

    class FakeContext:
        def load_default_certs(self):
            seen["default_certs"] = True

        def load_verify_locations(self, *, cafile=None):
            seen["cafiles"].append(cafile)

    def fake_urlopen(req, timeout=20, context=None):  # noqa: ANN001
        seen["timeout"] = timeout
        seen["context"] = context
        return _FakeResponse(200, {"ok": True})

    monkeypatch.setattr(github_https.ssl, "create_default_context", lambda: FakeContext())
    monkeypatch.setattr(github_https, "certifi", None)
    monkeypatch.setenv(sync.SYNC_CA_BUNDLE_ENV, str(cafile))
    monkeypatch.setattr(sync, "urlopen", fake_urlopen)

    status, payload, _headers = sync._request_raw_json(  # noqa: SLF001
        method="GET",
        url="https://api.github.com/test",
        headers={"Accept": "application/json"},
        timeout_s=12,
    )

    assert status == 200
    assert payload["ok"] is True
    assert isinstance(seen["context"], FakeContext)
    assert seen["default_certs"] is True
    assert seen["cafiles"] == [str(cafile)]


def test_request_raw_json_uses_shared_ca_bundle(monkeypatch, tmp_path):
    cafile = tmp_path / "shared-ca.pem"
    cafile.write_text("-----BEGIN CERTIFICATE-----\nMIIB\n-----END CERTIFICATE-----\n")
    seen = {"cafiles": []}

    class FakeContext:
        def load_default_certs(self):
            return None

        def load_verify_locations(self, *, cafile=None):
            seen["cafiles"].append(cafile)

    def fake_urlopen(req, timeout=20, context=None):  # noqa: ANN001
        seen["timeout"] = timeout
        seen["context"] = context
        return _FakeResponse(200, {"ok": True})

    monkeypatch.setattr(github_https.ssl, "create_default_context", lambda: FakeContext())
    monkeypatch.setattr(github_https, "certifi", None)
    monkeypatch.setenv(github_https.GITHUB_CA_BUNDLE_ENV, str(cafile))
    monkeypatch.setattr(sync, "urlopen", fake_urlopen)

    status, payload, _headers = sync._request_raw_json(  # noqa: SLF001
        method="GET",
        url="https://api.github.com/test",
        headers={"Accept": "application/json"},
        timeout_s=12,
    )

    assert status == 200
    assert payload["ok"] is True
    assert isinstance(seen["context"], FakeContext)
    assert seen["cafiles"] == [str(cafile)]


def test_read_remote_snapshot_uses_ssl_context_for_default_runtime_opener(
    source_sync_test_root, monkeypatch
):
    source_sync_test_root.write_packaged_config()
    snapshot = {
        "schemaVersion": 1,
        "generatedAt": "2026-03-09T10:00:00+00:00",
        "source": {"name": "admin_bridge"},
        "active": [],
        "pending": [],
        "rejected": [],
    }
    encoded = base64.b64encode(json.dumps(snapshot).encode("utf-8")).decode("ascii")
    seen: dict[str, object] = {}

    def fake_urlopen(req, timeout=20, context=None):  # noqa: ANN001
        seen["timeout"] = timeout
        seen["context"] = context
        url = req.full_url
        if url.endswith("/access_tokens"):
            return _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"})
        return _FakeResponse(200, {"sha": "abc123", "content": encoded})

    monkeypatch.setattr(sync, "urlopen", fake_urlopen)
    cfg = sync.resolve_sync_config(settings={"enabled": True}, env=source_sync_test_root.env)
    original_build_jwt = sync.build_app_jwt
    try:
        sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
        result = sync.read_remote_snapshot(cfg, opener=sync.urlopen)
    finally:
        sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
    assert result["exists"] is True
    assert seen["timeout"] == sync.DEFAULT_TIMEOUT_S
    assert isinstance(seen["context"], ssl.SSLContext)
