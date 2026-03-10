import base64
import json
import threading
import unittest
import shutil
import uuid
from datetime import timedelta
from pathlib import Path
from urllib.error import HTTPError

from scripts import source_sync as sync


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

    def __call__(self, req, timeout=20):  # noqa: ANN001
        self.calls.append({
            "url": req.full_url,
            "method": req.get_method(),
            "headers": dict(req.header_items()),
            "body": req.data.decode("utf-8") if isinstance(req.data, bytes) else "",
            "timeout": timeout,
        })
        if not self.responses:
            raise AssertionError("No fake responses left")
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


class SourceSyncTests(unittest.TestCase):
    def setUp(self) -> None:
        temp_root = Path(__file__).resolve().parents[1] / ".codex-tmp-tests"
        temp_root.mkdir(parents=True, exist_ok=True)
        self.test_root = temp_root / f"source-sync-{uuid.uuid4().hex}"
        self.test_root.mkdir(parents=True, exist_ok=True)
        self.addCleanup(lambda: shutil.rmtree(self.test_root, ignore_errors=True))
        self.config_path = self.test_root / "github-app-sync-config.json"
        self.env = {sync.PACKAGED_SYNC_CONFIG_ENV: str(self.config_path)}
        sync._clear_runtime_state()  # noqa: SLF001
        with sync._RATE_LIMIT_LOCK:  # noqa: SLF001
            sync._RATE_LIMIT_STATE["calls"] = []  # noqa: SLF001
            sync._RATE_LIMIT_STATE["strike"] = 0  # noqa: SLF001
            sync._RATE_LIMIT_STATE["until"] = None  # noqa: SLF001

    def write_packaged_config(self, payload: dict | None = None) -> None:
        base = {
            "schemaVersion": 1,
            "appId": "123456",
            "installationId": "999999",
            "repo": "owner/repo",
            "branch": "main",
            "path": "baluffo/source-sync.json",
            "privateKeyPem": "-----BEGIN RSA PRIVATE KEY-----\nTEST\n-----END RSA PRIVATE KEY-----",
        }
        if payload:
            base.update(payload)
        self.config_path.write_text(json.dumps(base), encoding="utf-8")

    def test_config_status_reports_misconfigured_when_packaged_config_missing(self):
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
        status = sync.config_status(cfg)
        self.assertTrue(status["enabled"])
        self.assertFalse(status["ready"])
        self.assertEqual(status["state"], "misconfigured")
        self.assertIn("packaged_github_app_config", status["missing"])

    def test_config_status_reports_disabled_when_locally_disabled(self):
        self.write_packaged_config()
        cfg = sync.resolve_sync_config(settings={"enabled": False}, env=self.env)
        status = sync.config_status(cfg)
        self.assertFalse(status["enabled"])
        self.assertFalse(status["ready"])
        self.assertEqual(status["state"], "disabled")

    def test_encrypt_and_decrypt_private_key_round_trip(self):
        salt_b64 = sync._base64url_encode(b"unit-test-salt-123")  # noqa: SLF001
        private_key = "-----BEGIN RSA PRIVATE KEY-----\nabc123\n-----END RSA PRIVATE KEY-----"
        encrypted = sync.encrypt_private_key_pem(private_key, salt_b64=salt_b64, app_id="1", installation_id="2")
        decrypted = sync.decrypt_private_key_pem(encrypted, salt_b64=salt_b64, app_id="1", installation_id="2")
        self.assertEqual(decrypted, private_key)

    def test_passphrase_encrypt_and_decrypt_private_key_round_trip(self):
        salt_b64 = sync._base64url_encode(b"unit-test-salt-456")  # noqa: SLF001
        private_key = "-----BEGIN RSA PRIVATE KEY-----\nxyz789\n-----END RSA PRIVATE KEY-----"
        encrypted = sync.encrypt_private_key_pem_with_passphrase(
            private_key,
            salt_b64=salt_b64,
            app_id="1",
            installation_id="2",
            passphrase="unit-passphrase",
        )
        decrypted = sync.decrypt_private_key_pem_with_passphrase(
            encrypted,
            salt_b64=salt_b64,
            app_id="1",
            installation_id="2",
            passphrase="unit-passphrase",
        )
        self.assertEqual(decrypted, private_key)

    def test_config_status_reports_misconfigured_when_passphrase_missing(self):
        salt_b64 = sync._base64url_encode(b"unit-test-salt-789")  # noqa: SLF001
        private_key = "-----BEGIN RSA PRIVATE KEY-----\nabc123\n-----END RSA PRIVATE KEY-----"
        encrypted = sync.encrypt_private_key_pem_with_passphrase(
            private_key,
            salt_b64=salt_b64,
            app_id="123456",
            installation_id="999999",
            passphrase="shared-secret",
        )
        self.write_packaged_config(
            {
                "keyDerivation": "passphrase",
                "keySalt": salt_b64,
                "privateKeyPemEnc": encrypted,
                "privateKeyPem": "",
            }
        )
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
        status = sync.config_status(cfg)
        self.assertFalse(status["ready"])
        self.assertEqual(status["state"], "misconfigured")
        self.assertIn("privateKeyPemEnc", status["missing"])
        self.assertIn(sync.PACKAGED_SYNC_PASSPHRASE_ENV, status["message"])

    def test_config_status_ready_when_passphrase_is_provided(self):
        salt_b64 = sync._base64url_encode(b"unit-test-salt-012")  # noqa: SLF001
        private_key = "-----BEGIN RSA PRIVATE KEY-----\nabc123\n-----END RSA PRIVATE KEY-----"
        encrypted = sync.encrypt_private_key_pem_with_passphrase(
            private_key,
            salt_b64=salt_b64,
            app_id="123456",
            installation_id="999999",
            passphrase="shared-secret",
        )
        self.write_packaged_config(
            {
                "keyDerivation": "passphrase",
                "keySalt": salt_b64,
                "privateKeyPemEnc": encrypted,
                "privateKeyPem": "",
            }
        )
        env = dict(self.env)
        env[sync.PACKAGED_SYNC_PASSPHRASE_ENV] = "shared-secret"
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=env)
        status = sync.config_status(cfg)
        self.assertTrue(status["ready"])
        self.assertEqual(status["state"], "ready")

    def test_config_status_ready_for_embedded_derivation(self):
        salt_b64 = sync._base64url_encode(b"unit-test-salt-013")  # noqa: SLF001
        hint = "embedded-hint-01"
        version = "v1"
        private_key = "-----BEGIN RSA PRIVATE KEY-----\nabc123\n-----END RSA PRIVATE KEY-----"
        passphrase = sync.build_embedded_passphrase(hint=hint, version=version)
        encrypted = sync.encrypt_private_key_pem_with_passphrase(
            private_key,
            salt_b64=salt_b64,
            app_id="123456",
            installation_id="999999",
            passphrase=passphrase,
        )
        self.write_packaged_config(
            {
                "keyDerivation": "embedded",
                "embeddedKeyHint": hint,
                "embeddedKeyVersion": version,
                "keySalt": salt_b64,
                "privateKeyPemEnc": encrypted,
                "privateKeyPem": "",
            }
        )
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
        status = sync.config_status(cfg)
        self.assertTrue(status["ready"])
        self.assertEqual(status["state"], "ready")

    def test_allowlist_mismatch_marks_misconfigured(self):
        self.write_packaged_config(
            {
                "allowedRepo": "other/repo",
                "allowedBranch": "main",
                "allowedPathPrefix": "baluffo/source-sync.json",
            }
        )
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
        status = sync.config_status(cfg)
        self.assertFalse(status["ready"])
        self.assertEqual(status["state"], "misconfigured")
        self.assertIn("allowlist", status["missing"])

    def test_sync_disable_env_forces_disabled_state(self):
        self.write_packaged_config()
        env = dict(self.env)
        env[sync.SYNC_DISABLE_ENV] = "1"
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=env)
        status = sync.config_status(cfg)
        self.assertFalse(status["ready"])
        self.assertEqual(status["state"], "disabled")
        self.assertIn(sync.SYNC_DISABLE_ENV, status["message"])

    def test_build_app_jwt_has_rs256_shape(self):
        original_sign = sync._rsa_pkcs1_sign_sha256  # noqa: SLF001
        try:
            sync._rsa_pkcs1_sign_sha256 = lambda _msg, _pem: b"sig-bytes"  # type: ignore[assignment]
            token = sync.build_app_jwt("123456", "pem", issued_at=sync.now_utc())
        finally:
            sync._rsa_pkcs1_sign_sha256 = original_sign  # type: ignore[assignment]
        parts = token.split(".")
        self.assertEqual(len(parts), 3)
        header = json.loads(base64.urlsafe_b64decode(parts[0] + "==").decode("utf-8"))
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + "==").decode("utf-8"))
        self.assertEqual(header["alg"], "RS256")
        self.assertEqual(payload["iss"], "123456")
        self.assertGreater(int(payload["exp"]), int(payload["iat"]))

    def test_github_app_auth_reuses_cached_installation_token(self):
        packaged = sync.PackagedGitHubAppConfig(
            app_id="123",
            installation_id="456",
            repo="owner/repo",
            branch="main",
            path="baluffo/source-sync.json",
            private_key_pem="pem",
            config_path=str(self.config_path),
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
            self.assertEqual(auth.get_installation_token(), "inst_token")
            self.assertEqual(auth.get_installation_token(), "inst_token")
        finally:
            auth._refresh_installation_token = original_refresh  # type: ignore[assignment]
        self.assertEqual(calls["count"], 1)

    def test_github_app_auth_concurrent_access_refreshes_once(self):
        packaged = sync.PackagedGitHubAppConfig(
            app_id="123",
            installation_id="456",
            repo="owner/repo",
            branch="main",
            path="baluffo/source-sync.json",
            private_key_pem="pem",
            config_path=str(self.config_path),
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
        self.assertEqual(calls["count"], 1)
        self.assertTrue(all(item == "shared_token" for item in results))

    def test_read_remote_snapshot_parses_contents_payload(self):
        self.write_packaged_config()
        snapshot = {
            "schemaVersion": 1,
            "generatedAt": "2026-03-09T10:00:00+00:00",
            "source": {"name": "admin_bridge"},
            "active": [{"adapter": "teamtailor", "company": "A", "id": "teamtailor:name:a"}],
            "pending": [],
            "rejected": [],
        }
        encoded = base64.b64encode(json.dumps(snapshot).encode("utf-8")).decode("ascii")
        opener = _Recorder([
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            _FakeResponse(200, {"sha": "abc123", "content": encoded}),
        ])
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
        original_build_jwt = sync.build_app_jwt
        try:
            sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
            result = sync.read_remote_snapshot(cfg, opener=opener)
        finally:
            sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
        self.assertTrue(result["exists"])
        self.assertEqual(result["sha"], "abc123")
        self.assertEqual(len(result["snapshot"]["active"]), 1)

    def test_pull_and_merge_sources_replaces_local_with_remote_latest(self):
        self.write_packaged_config()
        local = {
            "active": [{"adapter": "static", "listing_url": "https://a.com/jobs"}],
            "pending": [],
            "rejected": [],
        }
        remote_snapshot = {
            "schemaVersion": 1,
            "generatedAt": "2026-03-09T11:00:00+00:00",
            "active": [{"adapter": "static", "listing_url": "https://b.com/jobs", "studio": "Remote"}],
            "pending": [{"adapter": "teamtailor", "listing_url": "https://c.com/jobs"}],
            "rejected": [],
        }
        encoded = base64.b64encode(json.dumps(remote_snapshot).encode("utf-8")).decode("ascii")
        opener = _Recorder([
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            _FakeResponse(200, {"sha": "s1", "content": encoded}),
        ])
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
        original_build_jwt = sync.build_app_jwt
        try:
            sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
            result = sync.pull_and_merge_sources(cfg, local, opener=opener)
        finally:
            sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
        self.assertTrue(result["changed"])
        merged = result["mergedState"]
        self.assertEqual(len(merged["active"]), 1)
        self.assertEqual(merged["active"][0].get("studio"), "Remote")
        self.assertEqual(len(merged["pending"]), 1)

    def test_push_sources_snapshot_serializes_expected_payload(self):
        self.write_packaged_config()
        opener = _Recorder([
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            _FakeResponse(201, {"content": {"sha": "newsha"}}),
        ])
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
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
        self.assertTrue(result["pushed"])
        self.assertEqual(result["remoteSha"], "newsha")
        put_call = opener.calls[2]
        self.assertEqual(put_call["method"], "PUT")
        body = json.loads(put_call["body"])
        decoded = json.loads(base64.b64decode(body["content"]).decode("utf-8"))
        self.assertIn("active", decoded)
        self.assertIn("pending", decoded)
        self.assertIn("rejected", decoded)

    def test_push_sources_snapshot_preserves_remote_active_and_pending(self):
        self.write_packaged_config()
        remote_snapshot = {
            "schemaVersion": 1,
            "generatedAt": "2026-03-09T10:00:00+00:00",
            "active": [{"adapter": "static", "listing_url": "https://remote-active.example/jobs"}],
            "pending": [{"adapter": "teamtailor", "name": "Remote Pending"}],
            "rejected": [],
        }
        encoded = base64.b64encode(json.dumps(remote_snapshot).encode("utf-8")).decode("ascii")
        opener = _Recorder([
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            _FakeResponse(200, {"sha": "s1", "content": encoded}),
            _FakeResponse(201, {"content": {"sha": "newsha"}}),
        ])
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
        local = {"active": [], "pending": [], "rejected": []}
        original_build_jwt = sync.build_app_jwt
        try:
            sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
            result = sync.push_sources_snapshot(cfg, local, opener=opener)
        finally:
            sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
        self.assertTrue(result["pushed"])
        put_call = opener.calls[2]
        body = json.loads(put_call["body"])
        decoded = json.loads(base64.b64decode(body["content"]).decode("utf-8"))
        self.assertEqual(len(decoded["active"]), 1)
        self.assertEqual(len(decoded["pending"]), 1)

    def test_push_sources_snapshot_allows_local_rejected_to_remove_remote_source(self):
        self.write_packaged_config()
        remote_snapshot = {
            "schemaVersion": 1,
            "generatedAt": "2026-03-09T10:00:00+00:00",
            "active": [{"adapter": "static", "listing_url": "https://remove-me.example/jobs"}],
            "pending": [],
            "rejected": [],
        }
        encoded = base64.b64encode(json.dumps(remote_snapshot).encode("utf-8")).decode("ascii")
        opener = _Recorder([
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            _FakeResponse(200, {"sha": "s1", "content": encoded}),
            _FakeResponse(201, {"content": {"sha": "newsha"}}),
        ])
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
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
        self.assertTrue(result["pushed"])
        put_call = opener.calls[2]
        body = json.loads(put_call["body"])
        decoded = json.loads(base64.b64decode(body["content"]).decode("utf-8"))
        self.assertEqual(len(decoded["active"]), 0)
        self.assertEqual(len(decoded["rejected"]), 1)

    def test_401_triggers_installation_token_refresh(self):
        self.write_packaged_config()
        snapshot = {"schemaVersion": 1, "generatedAt": "2026-03-09T10:00:00+00:00", "source": {}, "active": [], "pending": [], "rejected": []}
        encoded = base64.b64encode(json.dumps(snapshot).encode("utf-8")).decode("ascii")
        opener = _Recorder([
            _FakeResponse(201, {"token": "token_a", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(url="https://api.github.com/test", code=401, msg="Unauthorized", hdrs={}, fp=None),
            _FakeResponse(201, {"token": "token_b", "expires_at": "2099-03-10T11:00:00Z"}),
            _FakeResponse(200, {"sha": "abc", "content": encoded}),
        ])
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
        original_build_jwt = sync.build_app_jwt
        try:
            sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
            result = sync.read_remote_snapshot(cfg, opener=opener)
        finally:
            sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
        self.assertTrue(result["exists"])
        post_calls = [call for call in opener.calls if call["method"] == "POST"]
        self.assertEqual(len(post_calls), 2)

    def test_rate_limited_error_sets_runtime_state(self):
        self.write_packaged_config()
        opener = _Recorder([
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(url="https://api.github.com/test", code=429, msg="Too Many Requests", hdrs={}, fp=None),
        ])
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
        original_build_jwt = sync.build_app_jwt
        try:
            sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
            with self.assertRaises(sync.SyncOperationError) as ctx:
                sync.read_remote_snapshot(cfg, opener=opener)
        finally:
            sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
        self.assertEqual(ctx.exception.code, sync.RUNTIME_STATE_RATE_LIMITED)
        status = sync.config_status(cfg)
        self.assertEqual(status["state"], sync.RUNTIME_STATE_RATE_LIMITED)

    def test_remote_conflict_error_sets_runtime_state(self):
        self.write_packaged_config()
        opener = _Recorder([
            _FakeResponse(201, {"token": "inst_token", "expires_at": "2099-03-10T10:00:00Z"}),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync.json?ref=main",
                code=404,
                msg="Not Found",
                hdrs={},
                fp=None,
            ),
            HTTPError(
                url="https://api.github.com/repos/owner/repo/contents/baluffo/source-sync.json",
                code=409,
                msg="Conflict",
                hdrs={},
                fp=None,
            ),
        ])
        cfg = sync.resolve_sync_config(settings={"enabled": True}, env=self.env)
        original_build_jwt = sync.build_app_jwt
        try:
            sync.build_app_jwt = lambda *_a, **_k: "app.jwt.token"  # type: ignore[assignment]
            with self.assertRaises(sync.SyncOperationError) as ctx:
                sync.push_sources_snapshot(cfg, {"active": [], "pending": [], "rejected": []}, opener=opener)
        finally:
            sync.build_app_jwt = original_build_jwt  # type: ignore[assignment]
        self.assertEqual(ctx.exception.code, sync.RUNTIME_STATE_REMOTE_CONFLICT)
        status = sync.config_status(cfg)
        self.assertEqual(status["state"], sync.RUNTIME_STATE_REMOTE_CONFLICT)


if __name__ == "__main__":
    unittest.main()
