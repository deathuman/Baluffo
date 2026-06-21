"""Tests for desktop update network behavior."""

import json
import ssl
from pathlib import Path
from unittest import mock
from urllib.error import URLError

import pytest

from src import app_version
from src.ship import desktop_update_shared as du_shared
from tests.helpers.desktop_update_leaf_namespace import du
from tests.helpers.temp_paths import workspace_tmpdir


def test_get_app_version_honors_env_override(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv(app_version.APP_VERSION_OVERRIDE_ENV, "0.0.9")

    assert app_version.get_app_version() == "0.0.9"


def test_write_json_atomic_retries_transient_permission_error() -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        target = Path(tmp) / "portable" / "ship" / "data" / "updater" / "install-state.json"
        calls = {"count": 0}
        original_replace = du_shared.os.replace

        def flaky_replace(src, dst):  # noqa: ANN001
            calls["count"] += 1
            if calls["count"] == 1:
                raise PermissionError(32, "sharing violation")
            return original_replace(src, dst)

        with mock.patch.object(du_shared.os, "replace", side_effect=flaky_replace):
            du.write_json_atomic(target, {"ok": True})

        assert json.loads(target.read_text(encoding="utf-8"))["ok"] is True
        assert calls["count"] == 2


def test_download_file_retries_transient_permission_error_on_finalize() -> None:
    class FakeResponse:
        def __init__(self, content: bytes) -> None:
            self._content = content
            self._offset = 0
            self.headers = {"Content-Length": str(len(content))}

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
            return None

        def read(self, size: int) -> bytes:
            if self._offset >= len(self._content):
                return b""
            chunk = self._content[self._offset : self._offset + size]
            self._offset += len(chunk)
            return chunk

    with workspace_tmpdir("desktop-update") as tmp:
        target = Path(tmp) / "portable" / "ship" / "data" / "updater" / "downloads" / "app.zip"
        content = b"portable-zip"
        calls = {"count": 0}
        seen = {}
        original_replace = du_shared.os.replace

        def flaky_replace(src, dst):  # noqa: ANN001
            calls["count"] += 1
            if calls["count"] == 1:
                raise PermissionError(32, "sharing violation")
            return original_replace(src, dst)

        def fake_urlopen(request, timeout=300.0, context=None):  # noqa: ANN001
            seen["timeout"] = timeout
            seen["context"] = context
            return FakeResponse(content)

        with (
            mock.patch.object(du_shared, "urlopen", side_effect=fake_urlopen),
            mock.patch.object(du_shared.os, "replace", side_effect=flaky_replace),
        ):
            result = du.download_file("https://example.com/app.zip", target)

        assert result == target
        assert target.read_bytes() == content
        assert calls["count"] == 2
        assert seen["timeout"] == 300.0
        assert isinstance(seen["context"], ssl.SSLContext)
        assert list(target.parent.glob("*.download")) == []


def test_fetch_json_uses_ssl_context_for_default_https_urlopen() -> None:
    seen = {}

    class FakeResponse:
        headers = {}

        def __enter__(self) -> "FakeResponse":
            return self

        def __exit__(self, exc_type, exc, tb) -> None:  # noqa: ANN001
            return None

        def read(self) -> bytes:
            return b'{"ok": true}'

    def fake_urlopen(request, timeout=20.0, context=None):  # noqa: ANN001
        seen["timeout"] = timeout
        seen["context"] = context
        return FakeResponse()

    with mock.patch.object(du_shared, "urlopen", side_effect=fake_urlopen):
        payload = du.fetch_json("https://api.github.com/repos/example/app/releases", timeout_s=12.0)

    assert payload == {"ok": True}
    assert seen["timeout"] == 12.0
    assert isinstance(seen["context"], ssl.SSLContext)


def test_fetch_json_wraps_certificate_verify_failures_for_https() -> None:
    def failing_urlopen(_request, timeout=20.0, context=None):  # noqa: ANN001,ARG001
        raise URLError(ssl.SSLError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed"))

    with (
        mock.patch.object(du_shared, "urlopen", side_effect=failing_urlopen),
        pytest.raises(
            RuntimeError,
            match="SSL certificate verification failed while connecting to GitHub",
        ),
    ):
        du.fetch_json("https://api.github.com/repos/example/app/releases", timeout_s=12.0)


def test_download_file_wraps_certificate_verify_failures_for_https(tmp_path: Path) -> None:
    target = tmp_path / "app.zip"

    def failing_urlopen(_request, timeout=300.0, context=None):  # noqa: ANN001,ARG001
        raise URLError(ssl.SSLError("[SSL: CERTIFICATE_VERIFY_FAILED] certificate verify failed"))

    with (
        mock.patch.object(du_shared, "urlopen", side_effect=failing_urlopen),
        pytest.raises(
            RuntimeError,
            match="SSL certificate verification failed while connecting to GitHub",
        ),
    ):
        du.download_file("https://example.com/app.zip", target)
