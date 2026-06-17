from __future__ import annotations

from pathlib import Path
from typing import Any

from src.bridge.routes.post_routes import handle_post
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def test_check_for_update_exception_returns_structured_error(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    def fail_check_for_update(**_: Any) -> dict[str, Any]:
        raise RuntimeError("check failed")

    api.check_for_update = fail_check_for_update  # type: ignore[assignment]

    handler = FakeHandler()
    result = handle_post(handler, api=api, path="/app/check-for-update", payload={"force": True})

    assert result is True
    assert handler.sent[-1]["status"] == 500
    assert handler.sent[-1]["payload"] == {"started": False, "error": "check failed"}


def test_download_update_exception_returns_structured_error(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    def fail_download_update() -> dict[str, Any]:
        raise RuntimeError("download failed")

    api.download_update = fail_download_update  # type: ignore[assignment]

    handler = FakeHandler()
    result = handle_post(handler, api=api, path="/app/download-update", payload={})

    assert result is True
    assert handler.sent[-1]["status"] == 500
    assert handler.sent[-1]["payload"] == {"started": False, "error": "download failed"}


def test_install_update_exception_returns_structured_error(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    def fail_install_update() -> dict[str, Any]:
        raise RuntimeError("install failed")

    api.install_update = fail_install_update  # type: ignore[assignment]

    handler = FakeHandler()
    result = handle_post(handler, api=api, path="/app/install-update", payload={})

    assert result is True
    assert handler.sent[-1]["status"] == 500
    assert handler.sent[-1]["payload"] == {"started": False, "error": "install failed"}
