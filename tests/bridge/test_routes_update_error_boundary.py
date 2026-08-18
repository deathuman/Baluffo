from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from src.bridge.routes.post_routes import handle_post
from src.bridge.routes.post_routes_update import handle_post as handle_update_post
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


class MinimalUpdatePostRouteApi:
    def __init__(self, *, container_mode: bool = False) -> None:
        self.runtime_config = SimpleNamespace(container_mode=container_mode)
        self.check_force: bool | None = None

    def check_for_update(self, *, force: bool = False) -> dict[str, Any]:
        self.check_force = force
        return {"started": True, "action": "check", "force": force}

    def download_update(self) -> dict[str, Any]:
        return {"started": True, "action": "download"}

    def install_update(self) -> dict[str, Any]:
        return {"started": True, "action": "install"}


def test_update_post_routes_accept_minimal_capability_object() -> None:
    api = MinimalUpdatePostRouteApi()

    check_handler = FakeHandler()
    assert (
        handle_update_post(
            check_handler,
            api=api,
            path="/app/check-for-update",
            payload={"force": True},
        )
        is True
    )
    assert check_handler.sent[-1]["payload"] == {
        "started": True,
        "action": "check",
        "force": True,
    }
    assert api.check_force is True

    download_handler = FakeHandler()
    assert (
        handle_update_post(
            download_handler,
            api=api,
            path="/app/download-update",
            payload={},
        )
        is True
    )
    assert download_handler.sent[-1]["payload"] == {"started": True, "action": "download"}

    install_handler = FakeHandler()
    assert (
        handle_update_post(
            install_handler,
            api=api,
            path="/app/install-update",
            payload={},
        )
        is True
    )
    assert install_handler.sent[-1]["payload"] == {"started": True, "action": "install"}


def test_update_post_routes_minimal_capability_object_preserves_container_unavailable() -> None:
    handler = FakeHandler()

    assert (
        handle_update_post(
            handler,
            api=MinimalUpdatePostRouteApi(container_mode=True),
            path="/app/download-update",
            payload={},
        )
        is True
    )

    assert handler.sent[-1]["status"] == 409
    assert handler.sent[-1]["payload"] == {"ok": False, "error": "not available in container mode"}


def test_check_for_update_exception_returns_structured_error(tmp_path: Path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)

    def fail_check_for_update(**_: Any) -> dict[str, Any]:
        raise RuntimeError("check failed")

    api.check_for_update = fail_check_for_update

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

    api.download_update = fail_download_update

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

    api.install_update = fail_install_update

    handler = FakeHandler()
    result = handle_post(handler, api=api, path="/app/install-update", payload={})

    assert result is True
    assert handler.sent[-1]["status"] == 500
    assert handler.sent[-1]["payload"] == {"started": False, "error": "install failed"}
