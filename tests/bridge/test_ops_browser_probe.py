from __future__ import annotations

import threading
from types import SimpleNamespace
from typing import Any, cast

import src.jobs.browser_fallback_pool as browser_pool_module
from src.bridge.ops_api_health import OpsApiHealthMixin
from src.bridge.routes.get_routes import handle_get
from tests.helpers.bridge_api import (
    FakeDesktopLocalDataStore,
    FakeHandler,
    make_stub_bridge_api,
)


class _ProbeApi(OpsApiHealthMixin):
    def __init__(self) -> None:
        self._deps = cast(Any, SimpleNamespace(now_iso=lambda: "2026-11-20T00:00:00Z"))


class _FakePool:
    instances: list[_FakePool] = []

    def __init__(self) -> None:
        self.closed = False
        self.timeouts: list[int] = []
        type(self).instances.append(self)

    def health_probe(self, timeout_s: int = 5) -> dict[str, Any]:
        self.timeouts.append(timeout_s)
        return {
            "ok": False,
            "stage": "launch",
            "error": "browserType.launch: Executable doesn't exist at /ms-playwright",
            "errorType": "Error",
            "durationMs": 42,
            "probeUrl": "data:text/html,probe",
            "lastFailureAt": "2026-11-20T00:00:00Z",
            "forensics": {
                "available": True,
                "capturedAt": "2026-11-20T00:00:00Z",
                "cgroup": {"available": True, "memoryMax": "max"},
                "browserStderr": {"available": True, "tail": "x" * 9000},
            },
        }

    def close(self) -> None:
        self.closed = True


def test_probe_browser_fallback_health_returns_compacted_probe(monkeypatch) -> None:
    _FakePool.instances.clear()
    monkeypatch.setattr(browser_pool_module, "BrowserFallbackPool", _FakePool)

    payload = _ProbeApi().probe_browser_fallback_health(timeout_s=7)

    assert payload["ok"] is True
    assert payload["probedAt"] == "2026-11-20T00:00:00Z"
    health = payload["browserFallbackHealth"]
    assert health["ok"] is False
    assert health["stage"] == "launch"
    assert health["error"].startswith("browserType.launch")
    assert health["errorType"] == "Error"
    assert health["durationMs"] == 42
    assert health["lastFailureAt"] == "2026-11-20T00:00:00Z"
    assert health["forensics"]["cgroup"]["memoryMax"] == "max"
    assert len(health["forensics"]["browserStderr"]["tail"]) == 4096
    assert _FakePool.instances[-1].timeouts == [7]
    assert _FakePool.instances[-1].closed is True


def test_probe_browser_fallback_health_rejects_a_concurrent_probe(monkeypatch) -> None:
    started = threading.Event()
    release = threading.Event()

    class _BlockingPool:
        def __init__(self) -> None:
            pass

        def health_probe(self, timeout_s: int = 5) -> dict[str, Any]:
            started.set()
            assert release.wait(5)
            return {"ok": True, "stage": "ready", "durationMs": 1}

        def close(self) -> None:
            pass

    monkeypatch.setattr(browser_pool_module, "BrowserFallbackPool", _BlockingPool)
    api = _ProbeApi()
    result: dict[str, Any] = {}
    busy: dict[str, Any] | None = None
    worker = threading.Thread(
        target=lambda: result.update(api.probe_browser_fallback_health(timeout_s=5))
    )
    worker.start()
    try:
        assert started.wait(5)
        busy = api.probe_browser_fallback_health(timeout_s=5)
    finally:
        release.set()
        worker.join(5)

    assert busy == {"ok": False, "busy": True, "error": "browser probe already running"}
    assert result["ok"] is True


def test_ops_browser_probe_route_clamps_timeout_and_reports_busy(tmp_path) -> None:
    store = FakeDesktopLocalDataStore()
    api = make_stub_bridge_api(tmp_path, store)
    seen: list[int] = []

    def probe(*, timeout_s: int = 10) -> dict[str, Any]:
        seen.append(timeout_s)
        return {"ok": False, "busy": True, "error": "browser probe already running"}

    api.probe_browser_fallback_health = probe
    handler = FakeHandler()
    assert (
        handle_get(handler, api=api, path="/ops/browser-probe", query={"timeoutS": ["99"]}) is True
    )
    assert seen == [30]
    assert handler.sent[-1]["status"] == 409
    assert handler.sent[-1]["payload"]["busy"] is True

    handler = FakeHandler()
    assert (
        handle_get(handler, api=api, path="/ops/browser-probe", query={"timeoutS": ["nope"]})
        is True
    )
    assert seen[-1] == 10

    api.probe_browser_fallback_health = lambda *, timeout_s=10: {
        "ok": True,
        "browserFallbackHealth": {"ok": True, "stage": "ready", "durationMs": 1},
    }
    handler = FakeHandler()
    assert handle_get(handler, api=api, path="/ops/browser-probe", query={}) is True
    assert handler.sent[-1]["status"] == 200
    assert handler.sent[-1]["payload"]["browserFallbackHealth"]["stage"] == "ready"


def test_probe_browser_fallback_health_releases_the_lock_after_a_launch_error(monkeypatch) -> None:
    class _BrokenPool:
        def __init__(self) -> None:
            raise RuntimeError("pool unavailable")

    monkeypatch.setattr(browser_pool_module, "BrowserFallbackPool", _BrokenPool)
    api = _ProbeApi()

    first = api.probe_browser_fallback_health()
    second = api.probe_browser_fallback_health()

    assert first["ok"] is False
    assert "pool unavailable" in first["error"]
    assert first["errorType"] == "RuntimeError"
    assert second["ok"] is False
    assert second.get("busy") is not True
