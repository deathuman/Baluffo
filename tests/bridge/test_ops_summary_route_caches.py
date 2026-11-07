from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

import src.bridge.ops_health as ops_health
from src.bridge.fetch_report_summary import (
    compact_browser_fallback_health,
    fetch_report_summary_path,
    load_fetch_report_summary_artifact,
)
from src.bridge.routes.get_ops_status import (
    OPS_DASHBOARD_SUMMARY_CACHE_TTL_S,
    OPS_FETCH_KPIS_CACHE_TTL_S,
    handle_ops_status_routes,
)
from tests.helpers.bridge_api import (
    FakeDesktopLocalDataStore,
    FakeHandler,
    make_stub_bridge_api,
)


class CountingSummaryApi:
    """Route-level fake; active evidence comes via the OpsApi summary probe."""

    def __init__(self, active: dict[str, Any] | None = None) -> None:
        self.active = active or {}
        self.dashboard_calls = 0
        self.kpis_calls = 0

    def _active_pipeline_or_fetch_summary(self) -> dict[str, Any]:
        return dict(self.active)

    def compute_ops_dashboard_health_summary(self) -> dict[str, Any]:
        self.dashboard_calls += 1
        payload: dict[str, Any] = {
            "ok": True,
            "dashboard": "summary",
            "call": self.dashboard_calls,
        }
        if self.active:
            payload["deferredDuringActiveRun"] = True
            payload["activePipeline"] = dict(self.active)
        return payload

    def compute_ops_fetch_kpis_summary(self) -> dict[str, Any]:
        self.kpis_calls += 1
        payload: dict[str, Any] = {"ok": True, "kpis": {"call": self.kpis_calls}}
        if self.active:
            payload["activePipelineOrFetchRunning"] = True
            payload["activePipeline"] = dict(self.active)
        return payload

    def compute_ops_health(self) -> dict[str, Any]:
        return {"ok": True, "detailLevel": "full"}

    def compute_ops_health_ready(self) -> dict[str, Any]:
        return {"ok": True, "detailLevel": "ready"}

    def compute_ops_dashboard_health(self) -> dict[str, Any]:
        return {"ok": True, "dashboard": "full"}

    def get_current_task_state_payload(self) -> dict[str, Any]:
        return {}

    def get_current_task_state_summary_payload(self) -> dict[str, Any]:
        return {}

    def get_lifecycle_run_history_rows(self) -> list[Any]:
        return []

    def get_task_live_payload(self, task_type: str, *, summary: bool = False) -> dict[str, Any]:
        return {}


class RawPipelineStatusApi(CountingSummaryApi):
    """Fallback probe shape (no OpsApi mixin): raw pipeline status getter."""

    _active_pipeline_or_fetch_summary = None  # type: ignore[assignment]

    def get_jobs_pipeline_status_payload(self) -> dict[str, Any]:
        return dict(self.active)


def _drive(api: Any, path: str, view: str) -> dict[str, Any]:
    handler = FakeHandler()
    assert handle_ops_status_routes(handler, api=api, path=path, query={"view": [view]}) is True
    return cast(dict[str, Any], handler.sent[-1]["payload"])


def _write_browser_health(sidecar_path: Path, health: dict[str, Any]) -> None:
    sidecar_path.write_text(
        json.dumps({"runtime": {"browserFallbackHealth": health}}),
        encoding="utf-8",
    )


def _browser_health_kpis(api: Any) -> dict[str, Any]:
    artifact = load_fetch_report_summary_artifact(api.JOBS_FETCH_REPORT_PATH)
    runtime = artifact.get("runtime")
    health = compact_browser_fallback_health(
        runtime.get("browserFallbackHealth") if isinstance(runtime, dict) else None
    )
    return {"ok": True, "summaryView": True, "kpis": {"browserFallbackHealth": health}}


def test_dashboard_health_summary_serves_cache_within_ttl(monkeypatch) -> None:
    api = CountingSummaryApi()

    first = _drive(api, "/ops/dashboard-health", "summary")
    assert api.dashboard_calls == 1

    second = _drive(api, "/ops/dashboard-health", "summary")
    assert api.dashboard_calls == 1
    assert second == first


def test_dashboard_health_summary_recomputes_after_ttl(monkeypatch) -> None:
    api = CountingSummaryApi()
    monotonic = [100.0]
    monkeypatch.setattr("src.bridge.routes.get_ops_status.time.monotonic", lambda: monotonic[0])

    _drive(api, "/ops/dashboard-health", "summary")
    assert api.dashboard_calls == 1

    # TTL measured from completion: advancing less than the full window keeps hit.
    monotonic[0] += OPS_DASHBOARD_SUMMARY_CACHE_TTL_S - 1.0
    _drive(api, "/ops/dashboard-health", "summary")
    assert api.dashboard_calls == 1

    monotonic[0] += 2.0
    _drive(api, "/ops/dashboard-health", "summary")
    assert api.dashboard_calls == 2


def test_fetch_kpis_summary_serves_cache_within_ttl(monkeypatch) -> None:
    api = CountingSummaryApi()

    _drive(api, "/ops/fetch-kpis", "summary")
    assert api.kpis_calls == 1

    second = _drive(api, "/ops/fetch-kpis", "summary")
    assert api.kpis_calls == 1
    assert second["kpis"]["call"] == 1


def test_fetch_kpis_summary_refreshes_browser_health_when_sidecar_changes(
    tmp_path: Path,
) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    sidecar_path = fetch_report_summary_path(api.JOBS_FETCH_REPORT_PATH)

    api.compute_ops_fetch_kpis_summary = lambda: _browser_health_kpis(api)
    _write_browser_health(
        sidecar_path,
        {
            "ok": False,
            "stage": "launch",
            "errorType": "executable_does_not_exist",
            "error": "Executable doesn't exist at /ms-playwright/chromium-1200",
            "durationMs": 120,
        },
    )
    first = _drive(api, "/ops/fetch-kpis", "summary")
    assert first["kpis"]["browserFallbackHealth"] == {
        "ok": False,
        "durationMs": 120,
        "stage": "launch",
        "errorType": "executable_does_not_exist",
        "error": "Executable doesn't exist at /ms-playwright/chromium-1200",
    }

    # Both calls are inside the 15s TTL; the changed sidecar stat must invalidate.
    _write_browser_health(sidecar_path, {"ok": True, "stage": "navigation", "durationMs": 42})
    second = _drive(api, "/ops/fetch-kpis", "summary")
    assert second["kpis"]["browserFallbackHealth"] == {
        "ok": True,
        "durationMs": 42,
        "stage": "navigation",
    }


def test_fetch_kpis_summary_refreshes_after_sidecar_delete_and_recreate(
    tmp_path: Path,
) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    sidecar_path = fetch_report_summary_path(api.JOBS_FETCH_REPORT_PATH)
    api.compute_ops_fetch_kpis_summary = lambda: _browser_health_kpis(api)

    _write_browser_health(
        sidecar_path,
        {
            "ok": False,
            "stage": "launch",
            "errorType": "executable_does_not_exist",
            "durationMs": 120,
        },
    )
    first = _drive(api, "/ops/fetch-kpis", "summary")
    assert first["kpis"]["browserFallbackHealth"]["ok"] is False

    # A missing sidecar has no stat key; the cached payload must not survive.
    sidecar_path.unlink()
    missing = _drive(api, "/ops/fetch-kpis", "summary")
    assert missing["kpis"]["browserFallbackHealth"] == {}

    # Recreating the sidecar changes the key again, even inside the TTL.
    _write_browser_health(sidecar_path, {"ok": True, "stage": "navigation", "durationMs": 42})
    recreated = _drive(api, "/ops/fetch-kpis", "summary")
    assert recreated["kpis"]["browserFallbackHealth"] == {
        "ok": True,
        "durationMs": 42,
        "stage": "navigation",
    }


def _dashboard_from_sidecars(api: Any) -> dict[str, Any]:
    """The two receipts the real dashboard summary embeds, read from their sidecars."""
    data_dir = Path(api.DISCOVERY_REPORT_PATH).parent

    def _read(filename: str) -> Any:
        try:
            return json.loads((data_dir / filename).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}

    return {
        "ok": True,
        "summaryView": True,
        "probeLifecycle": _read("gamedevmap-audit-receipt.json"),
        "pageFetchLifecycle": _read("page-fetch-lifecycle.v1.json"),
    }


def test_dashboard_health_summary_refreshes_when_sidecar_changes(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.compute_ops_dashboard_health_summary = lambda: _dashboard_from_sidecars(api)
    data_dir = Path(api.DISCOVERY_REPORT_PATH).parent
    probe_receipt = data_dir / "gamedevmap-audit-receipt.json"
    page_fetch_sidecar = data_dir / "page-fetch-lifecycle.v1.json"

    probe_receipt.write_text(
        json.dumps({"runId": "probe-1", "finishedAt": "2026-11-06T10:00:00+00:00"}),
        encoding="utf-8",
    )
    first = _drive(api, "/ops/dashboard-health", "summary")
    assert first["probeLifecycle"]["runId"] == "probe-1"
    assert first["pageFetchLifecycle"] == {}

    # Both calls are inside the 10s TTL; the republished receipt must invalidate.
    probe_receipt.write_text(
        json.dumps({"runId": "probe-2", "finishedAt": "2026-11-06T10:05:00+00:00"}),
        encoding="utf-8",
    )
    second = _drive(api, "/ops/dashboard-health", "summary")
    assert second["probeLifecycle"]["runId"] == "probe-2"

    # The page-fetch sidecar is the other embedded receipt and must key too.
    page_fetch_sidecar.write_text(
        json.dumps({"runId": "page-fetch-1", "sources": {"greenhouse": {"failureCount": 1}}}),
        encoding="utf-8",
    )
    third = _drive(api, "/ops/dashboard-health", "summary")
    assert third["pageFetchLifecycle"]["runId"] == "page-fetch-1"

    # A deleted receipt has no stat; the old payload must not survive.
    probe_receipt.unlink()
    missing = _drive(api, "/ops/dashboard-health", "summary")
    assert missing["probeLifecycle"] == {}

    # Recreating it changes the key again, even inside the TTL.
    probe_receipt.write_text(
        json.dumps({"runId": "probe-3", "finishedAt": "2026-11-06T10:10:00+00:00"}),
        encoding="utf-8",
    )
    recreated = _drive(api, "/ops/dashboard-health", "summary")
    assert recreated["probeLifecycle"]["runId"] == "probe-3"


def test_active_run_bypasses_caches_and_is_never_served_idle() -> None:
    api = CountingSummaryApi()
    _drive(api, "/ops/dashboard-health", "summary")
    _drive(api, "/ops/fetch-kpis", "summary")
    assert api.dashboard_calls == 1
    assert api.kpis_calls == 1

    # Active: always recompute, never serve the idle snapshot.
    api.active = {"active": True, "runId": "pipeline_live", "stage": "fetch"}
    dashboard_payload = _drive(api, "/ops/dashboard-health", "summary")
    kpis_payload = _drive(api, "/ops/fetch-kpis", "summary")
    assert api.dashboard_calls == 2
    assert api.kpis_calls == 2
    assert dashboard_payload["deferredDuringActiveRun"] is True
    assert kpis_payload["activePipelineOrFetchRunning"] is True

    # Idle again: the cached active-run payload must NOT be served; recompute.
    api.active = {}
    idle_dashboard = _drive(api, "/ops/dashboard-health", "summary")
    idle_kpis = _drive(api, "/ops/fetch-kpis", "summary")
    assert api.dashboard_calls == 3
    assert api.kpis_calls == 3
    assert "deferredDuringActiveRun" not in idle_dashboard
    assert "activePipelineOrFetchRunning" not in idle_kpis

    # And that fresh idle payload is now cacheable.
    _drive(api, "/ops/dashboard-health", "summary")
    assert api.dashboard_calls == 3


def test_fallback_probe_uses_raw_pipeline_status() -> None:
    api = RawPipelineStatusApi()
    _drive(api, "/ops/fetch-kpis", "summary")
    assert api.kpis_calls == 1
    second = _drive(api, "/ops/fetch-kpis", "summary")
    assert api.kpis_calls == 1
    assert second["kpis"]["call"] == 1

    api.active = {"active": True, "runId": "fetch_live", "stage": "starting"}
    _drive(api, "/ops/fetch-kpis", "summary")
    assert api.kpis_calls == 2


class RecordingLock:
    def __init__(self) -> None:
        self.events: list[str] = []

    def __enter__(self) -> None:
        self.events.append("enter")

    def __exit__(self, *_args: Any) -> None:
        self.events.append("exit")


def test_finalize_alerts_holds_lock_across_read_modify_write() -> None:
    lock = RecordingLock()
    saves: list[dict[str, Any]] = []
    ops_health._finalize_alerts(
        [],
        load_alert_state_fn=lambda: {"acked": {"stale_alert": True}},
        save_alert_state_fn=saves.append,
        state_lock=lock,
    )
    assert saves == [{"acked": {}}]
    assert lock.events == ["enter", "exit"]


def test_finalize_alerts_saves_only_on_state_change() -> None:
    saves: list[dict[str, Any]] = []
    loaded = {"schemaVersion": 1, "acked": {"stale_alert": True}}

    def load() -> dict[str, Any]:
        if saves:
            return dict(saves[-1])
        return dict(loaded)

    def save(payload: dict[str, Any]) -> None:
        saves.append(payload)

    # Stale ack for a no-longer-active alert must be cleaned up -> one save.
    first = ops_health._finalize_alerts([], load_alert_state_fn=load, save_alert_state_fn=save)
    assert first["alerts"] == []
    assert saves == [{"acked": {}}]

    # Identical state on the next poll -> no save (the poll-path write-skip).
    ops_health._finalize_alerts([], load_alert_state_fn=load, save_alert_state_fn=save)
    assert len(saves) == 1


def test_fetch_kpis_constants_unchanged_contract() -> None:
    assert OPS_DASHBOARD_SUMMARY_CACHE_TTL_S == 10.0
    assert OPS_FETCH_KPIS_CACHE_TTL_S == 15.0
