"""The probe-wave receipt on the Admin/Ops health payload.

The receipt answers two operational questions that used to require opening the GameDevMap audit
artifact by hand: how many probe fetches its per-call bound abandoned, and whether any batch left its
shared HTTP client unclosed. The audit publishes the counters as their own small file beside the
artifact, so this payload reads hundreds of bytes rather than the tens of MB the receipt summarizes.

The projection itself lives in `src/bridge/discovery_audit_artifacts.py`, whose own tests pin the
counters and the file the audit writes them to. This module pins the wiring: the receipt reaches the
KPI, an absent receipt stays absent rather than reading as a clean wave, and a failing read degrades
to an empty KPI without disturbing the rest of the payload.
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from src import admin_bridge
from src.bridge import ops_health
from src.bridge.discovery_audit_artifacts import PROBE_LIFECYCLE_RECEIPT_FILENAME


def _probe_lifecycle_receipt_payload() -> dict[str, Any]:
    return {
        "schemaVersion": 1,
        "runId": "gamedevmap-run-7",
        "startedAt": "2026-10-12T14:53:50+00:00",
        "finishedAt": "2026-10-12T14:54:04+00:00",
        "sourceArtifact": "gamedevmap-active-source-dry-run.json",
        "probeLifecycle": {
            "batches": 1,
            "fetchesStarted": 17,
            "fetchesBoundedOut": 2,
            "fetchesOutstandingAtExit": 1,
            "maxFetchesOutstandingAtExit": 1,
            "clientFetchesOutstandingAtExit": 1,
            "clientsRetiredByBatch": 0,
            "clientsRetiredByLastFetch": 1,
            "clientsUnclosed": 1,
            "maxClientCloseDeferredMs": 8173,
            "poolThreadsAliveNow": 0,
            "deadlineExits": 0,
        },
    }


def test_dashboard_health_surfaces_the_published_probe_lifecycle(admin_bridge_entrypoint_root):
    """An operator sees the abandoned fetches and the unclosed client without opening the artifact."""
    receipt_path = admin_bridge.DISCOVERY_REPORT_PATH.parent / PROBE_LIFECYCLE_RECEIPT_FILENAME
    admin_bridge.save_json_atomic(receipt_path, _probe_lifecycle_receipt_payload())

    probe_lifecycle = admin_bridge.compute_ops_dashboard_health()["kpis"]["probeLifecycle"]

    assert probe_lifecycle["fetchesStarted"] == 17
    assert probe_lifecycle["fetchesBoundedOut"] == 2, "the abandoned probe count reaches the KPI"
    assert probe_lifecycle["clientsUnclosed"] == 1, "the leak flag reaches the KPI"
    assert probe_lifecycle["runId"] == "gamedevmap-run-7"
    assert str(probe_lifecycle["modifiedAt"]).strip()


def test_dashboard_health_reports_an_absent_probe_lifecycle_as_empty(admin_bridge_entrypoint_root):
    """No published receipt is an empty KPI, not an error and not a zeroed receipt."""
    probe_lifecycle = admin_bridge.compute_ops_dashboard_health()["kpis"]["probeLifecycle"]

    # Empty, not zeroed: a run that never published a receipt must not be rendered as a clean wave.
    assert probe_lifecycle == {}


def test_dashboard_health_keeps_its_shape_when_the_receipt_reader_fails() -> None:
    """A failing receipt read degrades to an empty KPI; the rest of the payload is untouched."""
    deps = SimpleNamespace(
        get_history=lambda: [],
        get_fetch_report=lambda: {},
        get_state=lambda: {"pending": []},
        get_registry_summary_payload=lambda: {
            "generation": "",
            "reason": "json_summary",
            "activeCount": 1,
            "pendingCount": 0,
            "rejectedCount": 0,
            "tombstoneCount": 0,
            "summaryExact": False,
        },
        get_tombstones=lambda: {},
        get_sync_status_payload=lambda: {},
        now_iso=lambda: "2026-05-14T10:00:00+00:00",
        desktop_mode=True,
        desktop_last_activity_at="2026-05-14T10:00:00+00:00",
        owner_state={"startedAt": "2026-05-14T09:59:00+00:00"},
        load_alert_state_fn=lambda: {},
        save_alert_state_fn=lambda _payload: None,
        parse_schedule_metadata_fn=lambda: {"fetcher": {}, "discovery": {}},
        parse_iso=lambda _value: None,
        now_utc=lambda: datetime(2026, 5, 14, 10, 0, tzinfo=UTC),
        get_source_policy_soak_report=lambda: {},
        get_updater_status_payload=lambda: {},
        get_probe_lifecycle_payload=lambda: (_ for _ in ()).throw(OSError("receipt unreadable")),
        app_version="0.0.0-test",
        startup_ready=True,
    )

    health = ops_health.compute_ops_health(deps)

    assert health["kpis"]["probeLifecycle"] == {}
    assert health["kpis"]["registrySync"]["activeCount"] == 1


def test_dashboard_health_passes_a_tested_receipt_through_unchanged() -> None:
    receipt = {"fetchesStarted": 3, "clientsUnclosed": 0}
    deps = SimpleNamespace(
        get_history=lambda: [],
        get_fetch_report=lambda: {},
        get_state=lambda: {"pending": []},
        get_registry_summary_payload=lambda: {
            "generation": "",
            "reason": "json_summary",
            "activeCount": 1,
            "pendingCount": 0,
            "rejectedCount": 0,
            "tombstoneCount": 0,
            "summaryExact": False,
        },
        get_tombstones=lambda: {},
        get_sync_status_payload=lambda: {},
        now_iso=lambda: "2026-05-14T10:00:00+00:00",
        desktop_mode=True,
        desktop_last_activity_at="2026-05-14T10:00:00+00:00",
        owner_state={"startedAt": "2026-05-14T09:59:00+00:00"},
        load_alert_state_fn=lambda: {},
        save_alert_state_fn=lambda _payload: None,
        parse_schedule_metadata_fn=lambda: {"fetcher": {}, "discovery": {}},
        parse_iso=lambda _value: None,
        now_utc=lambda: datetime(2026, 5, 14, 10, 0, tzinfo=UTC),
        get_source_policy_soak_report=lambda: {},
        get_updater_status_payload=lambda: {},
        get_probe_lifecycle_payload=lambda: receipt,
        app_version="0.0.0-test",
        startup_ready=True,
    )

    assert ops_health.compute_ops_health(deps)["kpis"]["probeLifecycle"] == receipt


def test_summary_health_carries_the_published_probe_lifecycle(admin_bridge_entrypoint_root):
    """The Admin page's steady-cadence poll sees the receipt, not only the full payload.

    `?view=summary` omits most KPIs by design, but the receipt is a few hundred bytes beside the
    report: leaving it out made this view read as "no probe-wave receipt published" until a full
    payload happened to be fetched, which is a different statement from a wave that left nothing.
    """
    receipt_path = admin_bridge.DISCOVERY_REPORT_PATH.parent / PROBE_LIFECYCLE_RECEIPT_FILENAME
    admin_bridge.save_json_atomic(receipt_path, _probe_lifecycle_receipt_payload())
    api = admin_bridge.build_bridge_api(admin_bridge.RUNTIME_CONFIG)

    probe_lifecycle = api.compute_ops_dashboard_health_summary()["kpis"]["probeLifecycle"]

    assert probe_lifecycle["fetchesBoundedOut"] == 2
    assert probe_lifecycle["clientsUnclosed"] == 1
    assert probe_lifecycle["runId"] == "gamedevmap-run-7"


def test_summary_health_reports_an_absent_probe_lifecycle_as_empty(admin_bridge_entrypoint_root):
    """No published receipt reads as empty here too, rather than as a zeroed clean wave."""
    api = admin_bridge.build_bridge_api(admin_bridge.RUNTIME_CONFIG)

    assert api.compute_ops_dashboard_health_summary()["kpis"]["probeLifecycle"] == {}
