"""Fetch-KPI storage alert: the WAL warning follows the live file, not the record.

W1-W3 of `docs/plans/admin-warnings-0-3-064-remediation-plan.md`. Split out of
`test_admin_bridge_ops_health.py`, which is at its line budget.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from src import admin_bridge
from src.bridge import ops_health, storage_health


def _storage_health_payload(storage: dict[str, Any]) -> dict[str, Any]:
    return {"ok": storage.get("healthy") is not False, "storage": storage}


def test_fetch_kpis_summary_omits_a_busy_record_whose_wal_is_already_empty(
    admin_bridge_entrypoint_root, monkeypatch
) -> None:
    # The live 0.3.064 shape: healthy store, 0-byte WAL, and a record still naming
    # the 883.4 MB the 2026-11-17 attempt saw. The record is rewritten only by a
    # later attempt, so following it alone keeps a resolved condition visible.
    monkeypatch.setattr(
        "src.bridge.ops_api_health._storage_health.get_storage_health_payload",
        lambda _data_dir: _storage_health_payload(
            {
                "healthy": True,
                "quickCheck": "deferred-large-database",
                "quickCheckDeferred": True,
                "walBytes": 0,
                "databaseBytes": 272695296,
                "walMaintenance": {
                    "status": "busy",
                    "mode": "PASSIVE",
                    "attemptedAt": "2026-11-17T20:43:02.726852Z",
                    "attemptAgeS": 44520,
                    "walBytes": 926311992,
                    "databaseBytes": 545017856,
                    "checkpointThresholdBytes": 67108864,
                },
            }
        ),
    )

    api: Any = admin_bridge._get_ops_api()
    payload = api.compute_ops_fetch_kpis_summary()

    assert all(alert.get("id") != "storage_unhealthy" for alert in payload["alerts"])


def test_fetch_kpis_summary_reports_live_sizes_for_a_checkpoint_still_outstanding(
    admin_bridge_entrypoint_root, monkeypatch
) -> None:
    monkeypatch.setattr(
        "src.bridge.ops_api_health._storage_health.get_storage_health_payload",
        lambda _data_dir: _storage_health_payload(
            {
                "healthy": True,
                "quickCheck": "deferred-large-database",
                "quickCheckDeferred": True,
                "walBytes": 943718400,
                "databaseBytes": 272695296,
                "walMaintenance": {
                    "status": "busy",
                    "attemptedAt": "2026-11-18T08:35:00+00:00",
                    "attemptAgeS": 1500,
                    "walBytes": 943718400,
                    "databaseBytes": 272695296,
                    "checkpointThresholdBytes": 67108864,
                },
            }
        ),
    )

    api: Any = admin_bridge._get_ops_api()
    payload = api.compute_ops_fetch_kpis_summary()

    alert = next(a for a in payload["alerts"] if a.get("id") == "storage_unhealthy")
    assert alert["severity"] == "warning"
    assert "WAL checkpoint could not finish" in alert["message"]
    assert "900.0 MB WAL on 260.1 MB database" in alert["message"]
    assert "25 min ago" in alert["message"]
    assert payload["status"] == "warning"


def test_storage_alert_names_the_attempt_sizes_only_when_they_differ() -> None:
    alert = ops_health.storage_health_alert(
        {
            "storage": {
                "healthy": True,
                "walBytes": 734003200,
                "databaseBytes": 272695296,
                "walMaintenance": {
                    "status": "busy",
                    "attemptedAt": "2026-11-18T08:35:00+00:00",
                    "attemptAgeS": 90,
                    "walBytes": 943718400,
                    "databaseBytes": 272695296,
                    "checkpointThresholdBytes": 67108864,
                },
            }
        }
    )

    assert alert is not None
    assert "700.0 MB WAL on 260.1 MB database" in alert["message"]
    assert "the attempt saw 900.0 MB WAL on 260.1 MB database" in alert["message"]
    assert "1 min ago" in alert["message"]


@pytest.mark.parametrize("wal_bytes", [None, "", "  ", "n/a", -1, True])
def test_an_unmeasured_live_wal_keeps_the_warning(wal_bytes: Any) -> None:
    # A null, blank, unparsable or negative size is unknown, not zero. Reading it as
    # an empty WAL is how the gate would go silent on a checkpoint that is still
    # stuck, so an unknown size must keep the record-based warning.
    alert = ops_health.storage_health_alert(
        _storage_health_payload(
            {
                "healthy": True,
                "walBytes": wal_bytes,
                "databaseBytes": 272695296,
                "walMaintenance": {
                    "status": "busy",
                    "attemptedAt": "2026-11-17T20:43:02Z",
                    "attemptAgeS": 3600,
                    "checkpointThresholdBytes": 67108864,
                },
            }
        )
    )

    assert alert is not None
    assert alert["severity"] == "warning"
    assert alert["id"] == "storage_unhealthy"


def test_a_stalled_storage_read_carries_no_record_for_the_gate(tmp_path: Path) -> None:
    # A read that could not run reports `healthStalled` with fresh file sizes and no
    # `walMaintenance`, so the gate never sees a record without its own measurement.
    # `healthy` stays true there by design: a diagnostic that did not run is not
    # evidence of corruption.
    payload = storage_health._stalled_storage_payload(tmp_path, reason="store open busy")

    assert payload["storage"]["healthStalled"] is True
    assert payload["storage"]["healthy"] is True
    assert "walMaintenance" not in payload["storage"]
    assert ops_health.storage_health_alert(payload) is None


_GATE_CASES = json.loads(
    (Path(__file__).resolve().parents[1] / "fixtures" / "storage-alert-gate-cases.json").read_text(
        encoding="utf-8"
    )
)


@pytest.mark.parametrize("case", _GATE_CASES, ids=lambda case: case["name"])
def test_the_storage_gate_matches_the_shared_matrix(case: dict[str, Any]) -> None:
    # tests/frontend/unit/admin-action-center-storage-signals.test.mjs runs the same
    # matrix through the Action Center gate; one verdict per payload, or the surfaces
    # drift again, which is the 0.3.064 defect this matrix exists to stop.
    alert = ops_health.storage_health_alert(case["payload"])
    assert (alert is not None) is case["expect"]["alert"]
    if alert is not None:
        assert alert["id"] == "storage_unhealthy"
        assert alert["severity"] == case["expect"]["severity"]


def test_the_shared_gate_matrix_covers_each_verdict() -> None:
    # A deleted case would narrow both suites at once: keep one payload per verdict, and
    # keep the null live size that used to read as an empty WAL.
    verdicts = {(case["expect"]["alert"], case["expect"].get("severity")) for case in _GATE_CASES}
    assert verdicts == {(False, None), (True, "warning"), (True, "critical")}
    assert any(
        case["payload"]["storage"].get("walBytes") is None and case["expect"]["alert"]
        for case in _GATE_CASES
    )
