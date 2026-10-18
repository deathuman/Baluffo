from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import pytest

from src.ship import runtime_launcher as rl
from src.ship import startup_telemetry as telemetry
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT
from tests.helpers.temp_paths import workspace_tmpdir
from tests.test_runtime_launcher import _seed_ship_root


def test_desktop_startup_trace_target_covers_shipped_desktop_launches() -> None:
    with workspace_tmpdir("runtime-launcher-trace-env") as tmp:
        data_dir = str(Path(tmp) / "data")

        assert telemetry.startup_trace_target(
            {"BALUFFO_DESKTOP_MODE": "1", "BALUFFO_DATA_DIR": data_dir}
        ) == (Path(data_dir).resolve(), True)
        assert telemetry.startup_trace_target({"BALUFFO_DESKTOP_MODE": "1"}) == (None, False)
        assert telemetry.startup_trace_target({"BALUFFO_DATA_DIR": data_dir}) == (None, False)
        assert telemetry.startup_trace_target(
            {
                "BALUFFO_DESKTOP_MODE": "1",
                "BALUFFO_DATA_DIR": data_dir,
                telemetry.STARTUP_TRACE_DISABLED_ENV: "1",
            }
        ) == (None, False)
        assert telemetry.startup_trace_target(
            {"BALUFFO_DATA_DIR": data_dir, telemetry.STARTUP_TRACE_ENV: "1"}
        ) == (Path(data_dir).resolve(), True)
        assert telemetry.startup_trace_target(
            {
                "BALUFFO_DATA_DIR": data_dir,
                "BALUFFO_STARTUP_PROBE": "1",
                telemetry.STARTUP_TRACE_DISABLED_ENV: "1",
            }
        ) == (Path(data_dir).resolve(), True)


def test_append_startup_trace_rotates_the_metrics_journal(monkeypatch) -> None:
    with workspace_tmpdir("runtime-launcher-trace-rotation") as tmp:
        data_dir = Path(tmp) / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        metrics_path = data_dir / telemetry.STARTUP_TRACE_METRICS_NAME
        metrics_path.write_text(
            json.dumps({"schemaVersion": 1, "event": "old_row"}) + "\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(telemetry, "STARTUP_METRIC_MAX_BYTES", 1)

        telemetry.append_startup_trace(data_dir, "desktop_bridge_child_start", pid=7)

        rotated_path = metrics_path.with_name(
            f"{metrics_path.name}{telemetry.STARTUP_METRIC_ROTATED_SUFFIX}"
        )
        rotated_rows = [json.loads(line) for line in rotated_path.read_text("utf-8").splitlines()]
        current_rows = [json.loads(line) for line in metrics_path.read_text("utf-8").splitlines()]

    assert [row["event"] for row in rotated_rows] == ["old_row"]
    assert [row["event"] for row in current_rows] == ["desktop_bridge_child_start"]
    assert current_rows[0]["fields"] == {"pid": 7}


def test_run_bridge_server_emits_child_boot_phases(monkeypatch) -> None:
    with workspace_tmpdir("runtime-launcher-bridge-phases") as tmp:
        root = Path(tmp) / "ship"
        _seed_ship_root(root, version="5.0.0")
        events: list[tuple[str, dict[str, object]]] = []

        def _capture_run_path(script: str, run_name: str) -> None:
            raise RuntimeError("stop-after-script")

        monkeypatch.setenv("BALUFFO_DATA_DIR", str(root / "data"))
        monkeypatch.setenv("BALUFFO_DESKTOP_MODE", "1")
        with (
            mock.patch.object(
                rl,
                "_append_runtime_startup_trace",
                side_effect=lambda event, **fields: events.append((event, dict(fields))),
            ),
            mock.patch.object(rl.runpy, "run_path", side_effect=_capture_run_path),
        ):
            with pytest.raises(RuntimeError, match="stop-after-script"):
                rl.run_bridge_server(root, port=ADMIN_BRIDGE_TEST_PORT)

    event_names = [event for event, _fields in events]
    assert event_names[0] == "desktop_bridge_child_start"
    assert "desktop_bridge_entrypoint_start" in event_names
    entrypoint_fields = dict(events)["desktop_bridge_entrypoint_start"]
    boot_ms = entrypoint_fields["bootMs"]
    assert isinstance(boot_ms, int)
    assert boot_ms >= 0
    assert str(entrypoint_fields["bridgeScript"]).endswith("admin_bridge.py")
