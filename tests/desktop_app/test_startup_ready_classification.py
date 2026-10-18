"""Startup readiness classification and phase-evidence tests."""

from __future__ import annotations

import json
import socket
from pathlib import Path

import pytest

from src.ship import desktop_app
from src.ship.startup_telemetry import append_startup_trace
from tests.desktop_app._helpers import desktop_runtime_config
from tests.helpers.temp_paths import workspace_tmpdir


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
        probe.bind(("127.0.0.1", 0))
        return int(probe.getsockname()[1])


def test_classify_reports_unbound_when_nothing_is_listening() -> None:
    reason, payload = desktop_app.classify_desktop_startup_state(
        _free_port(), app_version="0.0.0", timeout_s=0.2
    )

    assert reason == "bridge_unbound"
    assert payload == {}


def test_classify_reports_bound_listener_that_does_not_answer() -> None:
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(8)
    try:
        port = int(listener.getsockname()[1])
        reason, payload = desktop_app.classify_desktop_startup_state(
            port, app_version="0.0.0", timeout_s=0.2
        )
    finally:
        listener.close()

    assert reason == "bridge_bound_not_accepting"
    assert payload == {}


def test_classify_reports_health_mismatch_for_a_non_bridge_responder() -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
        listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        port = int(listener.getsockname()[1])

        import threading

        def respond() -> None:
            try:
                connection, _address = listener.accept()
            except OSError:
                return
            with connection:
                connection.recv(4096)
                body = json.dumps({"service": "something-else"}).encode("utf-8")
                connection.sendall(
                    b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                    + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                    + body
                )

        worker = threading.Thread(target=respond, daemon=True)
        worker.start()
        try:
            reason, payload = desktop_app.classify_desktop_startup_state(
                port, app_version="0.0.0", timeout_s=1.0
            )
        finally:
            worker.join(timeout=2.0)

    assert reason == "bridge_health_mismatch"
    assert payload["service"] == "something-else"


def test_latest_bridge_startup_phases_reads_newest_bridge_events_in_order() -> None:
    with workspace_tmpdir("desktop-startup-phases") as data_dir:
        append_startup_trace(data_dir, "desktop_launch_start", elapsedMs=0)
        append_startup_trace(data_dir, "desktop_bridge_child_start", pid=1)
        append_startup_trace(data_dir, "desktop_site_ready", elapsedMs=1200)
        append_startup_trace(
            data_dir,
            "desktop_bridge_startup_step_finished",
            step="startup_sync_schedule",
            durationMs=14000,
        )

        phases = desktop_app.latest_bridge_startup_phases(data_dir, limit=5)

    assert [row["event"] for row in phases] == [
        "desktop_bridge_child_start",
        "desktop_bridge_startup_step_finished",
    ]
    assert phases[-1]["fields"]["step"] == "startup_sync_schedule"


def test_wait_for_startup_ready_attaches_phase_evidence_on_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("desktop-startup-timeout-phases") as data_dir:
        config = desktop_runtime_config(ship_root=Path(data_dir), data_dir=Path(data_dir))
        append_startup_trace(
            config.data_dir,
            "desktop_bridge_startup_step_started",
            step="stale_task_cleanup",
        )
        monkeypatch.setattr(
            desktop_app,
            "classify_desktop_startup_state",
            lambda *_args, **_kwargs: (
                "bridge_bound_not_accepting",
                {},
            ),
        )

        with pytest.raises(desktop_app.DesktopStartupReadyTimeout) as excinfo:
            desktop_app.wait_for_desktop_startup_ready(
                config.bridge_port,
                app_version="0.0.0",
                timeout_s=1.0,
                data_dir=config.data_dir,
            )

    assert excinfo.value.reason == "bridge_bound_not_accepting"
    phases = excinfo.value.payload["startupPhases"]
    assert phases[0]["event"] == "desktop_bridge_startup_step_started"
    assert phases[0]["fields"]["step"] == "stale_task_cleanup"


def test_startup_timeout_helpers_extract_and_render_phase_evidence() -> None:
    exc = desktop_app.DesktopStartupReadyTimeout(
        "bridge_bound_not_accepting",
        "Baluffo bridge bound its port, but it did not answer the desktop health endpoint in time.",
        payload={
            "startupPhases": [
                {
                    "event": "desktop_bridge_startup_step_started",
                    "ts": "2026-10-17T14:08:00+00:00",
                    "fields": {"step": "startup_sync_schedule"},
                },
                "not-a-row",
                {"fields": {"step": "missing-event"}},
            ]
        },
    )

    phases = desktop_app.startup_timeout_phases(exc)
    text = desktop_app.format_startup_timeout_diagnostics(
        exc, open_url="http://127.0.0.1:8080/jobs.html", phases=phases
    )

    assert [row["event"] for row in phases] == ["desktop_bridge_startup_step_started"]
    assert "Reason: bridge_bound_not_accepting" in text
    assert "Recovery URL: http://127.0.0.1:8080/jobs.html" in text
    assert "- desktop_bridge_startup_step_started step=startup_sync_schedule" in text
    assert desktop_app.startup_timeout_phases(RuntimeError("plain")) == []
