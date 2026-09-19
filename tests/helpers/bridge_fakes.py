from __future__ import annotations

import time
from unittest import mock

from src import admin_bridge
from src.bridge.job_availability_service import JobAvailabilityService
from src.source_discovery.directory_page_recovery import DirectoryRecoveryRequest
from tests.helpers.ports import ADMIN_BRIDGE_TEST_PORT


def wait_for_terminal_status(service: JobAvailabilityService, run_id: str) -> dict:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        status = service.status(run_id)
        if status.get("status") != "running":
            return status
        time.sleep(0.001)
    raise AssertionError(f"availability task {run_id} did not finish")


def directory_recovery_request(
    key: str = "https://studio.example.com/",
    *,
    html: str = "<html><body>No openings here</body></html>",
) -> DirectoryRecoveryRequest:
    return DirectoryRecoveryRequest(
        key=key,
        adapter="gameprog",
        discovery_method="gameprog",
        name="Studio",
        studio="Studio",
        page_url=key,
        html=html,
        payload={"studio": "Studio"},
    )


def configure_expired_regular_close(admin_bridge_entrypoint_root) -> None:
    cfg = admin_bridge.RuntimeConfig(
        root=admin_bridge_entrypoint_root,
        data_dir=admin_bridge_entrypoint_root,
        host="127.0.0.1",
        port=ADMIN_BRIDGE_TEST_PORT,
        log_format="human",
        log_level="info",
        quiet_requests=False,
        desktop_mode=True,
        owner_mode="desktop-window",
        owner_token="owner-1",
        desktop_session_id="session-1",
        started_by="test",
        owner_idle_timeout_s=15.0,
    )
    admin_bridge.configure_runtime_paths(cfg)
    with mock.patch.object(admin_bridge, "now_iso", return_value="2026-03-01T00:00:00+00:00"):
        admin_bridge.update_desktop_session_lifecycle(
            owner_token="owner-1",
            session_id="session-1",
            page_id="page-1",
            state="closing",
            reason="beforeunload",
        )
