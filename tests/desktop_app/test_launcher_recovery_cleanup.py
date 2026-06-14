from unittest import mock

import pytest

from src.ship import desktop_app
from src.ship.desktop_app import launcher_recovery


@pytest.fixture(autouse=True)
def _isolate_desktop_job(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(desktop_app, "_windows_close_desktop_job", mock.Mock())


def test_cleanup_runtime_launch_closes_windows_job_before_process_termination(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls: list[tuple[str, object]] = []
    monkeypatch.setattr(
        desktop_app,
        "_windows_close_desktop_job",
        lambda job: calls.append(("job", job)),
    )
    monkeypatch.setattr(
        desktop_app,
        "terminate_process",
        lambda process: calls.append(("terminate", process)),
    )
    monkeypatch.setattr(
        desktop_app,
        "release_instance_lock",
        lambda lock: calls.append(("release", lock)),
    )
    monkeypatch.setattr(
        desktop_app,
        "clear_session_state",
        lambda: calls.append(("clear", "")),
    )

    launcher_recovery.cleanup_runtime_launch(
        instance_lock="lock",
        session_state_written=True,
        desktop_job=123,
        browser_process="browser",
        bridge_process="bridge",
        site_process="site",
    )

    assert calls == [
        ("job", 123),
        ("terminate", "bridge"),
        ("terminate", "site"),
        ("terminate", "browser"),
        ("release", "lock"),
        ("clear", ""),
    ]
