import argparse
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_app
from tests.helpers.temp_paths import workspace_tmpdir


def test_create_runtime_config_defaults_to_fixed_desktop_ports() -> None:
    root = Path("C:/tmp/baluffo-ship")
    args = argparse.Namespace(
        root=str(root),
        site_port=0,
        bridge_port=0,
        bridge_host="127.0.0.1",
        data_dir="",
        open_path="admin.html",
        title="",
        port=0,
        bind_host="127.0.0.1",
        child_mode="",
        desktop_runtime=False,
        startup_probe=False,
    )
    with mock.patch.object(desktop_app, "resolve_ship_root", return_value=root):
        config = desktop_app.create_runtime_config(args)

    assert config.ship_root == root
    assert config.site_port == desktop_app.DEFAULT_SITE_PORT
    assert config.bridge_port == desktop_app.DEFAULT_BRIDGE_PORT
    assert not config.site_port_explicit
    assert not config.bridge_port_explicit
    assert config.no_browser is False
    assert config.data_dir == root / "data"
    assert config.open_path == "admin.html"
    assert config.jobs_cold_start is False
    assert config.title == desktop_app.WINDOW_TITLE


def test_create_runtime_config_defaults_to_jobs_entry() -> None:
    root = Path("C:/tmp/baluffo-ship")
    args = argparse.Namespace(
        root=str(root),
        site_port=0,
        bridge_port=0,
        bridge_host="127.0.0.1",
        data_dir="",
        open_path="",
        title="",
        port=0,
        bind_host="127.0.0.1",
        child_mode="",
        desktop_runtime=False,
        startup_probe=False,
    )
    with mock.patch.object(desktop_app, "resolve_ship_root", return_value=root):
        config = desktop_app.create_runtime_config(args)

    assert config.open_path == "jobs.html"
    assert config.jobs_cold_start is True


def test_create_runtime_config_skips_jobs_cold_start_for_successful_local_feed() -> None:
    with workspace_tmpdir("desktop-config-returning-jobs") as tmp:
        root = Path(tmp) / "ship"
        data_dir = root / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        (data_dir / "jobs-fetch-report.json").write_text(
            '{"finishedAt":"2026-05-17T10:00:00+00:00","summary":{"status":"ok","outputCount":1}}',
            encoding="utf-8",
        )
        (data_dir / "jobs-unified.json").write_text('[{"id":"job-1"}]', encoding="utf-8")
        (data_dir / "jobs-unified-light.json").write_text('[{"id":"job-1"}]', encoding="utf-8")
        (data_dir / "jobs-unified.csv").write_text(
            "id,title\njob-1,Tools Programmer\n", encoding="utf-8"
        )
        args = argparse.Namespace(
            root=str(root),
            site_port=0,
            bridge_port=0,
            bridge_host="127.0.0.1",
            data_dir="",
            open_path="jobs.html",
            title="",
            port=0,
            bind_host="127.0.0.1",
            child_mode="",
            desktop_runtime=False,
            startup_probe=False,
        )
        with mock.patch.object(desktop_app, "resolve_ship_root", return_value=root):
            config = desktop_app.create_runtime_config(args)

    assert config.jobs_cold_start is False


def test_create_runtime_config_can_enable_test_no_browser_mode_from_env() -> None:
    root = Path("C:/tmp/baluffo-ship")
    args = argparse.Namespace(
        root=str(root),
        site_port=0,
        bridge_port=0,
        bridge_host="127.0.0.1",
        data_dir="",
        open_path="jobs.html",
        title="",
        port=0,
        bind_host="127.0.0.1",
        child_mode="",
        desktop_runtime=False,
        startup_probe=False,
    )
    with (
        mock.patch.object(desktop_app, "resolve_ship_root", return_value=root),
        mock.patch.dict(desktop_app.os.environ, {desktop_app.NO_BROWSER_ENV: "1"}, clear=False),
    ):
        config = desktop_app.create_runtime_config(args)

    assert config.no_browser is True


def test_build_open_url_marks_desktop_mode() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
    )
    assert (
        desktop_app.build_open_url(config)
        == "http://127.0.0.1:8080/jobs.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1"
    )


def test_build_open_url_marks_startup_probe_when_enabled() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=True,
    )
    assert (
        desktop_app.build_open_url(config)
        == "http://127.0.0.1:8080/jobs.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1&startupProbe=1"
    )


def test_build_open_url_marks_jobs_cold_start_when_enabled() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
        jobs_cold_start=True,
    )
    assert (
        desktop_app.build_open_url(config)
        == "http://127.0.0.1:8080/jobs.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1&jobsColdStart=1"
    )


def test_build_open_url_does_not_mark_jobs_cold_start_for_non_jobs_entry() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="admin.html",
        title="Baluffo",
        startup_probe=False,
        jobs_cold_start=True,
    )
    assert (
        desktop_app.build_open_url(config)
        == "http://127.0.0.1:8080/admin.html?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1"
    )


def test_resolve_runtime_ports_falls_back_to_free_ports_for_defaults() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
    )
    availability = {
        ("127.0.0.1", 8080): False,
        ("127.0.0.1", 19080): True,
        ("127.0.0.1", 8877): False,
        ("127.0.0.1", 19877): True,
    }
    with (
        mock.patch.object(
            desktop_app,
            "_port_is_available",
            side_effect=lambda host, port: availability.get((str(host), int(port)), True),
        ),
        mock.patch.object(desktop_app, "choose_free_port", side_effect=[19080, 19877]),
    ):
        resolved = desktop_app.resolve_runtime_ports(config)

    assert resolved.site_port == 19080
    assert resolved.bridge_port == 19877


def test_resolve_runtime_ports_keeps_explicit_port_fail_fast() -> None:
    config = desktop_app.DesktopRuntimeConfig(
        ship_root=Path("C:/tmp/baluffo-ship"),
        site_port=8080,
        bridge_port=8877,
        bridge_host="127.0.0.1",
        data_dir=Path("C:/tmp/baluffo-ship/data"),
        open_path="jobs.html",
        title="Baluffo",
        startup_probe=False,
        site_port_explicit=True,
    )

    with mock.patch.object(desktop_app, "_port_is_available", return_value=False):
        with pytest.raises(RuntimeError, match="site port 8080 is already in use"):
            desktop_app.resolve_runtime_ports(config)


def test_resolve_browser_session_root_falls_back_to_runtime_temp_when_standard_locations_fail(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("desktop-session-root") as tmp:
        root = Path(tmp)
        temp_root = root / "temp"
        localappdata_root = root / "localappdata"
        env = {"LOCALAPPDATA": str(localappdata_root), "USERNAME": "tester"}
        local_candidate = (localappdata_root / "Baluffo").resolve()
        temp_candidate = (temp_root / "Baluffo-tester").resolve()
        original_write_text = Path.write_text

        monkeypatch.setattr(desktop_app.tempfile, "gettempdir", lambda: str(temp_root))

        def blocked_write_text(self: Path, *args: object, **kwargs: object) -> int:
            if self.name == ".baluffo-write-probe" and self.parent in {
                local_candidate,
                temp_candidate,
            }:
                raise OSError("blocked for test")
            return original_write_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", blocked_write_text)

        resolved = desktop_app.resolve_browser_session_root(env)

    assert "BaluffoRuntime" in str(resolved)
    assert desktop_app.last_session_root_resolution()["strategy"] == "runtime-temp"
