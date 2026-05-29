import argparse
import json
from pathlib import Path
from unittest import mock

import pytest

from src.ship import desktop_app, windows_user_paths
from src.ship.desktop_app import config as config_mod
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
    assert config.owner_idle_timeout_s == 0.0


@pytest.mark.windows
def test_windows_user_paths_resolve_roaming_local_cache_and_fallbacks() -> None:
    env = {
        "APPDATA": "C:/Users/Andrea/AppData/Roaming",
        "LOCALAPPDATA": "C:/Users/Andrea/AppData/Local",
    }
    fallback_env = {"USERPROFILE": "C:/Users/Andrea"}

    assert windows_user_paths.windows_roaming_app_data_dir(env) == Path(
        "C:/Users/Andrea/AppData/Roaming/Baluffo"
    )
    assert windows_user_paths.windows_local_app_data_dir(env) == Path(
        "C:/Users/Andrea/AppData/Local/Baluffo"
    )
    assert windows_user_paths.windows_cache_dir(env) == Path(
        "C:/Users/Andrea/AppData/Local/Baluffo/cache"
    )
    assert windows_user_paths.windows_roaming_app_data_dir(fallback_env) == Path(
        "C:/Users/Andrea/AppData/Roaming/Baluffo"
    )
    assert windows_user_paths.windows_local_app_data_dir(fallback_env) == Path(
        "C:/Users/Andrea/AppData/Local/Baluffo"
    )


@pytest.mark.windows
def test_windows_legacy_user_data_migration_copies_without_overwriting() -> None:
    with workspace_tmpdir("windows-user-data-migration") as tmp:
        legacy = Path(tmp) / "portable" / "ship" / "data"
        target = Path(tmp) / "AppData" / "Roaming" / "Baluffo"
        (legacy / "local-user-data").mkdir(parents=True)
        (legacy / "local-user-data" / "profile.json").write_text(
            '{"name":"legacy"}', encoding="utf-8"
        )
        (legacy / "jobs-unified.json").write_text("[{}]", encoding="utf-8")
        (target / "jobs-unified.json").parent.mkdir(parents=True)
        (target / "jobs-unified.json").write_text("existing", encoding="utf-8")

        report = windows_user_paths.migrate_legacy_windows_user_data(legacy, target)
        report_path = windows_user_paths.windows_user_data_migration_report_path(target)

        assert report["status"] == "copied_with_conflicts"
        assert (target / "local-user-data" / "profile.json").read_text(
            encoding="utf-8"
        ) == '{"name":"legacy"}'
        assert (target / "jobs-unified.json").read_text(encoding="utf-8") == "existing"
        assert (legacy / "jobs-unified.json").is_file()
        persisted = json.loads(report_path.read_text(encoding="utf-8"))
        assert persisted["completed"] is True
        assert persisted["conflicts"] == ["jobs-unified.json"]

        second_report = windows_user_paths.migrate_legacy_windows_user_data(legacy, target)

    assert second_report["status"] == "already_migrated"


@pytest.mark.windows
def test_windows_legacy_user_data_migration_skips_packaged_smoke_rehearsal_source() -> None:
    with workspace_tmpdir("windows-user-data-migration-rehearsal") as tmp:
        root = Path(tmp)
        appdata = root / "AppData" / "Roaming"
        legacy = (
            root
            / ".tmp"
            / "packaged-desktop-smoke"
            / "run-1"
            / "portable-install"
            / "ship"
            / "data"
        )
        target = appdata / "Baluffo"
        profiles_path = legacy / "local-user-data" / "profiles.json"
        profiles_path.parent.mkdir(parents=True)
        profiles_path.write_text(
            json.dumps(
                [
                    {
                        "id": "local_packaged_update_rehearsal",
                        "name": "Packaged Update Rehearsal",
                        "email": "",
                    }
                ]
            ),
            encoding="utf-8",
        )
        (legacy / "jobs-unified.json").write_text("[{}]", encoding="utf-8")

        report = windows_user_paths.migrate_legacy_windows_user_data(
            legacy,
            target,
            env_map={"APPDATA": str(appdata)},
        )

        assert report["status"] == "skipped_packaged_smoke_rehearsal"
        assert report["completed"] is True
        assert not (target / "local-user-data" / "profiles.json").exists()
        assert not (target / "jobs-unified.json").exists()
        persisted = json.loads(
            windows_user_paths.windows_user_data_migration_report_path(target).read_text(
                encoding="utf-8"
            )
        )
        assert persisted["status"] == "skipped_packaged_smoke_rehearsal"


@pytest.mark.windows
def test_windows_legacy_user_data_migration_marks_missing_legacy_data() -> None:
    with workspace_tmpdir("windows-user-data-migration-missing") as tmp:
        legacy = Path(tmp) / "portable" / "ship" / "data"
        target = Path(tmp) / "AppData" / "Roaming" / "Baluffo"

        report = windows_user_paths.migrate_legacy_windows_user_data(legacy, target)

        assert report["status"] == "legacy_missing"
        assert windows_user_paths.windows_user_data_migration_report_path(target).is_file()


@pytest.mark.windows
def test_create_runtime_config_windows_packaged_uses_appdata_and_migrates_legacy() -> None:
    with workspace_tmpdir("desktop-config-windows-appdata") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        appdata = Path(tmp) / "AppData" / "Roaming"
        localappdata = Path(tmp) / "AppData" / "Local"
        legacy_data = ship_root / "data"
        legacy_data.mkdir(parents=True)
        (legacy_data / "jobs-unified.json").write_text("[{}]", encoding="utf-8")
        args = argparse.Namespace(
            root=str(ship_root),
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
        with (
            mock.patch.object(desktop_app, "resolve_ship_root", return_value=ship_root),
            mock.patch.object(config_mod, "_is_windows_packaged_runtime", return_value=True),
            mock.patch.dict(
                config_mod.os.environ,
                {
                    "APPDATA": str(appdata),
                    "LOCALAPPDATA": str(localappdata),
                    "BALUFFO_DATA_DIR": "",
                },
                clear=False,
            ),
        ):
            config = desktop_app.create_runtime_config(args)

        assert config.data_dir == (appdata / "Baluffo").resolve()
        assert (config.data_dir / "jobs-unified.json").read_text(encoding="utf-8") == "[{}]"
        assert (legacy_data / "jobs-unified.json").is_file()
        assert windows_user_paths.windows_user_data_migration_report_path(config.data_dir).is_file()


@pytest.mark.windows
def test_create_runtime_config_preserves_env_data_dir_override_for_packaged_windows() -> None:
    with workspace_tmpdir("desktop-config-windows-env-data-dir") as tmp:
        ship_root = Path(tmp) / "portable" / "ship"
        override_data = Path(tmp) / "override-data"
        (ship_root / "data").mkdir(parents=True)
        args = argparse.Namespace(
            root=str(ship_root),
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
        with (
            mock.patch.object(desktop_app, "resolve_ship_root", return_value=ship_root),
            mock.patch.object(config_mod, "_is_windows_packaged_runtime", return_value=True),
            mock.patch.dict(
                config_mod.os.environ,
                {"BALUFFO_DATA_DIR": str(override_data)},
                clear=False,
            ),
        ):
            config = desktop_app.create_runtime_config(args)

        assert config.data_dir == override_data.resolve()
        assert not windows_user_paths.windows_user_data_migration_report_path(
            override_data
        ).exists()


def test_create_runtime_config_preserves_owner_idle_timeout_override() -> None:
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
        owner_idle_timeout_s=7.5,
    )
    with mock.patch.object(desktop_app, "resolve_ship_root", return_value=root):
        config = desktop_app.create_runtime_config(args)

    assert config.owner_idle_timeout_s == 7.5


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
        xdg_root = root / "xdg-data"
        env = {
            "LOCALAPPDATA": str(localappdata_root),
            "USERNAME": "tester",
            "XDG_DATA_HOME": str(xdg_root),
        }
        local_candidate = (localappdata_root / "Baluffo").resolve()
        xdg_candidate = (xdg_root / "Baluffo").resolve()
        temp_candidate = (temp_root / "Baluffo-tester").resolve()
        blocked_parents = {local_candidate, xdg_candidate, temp_candidate}
        original_write_text = Path.write_text

        monkeypatch.setattr(desktop_app.tempfile, "gettempdir", lambda: str(temp_root))

        def blocked_write_text(self: Path, *args: object, **kwargs: object) -> int:
            if self.name == ".baluffo-write-probe" and self.parent in blocked_parents:
                raise OSError("blocked for test")
            return original_write_text(self, *args, **kwargs)

        monkeypatch.setattr(Path, "write_text", blocked_write_text)

        resolved = desktop_app.resolve_browser_session_root(env)

    assert "BaluffoRuntime" in str(resolved)
    assert desktop_app.last_session_root_resolution()["strategy"] == "runtime-temp"
