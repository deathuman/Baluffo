import contextlib
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from src.ship import desktop_app


def desktop_runtime_config(**overrides: object) -> desktop_app.DesktopRuntimeConfig:
    ship_root = Path(overrides.pop("ship_root", "C:/tmp/baluffo-ship"))
    values = {
        "ship_root": ship_root,
        "site_port": 8080,
        "bridge_port": 8877,
        "bridge_host": "127.0.0.1",
        "data_dir": ship_root / "data",
        "open_path": "jobs.html",
        "title": "Baluffo",
        "startup_probe": False,
    }
    values.update(overrides)
    return desktop_app.DesktopRuntimeConfig(**values)


def launcher_session(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "launcherPid": 1234,
        "launcherToken": "existing-launcher-token",
        "bridgePort": 8877,
        "url": "http://127.0.0.1:8080/jobs.html?desktop=1",
        "browserPath": "C:/Edge/msedge.exe",
    }
    values.update(overrides)
    return values


def stale_launcher_session(**overrides: object) -> dict[str, object]:
    values: dict[str, object] = {
        "launcherPid": 321,
        "launcherToken": "stale-launcher-token",
        "desktopSessionId": "stale-session",
        "desktopOwnerToken": "stale-owner",
        "launcherStartedAt": "2026-04-20T05:00:00+00:00",
        "sitePort": 8080,
        "bridgePort": 8877,
        "bridgeHost": "127.0.0.1",
        "exePath": "C:/tmp/Baluffo.exe",
        "dataDir": "C:/tmp/baluffo-ship/data",
    }
    values.update(overrides)
    return values


@contextlib.contextmanager
def _patch_windows_desktop_app(
    kernel32: object,
    *,
    format_error: str = "Access is denied.",
):
    class _FakeJobInfo:
        def __init__(self) -> None:
            self.BasicLimitInformation = SimpleNamespace(LimitFlags=0)

    fake_ctypes = SimpleNamespace(
        windll=SimpleNamespace(kernel32=kernel32),
        FormatError=mock.Mock(return_value=format_error),
        byref=lambda obj: SimpleNamespace(_obj=obj),
        sizeof=lambda _obj: 1,
        wintypes=SimpleNamespace(DWORD=lambda value=0: SimpleNamespace(value=int(value))),
    )

    with contextlib.ExitStack() as stack:
        stack.enter_context(mock.patch.object(desktop_app.os, "name", "nt"))
        stack.enter_context(mock.patch.object(desktop_app, "ctypes", fake_ctypes, create=True))
        stack.enter_context(
            mock.patch.object(desktop_app, "_PROCESS_ASSIGN_TO_JOB_ACCESS", 0x0101, create=True)
        )
        stack.enter_context(
            mock.patch.object(desktop_app, "_HANDLE_FLAG_INHERIT", 0x00000001, create=True)
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "_JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE",
                0x2000,
                create=True,
            )
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "_JOB_OBJECT_EXTENDED_LIMIT_INFORMATION_CLASS",
                9,
                create=True,
            )
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "_JOBOBJECT_EXTENDED_LIMIT_INFORMATION",
                _FakeJobInfo,
                create=True,
            )
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "_PROCESS_SYNCHRONIZE",
                0x00100000,
                create=True,
            )
        )
        stack.enter_context(
            mock.patch.object(
                desktop_app,
                "_PROCESS_QUERY_LIMITED_INFORMATION",
                0x1000,
                create=True,
            )
        )
        stack.enter_context(
            mock.patch.object(desktop_app, "_WAIT_TIMEOUT", 0x00000102, create=True)
        )
        stack.enter_context(mock.patch.object(desktop_app, "_STILL_ACTIVE", 259, create=True))
        yield fake_ctypes
