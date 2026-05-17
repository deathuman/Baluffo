from __future__ import annotations

from types import SimpleNamespace

from src.bridge import desktop_attention as attention


class FakeUser32:
    def __init__(self, windows: dict[int, dict[str, object]], foreground: int = 0) -> None:
        self.windows = windows
        self.foreground = foreground
        self.flash_calls: list[dict[str, int]] = []

    @staticmethod
    def _value(value: object) -> int:
        raw = getattr(value, "value", value)
        return int(raw or 0)

    def IsWindow(self, hwnd: object) -> bool:
        return bool(self.windows.get(self._value(hwnd), {}).get("is_window", True))

    def IsWindowVisible(self, hwnd: object) -> bool:
        return bool(self.windows.get(self._value(hwnd), {}).get("visible", True))

    def GetWindowThreadProcessId(self, hwnd: object, pid_pointer: object) -> int:
        pid = int(self.windows.get(self._value(hwnd), {}).get("pid", 0) or 0)
        pid_pointer._obj.value = pid
        return 1

    def GetForegroundWindow(self) -> int:
        return int(self.foreground)

    def FlashWindowEx(self, info_pointer: object) -> int:
        info = info_pointer._obj
        self.flash_calls.append(
            {
                "hwnd": self._value(info.hwnd),
                "flags": int(info.dwFlags),
                "timeout": int(info.dwTimeout),
            }
        )
        return 0

    def EnumWindows(self, callback: object, _lparam: int) -> bool:
        for hwnd in self.windows:
            callback(hwnd, 0)
        return True


def _runtime(**overrides: object) -> SimpleNamespace:
    values = {
        "desktop_mode": True,
        "owner_mode": "desktop-window",
        "desktop_session_id": "session-1",
    }
    values.update(overrides)
    return SimpleNamespace(**values)


def _completion(**overrides: object) -> dict[str, object]:
    values = {"runId": "pipeline_1", "durationSeconds": 120.0}
    values.update(overrides)
    return values


def _install_fake_windows(monkeypatch, user32: FakeUser32) -> None:
    monkeypatch.setattr(attention.os, "name", "nt")
    monkeypatch.setattr(attention.ctypes, "windll", SimpleNamespace(user32=user32), raising=False)


def test_pipeline_attention_noops_outside_windows(monkeypatch) -> None:
    monkeypatch.setattr(attention.os, "name", "posix")

    result = attention.notify_pipeline_completion_attention(
        runtime_config=_runtime(),
        completion=_completion(),
    )

    assert result == {"notified": False, "reason": "not_windows", "hwnd": 0}


def test_pipeline_attention_noops_when_not_desktop_owner(monkeypatch) -> None:
    _install_fake_windows(monkeypatch, FakeUser32({101: {"pid": 202}}))

    result = attention.notify_pipeline_completion_attention(
        runtime_config=_runtime(owner_mode="dev-supervisor"),
        completion=_completion(),
    )

    assert result["notified"] is False
    assert result["reason"] == "not_desktop_window_owner"


def test_pipeline_attention_noops_on_session_mismatch(monkeypatch) -> None:
    _install_fake_windows(monkeypatch, FakeUser32({101: {"pid": 202}}))
    monkeypatch.setattr(
        attention,
        "_read_desktop_session",
        lambda _env=None: {
            "desktopSessionId": "other-session",
            "windowHwnd": 101,
            "windowPid": 202,
        },
    )

    result = attention.notify_pipeline_completion_attention(
        runtime_config=_runtime(),
        completion=_completion(),
    )

    assert result["notified"] is False
    assert result["reason"] == "session_mismatch"


def test_pipeline_attention_noops_for_short_run(monkeypatch) -> None:
    _install_fake_windows(monkeypatch, FakeUser32({101: {"pid": 202}}))

    result = attention.notify_pipeline_completion_attention(
        runtime_config=_runtime(),
        completion=_completion(durationSeconds=59.9),
    )

    assert result["notified"] is False
    assert result["reason"] == "run_too_short"


def test_pipeline_attention_noops_when_window_is_foreground(monkeypatch) -> None:
    user32 = FakeUser32({101: {"pid": 202}}, foreground=101)
    _install_fake_windows(monkeypatch, user32)
    monkeypatch.setattr(
        attention,
        "_read_desktop_session",
        lambda _env=None: {
            "desktopSessionId": "session-1",
            "windowHwnd": 101,
            "windowPid": 202,
        },
    )

    result = attention.notify_pipeline_completion_attention(
        runtime_config=_runtime(),
        completion=_completion(),
    )

    assert result == {"notified": False, "reason": "foreground_window", "hwnd": 101}
    assert user32.flash_calls == []


def test_pipeline_attention_flashes_valid_saved_hwnd(monkeypatch) -> None:
    user32 = FakeUser32({101: {"pid": 202}})
    _install_fake_windows(monkeypatch, user32)
    monkeypatch.setattr(
        attention,
        "_read_desktop_session",
        lambda _env=None: {
            "desktopSessionId": "session-1",
            "windowHwnd": 101,
            "windowPid": 202,
            "browserPid": 303,
        },
    )

    result = attention.notify_pipeline_completion_attention(
        runtime_config=_runtime(),
        completion=_completion(),
    )

    assert result == {"notified": True, "reason": "notified", "hwnd": 101}
    assert user32.flash_calls == [
        {"hwnd": 101, "flags": attention.FLASHW_COMPLETION_FLAGS, "timeout": 0}
    ]


def test_pipeline_attention_rejects_reused_hwnd_and_falls_back_by_pid(monkeypatch) -> None:
    user32 = FakeUser32(
        {
            101: {"pid": 999},
            102: {"pid": 202},
        }
    )
    _install_fake_windows(monkeypatch, user32)
    monkeypatch.setattr(
        attention,
        "_read_desktop_session",
        lambda _env=None: {
            "desktopSessionId": "session-1",
            "windowHwnd": 101,
            "windowPid": 202,
            "browserPid": 303,
        },
    )

    result = attention.notify_pipeline_completion_attention(
        runtime_config=_runtime(),
        completion=_completion(),
    )

    assert result == {"notified": True, "reason": "notified", "hwnd": 102}
    assert [call["hwnd"] for call in user32.flash_calls] == [102]


def test_pipeline_attention_noops_when_session_read_fails(monkeypatch) -> None:
    _install_fake_windows(monkeypatch, FakeUser32({101: {"pid": 202}}))

    def _raise(_env=None):
        raise OSError("session unavailable")

    monkeypatch.setattr(attention, "_read_desktop_session", _raise)

    result = attention.notify_pipeline_completion_attention(
        runtime_config=_runtime(),
        completion=_completion(),
    )

    assert result["notified"] is False
    assert result["reason"] == "session_unavailable"
