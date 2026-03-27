"""Tests for small admin_bridge utility helpers."""

from src import admin_bridge


def test_parse_iso_handles_supported_and_invalid_inputs() -> None:
    cases = [
        (None, None),
        ("", None),
        ("   ", None),
        ("not-a-date", None),
        ("2026-03", None),
    ]
    for value, expected in cases:
        assert admin_bridge.parse_iso(value) is expected

    result = admin_bridge.parse_iso("2026-03-21T12:00:00")
    assert result is not None
    assert (result.year, result.month, result.day, result.hour) == (2026, 3, 21, 12)
    timezone_result = admin_bridge.parse_iso("2026-03-21T12:00:00+00:00")
    z_result = admin_bridge.parse_iso("2026-03-21T12:00:00Z")
    assert timezone_result is not None and timezone_result.tzinfo is not None
    assert z_result is not None and z_result.tzinfo is not None


def test_pid_is_running_rejects_non_positive_and_missing_pids() -> None:
    for pid in (0, -1, -100, None):
        assert admin_bridge.pid_is_running(pid) is False


def test_log_enabled_with_default_config(admin_bridge_entrypoint_root) -> None:
    assert isinstance(admin_bridge._log_enabled("info"), bool)
