from __future__ import annotations

import json
from unittest import mock

import pytest

from src.ship import runtime_launcher


def test_runtime_launcher_main_reports_expected_site_startup_failure(capsys) -> None:
    with mock.patch.object(
        runtime_launcher,
        "run_site_server",
        side_effect=RuntimeError("active version directory missing"),
    ):
        exit_code = runtime_launcher.main(["site", "--port", "0"])

    assert exit_code == 1
    payload = json.loads(capsys.readouterr().out)
    assert payload == {"ok": False, "error": "active version directory missing"}


def test_runtime_launcher_main_does_not_swallow_unexpected_site_bug() -> None:
    with mock.patch.object(
        runtime_launcher,
        "run_site_server",
        side_effect=AssertionError("unexpected site bug"),
    ):
        with pytest.raises(AssertionError, match="unexpected site bug"):
            runtime_launcher.main(["site", "--port", "0"])


def test_runtime_launcher_main_preserves_keyboard_interrupt_exit_code() -> None:
    with mock.patch.object(runtime_launcher, "run_bridge_server", side_effect=KeyboardInterrupt):
        assert runtime_launcher.main(["bridge", "--port", "0"]) == 0
