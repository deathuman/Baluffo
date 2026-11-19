"""Venv interpreter rules in ``scripts/ai_env_check.py``.

The row used to be ``Check("python_env", "ok", ".venv present")`` — a directory test. This
clone's ``.venv`` is a Linux/WSL venv whose ``bin/python`` symlinks to ``/usr/bin/python3``,
which resolves to nothing on Windows, so the preflight reported a ready environment while
``.venv/bin/python`` failed to start at all. These tests pin the replacement: the interpreter
must exist as a real entry, run, and satisfy the pin in ``src/python_version_guard.py``.
"""

from __future__ import annotations

import sys
from collections.abc import Mapping
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts import ai_env_check


@pytest.fixture
def repo(tmp_path, monkeypatch):
    """Point the check at a scratch repo root so it never reads this machine's state."""
    monkeypatch.setattr(ai_env_check, "ROOT", tmp_path)
    return tmp_path


def _fake_capture(versions: Mapping[str, str | None]):
    """Return a ``_capture`` replacement keyed on the interpreter's file name."""

    def capture(*args: str, timeout: int = 15):
        # The check must ask the interpreter for its version, not start repo work.
        assert args[1] == "-c", args
        version = versions.get(Path(args[0]).name)
        if version is None:
            return None

        class Done:
            returncode = 0
            stdout = f"{version}\n"
            stderr = ""

        return Done()

    return capture


def test_missing_venv_warns(repo) -> None:
    check = ai_env_check._check_python_env()

    assert check.status == "warn"
    assert "missing" in check.detail


def test_venv_without_an_interpreter_warns(repo) -> None:
    (repo / ".venv" / "bin").mkdir(parents=True)

    check = ai_env_check._check_python_env()

    assert check.status == "warn"
    assert "no Scripts/python.exe or bin/python" in check.detail
    assert "recreate" in check.detail


def test_runnable_interpreter_at_the_pinned_version_is_ok(repo, monkeypatch) -> None:
    (repo / ".venv" / "bin").mkdir(parents=True)
    (repo / ".venv" / "bin" / "python").touch()
    monkeypatch.setattr(ai_env_check, "_capture", _fake_capture({"python": "3.13.7"}))

    check = ai_env_check._check_python_env()

    assert check.status == "ok"
    assert "3.13.7" in check.detail
    assert "bin/python" in check.detail


def test_windows_scripts_layout_is_accepted(repo, monkeypatch) -> None:
    (repo / ".venv" / "Scripts").mkdir(parents=True)
    (repo / ".venv" / "Scripts" / "python.exe").touch()
    monkeypatch.setattr(ai_env_check, "_capture", _fake_capture({"python.exe": "3.14.0"}))

    check = ai_env_check._check_python_env()

    assert check.status == "ok"
    assert "Scripts/python.exe" in check.detail


def test_interpreter_below_the_pinned_version_warns(repo, monkeypatch) -> None:
    (repo / ".venv" / "bin").mkdir(parents=True)
    (repo / ".venv" / "bin" / "python").touch()
    monkeypatch.setattr(ai_env_check, "_capture", _fake_capture({"python": "3.12.4"}))

    check = ai_env_check._check_python_env()

    assert check.status == "warn"
    assert "3.12.4" in check.detail
    assert f"{ai_env_check.MIN_VERSION[0]}.{ai_env_check.MIN_VERSION[1]}+" in check.detail


def test_interpreter_that_cannot_start_is_not_green(repo, monkeypatch) -> None:
    """The exact false green: the entry exists but no process on this OS can run it."""
    (repo / ".venv" / "bin").mkdir(parents=True)
    (repo / ".venv" / "bin" / "python").touch()
    monkeypatch.setattr(ai_env_check, "_capture", _fake_capture({}))

    check = ai_env_check._check_python_env()

    assert check.status != "ok"
    assert "bin/python is not runnable here" in check.detail
