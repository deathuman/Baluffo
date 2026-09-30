from __future__ import annotations

import subprocess
import sys
from collections.abc import Callable

from scripts import install_git_hooks


def _recording_run(
    calls: list[list[str]],
    *,
    returncode: int = 0,
    stdout: str = "",
    stderr: str = "",
) -> Callable[..., subprocess.CompletedProcess[str]]:
    """A ``subprocess.run`` stand-in that records commands and returns a fixed result.

    Returns the real ``CompletedProcess`` rather than a hand-rolled stub class,
    so the fake cannot drift from the three attributes ``install_git_hooks``
    actually reads (``returncode``, ``stdout``, ``stderr``). The three tests below
    previously each carried their own copy of this function.
    """

    def fake_run(command: list[str], **_kwargs: object) -> subprocess.CompletedProcess[str]:
        calls.append(command)
        return subprocess.CompletedProcess(command, returncode, stdout, stderr)

    return fake_run


def test_install_git_hooks_fails_fast_when_mypy_is_missing(monkeypatch, capsys) -> None:
    calls: list[list[str]] = []

    monkeypatch.setattr(
        install_git_hooks.subprocess,
        "run",
        _recording_run(calls, returncode=1, stderr="No module named mypy"),
    )
    monkeypatch.delenv("CI", raising=False)

    assert install_git_hooks.main() == 1
    assert calls == [[sys.executable, "-m", "mypy", "--version"]]
    err = capsys.readouterr().err
    assert "python -m mypy --version failed" in err
    assert "Install mypy in this interpreter before setting up hooks." in err


def test_install_git_hooks_sets_core_hooks_path_after_mypy_check(monkeypatch, capsys) -> None:
    calls: list[list[str]] = []

    monkeypatch.setattr(
        install_git_hooks.subprocess, "run", _recording_run(calls, stdout="mypy 2.3.1")
    )
    monkeypatch.delenv("CI", raising=False)

    assert install_git_hooks.main() == 0
    assert calls == [
        [sys.executable, "-m", "mypy", "--version"],
        ["git", "config", "--local", "core.hooksPath", install_git_hooks.HOOKS_PATH],
    ]
    assert (
        f"Configured git core.hooksPath to {install_git_hooks.HOOKS_PATH}"
        in capsys.readouterr().out
    )


def test_install_git_hooks_skips_mypy_check_in_ci(monkeypatch, capsys) -> None:
    calls: list[list[str]] = []

    monkeypatch.setattr(install_git_hooks.subprocess, "run", _recording_run(calls))
    monkeypatch.setenv("CI", "true")

    assert install_git_hooks.main() == 0
    assert calls == [
        ["git", "config", "--local", "core.hooksPath", install_git_hooks.HOOKS_PATH],
    ]
    assert (
        f"Configured git core.hooksPath to {install_git_hooks.HOOKS_PATH}"
        in capsys.readouterr().out
    )
