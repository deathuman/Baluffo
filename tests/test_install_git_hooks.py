from __future__ import annotations

import sys

from scripts import install_git_hooks


def test_install_git_hooks_fails_fast_when_mypy_is_missing(monkeypatch, capsys) -> None:
    calls: list[list[str]] = []

    def fake_run(
        command: list[str],
        cwd=None,
        check=False,
        capture_output=False,
        text=False,
    ):  # type: ignore[no-untyped-def]
        calls.append(command)

        class Result:
            returncode = 1
            stdout = ""
            stderr = "No module named mypy"

        return Result()

    monkeypatch.setattr(install_git_hooks.subprocess, "run", fake_run)

    assert install_git_hooks.main() == 1
    assert calls == [[sys.executable, "-m", "mypy", "--version"]]
    err = capsys.readouterr().err
    assert "python -m mypy --version failed" in err
    assert "Install mypy in this interpreter before setting up hooks." in err


def test_install_git_hooks_sets_core_hooks_path_after_mypy_check(monkeypatch, capsys) -> None:
    calls: list[list[str]] = []

    def fake_run(
        command: list[str],
        cwd=None,
        check=False,
        capture_output=False,
        text=False,
    ):  # type: ignore[no-untyped-def]
        calls.append(command)

        class Result:
            returncode = 0
            stdout = "mypy 1.20.2"
            stderr = ""

        return Result()

    monkeypatch.setattr(install_git_hooks.subprocess, "run", fake_run)

    assert install_git_hooks.main() == 0
    assert calls == [
        [sys.executable, "-m", "mypy", "--version"],
        ["git", "config", "--local", "core.hooksPath", install_git_hooks.HOOKS_PATH],
    ]
    assert (
        f"Configured git core.hooksPath to {install_git_hooks.HOOKS_PATH}"
        in capsys.readouterr().out
    )
