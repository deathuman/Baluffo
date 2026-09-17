from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from scripts import ai_env_check


def _fake_git(responses: Mapping[tuple[str, ...], str | None], calls: list[tuple[str, ...]]):
    """Return a subprocess.run replacement keyed on the git args after ``-C <vault>``."""

    def fake_run(command: list[str], **kwargs):
        # command is ["git", "-C", str(vault), *args]
        key = tuple(command[3:])
        calls.append(key)

        class Result:
            returncode = 0
            stdout = ""

        result = Result()
        payload = responses.get(key)
        if payload is None:
            result.returncode = 1
        else:
            result.stdout = payload
        return result

    return fake_run


def _install_vault(monkeypatch, tmp_path: Path) -> Path:
    vault = tmp_path / "BaluffoMemory"
    vault.mkdir()
    monkeypatch.setenv(ai_env_check.MEMORY_VAULT_ENV_VAR, str(vault))
    return vault


def test_memory_vault_absent_skips_check(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv(ai_env_check.MEMORY_VAULT_ENV_VAR, str(tmp_path / "nope"))
    assert ai_env_check._check_memory_vault() is None


def test_memory_vault_reports_ok_when_clean_and_pushed(monkeypatch, tmp_path: Path) -> None:
    _install_vault(monkeypatch, tmp_path)
    responses = {
        ("rev-parse", "--is-inside-work-tree"): "true\n",
        ("status", "--porcelain"): "",
        ("rev-list", "--count", "origin/main..HEAD"): "0\n",
    }
    monkeypatch.setattr(ai_env_check.subprocess, "run", _fake_git(responses, []))

    check = ai_env_check._check_memory_vault()

    assert check is not None
    assert check.status == "ok"
    assert "clean and pushed" in check.detail


def test_memory_vault_warns_on_uncommitted_notes(monkeypatch, tmp_path: Path) -> None:
    """The exact drift that went unnoticed twice: notes written but never committed."""
    _install_vault(monkeypatch, tmp_path)
    responses = {
        ("rev-parse", "--is-inside-work-tree"): "true\n",
        ("status", "--porcelain"): " M baluffo/current-focus.md\n?? notes/new-note.md\n",
        ("rev-list", "--count", "origin/main..HEAD"): "0\n",
    }
    monkeypatch.setattr(ai_env_check.subprocess, "run", _fake_git(responses, []))

    check = ai_env_check._check_memory_vault()

    assert check is not None
    assert check.status == "warn"
    assert "1 uncommitted" in check.detail
    assert "1 untracked" in check.detail
    assert "commit and push before closeout" in check.detail


def test_memory_vault_warns_on_unpushed_commits(monkeypatch, tmp_path: Path) -> None:
    _install_vault(monkeypatch, tmp_path)
    responses = {
        ("rev-parse", "--is-inside-work-tree"): "true\n",
        ("status", "--porcelain"): "",
        ("rev-list", "--count", "origin/main..HEAD"): "3\n",
    }
    monkeypatch.setattr(ai_env_check.subprocess, "run", _fake_git(responses, []))

    check = ai_env_check._check_memory_vault()

    assert check is not None
    assert check.status == "warn"
    assert "3 unpushed commit(s)" in check.detail


def test_memory_vault_warns_when_not_a_git_repo(monkeypatch, tmp_path: Path) -> None:
    _install_vault(monkeypatch, tmp_path)
    monkeypatch.setattr(ai_env_check.subprocess, "run", _fake_git({}, []))

    check = ai_env_check._check_memory_vault()

    assert check is not None
    assert check.status == "warn"
    assert "not a git repo" in check.detail


def test_memory_vault_warns_when_git_is_unavailable(monkeypatch, tmp_path: Path) -> None:
    _install_vault(monkeypatch, tmp_path)

    def boom(*args, **kwargs):
        raise OSError("git missing")

    monkeypatch.setattr(ai_env_check.subprocess, "run", boom)

    check = ai_env_check._check_memory_vault()

    assert check is not None
    assert check.status == "warn"
    assert "not a git repo" in check.detail


def test_memory_vault_check_is_appended_to_the_run(monkeypatch, tmp_path: Path) -> None:
    """The check must actually run as part of the normal preflight."""
    _install_vault(monkeypatch, tmp_path)
    responses = {
        ("rev-parse", "--is-inside-work-tree"): "true\n",
        ("status", "--porcelain"): "",
        ("rev-list", "--count", "origin/main..HEAD"): "0\n",
    }
    monkeypatch.setattr(ai_env_check.subprocess, "run", _fake_git(responses, []))
    monkeypatch.setattr(
        ai_env_check, "_check_python", lambda: ai_env_check.Check("python", "ok", "x")
    )
    monkeypatch.setattr(ai_env_check, "_check_node", lambda: ai_env_check.Check("node", "ok", "x"))
    monkeypatch.setattr(ai_env_check, "_check_npm", lambda: ai_env_check.Check("npm", "ok", "x"))
    monkeypatch.setattr(
        ai_env_check, "_check_serena", lambda _u: ai_env_check.Check("serena", "ok", "x")
    )
    monkeypatch.setattr(
        ai_env_check, "_check_lockfiles", lambda: ai_env_check.Check("lockfiles", "ok", "x")
    )
    monkeypatch.setattr(
        ai_env_check, "_check_node_modules", lambda: ai_env_check.Check("node_modules", "ok", "x")
    )
    monkeypatch.setattr(
        ai_env_check, "_check_python_env", lambda: ai_env_check.Check("python_env", "ok", "x")
    )
    monkeypatch.setattr(
        ai_env_check, "_check_git_hooks", lambda: ai_env_check.Check("git_hooks", "ok", "x")
    )
    monkeypatch.setattr(
        ai_env_check, "_check_toolbelt", lambda _s: ai_env_check.Check("toolbelt", "ok", "x")
    )
    monkeypatch.setattr(
        ai_env_check, "_check_playwright", lambda: ai_env_check.Check("playwright", "ok", "x")
    )
    monkeypatch.setattr(
        ai_env_check, "_check_path_location", lambda: ai_env_check.Check("repo_path", "ok", "x")
    )

    checks = ai_env_check._checks(smoke=False, check_updates=False)

    assert "memory_vault" in [check.name for check in checks]
