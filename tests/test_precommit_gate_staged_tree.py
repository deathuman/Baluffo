"""Changed mode must judge the index, not the working tree.

A commit contains only what was staged, so an unstaged edit must not decide
whether the commit passes. The gate used to measure the whole working tree,
which made a partial slice fail on whatever later slice happened to be sitting
in the tree -- six commits in a row needed `--no-verify` for exactly that
reason. These tests pin the staged-tree snapshot and the gate that uses it.
"""

from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

from scripts import precommit_gate
from tools.repo_health import loc_budget


def _run(repo: Path, *args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(  # noqa: S603
        ["git", *args],
        cwd=repo,
        capture_output=True,
        text=True,
        check=False,
    )


def _init_repo(tmp_path: Path) -> Path:
    repo = tmp_path / "repo"
    repo.mkdir()
    assert _run(repo, "init", "-q", "-b", "main").returncode == 0
    _run(repo, "config", "user.email", "test@example.com")
    _run(repo, "config", "user.name", "Test Runner")
    return repo


def _write(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def _commit_all(repo: Path, message: str) -> None:
    _run(repo, "add", "-A")
    completed = _run(repo, "commit", "-q", "-m", message)
    assert completed.returncode == 0, completed.stderr


def _baseline(repo: Path, src_lines: int) -> None:
    _write(
        repo / "tools" / "repo_health" / "loc_baseline.json",
        json.dumps({"areas": {"src": src_lines}}, indent=2) + "\n",
    )


def _partial_slice_with_a_later_slice_unstaged(tmp_path: Path) -> Path:
    """A base commit, slice A staged, and slice B only in the working tree."""
    repo = _init_repo(tmp_path)
    _write(repo / "src" / "app.py", "a = 1\nb = 2\nc = 3\n")
    _baseline(repo, 3)
    _commit_all(repo, "base")

    _write(repo / "src" / "app.py", "a = 1\nb = 2\nc = 3\nd = 4\n")
    _baseline(repo, 4)
    _run(repo, "add", "-A")

    # Slice B is being written but its ratchet is not updated yet. That is the
    # normal work-in-progress state, and exactly what the old working-tree
    # measurement reported as growth on a commit that carried none of it.
    _write(repo / "src" / "app.py", "a = 1\nb = 2\nc = 3\nd = 4\ne = 5\nf = 6\n")
    return repo


def test_partial_slice_passes_while_a_later_slice_is_unstaged(tmp_path: Path) -> None:
    """The LOC ratchet must judge the staged lines, not the working tree."""
    repo = _partial_slice_with_a_later_slice_unstaged(tmp_path)

    assert loc_budget.check_loc_budget(repo), "the working tree is what used to be measured"

    with precommit_gate.staged_tree(repo) as tree:
        staged_app = (tree / "src" / "app.py").read_text(encoding="utf-8")
        assert staged_app.count("\n") == 4
        assert loc_budget.check_loc_budget(tree) == []

    # The later slice is still in the working tree, untouched.
    assert "f = 6" in (repo / "src" / "app.py").read_text(encoding="utf-8")


def test_staged_tree_is_removed_after_use(tmp_path: Path) -> None:
    repo = _partial_slice_with_a_later_slice_unstaged(tmp_path)

    with precommit_gate.staged_tree(repo) as tree:
        assert tree.is_dir()

    assert not tree.exists()
    assert _run(repo, "worktree", "list", "--porcelain").stdout.count("worktree ") == 1


def test_node_modules_link_never_removes_the_repo_modules(tmp_path: Path) -> None:
    """eslint needs the repo's modules inside the snapshot; teardown must unlink only."""
    repo = _init_repo(tmp_path)
    _write(repo / ".gitignore", "node_modules/\n")
    _write(repo / "node_modules" / "eslint" / "bin" / "eslint.js", "// eslint\n")
    _write(repo / "src" / "app.py", "a = 1\n")
    _commit_all(repo, "base")
    _write(repo / "src" / "app.py", "a = 2\n")
    _run(repo, "add", "-A")

    with precommit_gate.staged_tree(repo) as tree:
        assert (tree / "node_modules" / "eslint" / "bin" / "eslint.js").is_file()

    assert (repo / "node_modules" / "eslint" / "bin" / "eslint.js").is_file()


def test_node_modules_junction_fallback_never_removes_the_repo_modules(
    tmp_path, monkeypatch
) -> None:
    """Without symlink privilege the gate falls back to a Windows junction.

    `mklink /J` works for an ordinary user where a directory symlink needs
    Developer Mode or elevation. The fallback must still expose eslint inside
    the snapshot, and teardown must remove the link rather than the repo's
    modules it points at. On POSIX the command is simulated with a symlink.
    `_unlink_node_modules` is exercised directly and its teardown call is
    spied on, so `git worktree remove` cannot hide a teardown that failed to
    unlink the snapshot entry.
    """
    repo = _init_repo(tmp_path)
    _write(repo / ".gitignore", "node_modules/\n")
    _write(repo / "node_modules" / "eslint" / "bin" / "eslint.js", "// eslint\n")
    _write(repo / "src" / "app.py", "a = 1\n")
    _commit_all(repo, "base")
    _write(repo / "src" / "app.py", "a = 2\n")
    _run(repo, "add", "-A")

    def symlink_denied(self, target, target_is_directory=False):
        raise OSError("A required privilege is not held by the client")

    monkeypatch.setattr(Path, "symlink_to", symlink_denied)
    if os.name != "nt":
        # The junction fallback is Windows-only. Present that platform to the
        # gate and stand in for `mklink` with a symlink. The proxy delegates
        # every other os attribute the gate uses.
        class _WindowsPlatform:
            name = "nt"

            def __getattr__(self, attribute):
                return getattr(os, attribute)

        monkeypatch.setattr(precommit_gate, "os", _WindowsPlatform())

    real_run = subprocess.run
    mklink_commands: list[list[str]] = []

    def run_or_mklink(command, *args, **kwargs):
        if command[:3] == ["cmd", "/c", "mklink"]:
            mklink_commands.append(command)
            if os.name != "nt":
                os.symlink(command[-1], command[-2], target_is_directory=True)
                return subprocess.CompletedProcess(command, 0)
        return real_run(command, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", run_or_mklink)

    # The direct call below bypasses the spy and proves the helper works. The
    # spy therefore records only `staged_tree`'s teardown; without it,
    # `git worktree remove`/`shutil.rmtree` could hide a teardown that never
    # called `_unlink_node_modules`.
    teardown_unlinks: list[Path] = []
    real_unlink = precommit_gate._unlink_node_modules

    def spy_unlink(tree_path: Path) -> None:
        teardown_unlinks.append(tree_path)
        real_unlink(tree_path)

    monkeypatch.setattr(precommit_gate, "_unlink_node_modules", spy_unlink)

    with precommit_gate.staged_tree(repo) as tree:
        link = tree / "node_modules"
        repo_module = repo / "node_modules" / "eslint" / "bin" / "eslint.js"
        assert mklink_commands, "the denied symlink must fall back to a junction"
        assert mklink_commands[0][3:] == ["/J", str(link), str(repo / "node_modules")]
        if os.name == "nt":
            assert link.is_dir() and not link.is_symlink()
        else:
            assert link.is_symlink()
        assert (link / "eslint" / "bin" / "eslint.js").is_file()
        assert repo_module.is_file()

        # Exercise the link teardown directly, bypassing the spy. `git worktree
        # remove` (and the shutil.rmtree fallback) can delete a symlink or
        # junction while leaving the repo modules intact, which would hide a
        # broken unlink path.
        real_unlink(tree)
        assert not link.exists()
        assert not link.is_symlink()
        assert repo_module.is_file()

    assert tree in teardown_unlinks, "staged_tree teardown must call _unlink_node_modules"
    assert not tree.exists()
    assert repo_module.is_file()
    assert repo_module.read_text(encoding="utf-8") == "// eslint\n"


@pytest.mark.skipif(os.name != "nt", reason="directory junctions are Windows-only")
def test_unlink_node_modules_rmdir_fallback_removes_the_junction_only(
    tmp_path, monkeypatch
) -> None:
    """When unlink fails on a junction, rmdir must remove the link, not the target."""
    repo_modules = tmp_path / "repo" / "node_modules"
    eslint = repo_modules / "eslint" / "bin" / "eslint.js"
    eslint.parent.mkdir(parents=True)
    eslint.write_text("// eslint\n", encoding="utf-8")
    tree = tmp_path / "tree"
    tree.mkdir()
    link = tree / "node_modules"
    created = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(repo_modules)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert created.returncode == 0, created.stderr or created.stdout
    assert os.path.isjunction(link)

    real_unlink = Path.unlink
    real_rmdir = Path.rmdir
    unlink_attempts: list[Path] = []
    rmdir_attempts: list[Path] = []

    def unlink_denied(self, *args, **kwargs):
        if self == link:
            unlink_attempts.append(self)
            raise OSError("directory junction cannot be unlinked")
        return real_unlink(self, *args, **kwargs)

    def rmdir_recording(self, *args, **kwargs):
        rmdir_attempts.append(self)
        return real_rmdir(self, *args, **kwargs)

    monkeypatch.setattr(Path, "unlink", unlink_denied)
    monkeypatch.setattr(Path, "rmdir", rmdir_recording)

    precommit_gate._unlink_node_modules(tree)

    assert link in unlink_attempts, "the junction unlink must be attempted"
    assert link in rmdir_attempts, "unlink failure must fall back to rmdir"
    assert not os.path.lexists(link)
    assert eslint.is_file()
    assert eslint.read_text(encoding="utf-8") == "// eslint\n"


def test_staged_tree_is_removed_when_node_modules_cannot_be_exposed(tmp_path, monkeypatch) -> None:
    """A denied link must not leak the worktree or the snapshot directory."""
    repo = _partial_slice_with_a_later_slice_unstaged(tmp_path)

    def denied(tree, root):
        raise OSError("symlink privilege not held")

    monkeypatch.setattr(precommit_gate, "_link_node_modules", denied)

    with pytest.raises(RuntimeError, match="node_modules"):
        with precommit_gate.staged_tree(repo):
            pass

    assert _run(repo, "worktree", "list", "--porcelain").stdout.count("worktree ") == 1
    tree_root = repo / precommit_gate.STAGED_TREE_DIR
    assert not tree_root.exists() or not any(tree_root.iterdir())


def test_run_changed_validates_the_staged_tree(tmp_path, monkeypatch) -> None:
    """Every stage runs in the snapshot, on the staged file list only."""
    repo = _partial_slice_with_a_later_slice_unstaged(tmp_path)
    _write(repo / "tests" / "later_test.py", "later\n")  # untracked, never staged

    seen: list[tuple[list[str], Path | None, dict[str, str] | None]] = []

    def fake_precommit(command, *, cwd=None, env=None):
        seen.append((command, cwd, env))
        return 0

    def fake_guardrails(groups=(), *, pending_container_version_gate=False, cwd=None, env=None):
        seen.append(([], cwd, None))
        return 0

    def fake_complexity(*, cwd=None, env=None):
        seen.append(([], cwd, None))
        return 0

    monkeypatch.setattr(precommit_gate, "ROOT", repo)
    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_precommit)
    monkeypatch.setattr(precommit_gate, "run_repo_guardrails", fake_guardrails)
    monkeypatch.setattr(precommit_gate, "run_complexity_baseline", fake_complexity)

    assert precommit_gate.run_changed() == 0

    roots = {cwd for _, cwd, _ in seen}
    assert len(roots) == 1, "every stage must validate the same tree"
    tree = roots.pop()
    assert tree is not None and tree != repo

    files = seen[0][0][seen[0][0].index("--files") + 1 :]
    assert files == ["src/app.py", "tools/repo_health/loc_baseline.json"]
    env = seen[0][2]
    assert env is not None
    # mypy scans the whole tree; the snapshot reuses the repo's module-keyed cache.
    assert env["MYPY_CACHE_DIR"] == str(repo / ".mypy_cache")
    assert not tree.exists()


def test_run_changed_keeps_the_snapshot_under_the_hook_index_environment(
    tmp_path, monkeypatch, capsys
) -> None:
    """`git commit` exports GIT_INDEX_FILE; the snapshot must not inherit it.

    The path is relative to the repo root, so a new worktree cannot create its
    own index lock from it, and a `read-tree` that inherited it would write the
    parent's index. The gate used to fall back to the working tree here, which
    is the isolation it exists to provide.
    """
    repo = _partial_slice_with_a_later_slice_unstaged(tmp_path)
    seen: list[tuple[dict[str, str] | None, str]] = []

    def fake_precommit(command, *, cwd=None, env=None):
        seen.append((env, (cwd / "src" / "app.py").read_text(encoding="utf-8")))
        return 0

    monkeypatch.setattr(precommit_gate, "ROOT", repo)
    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_precommit)
    monkeypatch.setattr(precommit_gate, "run_repo_guardrails", lambda *args, **kwargs: 0)
    monkeypatch.setattr(precommit_gate, "run_complexity_baseline", lambda *args, **kwargs: 0)
    monkeypatch.chdir(repo)
    monkeypatch.setenv("GIT_INDEX_FILE", ".git/index")

    assert precommit_gate.run_changed() == 0

    out = capsys.readouterr().out
    assert "validating the staged tree" in out
    assert "staged tree unavailable" not in out
    env, staged_app = seen[0]
    assert env is not None and "GIT_INDEX_FILE" not in env
    assert staged_app.count("\n") == 4


def test_hook_fixes_reach_the_working_tree_but_never_unstaged_edits(tmp_path, monkeypatch) -> None:
    """A fixer edits the snapshot; carry it back only where nothing is pending."""
    repo = _init_repo(tmp_path)
    _write(repo / "src" / "clean.py", "z=1\n")
    _write(repo / "src" / "edited.py", "e=1\n")
    _commit_all(repo, "base")
    _write(repo / "src" / "clean.py", "z=2\n")
    _write(repo / "src" / "edited.py", "e=2\n")
    _run(repo, "add", "src/clean.py", "src/edited.py")
    # The developer already has an unstaged edit on top of the staged version.
    _write(repo / "src" / "edited.py", "e=3\n")

    def fake_precommit(command, *, cwd=None, env=None):
        _write(cwd / "src" / "clean.py", "z = 2\n")
        _write(cwd / "src" / "edited.py", "e = 2\n")
        return 1

    monkeypatch.setattr(precommit_gate, "ROOT", repo)
    monkeypatch.setattr(precommit_gate, "_run_precommit_command", fake_precommit)
    monkeypatch.setattr(precommit_gate, "should_run_repo_guardrails", lambda files: False)
    monkeypatch.setattr(precommit_gate, "has_staged_changes", lambda: False)
    monkeypatch.setattr(precommit_gate, "run_repo_guardrails", lambda *args, **kwargs: 0)
    monkeypatch.setattr(precommit_gate, "run_complexity_baseline", lambda **kwargs: 0)

    assert precommit_gate.run_changed() == 1

    assert (repo / "src" / "clean.py").read_text(encoding="utf-8") == "z = 2\n"
    assert (repo / "src" / "edited.py").read_text(encoding="utf-8") == "e=3\n"
