"""Cleanup paths for the staged-tree gate that the happy-path tests miss."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

from scripts import precommit_gate
from tests.test_precommit_gate_staged_tree import (
    _commit_all,
    _init_repo,
    _partial_slice_with_a_later_slice_unstaged,
    _run,
    _write,
)


def _worktree_count(repo: Path) -> int:
    return _run(repo, "worktree", "list", "--porcelain").stdout.count("worktree ")


def test_remove_staged_tree_falls_back_when_git_remove_leaves_the_tree(
    tmp_path, monkeypatch
) -> None:
    """A successful `git worktree remove` that leaves the tree must not orphan it.

    `git worktree remove` can report success while a junction or locked file
    keeps the directory behind. `_remove_staged_tree` must then remove the
    directory itself, prune the worktree metadata, and leave the repo's
    `node_modules` target untouched.
    """
    repo = _init_repo(tmp_path)
    _write(repo / ".gitignore", "node_modules/\n")
    repo_module = repo / "node_modules" / "eslint" / "bin" / "eslint.js"
    _write(repo_module, "// eslint\n")
    _write(repo / "src" / "app.py", "a = 1\n")
    _commit_all(repo, "base")
    _write(repo / "src" / "app.py", "a = 2\n")
    _run(repo, "add", "-A")

    worktree_remove_calls: list[Path] = []
    prune_calls: list[list[str]] = []
    rmtree_calls: list[Path] = []

    real_run = subprocess.run
    real_rmtree = precommit_gate.shutil.rmtree

    def run_or_leave_tree(command, *args, **kwargs):
        if command[3:6] == ["worktree", "remove", "--force"]:
            worktree_remove_calls.append(Path(command[-1]))
            return subprocess.CompletedProcess(command, 0, "", "")
        if command[3:5] == ["worktree", "prune"]:
            prune_calls.append(command)
        return real_run(command, *args, **kwargs)

    def rmtree_recording(path, *args, **kwargs):
        rmtree_calls.append(Path(path))
        return real_rmtree(path, *args, **kwargs)

    with precommit_gate.staged_tree(repo) as tree:
        assert (tree / "node_modules" / "eslint" / "bin" / "eslint.js").is_file()
        monkeypatch.setattr(subprocess, "run", run_or_leave_tree)
        monkeypatch.setattr(precommit_gate.shutil, "rmtree", rmtree_recording)

    assert worktree_remove_calls == [tree]
    assert rmtree_calls == [tree]
    assert len(prune_calls) == 1
    assert not tree.exists()
    assert _worktree_count(repo) == 1
    assert repo_module.read_text(encoding="utf-8") == "// eslint\n"


def test_read_tree_failure_after_worktree_add_cleans_up_the_staged_tree(
    tmp_path, monkeypatch
) -> None:
    """A failed `git read-tree` must remove the worktree it was registered in."""
    repo = _partial_slice_with_a_later_slice_unstaged(tmp_path)
    failed_tree: Path | None = None
    real_run = subprocess.run

    def fail_read_tree(command, *args, **kwargs):
        nonlocal failed_tree
        if command[3:5] == ["read-tree", "--reset"]:
            failed_tree = Path(command[2])
            assert failed_tree.is_dir(), "the worktree must be registered before read-tree"
            assert _worktree_count(repo) == 2
            return subprocess.CompletedProcess(command, 1, "", "read-tree failed")
        return real_run(command, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", fail_read_tree)

    with pytest.raises(RuntimeError, match="read-tree failed"):
        precommit_gate._create_staged_tree(repo)

    assert failed_tree is not None
    assert not failed_tree.exists()
    assert _worktree_count(repo) == 1
