"""Cleanup paths for the staged-tree gate that the happy-path tests miss."""

from __future__ import annotations

import os
import shutil
import subprocess
import time
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


def _registered_worktrees(repo: Path) -> set[str]:
    listed = _run(repo, "worktree", "list", "--porcelain").stdout.splitlines()
    return {
        precommit_gate._normalized_path(Path(line.split(" ", 1)[1]))
        for line in listed
        if line.startswith("worktree ")
    }


def test_remove_staged_tree_falls_back_when_git_remove_leaves_the_tree(
    tmp_path, monkeypatch
) -> None:
    """A successful `git worktree remove` that leaves the tree must not orphan it.

    `git worktree remove` can report success while a junction or locked file
    keeps the directory behind. `_remove_staged_tree` must then remove the
    directory itself, drop that checkout's worktree metadata, and leave the
    repo's `node_modules` target untouched.
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
    assert rmtree_calls[0] == tree
    # The shared prune keeps the sweep's grace window, so the fallback drops
    # this checkout's admin entry itself.
    assert len(rmtree_calls) == 2
    assert rmtree_calls[1].parent == repo / ".git" / "worktrees"
    assert len(prune_calls) == 1
    assert not tree.exists()
    assert _worktree_count(repo) == 1
    assert repo_module.read_text(encoding="utf-8") == "// eslint\n"


def test_stale_staged_trees_are_swept_and_a_fresh_one_is_kept(tmp_path, monkeypatch) -> None:
    """A killed gate's worktree goes; a concurrent gate's fresh tree stays.

    Every run gets its own uuid, so an old tree can never be reused: it is only
    disk and `git worktree list` noise. The fresh tree stands in for the gate
    that created it seconds ago.
    """
    repo = _partial_slice_with_a_later_slice_unstaged(tmp_path)
    stale = precommit_gate._create_staged_tree(repo)
    fresh = precommit_gate._create_staged_tree(repo)
    old = time.time() - precommit_gate.STALE_STAGED_TREE_AGE_S - 60
    os.utime(stale, (old, old))

    prune_calls: list[list[str]] = []
    real_run = subprocess.run

    def run_recording_prune(command, *args, **kwargs):
        if command[3:5] == ["worktree", "prune"]:
            prune_calls.append(list(command))
        return real_run(command, *args, **kwargs)

    monkeypatch.setattr(subprocess, "run", run_recording_prune)

    try:
        removed = precommit_gate._sweep_stale_staged_trees(repo)

        assert removed == [stale]
        assert not stale.exists()
        assert fresh.is_dir()
        assert _worktree_count(repo) == 2
        assert len(prune_calls) == 1, "a sweep prunes once, not once per swept tree"
        assert any(part.startswith("--expire=") for part in prune_calls[0])
    finally:
        precommit_gate._remove_staged_tree(repo, fresh)


def test_sweep_removes_an_unregistered_stale_directory_and_keeps_a_recent_one(
    tmp_path,
) -> None:
    """A crash can leave a checkout git no longer lists; the sweep still removes it."""
    repo = _init_repo(tmp_path)
    _write(repo / "src" / "app.py", "a = 1\n")
    _commit_all(repo, "base")
    base = repo / precommit_gate.STAGED_TREE_DIR
    stale = base / "crashed"
    recent = base / "concurrent"
    _write(stale / "file.txt", "leftover\n")
    _write(recent / "file.txt", "in use\n")
    old = time.time() - precommit_gate.STALE_STAGED_TREE_AGE_S - 60
    os.utime(stale, (old, old))

    assert precommit_gate._sweep_stale_staged_trees(repo) == [stale]
    assert not stale.exists()
    assert recent.is_dir()


def test_sweep_does_not_count_a_tree_it_could_not_remove(tmp_path, monkeypatch) -> None:
    """A locked tree is reported by the remover, never counted as swept."""
    repo = _init_repo(tmp_path)
    _write(repo / "src" / "app.py", "a = 1\n")
    _commit_all(repo, "base")
    stale = repo / precommit_gate.STAGED_TREE_DIR / "crashed"
    _write(stale / "file.txt", "leftover\n")
    old = time.time() - precommit_gate.STALE_STAGED_TREE_AGE_S - 60
    os.utime(stale, (old, old))
    monkeypatch.setattr(precommit_gate, "_remove_staged_tree", lambda root, tree, **_kwargs: None)

    assert precommit_gate._sweep_stale_staged_trees(repo) == []
    assert stale.is_dir()


def test_validation_tree_sweeps_stale_trees_before_creating_its_own(tmp_path, monkeypatch) -> None:
    """Gate start is the sweep point, before the new worktree exists."""
    repo = _init_repo(tmp_path)
    _write(repo / "src" / "app.py", "a = 1\n")
    _commit_all(repo, "base")
    stale = repo / precommit_gate.STAGED_TREE_DIR / "crashed"
    _write(stale / "file.txt", "leftover\n")
    old = time.time() - precommit_gate.STALE_STAGED_TREE_AGE_S - 60
    os.utime(stale, (old, old))
    monkeypatch.setattr(precommit_gate, "ROOT", repo)

    with precommit_gate.validation_tree() as tree:
        assert not stale.exists()
        assert tree.is_dir()

    assert not tree.exists()
    assert _worktree_count(repo) == 1


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


def test_fallback_cleanup_leaves_an_unrelated_missing_worktree_registered(
    tmp_path, monkeypatch
) -> None:
    """A failed `worktree remove` must forget only its own checkout.

    A zero-grace prune is repo-wide, so it deregistered an unrelated worktree
    whose directory another tool had only just removed.
    """
    repo = _partial_slice_with_a_later_slice_unstaged(tmp_path)
    unrelated = tmp_path / "unrelated-checkout"
    added = _run(repo, "worktree", "add", "--detach", "--quiet", str(unrelated), "HEAD")
    assert added.returncode == 0, added.stderr
    shutil.rmtree(unrelated)

    prune_calls: list[list[str]] = []
    real_run = subprocess.run

    def fail_worktree_remove(command, *args, **kwargs):
        if command[3:6] == ["worktree", "remove", "--force"]:
            return subprocess.CompletedProcess(command, 1, "", "remove failed")
        if command[3:5] == ["worktree", "prune"]:
            prune_calls.append(list(command))
        return real_run(command, *args, **kwargs)

    with precommit_gate.staged_tree(repo) as tree:
        monkeypatch.setattr(subprocess, "run", fail_worktree_remove)

    assert not tree.exists()
    assert _registered_worktrees(repo) == {
        precommit_gate._normalized_path(repo),
        precommit_gate._normalized_path(unrelated),
    }
    assert prune_calls, "the fallback must still prune worktree metadata"
    window = f"--expire={precommit_gate.STALE_STAGED_TREE_AGE_S}.seconds.ago"
    assert all(window in command for command in prune_calls)
