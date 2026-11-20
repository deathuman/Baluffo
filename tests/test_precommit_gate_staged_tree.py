"""Changed mode must judge the index, not the working tree.

A commit contains only what was staged, so an unstaged edit must not decide
whether the commit passes. The gate used to measure the whole working tree,
which made a partial slice fail on whatever later slice happened to be sitting
in the tree -- six commits in a row needed `--no-verify` for exactly that
reason. These tests pin the staged-tree snapshot and the gate that uses it.
"""

from __future__ import annotations

import json
import subprocess
from pathlib import Path

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
