#!/usr/bin/env python3
"""Run pre-commit against the staged tree, or against the whole repository.

Changed mode validates the **index**, not the working tree. A commit contains
only what was staged, so an unstaged edit is not part of it; measuring the
working tree instead made a partial slice fail on whatever later slice happened
to be sitting in the tree, which is how six commits in a row ended up needing
`--no-verify`. The staged tree is materialised into a throwaway worktree and
every stage runs there.
"""

from __future__ import annotations

import argparse
import contextlib
import os
import shutil
import subprocess
import sys
import time
import uuid
from collections.abc import Callable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
# The gate is executed as a script (`python scripts/precommit_gate.py`), so only
# `scripts/` is on sys.path. The guardrail path filter imports the container
# policy from `tools/repo_health/`, which needs the repository root.
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
PYTHON = sys.executable


def _default_pre_commit_home() -> Path:
    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        return Path(local_app_data) / "pre-commit"
    return Path.home() / "AppData" / "Local" / "pre-commit"


PRE_COMMIT_HOME = _default_pre_commit_home()
# Tracked runtime artifacts that the app rewrites during normal local use. They are
# canonical files (`git ls-files data/`), so they are never deleted, but their working-tree
# churn must not gate a commit: CI runs the same gate with `--exclude-root data`, and
# without this list the local changed-mode gate fails on `end-of-file-fixer` diffs in
# files the developer never intended to touch.
#
# Each entry is written at runtime by src/. Keep this in sync with the writers when a new
# runtime artifact is tracked; `workflow_policy.test_runtime_written_data_files_are_excluded`
# fails when a tracked data file is written by src/ but omitted here.
EXCLUDED_FILES = {
    "data/desktop-startup-metrics.jsonl",
    "data/jobs-fetch-report.json",
    "data/jobs-fetch-tasks.json",
    "data/jobs-lifecycle-state.json",
    "data/jobs-source-state.json",
    "data/jobs-success-cache.json",
    "data/source-discovery-candidates.json",
    "data/source-discovery-report.json",
    "data/source-registry-tombstones.json.gz",
}
EXCLUDED_ROOT_PREFIXES = (
    ".pre-commit-home",
    ".tmp",
    ".pytest",
    ".mypy_cache",
    ".ruff_cache",
    ".venv",
    "pytest-cache-files-",
    "_out",
    "build",
    "dist",
    "tmp",
)
MAX_PRECOMMIT_FILES_PER_COMMAND = 200
# Where changed mode materialises the index. `.tmp` is ignored and excluded from
# tracked-file discovery, so the snapshot can live inside the repository and
# still resolve the repo's `node_modules` by walking up from it.
STAGED_TREE_DIR = Path(".tmp") / "precommit-staged-tree"
# Age, in seconds, past which a staged tree cannot plausibly belong to a running
# gate: changed mode takes about 35s and the all-files pass a few minutes, so a
# tree this old is what a killed or hung run left behind. The window is wide on
# purpose -- sweeping a live tree breaks the gate that owns it.
STALE_STAGED_TREE_AGE_S = 3600
# Variables git exports to a commit hook to point at the repository being
# committed. They resolve relative to the repo root, so inside the snapshot they
# name paths that do not exist (`.git` is a file in a worktree) -- and a
# `read-tree` that inherited one would write the parent's index.
_GIT_LOCATION_VARS = ("GIT_DIR", "GIT_WORK_TREE", "GIT_COMMON_DIR", "GIT_INDEX_FILE", "GIT_PREFIX")


def _env_without_git_location() -> dict[str, str]:
    return {name: value for name, value in os.environ.items() if name not in _GIT_LOCATION_VARS}


def _snapshot_env() -> dict[str, str]:
    """Environment for a stage that runs in the staged tree.

    The snapshot's own index *is* the staged tree, so the hook's `GIT_INDEX_FILE`
    is dropped rather than translated. `MYPY_CACHE_DIR` keeps mypy warm: its
    cache is keyed by module, not by checkout path (1.5s against 22.6s cold),
    and a seeded type error is still reported.
    """
    env = _env_without_git_location()
    env["MYPY_CACHE_DIR"] = str(ROOT / ".mypy_cache")
    return env


def _git_at(root: Path, *args: str, env: Mapping[str, str] | None = None) -> list[str]:
    """Run git in ``root`` and return its non-empty output lines."""
    completed = subprocess.run(
        ["git", "-C", str(root), *args],
        cwd=root,
        env=dict(env) if env is not None else None,
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        stderr = completed.stderr.strip()
        stdout = completed.stdout.strip()
        details = stderr or stdout or "no git output"
        raise RuntimeError(
            f"git {' '.join(args)} failed with exit code {completed.returncode}: {details}"
        )
    return [line.strip() for line in completed.stdout.splitlines() if line.strip()]


def _git_lines(*args: str) -> list[str]:
    return _git_at(ROOT, *args)


def _top_level_component(rel_path: str) -> str:
    parts = Path(rel_path).parts
    return parts[0] if parts else rel_path


def _is_excluded_root(rel_path: str, extra_roots: tuple[str, ...] = ()) -> bool:
    if rel_path in EXCLUDED_FILES:
        return True
    root_name = _top_level_component(rel_path)
    if any(root_name.startswith(prefix) for prefix in EXCLUDED_ROOT_PREFIXES):
        return True
    normalized_roots = tuple(prefix.strip().strip("/\\") for prefix in extra_roots)
    return any(root_name == prefix for prefix in normalized_roots if prefix)


def collect_staged_files() -> list[str]:
    """Return the files the next commit would add, modify, or rename.

    Only the index is read. An unstaged edit is not part of the commit, so it
    must not decide whether the commit passes: a partial slice has to be
    committable while the next slice is still being written in the same files.
    """
    files: list[str] = []
    seen: set[str] = set()
    for rel_path in _git_lines("diff", "--cached", "--name-only", "--diff-filter=ACMRTUXB"):
        if rel_path in seen or _is_excluded_root(rel_path):
            continue
        seen.add(rel_path)
        files.append(rel_path)
    return files


def collect_repo_files(exclude_roots: tuple[str, ...] = ()) -> list[str]:
    """Return tracked repository files, optionally excluding selected top-level roots."""
    files: list[str] = []
    for rel_path in _git_lines("ls-files"):
        if _is_excluded_root(rel_path, exclude_roots):
            continue
        abs_path = ROOT / rel_path
        if not abs_path.exists() or abs_path.is_dir():
            continue
        files.append(rel_path)
    return files


def _git_worktree_add(root: Path, tree: Path) -> None:
    completed = subprocess.run(
        ["git", "-C", str(root), "worktree", "add", "--detach", "--quiet", str(tree), "HEAD"],
        cwd=root,
        env=_env_without_git_location(),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip() or "no git output"
        raise RuntimeError(f"git worktree add failed: {details}")


def _link_node_modules(tree: Path, root: Path) -> None:
    """Expose the repo's ``node_modules`` inside the staged tree.

    ``npm run lint:js`` runs ``node ./node_modules/eslint/bin/eslint.js``, a path
    relative to the package root, so the eslint hook needs an entry at the
    snapshot root. Node would resolve imports by walking up to the repo's
    ``node_modules``, but the literal path in the script would not.
    """
    source = root / "node_modules"
    link = tree / "node_modules"
    if not source.is_dir() or link.exists() or link.is_symlink():
        return
    try:
        link.symlink_to(source, target_is_directory=True)
        return
    except OSError:
        if os.name != "nt":
            raise
    # Windows needs a directory junction: a symlink requires Developer Mode or
    # an elevated shell, while `mklink /J` works for the current user.
    completed = subprocess.run(
        ["cmd", "/c", "mklink", "/J", str(link), str(source)],
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode != 0:
        details = completed.stderr.strip() or completed.stdout.strip() or "no output"
        raise OSError(f"mklink /J failed: {details}")


def _unlink_node_modules(tree: Path) -> None:
    """Remove the link itself, never the repo's modules it points at."""
    link = tree / "node_modules"
    if not link.is_symlink() and not link.exists():
        return
    try:
        link.unlink()
        return
    except OSError:
        pass
    with contextlib.suppress(OSError):
        link.rmdir()


def _remove_staged_tree(root: Path, tree: Path) -> None:
    _unlink_node_modules(tree)
    completed = subprocess.run(
        ["git", "-C", str(root), "worktree", "remove", "--force", str(tree)],
        cwd=root,
        env=_env_without_git_location(),
        check=False,
        capture_output=True,
        text=True,
    )
    if completed.returncode == 0 and not tree.exists():
        return
    # A locked file (an editor, an antivirus scan) can defeat `worktree remove`
    # on Windows, and a junction it cannot unlink leaves the directory behind
    # even when git reports success. Drop the directory and prune the metadata
    # so the next commit does not trip over a worktree git still lists.
    shutil.rmtree(tree, ignore_errors=True)
    if tree.exists():
        print(f"pre-commit gate: could not remove the staged tree at {tree}", file=sys.stderr)
    subprocess.run(
        ["git", "-C", str(root), "worktree", "prune"],
        cwd=root,
        env=_env_without_git_location(),
        check=False,
        capture_output=True,
    )


def _prune_stale_worktree_entries(root: Path) -> None:
    """Drop worktree metadata whose checkout directory has been gone a while.

    `--expire` measures how long the checkout has been *missing*, not how old
    the tree is. A zero-grace prune is repo-wide, so it would also deregister
    any other worktree whose directory another tool has only just moved or
    removed; the same grace window as the directory sweep keeps this to entries
    nothing is using.
    """
    subprocess.run(
        [
            "git",
            "-C",
            str(root),
            "worktree",
            "prune",
            f"--expire={STALE_STAGED_TREE_AGE_S}.seconds.ago",
        ],
        cwd=root,
        env=_env_without_git_location(),
        check=False,
        capture_output=True,
    )


def _sweep_stale_staged_trees(root: Path | None = None) -> list[Path]:
    """Remove staged-tree worktrees an interrupted gate left behind.

    A killed gate never reaches `_remove_staged_tree`, so its worktree stays
    registered and its checkout stays under `.tmp/precommit-staged-tree`. Every
    run gets a fresh uuid, so an old tree can never be reused: it is only disk
    and `git worktree list` noise. Only entries older than
    `STALE_STAGED_TREE_AGE_S` are touched, because a concurrent gate's tree is
    seconds old and deleting it mid-run would fail a gate that was working.

    Returns the paths that were removed. A tree that cannot be removed is
    reported by `_remove_staged_tree` and not counted as swept.
    """
    base_root = ROOT if root is None else root
    base = base_root / STAGED_TREE_DIR
    removed: list[Path] = []
    if base.is_dir():
        cutoff = time.time() - STALE_STAGED_TREE_AGE_S
        for tree in sorted(base.iterdir()):
            try:
                stale = tree.is_dir() and tree.stat().st_mtime <= cutoff
            except OSError:
                continue  # another gate's sweep removed it after the listing
            if not stale:
                continue
            _remove_staged_tree(base_root, tree)
            if not tree.exists():
                removed.append(tree)
    _prune_stale_worktree_entries(base_root)
    if removed:
        print(f"pre-commit gate: removed {len(removed)} stale staged tree(s)")
    return removed


def _create_staged_tree(root: Path) -> Path:
    """Materialise the index as a detached worktree and return its path."""
    tree_sha = _git_at(root, "write-tree")[0]
    tree = root / STAGED_TREE_DIR / uuid.uuid4().hex[:12]
    tree.parent.mkdir(parents=True, exist_ok=True)
    try:
        _git_worktree_add(root, tree)
        # HEAD stays at the real commit and the index becomes the staged tree, so
        # `git diff --cached` inside the snapshot is exactly the commit's change.
        _git_at(tree, "read-tree", "--reset", "-u", tree_sha, env=_env_without_git_location())
    except RuntimeError:
        _remove_staged_tree(root, tree)
        raise
    try:
        _link_node_modules(tree, root)
    except OSError as exc:
        _remove_staged_tree(root, tree)
        raise RuntimeError(f"unable to expose node_modules in the staged tree: {exc}") from exc
    return tree


@contextlib.contextmanager
def staged_tree(root: Path | None = None) -> Iterator[Path]:
    """Yield a worktree holding exactly the staged tree, then remove it."""
    base = ROOT if root is None else root
    tree = _create_staged_tree(base)
    try:
        yield tree
    finally:
        _remove_staged_tree(base, tree)


@contextlib.contextmanager
def validation_tree() -> Iterator[Path]:
    """Yield the tree changed mode validates: the staged tree when available.

    A repository whose index cannot be materialised (an unborn HEAD, an unmerged
    index, or an unwritable `.tmp` directory) falls back to the working tree and
    says so. The gate still runs and can still block; it just loses the isolation.
    """
    _sweep_stale_staged_trees()
    try:
        tree = _create_staged_tree(ROOT)
    except (OSError, RuntimeError) as exc:
        print(f"pre-commit gate: staged tree unavailable ({exc}); validating the working tree")
        yield ROOT
        return
    print(f"pre-commit gate: validating the staged tree ({tree.relative_to(ROOT).as_posix()})")
    try:
        yield tree
    finally:
        _remove_staged_tree(ROOT, tree)


def _read_tree_bytes(tree: Path, files: list[str]) -> dict[str, bytes]:
    contents: dict[str, bytes] = {}
    for rel_path in files:
        path = tree / rel_path
        if path.is_file():
            contents[rel_path] = path.read_bytes()
    return contents


def _restore_hook_fixes(tree: Path, before: Mapping[str, bytes], files: list[str]) -> None:
    """Copy a fixer hook's edit from the staged tree into the working tree.

    Hooks run against the staged tree, so `ruff-check --fix` and
    `end-of-file-fixer` no longer edit the working tree by themselves. Copying
    the result back keeps the fix-and-restage workflow. A file whose working
    copy already diverged from what was staged is skipped: that edit belongs to
    the developer, and overwriting it would destroy unstaged work.
    """
    restored: list[str] = []
    for rel_path in files:
        original = before.get(rel_path)
        if original is None:
            continue
        staged_now = tree / rel_path
        working = ROOT / rel_path
        if not staged_now.is_file() or not working.is_file():
            continue
        fixed = staged_now.read_bytes()
        if fixed == original or working.read_bytes() != original:
            continue
        working.write_bytes(fixed)
        restored.append(rel_path)
    if restored:
        print(f"pre-commit gate: copied hook fixes into the working tree: {', '.join(restored)}")


def _precommit_base_command() -> list[str]:
    return [
        PYTHON,
        "-m",
        "pre_commit",
        "run",
        "--show-diff-on-failure",
        "--color=always",
    ]


def _run_precommit_command(
    command: list[str],
    *,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    run_env = dict(env) if env is not None else os.environ.copy()
    pre_commit_home = Path(run_env.setdefault("PRE_COMMIT_HOME", str(PRE_COMMIT_HOME)))
    pre_commit_home.mkdir(parents=True, exist_ok=True)
    completed = subprocess.run(command, cwd=cwd or ROOT, env=run_env)
    return completed.returncode


def build_changed_command(files: list[str]) -> list[str]:
    return [*_precommit_base_command(), "--files", *files]


# Hooks declared with `stages: [pre-push]` in .pre-commit-config.yaml. The
# default `pre-commit` stage never selects them, so before this list existed
# they ran only in CI -- which is how a mypy error reached main in e696be64
# while the local gate stayed green.
PRE_PUSH_HOOK_IDS = ("vulture", "mypy", "eslint")


def _pre_push_hook_commands() -> list[list[str]]:
    """One command per pre-push-stage hook, scanned repo-wide.

    ``pre-commit run`` takes exactly one hook id, so each hook gets its own
    command. ``--all-files`` is required rather than a chunked file list: mypy and
    eslint both set ``pass_filenames: false`` and scan the whole repo regardless,
    so running them once per 200-file chunk would multiply that cost by the chunk
    count.
    """
    return [
        [*_precommit_base_command(), hook_id, "--all-files", "--hook-stage", "pre-push"]
        for hook_id in PRE_PUSH_HOOK_IDS
    ]


def build_all_commands(files: list[str] | None = None) -> list[list[str]]:
    commands: list[list[str]] = []
    if files:
        for index in range(0, len(files), MAX_PRECOMMIT_FILES_PER_COMMAND):
            commands.append(
                [
                    *_precommit_base_command(),
                    "--files",
                    *files[index : index + MAX_PRECOMMIT_FILES_PER_COMMAND],
                ]
            )
    else:
        commands.append([*_precommit_base_command(), "--all-files"])
    commands.extend(_pre_push_hook_commands())
    return commands


def has_staged_changes() -> bool:
    """True when the index holds content, i.e. a commit is being created."""
    return bool(_git_lines("diff", "--cached", "--name-only"))


# Shippedness is not a question this gate answers for itself: the container version
# policy already classifies every tracked path, and `NON_SHIPPED_PATTERNS` is the
# single source for it. An entry below is needed only when a group *other than*
# `release` reads the path -- the fallback already runs `release` for everything
# else. `docs` walks `docs/`, `tools/**/*.md`, the root docs, `release-notes.md` and
# `opencode.json`; `workflow` reads `.github/`, `AGENTS.md`, the package manifests,
# and runs the policy-wiring checks defined in `tools/repo_health/`;
# `test-shape`/`line-budget`/`timing` read `tests/`.
_GUARDRAIL_JUDGED_NON_SHIPPED_ROOTS = ("docs/", "tests/", ".github/", "tools/")
_GUARDRAIL_JUDGED_NON_SHIPPED_FILES = frozenset(
    {
        "AGENTS.md",
        "CONTRIBUTING.md",
        "README.md",
        "opencode.json",
        "package-lock.json",
        "package.json",
        "release-notes.md",
    }
)


def should_run_repo_guardrails(files: list[str]) -> bool:
    """True when a commit can change a guardrail group's verdict.

    Shipped paths are derived from the container version policy's
    ``NON_SHIPPED_PATTERNS`` instead of a hand-maintained root list, so `styles/`,
    `Dockerfile`, and the runtime `data/` files -- all shipped, none of them in the
    old watched roots -- run every group instead of only `release`. `.githooks/` is
    shipped by that policy too, which keeps the hook scripts covered by the
    `workflow` group that asserts on them.
    """
    # Imported here so the module-level import surface stays stdlib-only; the gate
    # inserts ROOT on sys.path above for exactly this import.
    from tools.repo_health.container_version_policy import is_shipped_path

    return any(
        is_shipped_path(path)
        or path.startswith(_GUARDRAIL_JUDGED_NON_SHIPPED_ROOTS)
        or path in _GUARDRAIL_JUDGED_NON_SHIPPED_FILES
        for path in files
    )


def run_repo_guardrails(
    groups: tuple[str, ...] = (),
    *,
    pending_container_version_gate: bool = False,
    cwd: Path | None = None,
    env: Mapping[str, str] | None = None,
) -> int:
    # The script is invoked by relative path so a staged-tree run executes the
    # staged copy of the guardrails against the staged copy of the repository.
    command = [PYTHON, "tools/repo_health/repo_guardrails.py"]
    for group in groups:
        command.extend(["--group", group])
    # The container shipped-code version gate judges the staged commit here. It used
    # to be skipped entirely at commit time, so a shipped change could land on a
    # released version and only be flagged afterwards: the commit is the last moment
    # the decision can be stopped, and the window check cannot see a commit that does
    # not exist yet.
    if pending_container_version_gate:
        command.append("--pending-container-version-gate")
    # The window half of that gate runs in the all-files pass (pre-push and CI) and
    # reads intent from shipped commits only, so a `Release-tag:` line on a docs,
    # tests, or tools commit does not discharge it. A bump-first window closes on a
    # shipped commit declaring `Release-tag: v<current>`, which is valid while that
    # version is untagged -- see docs/RELEASE.md.
    completed = subprocess.run(
        command, cwd=cwd or ROOT, env=dict(env) if env is not None else None, check=False
    )
    return completed.returncode


def run_complexity_baseline(
    *, cwd: Path | None = None, env: Mapping[str, str] | None = None
) -> int:
    completed = subprocess.run(
        [PYTHON, "scripts/check_complexity_baseline.py"],
        cwd=cwd or ROOT,
        env=dict(env) if env is not None else None,
        check=False,
    )
    return completed.returncode


@dataclass(frozen=True)
class _StageFailure:
    """One gate stage that exited nonzero during an aggregated run."""

    stage: str
    return_code: int


def _run_stage(
    stage: str,
    runner: Callable[[], int],
    failures: list[_StageFailure],
    *,
    command: list[str] | None = None,
) -> None:
    """Run one stage and record its failure instead of aborting the gate.

    Each stage used to return immediately, so a red pre-commit pass hid whether
    the pre-push hooks, the guardrails, or the complexity baseline were also
    failing; the operator paid another full run to find out.
    """
    return_code = runner()
    if return_code == 0:
        return
    _report_failing_stage(stage, command or [], return_code)
    failures.append(_StageFailure(stage, return_code))


def _report_failing_stage(stage: str, command: list[str], return_code: int) -> None:
    """Print which stage failed and with what command, on stdout.

    Without this, a run whose guardrail groups all print "passed" and then exits
    nonzero is not diagnosable from the log alone -- the exit code was the only
    evidence of where the failure came from. Stdout rather than stderr keeps the
    stage line next to the per-group "passed" lines printed in between.
    """
    suffix = f" ({' '.join(command[1:])})" if command else ""
    print(f"pre-commit gate: stage={stage}{suffix} exit={return_code}")


def _report_summary(failures: list[_StageFailure]) -> int:
    """Print one closing line naming every failing stage; 1 if any failed."""
    if not failures:
        return 0
    stages: dict[str, int] = {}
    for failure in failures:
        stages.setdefault(failure.stage, failure.return_code)
    summary = ", ".join(f"{stage} ({code})" for stage, code in stages.items())
    print(f"pre-commit gate: {len(stages)} stage(s) failed: {summary}")
    return 1


def run_changed() -> int:
    files = collect_staged_files()
    if not files:
        print("No staged files found for pre-commit; skipping.")
        return 0
    failures: list[_StageFailure] = []
    with validation_tree() as tree:
        staged = tree != ROOT
        env = _snapshot_env() if staged else None
        before = _read_tree_bytes(tree, files) if staged else {}
        command = build_changed_command(files)
        _run_stage(
            "pre-commit",
            lambda: _run_precommit_command(command, cwd=tree, env=env),
            failures,
            command=command,
        )
        if staged:
            # `ruff-check --fix` and `end-of-file-fixer` edit the tree they run
            # in, which is now the snapshot. Carry their edits back so the
            # developer only has to re-stage, as before.
            _restore_hook_fixes(tree, before, files)
        # The pre-push-stage hooks used to run only in the all-files path, so a
        # commit could be green with a mypy error, an eslint finding, or a dead-code
        # finding and the operator hit it seconds later at push time. They are
        # whole-repo scans, so the cost is the same whether one file or the tree
        # changed. Measured on this tree: the hooks cost 2.5s (vulture) / 1.0s (mypy)
        # / 3.1s (eslint) in isolation, but the commit gate as a whole moved from
        # 24.4s to 34.1/33.5/37.0s across three runs, so ~+10.5s end to end. That is
        # higher than the 6.6s sum of the parts because each hook is a separate
        # `pre-commit` process. Against a ~108s push gate that already runs them,
        # this is cheap enough to close the hole at its source.
        for hook_command in _pre_push_hook_commands():
            _run_stage(
                "pre-commit --hook-stage pre-push",
                lambda hook_command=hook_command: _run_precommit_command(
                    hook_command, cwd=tree, env=env
                ),
                failures,
                command=hook_command,
            )
        if should_run_repo_guardrails(files):
            # Local commit: judge the staged change against the released version so the
            # offending commit never lands. The full post-bump window stays a pre-push
            # and CI check (run_all()), which also catches a run with hooks disabled.
            _run_stage(
                "repo-guardrails",
                lambda: run_repo_guardrails(pending_container_version_gate=True, cwd=tree, env=env),
                failures,
            )
        elif has_staged_changes():
            # The filter keeps the full guardrail run off commits no group judges
            # (release notes, memory notes, `tools/`, the release-tag bump script).
            # Shipped paths no longer land here: `styles/`, `Dockerfile`, and the
            # runtime `data/` files reach every group through the container policy.
            # What is left still needs the container version gate every commit needs.
            _run_stage(
                "repo-guardrails --group release",
                lambda: run_repo_guardrails(
                    groups=("release",), pending_container_version_gate=True, cwd=tree, env=env
                ),
                failures,
            )
        # The C901 baseline belongs to the commit path too, not just the all-files
        # pass: a new or grown hotspot is visible in the changed file long before the
        # push, and the check costs under a second.
        _run_stage(
            "complexity-baseline", lambda: run_complexity_baseline(cwd=tree, env=env), failures
        )
    return _report_summary(failures)


def run_all(exclude_roots: tuple[str, ...] = ()) -> int:
    files = collect_repo_files(exclude_roots) if exclude_roots else None
    if files is not None and not files:
        print("No tracked files found for pre-commit; skipping.")
        return 0
    failures: list[_StageFailure] = []
    for command in build_all_commands(files):
        # One command per pre-push hook follows the file-scoped runs, so the log
        # names the stage that actually failed rather than calling it all
        # "pre-commit".
        stage = "pre-commit --hook-stage pre-push" if "--hook-stage" in command else "pre-commit"
        _run_stage(
            stage,
            lambda command=command: _run_precommit_command(command),
            failures,
            command=command,
        )
    _run_stage("repo-guardrails", lambda: run_repo_guardrails(), failures)
    _run_stage("complexity-baseline", lambda: run_complexity_baseline(), failures)
    return _report_summary(failures)


def main() -> int:
    parser = argparse.ArgumentParser(description="Baluffo pre-commit gate")
    parser.add_argument(
        "--mode",
        choices=("changed", "all"),
        default="changed",
        help="Run only changed files or the full repository guardrail set.",
    )
    parser.add_argument(
        "--exclude-root",
        action="append",
        default=[],
        help="Exclude tracked files whose top-level path matches the given repo root.",
    )
    args = parser.parse_args()

    if args.mode == "all":
        return run_all(tuple(args.exclude_root))
    return run_changed()


if __name__ == "__main__":
    raise SystemExit(main())
