"""Guardrail: shipped container code must not land without a version bump or release-tag intent.

The 0.2.140 reuse trap: the container version was bumped once (commit
``f46baa4c``, 2026-08-27) and the Umbrel box updated to it, then ~11 commits of
container-affecting code landed on ``main`` over the following days under the
SAME version string. Every ``main`` push republished
``ghcr.io/deathuman/baluffo:0.2.140`` and ``:latest`` with newer code, but
Umbrel's app-store update detection compares the ``version:`` string in
``deathuman-baluffo/umbrel-app.yml`` — which never changed — so the box never
re-pulled and silently kept running the older 08-28 build while the image tag
drifted forward.

This guardrail fails when shipped container code commits land on the branch
after the most recent version bump without either advancing the version itself
or declaring an explicit release-tag intent. A commit declares intent with a
``Release-tag: vX.Y.Z`` line (or a ``release(vX.Y.Z):`` / ``chore(release):``
subject) naming a version strictly newer than the current one. That forces
every container-affecting change to either be its own release or to name the
release it belongs to, so code can never again ship to the container channel
invisibly under a frozen version string.

Scope: the window is the commits after the most recent commit that changed the
version in ``src/app_version.py`` or ``deathuman-baluffo/umbrel-app.yml``
(the release anchor). Any shipped commit in that window triggers the gate
unless the window carries a bump or an explicit intent declaration.
"""

from __future__ import annotations

import fnmatch
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.baluffo_version import compare_baluffo_versions

VERSION_FILES = (
    "src/app_version.py",
    "deathuman-baluffo/umbrel-app.yml",
)

# Paths that never change the container image or the store metadata the box
# consumes: docs, tests, repo tooling, CI config, and root-level docs/identity
# files. Everything else (src/, frontend/, scripts/, Dockerfile, requirements,
# data/contracts, data/defaults, deathuman-baluffo/, ...) is shipped code.
# Keep this superset of the container workflow's `paths-ignore` list so the
# gate and the republish trigger stay aligned.
NON_SHIPPED_PATTERNS = (
    "docs/**",
    "tests/**",
    "tools/**",
    ".github/**",
    "README.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "AGENTS.md",
    "LICENSE",
    "release-notes.md",
    "umbrel-app-store.yml",
)

_APP_VERSION_RE = re.compile(r'APP_VERSION\s*=\s*"([^"]+)"')
_UMBREL_VERSION_RE = re.compile(r'^version\s*:\s*"([^"]+)"', re.MULTILINE)
_RELEASE_TAG_LINE_RE = re.compile(
    r"^release-tag\s*:\s*v?(\d+\.\d+\.\d+)\s*$", re.IGNORECASE | re.MULTILINE
)
_RELEASE_SUBJECT_RE = re.compile(r"^release\(v?(\d+\.\d+\.\d+)\)\s*:", re.IGNORECASE)
_CHORE_SUBJECT_RE = re.compile(r"^chore\(release\)\s*:.*\bv?(\d+\.\d+\.\d+)\b", re.IGNORECASE)


@dataclass(frozen=True)
class WindowCommit:
    """One commit inside the post-bump window, with its changed files and message."""

    sha: str
    subject: str
    message: str
    files: tuple[str, ...]


def _git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.stdout if completed.returncode == 0 else ""


def _file_at(repo_root: Path, rev: str, rel_path: str) -> str:
    return _git(repo_root, "show", f"{rev}:{rel_path}")


def _version_at(repo_root: Path, rev: str) -> str | None:
    app_text = _file_at(repo_root, rev, "src/app_version.py")
    match = _APP_VERSION_RE.search(app_text)
    if match:
        return match.group(1).strip()
    umbrel_text = _file_at(repo_root, rev, "deathuman-baluffo/umbrel-app.yml")
    match = _UMBREL_VERSION_RE.search(umbrel_text)
    return match.group(1).strip() if match else None


def _last_version_bump_commit(repo_root: Path, head: str = "HEAD") -> str | None:
    """Return the most recent commit reachable from ``head`` that advanced the version."""
    shas = _git(repo_root, "log", "--format=%H", "--", *VERSION_FILES).split()
    for sha in shas:
        after = _version_at(repo_root, sha)
        before = _version_at(repo_root, f"{sha}^")
        if after and before and compare_baluffo_versions(after, before) > 0:
            return sha
    return None


def _window_commits(repo_root: Path, anchor: str, head: str = "HEAD") -> list[WindowCommit]:
    """Return the commits in ``anchor..head`` with their changed files and messages."""
    commits: list[WindowCommit] = []
    shas = _git(repo_root, "rev-list", "--reverse", f"{anchor}..{head}").split()
    for sha in shas:
        subject = _git(repo_root, "log", "-1", "--format=%s", sha).strip()
        message = _git(repo_root, "log", "-1", "--format=%B", sha)
        parent = _git(repo_root, "rev-parse", f"{sha}^").strip()
        files: list[str] = []
        if parent:
            files = [
                line
                for line in _git(
                    repo_root, "diff-tree", "--no-commit-id", "--name-only", "-r", parent, sha
                ).splitlines()
                if line.strip()
            ]
        commits.append(WindowCommit(sha=sha, subject=subject, message=message, files=tuple(files)))
    return commits


def _is_shipped_path(rel_path: str) -> bool:
    return not any(fnmatch.fnmatch(rel_path, pattern) for pattern in NON_SHIPPED_PATTERNS)


def _declared_release_tag_versions(message: str) -> list[str]:
    """Return every version explicitly declared as release-tag intent in a commit message."""
    declared: list[str] = []
    declared.extend(_RELEASE_TAG_LINE_RE.findall(message))
    first_line = message.splitlines()[0] if message.splitlines() else ""
    declared.extend(_RELEASE_SUBJECT_RE.findall(first_line))
    declared.extend(_CHORE_SUBJECT_RE.findall(first_line))
    return [version.strip() for version in declared if version.strip()]


def _has_valid_release_tag_intent(commit: WindowCommit, current_version: str) -> bool:
    return any(
        compare_baluffo_versions(version, current_version) > 0
        for version in _declared_release_tag_versions(commit.message)
    )


def evaluate_window(commits: list[WindowCommit], current_version: str) -> list[str]:
    """Return failures for a post-bump window that ships code without bump or intent."""
    shipped = [commit for commit in commits if any(_is_shipped_path(f) for f in commit.files)]
    if not shipped:
        return []
    if any(_has_valid_release_tag_intent(commit, current_version) for commit in shipped):
        return []
    listing = "\n".join(f"- {commit.sha[:8]} {commit.subject}" for commit in shipped)
    return [
        f"{len(shipped)} shipped container code commit(s) landed after the last version "
        f"bump ({current_version}) without a version bump or explicit release-tag intent:\n"
        f"{listing}\n"
        "Bump the version (`python scripts/bump_version.py <next>`) or add a "
        "`Release-tag: vX.Y.Z` line naming a version newer than the current one to a "
        "commit message in this window, so the Umbrel box can see the update."
    ]


def check_container_shipped_code_version_gate(repo_root: Path = ROOT) -> list[str]:
    """Fail when shipped container code lands after the last version bump without bump/intent."""
    if not (repo_root / ".git").exists():
        return []
    anchor = _last_version_bump_commit(repo_root)
    if anchor is None:
        return []
    current_version = _version_at(repo_root, "HEAD") or ""
    if not current_version:
        return []
    commits = _window_commits(repo_root, anchor)
    return evaluate_window(commits, current_version)
