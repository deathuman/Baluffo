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
# consumes: docs, tests, repo tooling, CI config, AI continuity notes, and
# root-level docs/identity files. Everything else (src/, frontend/, scripts/,
# Dockerfile, requirements, data/contracts, data/defaults, deathuman-baluffo/,
# ...) is shipped code.
# Keep this superset of the container workflow's `paths-ignore` list so the
# gate and the republish trigger stay aligned.
#
# `memory/**` is here because continuity notes are not runtime inputs: nothing
# under src/ or scripts/ reads them. While it was shipped, committing a note
# re-triggered Build Container and retagged the current version with newer code
# while umbrel-app.yml still declared the old version -- exactly the 0.2.140
# reuse trap this gate exists to prevent.
#
# `opencode.json` is the OpenCode client/agent config (MCP launchers). It is
# not a runtime input either -- nothing under src/ or scripts/ reads it -- so
# an MCP launcher edit must not re-trigger Build Container and retag the
# current version the same way.
NON_SHIPPED_PATTERNS = (
    "docs/**",
    "tests/**",
    "tools/**",
    "memory/**",
    ".agents/**",
    ".github/**",
    "README.md",
    "CONTRIBUTING.md",
    "SECURITY.md",
    "AGENTS.md",
    "opencode.json",
    "LICENSE",
    "release-notes.md",
    "umbrel-app-store.yml",
    # A manifest or lockfile edit is usually a dev-dependency edit -- a linter, a
    # type checker -- and those install into the image's *build* stage only.
    # `Dockerfile` runs `npm ci` in a frontend stage and copies forward just the
    # built `.container-frontend` bundle, so a dev-dep bump changes the build
    # stage without changing the published image. Treating either file as
    # shipped code republished the version tag for no content change, which is
    # how 0.3.001 reached five distinct digests in one session.
    #
    # The two are registered together on purpose. `package-lock.json` was added
    # first and `package.json` -- the manifest that produces it -- was not, so a
    # Dependabot dev-dep bump was still gated as shipped code and still
    # re-triggered Build Container. It declares no runtime `dependencies` (only
    # `devDependencies`), and nothing under `src/` or `frontend/` reads it.
    #
    # Nothing is lost: every release bumps `src/app_version.py`, which *is* a
    # shipped path, so the image still rebuilds when a rebuild is wanted. What
    # this buys is that a routine dependency bump needs neither a version bump
    # nor release-tag intent to land.
    "package.json",
    "package-lock.json",
    # `data/*` is excluded from the image and then re-included path by path in
    # .dockerignore. The three audit reports below are tracked but NOT among the
    # re-inclusions, so they were gated as shipped while being absent from the
    # image: editing one burned a version and republished a tag whose content had
    # not changed. Same class as the `.agents/` leak fixed in 0.3.003.
    #
    # The two *runtime* data files in `data/` are deliberately NOT listed here.
    # They are excluded from the image but read by the app, so they are shipped
    # code and changing them must still force a version move.
    "data/adapter-audit-report.md",
    "data/pipeline-audit-report.md",
    "data/release-repeatability-report.md",
)

# Tracked files that `.dockerignore` keeps out of the image but that the shipped-code
# gate must still treat as shipped, because the running app reads them at runtime.
#
# Every other tracked file that the image excludes is expected to be non-shipped. A
# tracked file that is in neither bucket is a *classification gap*, and that is
# exactly how the three `data/` audit reports came to be gated as shipped while
# being absent from the image: nothing ever had to state which side they were on.
# Listing them here forces that decision to be explicit and reviewable.
IMAGE_EXCLUDED_BUT_SHIPPED = (
    "data/social-sources-config.json",
    "data/source-registry-tombstones.json.gz",
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


def _has_valid_release_tag_intent(
    commit: WindowCommit,
    current_version: str,
    *,
    current_released: bool = False,
) -> bool:
    # "= current" counts as valid intent ONLY while `current` is unreleased: the
    # commit declaring it is the one carrying the release for a just-bumped
    # version, e.g. a follow-up fix inside the same release window.
    #
    # Once `current` is RELEASED that reasoning inverts. The version tag is
    # already on GHCR, so a shipped commit that does not bump mutates it: the
    # container would hold different code than the frozen release assets under
    # one version string, and because Umbrel's update check is string
    # inequality the change would never be offered to an existing install. That
    # happened on 2026-10-01, when pushing the Q4/Q5 CSS deletion moved the
    # published 0.3.001 tag from sha256:f6fa5b37 to sha256:0d0f194c.
    #
    # So a released version demands strictly newer intent -- i.e. a real bump.
    # Republishes BEFORE a version is tagged are deliberately still allowed:
    # nobody can hold an unreleased version string, so that churn is inert, and
    # blocking it would cost a release-window fix for nothing.
    required = 1 if current_released else 0
    return any(
        compare_baluffo_versions(version, current_version) >= required
        for version in _declared_release_tag_versions(commit.message)
    )


def evaluate_window(
    commits: list[WindowCommit],
    current_version: str,
    *,
    current_released: bool = False,
) -> list[str]:
    """Return failures for a post-bump window that ships code without bump or intent.

    ``current_released`` defaults to False, which preserves the pre-release
    behaviour where ``Release-tag: v<current>`` counts as intent.
    """
    shipped = [commit for commit in commits if any(_is_shipped_path(f) for f in commit.files)]
    if not shipped:
        return []
    listing = "\n".join(f"- {commit.sha[:8]} {commit.subject}" for commit in shipped)
    if current_released:
        # Intent must NOT short-circuit here. The caller has already removed the
        # shipped commits that are ancestors of the release tag (those are inside
        # the published image and mutate nothing), so everything left here would
        # overwrite a published tag. Declaring a newer Release-tag is not
        # permission to do that -- the version has to actually move.
        return [
            f"{len(shipped)} shipped container code commit(s) landed after "
            f"`v{current_version}` was released, without a version bump:\n{listing}\n"
            f"`v{current_version}` is a published container tag, so shipping against it "
            "overwrites the published image: the container would hold different code than "
            "the frozen release assets under one version string, and Umbrel's "
            "string-equality update check would never offer the change to an existing "
            "install. Declaring a newer `Release-tag:` does not authorise that -- the "
            "version has to actually move.\n"
            "Bump the version (`python scripts/bump_version.py <next>`) in this change, or "
            "keep the shipped code on a branch and release it by tagging."
        ]
    if any(_has_valid_release_tag_intent(commit, current_version) for commit in shipped):
        return []
    return [
        f"{len(shipped)} shipped container code commit(s) landed after the last version "
        f"bump ({current_version}) without a version bump or explicit release-tag intent:\n"
        f"{listing}\n"
        "Bump the version (`python scripts/bump_version.py <next>`) or add a "
        "`Release-tag: vX.Y.Z` line naming a version newer than the current one to a "
        "commit message in this window, so the Umbrel box can see the update."
    ]


def _version_is_released(repo_root: Path, version: str) -> bool:
    """True when ``version`` carries a git release tag.

    The tag is the local, offline proxy for "this version is already out there".
    It is deliberately not a GHCR query: this gate runs in pre-commit and
    pre-push, where a network call would add two round-trips and fail offline --
    and AGENTS.md bans ``--no-verify``, so a flaky gate means stuck commits
    rather than a bypass.

    Known limit: a version that reached GHCR without ever being tagged (bumped,
    pushed, never released) reads as unreleased here. That is the inert window
    anyway -- nothing can hold the version string, so the republish costs
    nothing. ``docs/RELEASE.md`` records the same limit.
    """
    if not version:
        return False
    return bool(_git(repo_root, "tag", "-l", f"v{version}").strip())


def _run_succeeds(repo_root: Path, *args: str) -> bool:
    """True when ``git`` exits zero for ``args``."""
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    return completed.returncode == 0


def check_container_shipped_code_version_gate(repo_root: Path = ROOT) -> list[str]:
    """Fail when shipped container code lands after the last version bump without bump/intent.

    Once the current version is **released**, the test stops being intent and
    becomes ancestry: shipped code that is already an ancestor of the release tag
    is inside the published image and mutates nothing, while shipped code after
    the tag would overwrite it. Intent cannot authorise that -- declaring
    ``Release-tag: v<next>`` while ``v<current>`` is still the published tag is
    exactly the state this exists to stop, so a released version demands a real
    bump or a branch.
    """
    if not (repo_root / ".git").exists():
        return []
    anchor = _last_version_bump_commit(repo_root)
    if anchor is None:
        return []
    current_version = _version_at(repo_root, "HEAD") or ""
    if not current_version:
        return []
    commits = _window_commits(repo_root, anchor)
    current_released = _version_is_released(repo_root, current_version)
    if current_released:
        release_tag = f"v{current_version}"
        already_published = [
            commit
            for commit in commits
            if _run_succeeds(repo_root, "merge-base", "--is-ancestor", commit.sha, release_tag)
        ]
        commits = [commit for commit in commits if commit not in already_published]
    return evaluate_window(commits, current_version, current_released=current_released)
