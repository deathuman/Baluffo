"""Corpus epochs: identity, provenance and supersession (S3 of the fetch perf plan).

An epoch is one recorded corpus plus the provenance needed to say what it is. The naming
follows the convention already in use for run tokens (``_out/perf-runs/`` uses
``<slug>-<YYYYMMDD>-<HHMMSS>``), so a corpus epoch is
``corpus-<scope>-<YYYYMMDD>-<HHMMSS>``.

Authority is **newest by ``recordedAt``**, deliberately with no pointer file: a missing or
corrupt pointer is exactly the failure class the source-registry work already hit, where a
corrupt live registry silently resolved to the committed seed. A superseded epoch records
``supersededBy`` but is never deleted -- a corpus cannot be regenerated, only re-recorded.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
from collections.abc import Mapping
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

EPOCH_FILE = "EPOCH.json"
DEFAULT_SCOPE = "subset"
EPOCH_PREFIX = "corpus-"


class EpochError(ValueError):
    """Raised when an epoch directory or manifest violates the epoch contract."""


def new_epoch_id(scope: str = DEFAULT_SCOPE, *, now: datetime | None = None) -> str:
    stamp = (now or datetime.now(UTC)).strftime("%Y%m%d-%H%M%S")
    clean_scope = "".join(char if char.isalnum() or char in "-_" else "-" for char in str(scope))
    return f"{EPOCH_PREFIX}{clean_scope.strip('-') or DEFAULT_SCOPE}-{stamp}"


def epoch_dir(parent: Path, scope: str = DEFAULT_SCOPE, *, now: datetime | None = None) -> Path:
    return Path(parent) / new_epoch_id(scope, now=now)


def git_identity(repo_root: Path) -> tuple[str, str]:
    """``(HEAD sha, hash of the uncommitted diff)`` -- provenance for a recorded corpus.

    A corpus recorded from a dirty tree is still evidence, as long as the artifact says
    which dirty tree: the hash makes two such corpora distinguishable.
    """

    root = Path(repo_root)

    def _run(*args: str) -> str:
        try:
            completed = subprocess.run(
                ["git", "-C", str(root), *args],
                check=False,
                capture_output=True,
                text=True,
            )
        except OSError:
            return ""
        return completed.stdout if completed.returncode == 0 else ""

    sha = _run("rev-parse", "HEAD").strip()
    dirty = _run("status", "--porcelain") + _run("diff", "HEAD")
    dirty_hash = hashlib.sha256(dirty.encode("utf-8")).hexdigest() if dirty.strip() else ""
    return sha, dirty_hash


def create_epoch(
    root: Path,
    *,
    scope: str = DEFAULT_SCOPE,
    recorded_with: str = "force_refresh_all",
    counts: Mapping[str, Any] | None = None,
    coverage_summary: Mapping[str, Any] | None = None,
    git_sha: str = "",
    dirty_diff_hash: str = "",
    notes: str = "",
    extra: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Write ``EPOCH.json`` for a freshly recorded corpus and return the manifest.

    ``extra`` carries recorder-specific fields (the recorded run directory and summary
    that a replay is checked against) so the manifest schema stays in one place.
    """

    root_path = Path(root)
    manifest: dict[str, Any] = {
        "epochId": root_path.name,
        "scope": str(scope),
        "recordedAt": (now or datetime.now(UTC)).isoformat().replace("+00:00", "Z"),
        "recordedWith": str(recorded_with),
        "gitSha": str(git_sha),
        "dirtyDiffHash": str(dirty_diff_hash),
        "counts": dict(counts or {}),
        "coverageSummary": dict(coverage_summary or {}),
        "supersededBy": "",
    }
    if notes:
        manifest["notes"] = str(notes)
    for key, value in dict(extra or {}).items():
        if key in manifest:
            raise EpochError(f"extra may not overwrite manifest field {key!r}")
        manifest[str(key)] = value
    root_path.mkdir(parents=True, exist_ok=True)
    write_epoch(root_path, manifest)
    return manifest


def write_epoch(root: Path, manifest: Mapping[str, Any]) -> Path:
    path = Path(root) / EPOCH_FILE
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(manifest), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def read_epoch(root: Path) -> dict[str, Any]:
    """The manifest for an epoch directory. Raises :class:`EpochError` when unusable."""

    path = Path(root) / EPOCH_FILE
    if not path.is_file():
        raise EpochError(f"no {EPOCH_FILE} in {Path(root)}")
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise EpochError(f"invalid {EPOCH_FILE} in {Path(root)}: {exc}") from exc
    if not isinstance(payload, dict):
        raise EpochError(f"{EPOCH_FILE} in {Path(root)} must be a JSON object")
    return payload


def _recorded_at_sort_key(manifest: Mapping[str, Any]) -> tuple[float, str]:
    raw = str(manifest.get("recordedAt") or "").strip()
    if not raw:
        return (0.0, "")
    try:
        parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except ValueError:
        return (0.0, raw)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return (parsed.timestamp(), raw)


def epoch_dirs(parent: Path) -> list[Path]:
    """Epoch directories under ``parent``, newest by ``recordedAt`` last.

    A directory without a readable manifest is skipped rather than sorted to the end:
    silently treating an unreadable epoch as the newest is how a corrupt manifest would
    win an A/B.
    """

    base = Path(parent)
    if not base.is_dir():
        return []
    found: list[tuple[tuple[float, str], Path]] = []
    for child in sorted(base.iterdir()):
        if not child.is_dir() or not (child / EPOCH_FILE).is_file():
            continue
        try:
            manifest = read_epoch(child)
        except EpochError:
            continue
        found.append((_recorded_at_sort_key(manifest), child))
    found.sort(key=lambda item: (item[0], item[1].name))
    return [path for _, path in found]


def resolve_current(parent: Path) -> Path | None:
    """The authoritative epoch: newest by ``recordedAt``, or ``None`` when there is none."""

    dirs = epoch_dirs(parent)
    return dirs[-1] if dirs else None


def mark_superseded(root: Path, *, superseded_by: str) -> dict[str, Any]:
    """Record that a newer epoch replaces this one. The corpus itself is untouched."""

    manifest = read_epoch(root)
    manifest["supersededBy"] = str(superseded_by)
    write_epoch(root, manifest)
    return manifest


def supersede_older_epochs(parent: Path, *, current: str) -> list[str]:
    """Mark every other epoch under ``parent`` as superseded by ``current``.

    Returns the epoch ids that were changed. Called after a new epoch is recorded, so
    "nobody needs to be appointed to declare an epoch superseded" holds.
    """

    changed: list[str] = []
    for path in epoch_dirs(parent):
        if path.name == str(current):
            continue
        manifest = read_epoch(path)
        if str(manifest.get("supersededBy") or "") == str(current):
            continue
        mark_superseded(path, superseded_by=str(current))
        changed.append(path.name)
    return changed


def describe_epoch(path: Path) -> str:
    """One-line human summary, used by the CLI entry points."""

    manifest = read_epoch(path)
    counts = manifest.get("counts") or {}
    superseded = str(manifest.get("supersededBy") or "")
    parts = [
        f"{manifest.get('epochId') or path.name}",
        f"recorded {manifest.get('recordedAt') or 'unknown'}",
        f"with {manifest.get('recordedWith') or 'unknown'}",
        f"entries={counts.get('entries', '?')}",
        f"bodies={counts.get('uniqueBodies', '?')}",
    ]
    if superseded:
        parts.append(f"supersededBy={superseded}")
    return " | ".join(parts)
