"""Full-disk relief for the copy gates, in the plan's ordered resolution.

Freeing space needs space: retention only fills the freelist, compaction refuses
below its own gate, and both VACUUM paths want a temp copy. Two levers remain —
removing a file that is not the live database, and asking whether the gate wants
more than the copy will actually use. This leaf owns those two and nothing else.
It never touches the live database or its ``-wal``/``-shm``, the retained
``.pre-rebuild`` copy, a rebuild journal, or any path this codebase does not name.

The order is the plan's §3 table: (1) measure free space against both gates,
(2) remove a staging copy no live run owns, (3) remove that copy's ``-wal``/``-shm``
and stale scratch journals, then (4) ask the measured gate. Step 5 — deleting the
retained copy — is a click, never automatic, so it is not implemented here; step 6
is the refusal payload the callers report.

Ownership is a live registry, not a file age: a copy claims its staging and
scratch paths for as long as its thread exists, because a killed process leaves a
journal that still says ``running`` and an age cutoff would keep the partial copy
blocking the retry it caused.

The measured gate is anchored on the last rehearsal's ``stagingBytes``: a payload
sample is not an allocation, and the copy carries index pages and b-tree slack the
sample never sees (2.4-4.5x on the shipped schema, measured against real copies),
while the bounded table breakdown the first version used is truncated on that same
schema and so could not be read at all. Until a rehearsal has reported a copy
size, ``measured_required_bytes`` is ``None`` and the heuristic asks.
"""

from __future__ import annotations

import json
import shutil
import threading
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.storage.current_feed_rebuild import (
    REBUILD_JOURNAL_SUFFIX,
    REBUILD_STAGING_SUFFIX,
    rebuild_required_free_bytes,
)
from src.storage.reclaim_policy import free_space_satisfied
from src.storage_metrics import file_size

# The measured gate: the last rehearsal's real copy plus a quarter, plus a fixed
# margin. The 1 % heuristic is probe-avoidance, not physics — 2.32 GB was asked
# for a copy the rehearsal measures at 0.5-1.5 GB.
RECLAIM_SPACE_MARGIN = 1.25
RECLAIM_SPACE_MARGIN_BYTES = 64 * 1024 * 1024
# Journal names this leaf reads or removes; ``storage_reclaim`` owns the writes.
RECLAIM_JOURNAL_SUFFIX = ".reclaim.json"
REHEARSAL_JOURNAL_SUFFIX = ".reclaim-rehearsal.json"

_LIVE_LOCK = threading.Lock()
_LIVE_ARTIFACTS: set[Path] = set()


def claim_live_artifacts(paths: Iterable[Path]) -> None:
    """Mark the artifacts a running copy owns, so relief never removes them."""
    with _LIVE_LOCK:
        _LIVE_ARTIFACTS.update(Path(path).resolve() for path in paths)


def release_live_artifacts(paths: Iterable[Path]) -> None:
    """Drop a copy's claim; its artifacts become relief candidates again."""
    with _LIVE_LOCK:
        for path in paths:
            _LIVE_ARTIFACTS.discard(Path(path).resolve())


def _live_artifact_names() -> set[str]:
    """The names relief must keep, as they appear beside the database."""
    with _LIVE_LOCK:
        return {path.name for path in _LIVE_ARTIFACTS}


def free_space_bytes(path: Path | str) -> int | None:
    """Free bytes on the filesystem holding ``path``; ``None`` when unmeasurable."""
    try:
        return int(shutil.disk_usage(path).free)
    except OSError:
        return None


def measured_required_bytes(*, staging_bytes: Any) -> int | None:
    """Step 4's gate from the last rehearsal's measured copy, or ``None``.

    ``None`` is the normal state before any rehearsal has run: a payload sample
    under-reads a real copy by 2.4-4.5x on the shipped schema, so there is nothing
    trustworthy to relax the heuristic with until the host has measured itself.
    """
    base = _non_negative_int(staging_bytes)
    if base is None or base <= 0:
        return None
    return int(base * RECLAIM_SPACE_MARGIN) + RECLAIM_SPACE_MARGIN_BYTES


def _reclaim_journal(data_dir: Path | str, db_path: Path | str) -> dict[str, Any]:
    db = Path(db_path)
    try:
        payload = json.loads(
            (Path(data_dir) / f"{db.name}{RECLAIM_JOURNAL_SUFFIX}").read_text(encoding="utf-8")
        )
    except (OSError, ValueError):
        return {}
    return payload if isinstance(payload, dict) else {}


@dataclass(frozen=True)
class MeasuredAnchor:
    """What the journal remembers about the last copy this host measured."""

    staging_bytes: int | None
    measured_at: str
    gate_adequate: bool | None


def measured_anchor(data_dir: Path | str, db_path: Path | str) -> MeasuredAnchor:
    """The gate's anchor: the copy it is built from, when it was measured, and
    whether the last rehearsal's own copy fit the gate it started on.

    One read, because the three answers describe one measurement and a panel that
    got them from separate reads could show a size beside another run's date.
    """
    payload = _reclaim_journal(data_dir, db_path)
    adequate = payload.get("lastRehearsalGateAdequate")
    return MeasuredAnchor(
        staging_bytes=_non_negative_int(payload.get("measuredStagingBytes")) or None,
        measured_at=str(payload.get("measuredStagingAt") or ""),
        gate_adequate=None if adequate is None else bool(adequate),
    )


def orphaned_artifacts(data_dir: Path | str, db_path: Path | str) -> list[Path]:
    """Steps 2-3: files beside the database that no live copy owns.

    Only names this codebase writes are candidates: ``<db>.<token>.rebuild.tmp``
    and its companions, the rebuild journal's temp file, and the rehearsal's
    scratch journal and its temp. The live database, its WAL, the rebuild journal
    and the retained copy are never candidates.
    """
    directory = Path(data_dir)
    db = Path(db_path)
    live = _live_artifact_names()
    candidates: list[Path] = []
    for staging in directory.glob(f"{db.name}.*{REBUILD_STAGING_SUFFIX}"):
        if staging.name in live:
            continue
        candidates.append(staging)
        candidates.extend(
            staging.with_name(f"{staging.name}{suffix}") for suffix in ("-wal", "-shm")
        )
    journal_tmp = db.with_name(f"{db.name}{REBUILD_JOURNAL_SUFFIX}.tmp")
    rehearsal = db.with_name(f"{db.name}{REHEARSAL_JOURNAL_SUFFIX}")
    candidates.extend([journal_tmp, rehearsal, rehearsal.with_name(f"{rehearsal.name}.tmp")])
    return [path for path in candidates if path.name not in live and path.is_file()]


def artifact_bytes(paths: Iterable[Path]) -> int:
    """Bytes the candidate artifacts occupy, for the pre-flight refusal's estimate."""
    return sum(max(0, file_size(path)) for path in paths)


def relieve_free_space(data_dir: Path | str, db_path: Path | str) -> dict[str, Any]:
    """Steps 1-3: remove the unowned artifacts, then re-measure free space."""
    before = free_space_bytes(data_dir)
    removed: list[str] = []
    relieved = 0
    for path in orphaned_artifacts(data_dir, db_path):
        size = max(0, file_size(path))
        try:
            path.unlink()
        except OSError:
            # A copy that failed to unlink is not relief; leave it a candidate.
            continue
        removed.append(path.name)
        relieved += size
    after = free_space_bytes(data_dir)
    free = after if after is not None else before
    return {
        "freeBytes": int(free or 0),
        "relievedBytes": relieved,
        "artifactsRemoved": sorted(removed),
    }


def space_for_copy(data_dir: Path | str, db_path: Path | str, store: Any) -> dict[str, Any]:
    """Steps 1-4 for a copy that is about to start: relieve, re-measure, decide."""
    database_bytes = int(store.file_sizes().get("databaseBytes") or 0)
    required = rebuild_required_free_bytes(database_bytes)
    measured = measured_required_bytes(
        staging_bytes=measured_anchor(data_dir, db_path).staging_bytes
    )
    relief = relieve_free_space(data_dir, db_path)
    free = int(relief["freeBytes"])
    return {
        **relief,
        "databaseBytes": database_bytes,
        "requiredBytes": required,
        "measuredRequiredBytes": measured,
        "enough": free_space_satisfied(
            free, required_bytes=required, measured_required_bytes=measured
        ),
    }


def _non_negative_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        candidate = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return candidate if candidate >= 0 else None
