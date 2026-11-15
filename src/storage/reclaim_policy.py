"""Automatic bloat reclaim: the read-only verdict leaf.

The verdict answers one question — is enough of the database file space that the
retention policy has already declared disposable — and says, by blocked reason,
why it will not act on the answer.

It reads the cleanup preview's payload estimates and the file sizes, and it never
subtracts payload from allocated table sizes: ``tableSizes`` counts pages
including the current generation's indexes and page overhead, so the difference
would call current-feed overhead reclaimable and could call bloat on a database
whose history is still inside the retention window. ``reclaimable`` is instead
the preview's payload sample plus the exact freelist bytes, a deliberate lower
bound that can delay a reclaim but cannot manufacture one.

Pure by design: no store, no clock, no journal. Callers pass the preview, the
sizes, and the conditions they already know about, and get one payload back.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from src.storage.current_feed_rebuild import rebuild_required_free_bytes

RECLAIM_ABSOLUTE_FLOOR_BYTES = 512 * 1024 * 1024
RECLAIM_MIN_RATIO_PCT_DEFAULT = 10.0
RECLAIMABLE_BASIS = "payload-sample+freelist"
# The verdict runs a bounded cleanup preview, while the reclaim modal polls its
# route; this is how long the route may answer with the verdict it already has.
RECLAIM_VERDICT_TTL_S = 60.0

BLOCKER_NO_CURRENT_GENERATION = "no-current-generation"
BLOCKER_ACTIVE_TASK = "active-task"
BLOCKER_COMPACTION_RUNNING = "compaction-running"
BLOCKER_BACKUP_EXISTS = "backup-exists"
BLOCKER_FREE_SPACE = "free-space"
BLOCKER_COOLDOWN = "cooldown"
BLOCKER_AUTO_DISABLED = "auto-disabled"
BLOCKER_STORE_UNAVAILABLE = "store-unavailable"
BLOCKER_SAMPLE_UNAVAILABLE = "sample-unavailable"


@dataclass(frozen=True)
class ReclaimContext:
    """Conditions the verdict is told about rather than measuring itself.

    Defaults describe a quiet store with the automatic trigger enabled. Free
    space has no default on purpose: an unmeasured reading fails closed, because
    a reclaim that guesses can fill the data disk.
    """

    active_task: bool = False
    compaction_running: bool = False
    backup_exists: bool = False
    free_space_bytes: int | None = None
    required_free_bytes: int | None = None
    cooldown_remaining_hours: float = 0.0
    auto_enabled: bool = True


def reclaim_verdict(
    preview: Mapping[str, Any] | None,
    file_sizes: Mapping[str, Any] | None,
    *,
    context: ReclaimContext | None = None,
    min_ratio_pct: float = RECLAIM_MIN_RATIO_PCT_DEFAULT,
) -> dict[str, Any]:
    """Decide whether a database is bloated and why a reclaim could not start.

    ``verdict`` is the measurement: ``bloat`` when the reclaimable payload lower
    bound clears both the ratio knob and the 512 MiB floor. ``blockers`` gates a
    start and never changes the measurement, so a bloated database under cooldown
    still reads ``bloat`` with ``cooldown`` listed.
    """
    ctx = context or ReclaimContext()
    blockers: list[str] = []

    sizes = dict(file_sizes or {})
    store_readable = "databaseBytes" in sizes
    database_bytes = _byte_count(sizes.get("databaseBytes")) if store_readable else 0
    freelist_bytes = _byte_count(sizes.get("freelistBytes")) if store_readable else 0
    if not store_readable:
        blockers.append(BLOCKER_STORE_UNAVAILABLE)

    samples = dict(preview or {})
    current_generation = str(samples.get("currentGeneration") or "").strip()
    current_bytes = _optional_byte_count(samples.get("currentGenerationEstimatedBytes"))
    historical_bytes = _optional_byte_count(samples.get("historicalGenerationEstimatedBytes"))
    if current_bytes is None or historical_bytes is None:
        blockers.append(BLOCKER_SAMPLE_UNAVAILABLE)
        current_bytes = current_bytes or 0
        historical_bytes = historical_bytes or 0

    if not current_generation:
        # With no published feed there is no protected generation to define
        # "history": every feed row would count as reclaimable, so the ratio is
        # meaningless rather than conservative.
        blockers.append(BLOCKER_NO_CURRENT_GENERATION)

    reclaimable = historical_bytes + freelist_bytes
    ratio = (reclaimable / database_bytes) if database_bytes > 0 else 0.0
    ratio_met = database_bytes > 0 and ratio >= float(min_ratio_pct) / 100.0
    floor_met = reclaimable >= RECLAIM_ABSOLUTE_FLOOR_BYTES
    measured = (
        BLOCKER_STORE_UNAVAILABLE not in blockers and BLOCKER_SAMPLE_UNAVAILABLE not in blockers
    )
    verdict = "bloat" if measured and current_generation and ratio_met and floor_met else "ok"

    if ctx.active_task:
        blockers.append(BLOCKER_ACTIVE_TASK)
    if ctx.compaction_running:
        blockers.append(BLOCKER_COMPACTION_RUNNING)
    if ctx.backup_exists:
        blockers.append(BLOCKER_BACKUP_EXISTS)
    required_free = (
        _byte_count(ctx.required_free_bytes)
        if ctx.required_free_bytes is not None
        else rebuild_required_free_bytes(database_bytes)
    )
    free_space = ctx.free_space_bytes
    if free_space is None or int(free_space) <= 0 or int(free_space) < required_free:
        blockers.append(BLOCKER_FREE_SPACE)
    cooldown_hours = max(0.0, float(ctx.cooldown_remaining_hours or 0.0))
    if cooldown_hours > 0:
        blockers.append(BLOCKER_COOLDOWN)
    if not ctx.auto_enabled:
        blockers.append(BLOCKER_AUTO_DISABLED)

    return {
        "verdict": verdict,
        "ratio": ratio,
        "reclaimable": reclaimable,
        "databaseBytes": database_bytes,
        "freelistBytes": freelist_bytes,
        "currentGenerationEstimatedBytes": current_bytes,
        "historicalGenerationEstimatedBytes": historical_bytes,
        "historicalGenerationCount": _byte_count(samples.get("historicalGenerationCount")),
        "estimatedReclaimableBytes": reclaimable,
        "reclaimableBasis": RECLAIMABLE_BASIS,
        "candidateGenerationCount": _byte_count(samples.get("candidateGenerationCount")),
        "capReached": bool(samples.get("capReached")),
        "blockers": blockers,
        "cooldownRemainingHours": cooldown_hours,
    }


def _byte_count(value: Any) -> int:
    try:
        return max(0, int(value))
    except (TypeError, ValueError, OverflowError):
        return 0


def _optional_byte_count(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return None if number < 0 else number
