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
# The verdict runs a bounded cleanup preview, while the reclaim modal polls its
# route; this is how long the route may answer with the verdict it already has.
RECLAIM_VERDICT_TTL_S = 60.0


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
    still reads ``bloat`` with ``cooldown`` listed. ``cooldown`` and
    ``auto-disabled`` guard only the automatic trigger; an explicit start
    overrides those two and never a safety blocker.
    """
    ctx = context or ReclaimContext()
    blockers: list[str] = []

    sizes = file_sizes or {}
    database_bytes = _non_negative_int(sizes.get("databaseBytes"))
    store_readable = database_bytes is not None
    database_bytes = database_bytes or 0
    freelist_bytes = (_non_negative_int(sizes.get("freelistBytes")) or 0) if store_readable else 0
    if not store_readable:
        blockers.append("store-unavailable")

    samples = preview or {}
    current_generation = str(samples.get("currentGeneration") or "").strip()
    current_bytes = _non_negative_int(samples.get("currentGenerationEstimatedBytes"))
    historical_bytes = _non_negative_int(samples.get("historicalGenerationEstimatedBytes"))
    sample_readable = current_bytes is not None and historical_bytes is not None
    if not sample_readable:
        blockers.append("sample-unavailable")
    current_bytes = current_bytes or 0
    historical_bytes = historical_bytes or 0

    if not current_generation:
        # With no published feed there is no protected generation to define
        # "history": every feed row would count as reclaimable, so the ratio is
        # meaningless rather than conservative.
        blockers.append("no-current-generation")

    reclaimable = historical_bytes + freelist_bytes
    ratio = (reclaimable / database_bytes) if database_bytes > 0 else 0.0
    ratio_met = database_bytes > 0 and ratio >= float(min_ratio_pct) / 100.0
    floor_met = reclaimable >= RECLAIM_ABSOLUTE_FLOOR_BYTES
    measured = store_readable and sample_readable
    verdict = "bloat" if measured and current_generation and ratio_met and floor_met else "ok"

    if ctx.active_task:
        blockers.append("active-task")
    if ctx.compaction_running:
        blockers.append("compaction-running")
    if ctx.backup_exists:
        blockers.append("backup-exists")
    required_free = rebuild_required_free_bytes(database_bytes)
    free_space = ctx.free_space_bytes
    if free_space is None or int(free_space) <= 0 or int(free_space) < required_free:
        blockers.append("free-space")
    cooldown_hours = max(0.0, float(ctx.cooldown_remaining_hours or 0.0))
    if cooldown_hours > 0:
        blockers.append("cooldown")
    if not ctx.auto_enabled:
        blockers.append("auto-disabled")

    return {
        "verdict": verdict,
        "ratio": ratio,
        "reclaimable": reclaimable,
        "databaseBytes": database_bytes,
        "freelistBytes": freelist_bytes,
        "currentGenerationEstimatedBytes": current_bytes,
        "historicalGenerationEstimatedBytes": historical_bytes,
        "historicalGenerationCount": _non_negative_int(samples.get("historicalGenerationCount"))
        or 0,
        "estimatedReclaimableBytes": reclaimable,
        "reclaimableBasis": "payload-sample+freelist",
        "candidateGenerationCount": _non_negative_int(samples.get("candidateGenerationCount")) or 0,
        "capReached": bool(samples.get("capReached")),
        "blockers": blockers,
        "cooldownRemainingHours": cooldown_hours,
    }


def _non_negative_int(value: Any) -> int | None:
    """A non-negative integer from a payload field, or ``None`` when it is not one."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return None if number < 0 else number
