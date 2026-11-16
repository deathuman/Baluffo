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
# A measured gate is a snapshot of one copy. The feed grows, so an anchor older
# than this is reported stale — not wrong, but no longer a description of the
# database that would be copied now.
RECLAIM_ANCHOR_STALE_AFTER_S = 30 * 24 * 60 * 60


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
    # Full-disk relief (§3): the measured staging gate, the copy size it is built
    # from, the last rehearsal's own verdict on that margin, and the bytes an
    # unowned artifact removal would release.
    measured_required_bytes: int | None = None
    measured_staging_bytes: int | None = None
    measured_staging_age_s: int | None = None
    last_rehearsal_gate_adequate: bool | None = None
    relievable_bytes: int = 0
    cooldown_remaining_hours: float = 0.0
    auto_enabled: bool = True
    # Auto mode's own state: the operator's switch (``None`` when unset, so the
    # host may arm itself) and the journal's arm record. The verdict reports both
    # so the panel can say why automatic starts are allowed without opening the
    # raw journal.
    auto_switch: bool | None = None
    auto_arm: Mapping[str, Any] | None = None


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
    relievable = max(0, int(ctx.relievable_bytes or 0))
    measured_gate = ctx.measured_required_bytes
    # An unreadable timestamp is not age zero: report "unknown", never "fresh".
    age = None if ctx.measured_staging_age_s is None else max(0, int(ctx.measured_staging_age_s))
    available = None if free_space is None else int(free_space) + relievable
    if not free_space_satisfied(
        available, required_bytes=required_free, measured_required_bytes=measured_gate
    ):
        blockers.append("free-space")
    # The measured gate is the operative ask once it exists; the heuristic stays
    # in the payload so the panel can show both numbers.
    operative_gate = required_free if measured_gate is None else max(0, int(measured_gate))
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
        "freeBytes": int(free_space or 0),
        "requiredBytes": required_free,
        "measuredRequiredBytes": measured_gate,
        "measuredStagingBytes": max(0, int(ctx.measured_staging_bytes or 0)),
        "measuredStagingAgeS": age,
        "measuredStagingStale": anchor_is_stale(age),
        "lastRehearsalGateAdequate": ctx.last_rehearsal_gate_adequate,
        "relievableBytes": relievable,
        "shortfallBytes": max(0, operative_gate - (available or 0)),
        "measuredFreeSpaceOk": bool(
            measured_gate is not None and (available or 0) >= int(measured_gate)
        ),
        "autoStart": auto_start_summary(ctx),
    }


def auto_start_summary(ctx: ReclaimContext) -> dict[str, Any]:
    """Why automatic starts are, or are not, allowed here, in one object.

    ``switch`` is the operator's word, ``armed`` and its evidence are the host's
    measurement, and ``reason`` is the one that actually decided: a set switch
    wins outright, so a kill switch reads as ``kill-switch`` even next to an armed
    rehearsal, and ``source`` says which of the two is in force.
    """
    switch = ctx.auto_switch
    arm = ctx.auto_arm if isinstance(ctx.auto_arm, Mapping) else {}
    armed = bool(arm.get("armed"))
    evidence = arm.get("evidence")
    if switch is True:
        source, reason = "env", "forced-on"
    elif switch is False:
        source, reason = "env", "kill-switch"
    elif armed:
        source, reason = "rehearsal", str(arm.get("reason") or "rehearsal-adequate")
    else:
        source, reason = "none", str(arm.get("reason") or "not-armed")
    return {
        "enabled": bool(ctx.auto_enabled),
        "source": source,
        "switch": "on" if switch is True else "off" if switch is False else "unset",
        "armed": armed,
        "reason": reason,
        "decidedAt": str(arm.get("decidedAt") or ""),
        "evidence": dict(evidence) if isinstance(evidence, Mapping) else None,
    }


def anchor_is_stale(age_s: int | None) -> bool | None:
    """Whether a measured gate's age has outlived its usefulness.

    ``None`` is the unmeasured-or-undated answer, and it is not a stale one: a host
    that never measured has nothing to re-measure, and an unreadable stamp is
    unknown rather than old. The verdict and the trigger that heals the anchor must
    agree, so both call this.
    """
    return None if age_s is None else int(age_s) >= RECLAIM_ANCHOR_STALE_AFTER_S


def auto_arm_decision(
    *,
    status: str,
    result: Mapping[str, Any],
    anchor_age_s: int | None,
    decided_at: str,
) -> dict[str, Any]:
    """The enablement decision a finished rehearsal justifies, with its evidence.

    Auto mode arms itself on measurement: a completed rehearsal is the first time
    the host knows what a copy actually costs, so it is the first time "is this
    gate adequate" has an answer. ``gateAdequate`` is ``None`` on a host that had
    no gate to breach — the first measurement, not a failure — so only an explicit
    ``False`` withholds the arm. The anchor must be fresh: a rehearsal that
    finished is not evidence about a database that has since outgrown it.
    """
    adequate = result.get("gateAdequate")
    evidence = {
        "runId": str(result.get("runId") or ""),
        "stagingBytes": _non_negative_int(result.get("stagingBytes")) or 0,
        "gateBytes": _non_negative_int(result.get("measuredGateBytes")),
        "gateAdequate": None if adequate is None else bool(adequate),
        "anchorAgeS": anchor_age_s,
    }
    reason = "rehearsal-adequate"
    armed = True
    if status != "complete":
        armed, reason = False, "rehearsal-failed"
    elif anchor_is_stale(anchor_age_s) is not False:
        # Unknown or old: the measurement no longer describes this database.
        armed, reason = False, "anchor-stale"
    elif evidence["gateAdequate"] is False:
        armed, reason = False, "rehearsal-inadequate"
    return {"armed": armed, "reason": reason, "decidedAt": decided_at, "evidence": evidence}


def free_space_satisfied(
    free_bytes: int | None,
    *,
    required_bytes: int,
    measured_required_bytes: int | None = None,
) -> bool:
    """Whether free space clears the gate; the measured gate wins when it exists.

    A non-positive reading is a failed ``stat``, not a small disk, so it fails
    closed rather than letting a copy guess.
    """
    if free_bytes is None or int(free_bytes) <= 0:
        return False
    gate = int(required_bytes) if measured_required_bytes is None else int(measured_required_bytes)
    return int(free_bytes) >= gate


def _non_negative_int(value: Any) -> int | None:
    """A non-negative integer from a payload field, or ``None`` when it is not one."""
    if value is None or isinstance(value, bool):
        return None
    try:
        number = int(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return None if number < 0 else number
