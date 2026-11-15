"""The reclaim verdict is the read-only gate the whole operation leans on.

These cases pin the two directions that matter independently: a database whose
history sits inside the retention window must not read as bloat, and a first-run
or reset database with no published feed must refuse rather than measure its own
rows as disposable.
"""

from __future__ import annotations

from typing import Any

from src.storage.reclaim_policy import ReclaimContext, reclaim_verdict

MIB = 1024 * 1024
GIB = 1024 * MIB


def _preview(
    *,
    current_generation: str = "gen-2026-11-15",
    current_bytes: int = 40 * MIB,
    historical_bytes: int = 0,
    historical_count: int = 0,
    candidates: int = 0,
    cap_reached: bool = False,
    **overrides: Any,
) -> dict[str, Any]:
    preview: dict[str, Any] = {
        "currentGeneration": current_generation,
        "currentGenerationEstimatedBytes": current_bytes,
        "historicalGenerationEstimatedBytes": historical_bytes,
        "historicalGenerationCount": historical_count,
        "estimatedReclaimableBytes": historical_bytes,
        "candidateGenerationCount": candidates,
        "capReached": cap_reached,
    }
    preview.update(overrides)
    return preview


def _sizes(*, database_bytes: int, freelist_bytes: int = 0) -> dict[str, int]:
    return {"databaseBytes": database_bytes, "freelistBytes": freelist_bytes}


def _quiet_context(**overrides: Any) -> ReclaimContext:
    """Enough free space that only the blocker under test can fire."""

    values: dict[str, Any] = {"free_space_bytes": 32 * GIB}
    values.update(overrides)
    return ReclaimContext(**values)


def test_verdict_calls_disposable_history_bloat() -> None:
    result = reclaim_verdict(
        _preview(historical_bytes=1536 * MIB, historical_count=9, candidates=4, cap_reached=True),
        _sizes(database_bytes=10 * GIB),
        context=_quiet_context(),
    )

    assert result["verdict"] == "bloat"
    assert result["ratio"] == 0.15
    assert result["reclaimable"] == 1536 * MIB
    assert result["reclaimableBasis"] == "payload-sample+freelist"
    assert result["blockers"] == []


def test_verdict_keeps_a_database_under_the_ratio_ok() -> None:
    result = reclaim_verdict(
        _preview(historical_bytes=900 * MIB, historical_count=3),
        _sizes(database_bytes=10 * GIB),
        context=_quiet_context(),
    )

    assert result["reclaimable"] >= 512 * MIB
    assert result["ratio"] == 900 * MIB / (10 * GIB)
    assert result["verdict"] == "ok"
    assert result["blockers"] == []


def test_verdict_requires_the_absolute_floor_as_well_as_the_ratio() -> None:
    small_file = reclaim_verdict(
        _preview(historical_bytes=500 * MIB, historical_count=2),
        _sizes(database_bytes=1 * GIB),
        context=_quiet_context(),
    )

    assert small_file["ratio"] > 0.4
    assert small_file["verdict"] == "ok"

    # Both gates are inclusive: exactly 512 MiB of exactly 10% is bloat.
    boundary = reclaim_verdict(
        _preview(historical_bytes=512 * MIB, historical_count=2),
        _sizes(database_bytes=5 * GIB),
        context=_quiet_context(),
    )

    assert boundary["ratio"] == 0.1
    assert boundary["verdict"] == "bloat"


def test_ratio_knob_moves_the_line_without_moving_the_floor() -> None:
    result = reclaim_verdict(
        _preview(historical_bytes=700 * MIB, historical_count=2),
        _sizes(database_bytes=10 * GIB),
        context=_quiet_context(),
        min_ratio_pct=5,
    )

    assert result["ratio"] == 700 * MIB / (10 * GIB)
    assert result["verdict"] == "bloat"


def test_free_pages_count_toward_reclaimable() -> None:
    result = reclaim_verdict(
        _preview(historical_bytes=0),
        _sizes(database_bytes=10 * GIB, freelist_bytes=1 * GIB),
        context=_quiet_context(),
    )

    assert result["reclaimable"] == 1 * GIB
    assert result["ratio"] == 0.1
    assert result["verdict"] == "bloat"


def test_no_current_generation_refuses_even_when_the_ratio_says_bloat() -> None:
    result = reclaim_verdict(
        _preview(current_generation="", historical_bytes=600 * MIB, historical_count=2),
        _sizes(database_bytes=1 * GIB),
        context=_quiet_context(),
    )

    assert result["ratio"] > 0.5
    assert result["verdict"] == "ok"
    assert result["blockers"] == ["no-current-generation"]


def test_unreadable_store_reports_store_unavailable_and_no_ratio() -> None:
    result = reclaim_verdict(
        _preview(historical_bytes=2 * GIB, historical_count=4),
        None,
        context=_quiet_context(),
    )

    assert result["verdict"] == "ok"
    assert result["ratio"] == 0.0
    assert result["reclaimable"] == 2 * GIB
    assert result["blockers"] == ["store-unavailable"]


def test_missing_sample_fields_report_sample_unavailable() -> None:
    preview = _preview(historical_bytes=2 * GIB, historical_count=4)
    preview.pop("historicalGenerationEstimatedBytes")

    result = reclaim_verdict(preview, _sizes(database_bytes=10 * GIB), context=_quiet_context())

    assert result["verdict"] == "ok"
    assert result["ratio"] == 0.0
    assert result["blockers"] == ["sample-unavailable"]


def test_reclaimable_ignores_a_preview_total_that_disagrees_with_its_parts() -> None:
    result = reclaim_verdict(
        _preview(historical_bytes=0, estimatedReclaimableBytes=9 * GIB),
        _sizes(database_bytes=10 * GIB),
        context=_quiet_context(),
    )

    assert result["reclaimable"] == 0
    assert result["estimatedReclaimableBytes"] == 0
    assert result["verdict"] == "ok"


def test_free_space_fails_closed_when_unmeasured_or_non_positive() -> None:
    unmeasured = reclaim_verdict(
        _preview(historical_bytes=2 * GIB, historical_count=4),
        _sizes(database_bytes=10 * GIB),
    )
    failed_stat = reclaim_verdict(
        _preview(historical_bytes=2 * GIB, historical_count=4),
        _sizes(database_bytes=10 * GIB),
        context=_quiet_context(free_space_bytes=0),
    )
    short = reclaim_verdict(
        _preview(historical_bytes=2 * GIB, historical_count=4),
        _sizes(database_bytes=10 * GIB),
        context=_quiet_context(free_space_bytes=64 * MIB),
    )

    assert unmeasured["blockers"] == ["free-space"]
    assert failed_stat["blockers"] == ["free-space"]
    assert short["blockers"] == ["free-space"]
    assert unmeasured["verdict"] == "bloat"


def test_activity_and_trigger_blockers_leave_the_measurement_alone() -> None:
    result = reclaim_verdict(
        _preview(historical_bytes=2 * GIB, historical_count=4),
        _sizes(database_bytes=10 * GIB),
        context=_quiet_context(
            active_task=True,
            compaction_running=True,
            backup_exists=True,
            cooldown_remaining_hours=42.5,
            auto_enabled=False,
        ),
    )

    assert result["verdict"] == "bloat"
    assert result["cooldownRemainingHours"] == 42.5
    assert result["blockers"] == [
        "active-task",
        "compaction-running",
        "backup-exists",
        "cooldown",
        "auto-disabled",
    ]


def test_free_space_is_asked_against_the_existing_rebuild_gate() -> None:
    sizes = _sizes(database_bytes=10 * GIB)
    generous = reclaim_verdict(
        _preview(historical_bytes=2 * GIB, historical_count=4),
        sizes,
        context=_quiet_context(free_space_bytes=330 * MIB),
    )
    short = reclaim_verdict(
        _preview(historical_bytes=2 * GIB, historical_count=4),
        sizes,
        context=_quiet_context(free_space_bytes=300 * MIB),
    )

    # The rebuild gate asks 256 MiB + 64 MiB when 1% of the file is smaller.
    assert generous["blockers"] == []
    assert short["blockers"] == ["free-space"]


def test_zero_database_bytes_is_a_zero_ratio_not_a_division_error() -> None:
    result = reclaim_verdict(
        _preview(historical_bytes=2 * GIB, historical_count=4),
        _sizes(database_bytes=0),
        context=_quiet_context(),
    )

    assert result["ratio"] == 0.0
    assert result["verdict"] == "ok"
