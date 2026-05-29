from __future__ import annotations

import src.jobs.state_source_state as jobs_state


def _structured_report(
    *,
    name: str,
    adapter: str,
    status: str,
    kept_count: int,
    duplicate_rate: float,
    duration_ms: int = 0,
) -> dict[str, object]:
    return {
        "name": name,
        "adapter": adapter,
        "status": status,
        "durationMs": duration_ms,
        "fetchedCount": kept_count,
        "keptCount": kept_count,
        "duplicateRate": duplicate_rate,
        "details": [{"name": name, "stats": {}}],
    }


def test_structured_migration_promotes_after_three_healthy_runs_and_demotes_on_regression() -> None:
    source_name = "Wolcen Studios (Manual Website)"
    source_row = {
        "name": source_name,
        "pages": ["https://wolcenstudios.bamboohr.com/jobs/"],
    }
    state_rows: dict[str, dict[str, object]] = {
        source_name: {
            "lastDurationMs": 6200,
            "lastStatus": "ok",
            "lastError": "",
            "lastFailureBucket": "static_listing",
            "lastKeptCount": 1,
        }
    }

    for idx in range(3):
        state_rows = jobs_state.update_source_state_rows(
            source_state_rows=state_rows,
            source_reports=[
                _structured_report(
                    name=source_name,
                    adapter="bamboohr",
                    status="ok",
                    kept_count=2,
                    duplicate_rate=0.0,
                    duration_ms=5400 - (idx * 200),
                )
            ],
            canonical_rows=[],
            finished_at=f"2026-03-26T10:0{idx}:00Z",
            circuit_breaker_failures=3,
            circuit_breaker_cooldown_minutes=30,
        )

    promoted = state_rows[source_name]
    assert promoted["structuredMigrationBaselineCapturedAt"] == "2026-03-26T10:00:00Z"
    assert promoted["structuredMigrationBaselineDurationMs"] == 6200
    assert promoted["structuredMigrationBaselineStatus"] == "ok"
    assert promoted["structuredMigrationBaselineError"] == ""
    assert promoted["structuredMigrationBaselineFailureBucket"] == "static_listing"
    assert promoted["structuredMigrationBaselineKeptCount"] == 1
    assert promoted["structuredMigrationTargetAdapter"] == "bamboohr"
    assert promoted["structuredMigrationShadowRunCount"] == 3
    assert promoted["structuredMigrationHealthyRunCount"] == 3
    assert promoted["structuredMigrationPromotedAt"] == "2026-03-26T10:02:00Z"
    assert jobs_state.should_skip_static_source_for_structured_migration(
        source_name, source_row, state_rows
    )

    regressed_state = jobs_state.update_source_state_rows(
        source_state_rows=state_rows,
        source_reports=[
            _structured_report(
                name=source_name,
                adapter="bamboohr",
                status="ok",
                kept_count=0,
                duplicate_rate=0.5,
                duration_ms=6100,
            )
        ],
        canonical_rows=[],
        finished_at="2026-03-26T10:03:00Z",
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=30,
    )

    regressed = regressed_state[source_name]
    assert regressed["structuredMigrationShadowRunCount"] == 4
    assert regressed["structuredMigrationHealthyRunCount"] == 0
    assert regressed["structuredMigrationDemotedAt"] == "2026-03-26T10:03:00Z"
    assert not jobs_state.should_skip_static_source_for_structured_migration(
        source_name, source_row, regressed_state
    )
