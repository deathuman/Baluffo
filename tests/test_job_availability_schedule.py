from src.jobs.availability_schedule import build_availability_sweep_plan


def test_sweep_prioritizes_saved_and_degrades_instead_of_exceeding_safe_limits() -> None:
    lifecycle = {
        "saved": {
            "availabilityId": "availability_saved",
            "availabilityStatus": "available",
            "availabilityVerifiedAt": "2026-07-01T00:00:00+00:00",
            "jobLink": "https://jobs.example.com/saved",
        },
        "old": {
            "availabilityId": "availability_old",
            "availabilityStatus": "available",
            "availabilityVerifiedAt": "2026-06-01T00:00:00+00:00",
            "jobLink": "https://jobs.example.com/old",
        },
    }
    plan = build_availability_sweep_plan(
        lifecycle,
        {"rows": [{"availabilityId": "availability_saved", "priority": "saved_daily"}]},
        finished_at="2026-07-14T00:00:00+00:00",
        max_checks=1,
        per_domain_limit=1,
    )

    assert plan["rows"][0]["availabilityId"] == "availability_saved"
    assert plan["deferredCount"] == 1
    assert plan["degradedCoverage"] is True
    assert plan["healthTargetMet"] is False


def test_sweep_keeps_unavailable_saved_jobs_but_not_ordinary_unavailable_rows() -> None:
    lifecycle = {
        "active-saved": {
            "availabilityId": "availability_active_saved",
            "availabilityStatus": "unavailable",
            "jobLink": "https://one.example/jobs/1",
        },
        "terminal-saved": {
            "availabilityId": "availability_terminal_saved",
            "availabilityStatus": "unavailable",
            "jobLink": "https://two.example/jobs/2",
        },
        "ordinary": {
            "availabilityId": "availability_ordinary",
            "availabilityStatus": "unavailable",
            "jobLink": "https://three.example/jobs/3",
        },
    }
    plan = build_availability_sweep_plan(
        lifecycle,
        {
            "rows": [
                {"availabilityId": "availability_terminal_saved", "priority": "saved_rotation"},
                {"availabilityId": "availability_active_saved", "priority": "saved_daily"},
            ]
        },
        finished_at="2026-07-14T00:00:00+00:00",
    )

    assert [row["availabilityId"] for row in plan["rows"]] == [
        "availability_active_saved",
        "availability_terminal_saved",
    ]
    assert plan["activeCount"] == 0
    assert plan["verifiedWithinSevenDaysCount"] == 0


def test_sweep_includes_private_custom_rows_without_counting_them_in_public_health() -> None:
    plan = build_availability_sweep_plan(
        {
            "canonical": {
                "availabilityId": "availability_public",
                "availabilityStatus": "available",
                "availabilityVerifiedAt": "2026-07-14T00:00:00+00:00",
                "jobLink": "https://public.example/jobs/1",
            }
        },
        {
            "schemaVersion": 2,
            "rows": [
                {
                    "availabilityId": "availability_custom_1",
                    "jobLink": "https://custom.example/jobs/2",
                    "priority": "saved_daily",
                    "scope": "custom_saved",
                }
            ],
        },
        finished_at="2026-07-14T00:00:00+00:00",
    )

    custom = next(row for row in plan["rows"] if row["availabilityId"] == "availability_custom_1")
    assert custom["scope"] == "custom_saved"
    assert plan["activeCount"] == 1
    assert plan["verifiedWithinSevenDaysCoverage"] == 1.0


def test_sweep_rotates_from_durable_direct_checkpoints() -> None:
    lifecycle = {
        "checked": {
            "availabilityId": "availability_checked",
            "availabilityStatus": "available",
            "availabilityVerifiedAt": "2026-07-14T00:00:00+00:00",
            "jobLink": "https://one.example/jobs/checked",
        },
        "unchecked": {
            "availabilityId": "availability_unchecked",
            "availabilityStatus": "available",
            "availabilityVerifiedAt": "2026-07-14T00:00:00+00:00",
            "jobLink": "https://two.example/jobs/unchecked",
        },
    }

    plan = build_availability_sweep_plan(
        lifecycle,
        None,
        finished_at="2026-07-14T12:00:00+00:00",
        direct_checkpoints={
            "rows": [
                {
                    "availabilityId": "availability_checked",
                    "checkedAt": "2026-07-14T11:00:00+00:00",
                }
            ]
        },
    )

    assert [row["availabilityId"] for row in plan["rows"]] == [
        "availability_unchecked",
        "availability_checked",
    ]
    assert plan["directCheckedWithinSevenDaysCount"] == 1
    assert plan["directCheckedWithinSevenDaysCoverage"] == 0.5
