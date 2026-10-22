from __future__ import annotations

from typing import Any

from src.ship.startup_profile import render_startup_summary, summarize_startup_metrics


def _row(event: str, elapsed_ms: int) -> dict[str, object]:
    return {"event": event, "fields": {"elapsedMs": elapsed_ms}}


def test_summarize_startup_metrics_has_no_regressions_under_threshold() -> None:
    rows = [
        _row("desktop_launch_start", 0),
        _row("desktop_site_ready", 400),
        _row("desktop_window_created", 700),
        _row("desktop_shell_window_shown", 900),
        _row("jobs_module_boot_start", 1100),
        _row("jobs_local_data_init_ready", 1300),
        _row("jobs_auth_ready", 1500),
        _row("jobs_first_render", 1800),
        _row("jobs_first_interactive", 2100),
    ]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")

    assert summary["status"] == "passed"
    assert summary["perfRegressions"] == []


def test_summarize_startup_metrics_reports_warning_and_critical_regressions() -> None:
    rows = [
        _row("desktop_launch_start", 0),
        _row("desktop_site_ready", 400),
        _row("desktop_window_created", 700),
        _row("desktop_shell_window_shown", 900),
        _row("jobs_module_boot_start", 1100),
        _row("jobs_local_data_init_ready", 1300),
        _row("jobs_auth_ready", 1500),
        _row("jobs_first_render", 1800),
        _row("jobs_first_interactive", 19000),
    ]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")

    assert summary["status"] == "failed"
    regressions = summary["perfRegressions"]
    assert {
        "stage": "total_launch_to_first_usable_ui",
        "durationMs": 19000,
        "thresholdMs": 18000,
        "severity": "critical",
    }.items() <= regressions[-1].items()
    assert any(row["severity"] == "warning" for row in regressions)
    rendered = render_startup_summary(summary)
    assert "Perf regressions:" in rendered
    assert "total_launch_to_first_usable_ui" in rendered


def test_summarize_startup_metrics_accepts_admin_ready_to_interactive_path() -> None:
    rows = [
        _row("desktop_launch_start", 0),
        _row("desktop_site_ready", 400),
        _row("desktop_window_created", 700),
        _row("desktop_shell_window_shown", 900),
        _row("admin_module_boot_start", 1100),
        _row("admin_ready", 1500),
        _row("admin_first_interactive", 1800),
        _row("admin_ops_health_first_render", 2100),
    ]

    summary = summarize_startup_metrics(rows, page="admin", profile_mode="cold")
    stages = {stage["key"]: stage for stage in summary["stages"]}

    assert summary["status"] == "passed"
    assert summary["firstUsableEvent"] == "admin_first_interactive"
    assert summary["firstUsableMs"] == 1800
    assert summary["missingEvents"] == []
    assert stages["first_render_to_first_interactive"]["startEvent"] == "admin_ready"
    assert stages["first_render_to_first_interactive"]["endEvent"] == "admin_first_interactive"
    assert (
        stages["admin_ready_to_ops_health_first_render"]["endEvent"]
        == "admin_ops_health_first_render"
    )


def test_summarize_startup_metrics_falls_back_to_admin_ready() -> None:
    rows = [
        _row("desktop_launch_start", 0),
        _row("desktop_site_ready", 400),
        _row("desktop_window_created", 700),
        _row("desktop_shell_window_shown", 900),
        _row("admin_module_boot_start", 1100),
        _row("admin_ready", 1500),
    ]

    summary = summarize_startup_metrics(rows, page="admin", profile_mode="cold")

    assert summary["status"] == "passed"
    assert summary["firstUsableEvent"] == "admin_ready"
    assert summary["firstUsableMs"] == 1500
    assert summary["missingEvents"] == []


def test_startup_profile_summary_counts_gate_wrapper_crashes() -> None:
    rows: list[dict[str, Any]] = [
        _row("desktop_launch_jobs_report_gate", 0),
        {
            "event": "jobs_row_artifact_quarantine_failed",
            "fields": {"process": "site", "error": "boom"},
        },
        {
            "event": "jobs_row_artifact_quarantine_failed",
            "fields": {"process": "bridge", "error": "boom"},
        },
        {
            "event": "jobs_row_artifact_quarantine_failed",
            "fields": {"process": "site", "error": "boom again"},
        },
    ]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")

    assert summary["jobsReportGate"] == {
        "quarantineFailureCount": 3,
        "quarantineFailureProcesses": ["bridge", "site"],
    }
    rendered = render_startup_summary(summary)
    assert "Gate wrapper crashes: 3 (bridge, site)" in rendered


def test_startup_profile_summary_reports_zero_gate_wrapper_crashes() -> None:
    rows: list[dict[str, Any]] = [_row("desktop_launch_jobs_report_gate_skipped", 0)]

    summary = summarize_startup_metrics(rows, page="jobs", profile_mode="cold")

    assert summary["jobsReportGate"] == {
        "quarantineFailureCount": 0,
        "quarantineFailureProcesses": [],
    }
    assert "Gate wrapper crashes" not in render_startup_summary(summary)


def test_render_startup_summary_reports_the_spawn_clock_when_recorded() -> None:
    summary = {
        "page": "jobs",
        "profileMode": "cold",
        "classification": "desktop bridge startup delayed",
        "stages": [],
        "spawnClock": {
            "bootMs": 120,
            "windowShownMs": 1424,
            "firstInteractiveMs": 1348,
            "firstInteractiveClock": "page",
            "firstInteractiveArrivalMs": 1540,
        },
    }

    rendered = render_startup_summary(summary)

    assert "Spawn clock:" in rendered
    assert "- Spawn -> launcher start: 120ms" in rendered
    assert "- Spawn -> window shown: 1424ms" in rendered
    assert "- Spawn -> first interactive: 1348ms (page clock; bridge arrival 1540ms)" in rendered


def test_render_startup_summary_shows_an_arrival_clock_without_a_page_clock() -> None:
    summary = {
        "page": "jobs",
        "profileMode": "cold",
        "classification": "desktop bridge startup delayed",
        "stages": [],
        "spawnClock": {
            "bootMs": 120,
            "windowShownMs": 1424,
            "firstInteractiveMs": 1540,
            "firstInteractiveClock": "arrival",
            "firstInteractiveArrivalMs": None,
        },
    }

    rendered = render_startup_summary(summary)

    assert "- Spawn -> first interactive: 1540ms (arrival clock)" in rendered
    assert "bridge arrival" not in rendered


def test_render_startup_summary_omits_the_spawn_clock_when_absent() -> None:
    summary = {
        "page": "jobs",
        "profileMode": "cold",
        "classification": "desktop bridge startup delayed",
        "stages": [],
    }

    assert "Spawn clock:" not in render_startup_summary(summary)
