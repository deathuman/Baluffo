from __future__ import annotations

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
