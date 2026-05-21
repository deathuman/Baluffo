from src.ship.startup_probe_policy import (
    classify_startup_probe_failure,
    refine_startup_probe_summary,
)


def test_classify_startup_probe_failure_marks_early_browser_exit_with_missing_page_events() -> None:
    rows = [
        {
            "event": "desktop_browser_launch_selected",
            "fields": {"mode": "chromium-app", "browser": "chrome"},
        },
        {
            "event": "desktop_browser_process_exited_waiting_for_bridge",
            "fields": {
                "browser": "chrome",
                "browserPath": "C:/Playwright/chrome.exe",
                "browserProfileDirHash": "abc123def456",
                "returnCode": 87,
            },
        },
    ]

    classification, category = classify_startup_probe_failure(
        rows,
        summary={"missingEvents": ["jobs_first_render", "jobs_first_interactive"]},
    )

    assert classification == "browser runtime startup failed"
    assert category == "browser_runtime_startup_failed"


def test_refine_startup_probe_summary_reports_browser_exit_missing_events() -> None:
    rows = [
        {"event": "desktop_browser_launch_selected", "fields": {"mode": "chromium-app"}},
        {
            "event": "desktop_browser_process_exited_waiting_for_bridge",
            "fields": {"returnCode": 87, "browserProfileDirHash": "abc123def456"},
        },
    ]

    summary = refine_startup_probe_summary(
        {"missingEvents": ["desktop_site_ready", "jobs_first_interactive"]},
        rows,
    )

    assert summary["browserExitedBeforeHandoff"] is True
    assert summary["browserExitReturnCode"] == 87
    assert summary["browserExitProfileDirHash"] == "abc123def456"
    assert summary["browserExitMissingEvents"] == ["jobs_first_interactive"]
