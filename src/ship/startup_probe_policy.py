#!/usr/bin/env python3
"""Probe-only startup policy and report helpers.

This module owns startup-probe semantics such as required events, managed
browser policy, and failure classification. It intentionally does not launch the
runtime or emit metrics; the regular desktop runtime imports only the minimal
telemetry helpers needed to observe real startup behavior.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, Protocol

STARTUP_REQUIRED_EVENTS = (
    "desktop_launch_start",
    "desktop_site_ready",
    "desktop_window_created",
    "desktop_shell_window_shown",
)
STARTUP_EVENT_ALIASES = {
    "desktop_shell_window_shown": (
        "desktop_shell_window_shown",
        "desktop_shell_window_shown_inferred",
    ),
}
REQUIRED_STARTUP_PROBE_LAUNCH_MODE = "chromium-app"
EMBEDDED_PAGE_PROBES = (
    {
        "name": "Embedded Jobs Ready",
        "openPath": "jobs.html",
        "requiredEvents": ("jobs_first_render", "jobs_first_interactive"),
    },
    {
        "name": "Embedded Saved Ready",
        "openPath": "saved.html",
        "requiredEvents": ("saved_auth_ready",),
    },
    {
        "name": "Embedded Admin Ready",
        "openPath": "admin.html",
        "requiredEvents": ("admin_ready", "admin_first_interactive"),
    },
)


class ChromiumAppModeSupported(Protocol):
    def __call__(self, candidate: dict[str, Any], *, env: dict[str, str] | None = None) -> bool: ...


def startup_profile_required_events(page: str) -> tuple[str, ...]:
    normalized = (page or "jobs").strip().lower() or "jobs"
    if normalized == "desktop-probe":
        return STARTUP_REQUIRED_EVENTS + (
            "desktop_probe_html_parse_start",
            "desktop_probe_ready",
        )
    if normalized == "desktop-probe-head":
        return STARTUP_REQUIRED_EVENTS + (
            "desktop_probe_head_html_parse_start",
            "desktop_probe_head_ready",
        )
    if normalized == "desktop-probe-css":
        return STARTUP_REQUIRED_EVENTS + (
            "desktop_probe_css_html_parse_start",
            "desktop_probe_css_ready",
        )
    if normalized == "desktop-probe-inline":
        return STARTUP_REQUIRED_EVENTS + (
            "desktop_probe_inline_html_parse_start",
            "desktop_probe_inline_ready",
        )
    page_events = {
        "admin": ("admin_ready", "admin_first_interactive"),
        "saved": ("saved_first_interactive",),
        "jobs": ("jobs_auth_ready", "jobs_first_render", "jobs_first_interactive"),
    }.get(normalized, ("jobs_first_render", "jobs_first_interactive"))
    return STARTUP_REQUIRED_EVENTS + (f"{normalized}_module_boot_start",) + tuple(page_events)


def required_startup_event_present(events: set[str], required_event: str) -> bool:
    aliases = STARTUP_EVENT_ALIASES.get(str(required_event or "").strip(), (required_event,))
    return any(str(alias or "").strip() in events for alias in aliases)


def startup_metric_fields(row: dict[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    payload = row.get("payload")
    if isinstance(payload, dict):
        merged.update(payload)
    fields = row.get("fields")
    if isinstance(fields, dict):
        merged.update(fields)
    return merged


def startup_probe_browser_details(
    rows: list[dict[str, Any]],
    *,
    preferred_browser_name: str = "",
    preferred_browser_path: str = "",
) -> dict[str, str | bool]:
    details: dict[str, str | bool] = {
        "preferredBrowserName": str(preferred_browser_name or "").strip().lower(),
        "preferredBrowserPath": str(preferred_browser_path or "").strip(),
        "selectedBrowserName": "",
        "selectedBrowserPath": "",
        "launchMode": "",
        "launchError": "",
        "launchErrorType": "",
        "windowClosedReason": "",
        "handoffEvidence": "",
        "handoffFailed": False,
    }
    for row in rows:
        event = str(row.get("event") or "").strip()
        fields = startup_metric_fields(row)
        if event == "desktop_browser_launch_selected":
            details["selectedBrowserName"] = str(fields.get("browser") or "").strip().lower()
            details["selectedBrowserPath"] = str(fields.get("browserPath") or "").strip()
            details["launchMode"] = str(fields.get("mode") or "").strip().lower()
        elif event == "desktop_launch_error":
            details["launchError"] = str(fields.get("error") or "").strip()
            details["launchErrorType"] = str(fields.get("errorType") or "").strip()
        elif event == "desktop_window_closed":
            details["windowClosedReason"] = str(fields.get("reason") or "").strip().lower()
        elif event == "desktop_browser_watchdog_handoff_confirmed":
            details["handoffEvidence"] = str(fields.get("evidence") or "").strip().lower()
        elif event == "desktop_browser_watchdog_handoff_failed":
            details["handoffFailed"] = True
    return details


def classify_startup_probe_failure(
    rows: list[dict[str, Any]],
    *,
    error_message: str = "",
    summary: dict[str, Any] | None = None,
) -> tuple[str, str]:
    details = startup_probe_browser_details(rows)
    missing_events = {
        str(event or "").strip() for event in ((summary or {}).get("missingEvents") or []) if event
    }
    error_text = str(error_message or "").strip()
    lowered = error_text.lower()
    if "no supported managed chromium probe browser available" in lowered:
        return "no managed chromium probe browser available", "probe_browser_unavailable"
    if details["launchMode"] == "default-browser":
        return "non-authoritative browser launch", "non_authoritative_browser_launch"
    if details["launchError"] and details["launchMode"] == REQUIRED_STARTUP_PROBE_LAUNCH_MODE:
        return "browser runtime startup failed", "browser_runtime_startup_failed"
    if details["handoffFailed"]:
        return "browser handoff/runtime startup failed", "browser_handoff_runtime_startup_failed"
    if (
        details["launchMode"] == REQUIRED_STARTUP_PROBE_LAUNCH_MODE
        and missing_events.intersection(
            {
                "jobs_module_boot_start",
                "jobs_first_render",
                "jobs_first_interactive",
                "admin_module_boot_start",
                "admin_ready",
                "admin_first_interactive",
            }
        )
        and (
            details["windowClosedReason"] == "browser_handoff_failed"
            or bool(details["handoffEvidence"])
            or details["windowClosedReason"] == "bridge_exit"
            or "actively refused" in lowered
            or "10061" in lowered
            or "10054" in lowered
            or "connection was forcibly closed" in lowered
        )
    ):
        return "browser runtime startup failed", "browser_runtime_startup_failed"
    return "", ""


def refine_startup_probe_summary(
    summary: dict[str, Any],
    rows: list[dict[str, Any]],
    *,
    error_message: str = "",
    preferred_browser_name: str = "",
    preferred_browser_path: str = "",
) -> dict[str, Any]:
    refined = dict(summary or {})
    details = startup_probe_browser_details(
        rows,
        preferred_browser_name=preferred_browser_name,
        preferred_browser_path=preferred_browser_path,
    )
    refined.update(details)
    classification, _category = classify_startup_probe_failure(
        rows, error_message=error_message, summary=refined
    )
    if classification:
        refined["classification"] = classification
        refined["status"] = "failed"
    return refined


def select_startup_probe_browser(
    candidates: Sequence[dict[str, Any]],
    *,
    chromium_app_mode_supported: ChromiumAppModeSupported,
    env: dict[str, str] | None = None,
) -> dict[str, str]:
    env_map = env if env is not None else None
    for candidate in candidates:
        if not chromium_app_mode_supported(candidate, env=env_map):
            continue
        browser_name = str(candidate.get("name") or "").strip().lower()
        browser_path = str(candidate.get("path") or "").strip()
        if browser_name and browser_path:
            return {
                "browserName": browser_name,
                "browserPath": browser_path,
            }
    raise RuntimeError(
        "No supported managed Chromium probe browser available. "
        "Install Chrome, Brave, or an Edge build that can launch in app mode."
    )
