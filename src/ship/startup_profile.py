#!/usr/bin/env python3
"""Helpers for summarizing Baluffo desktop startup traces."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any

PROFILE_THRESHOLDS_MS = {
    "cold": {
        "launch_to_site_ready": 2500,
        "site_ready_to_window_created": 800,
        "window_created_to_window_shown": 10000,
        "window_shown_to_page_loaded": 5000,
        "page_loaded_to_local_data_ready": 4000,
        "local_data_ready_to_auth_ready": 3000,
        "auth_ready_to_first_render": 4000,
        "first_render_to_first_interactive": 2500,
        "total_launch_to_first_usable_ui": 18000,
    },
    "warm": {
        "launch_to_site_ready": 1800,
        "site_ready_to_window_created": 600,
        "window_created_to_window_shown": 7000,
        "window_shown_to_page_loaded": 2500,
        "page_loaded_to_local_data_ready": 2500,
        "local_data_ready_to_auth_ready": 1500,
        "auth_ready_to_first_render": 2500,
        "first_render_to_first_interactive": 1800,
        "total_launch_to_first_usable_ui": 12000,
    },
}


def _parse_ts_ms(row: dict[str, Any], launch_ts_ms: int | None) -> int | None:
    ts_value = str(row.get("ts") or "").strip()
    if not ts_value or launch_ts_ms is None:
        return None
    try:
        return max(0, int(datetime.fromisoformat(ts_value).timestamp() * 1000) - int(launch_ts_ms))
    except ValueError:
        return None


def _parse_browser_ts_ms(row: dict[str, Any], launch_ts_ms: int | None) -> int | None:
    if launch_ts_ms is None:
        return None
    top_level_value = row.get("browserTsMs")
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    payload_value = payload.get("browserCreatedAtMs")
    for candidate in (top_level_value, payload_value):
        if isinstance(candidate, (int, float)):
            return max(0, int(candidate) - int(launch_ts_ms))
    return None


def _parse_payload_elapsed_ms(row: dict[str, Any]) -> int | None:
    fields = row.get("fields") if isinstance(row.get("fields"), dict) else {}
    payload = row.get("payload") if isinstance(row.get("payload"), dict) else {}
    for source in (fields, payload):
        value = source.get("elapsedMs")
        if isinstance(value, (int, float)):
            return int(value)
    return None


def _parse_row_ms(row: dict[str, Any], launch_ts_ms: int | None) -> int | None:
    browser_elapsed_ms = _parse_browser_ts_ms(row, launch_ts_ms)
    if browser_elapsed_ms is not None:
        return browser_elapsed_ms
    ts_elapsed_ms = _parse_ts_ms(row, launch_ts_ms)
    if ts_elapsed_ms is not None:
        return ts_elapsed_ms
    return _parse_payload_elapsed_ms(row)


def _launch_ts_ms(rows: list[dict[str, Any]]) -> int | None:
    for row in rows:
        if str(row.get("event") or "").strip() != "desktop_launch_start":
            continue
        ts_value = str(row.get("ts") or "").strip()
        if not ts_value:
            continue
        try:
            return int(datetime.fromisoformat(ts_value).timestamp() * 1000)
        except ValueError:
            return None
    return None


def event_index(rows: list[dict[str, Any]]) -> dict[str, int]:
    launch_ts_ms = _launch_ts_ms(rows)
    result: dict[str, int] = {}
    for row in rows:
        event = str(row.get("event") or "").strip()
        if not event or event in result:
            continue
        elapsed_ms = _parse_row_ms(row, launch_ts_ms)
        if elapsed_ms is None and event == "desktop_launch_start":
            elapsed_ms = 0
        if elapsed_ms is not None:
            result[event] = int(elapsed_ms)
    return result


def _pick_first(events: dict[str, int], names: list[str]) -> tuple[str, int] | tuple[None, None]:
    candidates = [(name, events[name]) for name in names if name in events]
    if not candidates:
        return None, None
    candidates.sort(key=lambda item: item[1])
    return candidates[0]


def _resolve_stage_event(
    events: dict[str, int], event_ref: str | list[str] | tuple[str, ...] | None
) -> tuple[str | None, int | None]:
    if event_ref is None:
        return None, None
    if isinstance(event_ref, (list, tuple)):
        return _pick_first(
            events, [str(name or "").strip() for name in event_ref if str(name or "").strip()]
        )
    name = str(event_ref or "").strip()
    if not name:
        return None, None
    return (name, events.get(name)) if name in events else (name, None)


def _classify_stages(
    *,
    stages: list[dict[str, Any]],
    missing_events: list[str],
    classification_map: dict[str, str],
) -> str:
    if missing_events:
        return "metrics incomplete"
    ranked = [
        stage
        for stage in stages
        if stage["key"] != "total_launch_to_first_usable_ui" and stage["durationMs"] is not None
    ]
    if not ranked:
        return "insufficient data"
    slow_ranked = [stage for stage in ranked if stage.get("status") == "slow"]
    selected = slow_ranked if slow_ranked else ranked
    selected.sort(key=lambda item: int(item["durationMs"] or 0), reverse=True)
    top = str(selected[0]["key"] or "").strip()
    return classification_map.get(top, "ok" if not slow_ranked else "startup bottleneck unclear")


def summarize_startup_metrics(
    rows: list[dict[str, Any]], *, page: str = "jobs", profile_mode: str = "cold"
) -> dict[str, Any]:
    safe_page = str(page or "jobs").strip().lower() or "jobs"
    mode = "warm" if str(profile_mode or "").strip().lower() == "warm" else "cold"
    thresholds = PROFILE_THRESHOLDS_MS[mode]
    events = event_index(rows)

    if safe_page in {
        "desktop-probe",
        "desktop-probe-head",
        "desktop-probe-css",
        "desktop-probe-inline",
    }:
        parse_event = {
            "desktop-probe": "desktop_probe_html_parse_start",
            "desktop-probe-head": "desktop_probe_head_html_parse_start",
            "desktop-probe-css": "desktop_probe_css_html_parse_start",
            "desktop-probe-inline": "desktop_probe_inline_html_parse_start",
        }[safe_page]
        ready_event = {
            "desktop-probe": "desktop_probe_ready",
            "desktop-probe-head": "desktop_probe_head_ready",
            "desktop-probe-css": "desktop_probe_css_ready",
            "desktop-probe-inline": "desktop_probe_inline_ready",
        }[safe_page]
        stage_defs = [
            (
                "launch_to_site_ready",
                "Launch -> Site Ready",
                "desktop_launch_start",
                "desktop_site_ready",
            ),
            (
                "site_ready_to_window_created",
                "Site Ready -> Window Created",
                "desktop_site_ready",
                "desktop_window_created",
            ),
            (
                "window_created_to_window_shown",
                "Window Created -> Window Shown",
                "desktop_window_created",
                ("desktop_window_shown", "desktop_shell_window_shown"),
            ),
            (
                "window_shown_to_page_loaded",
                "Window Shown -> Page Loaded",
                ("desktop_window_shown", "desktop_shell_window_shown"),
                ("desktop_page_loaded", parse_event),
            ),
            (
                "total_launch_to_first_usable_ui",
                "Launch -> First Usable UI",
                "desktop_launch_start",
                ready_event,
            ),
        ]
        stages: list[dict[str, Any]] = []
        missing_events: list[str] = []
        for key, label, start_ref, end_ref in stage_defs:
            start_event, start_ms = _resolve_stage_event(events, start_ref)
            end_event, end_ms = _resolve_stage_event(events, end_ref)
            threshold_ms = int(thresholds.get(key, 0))
            if start_ms is None:
                if isinstance(start_ref, (list, tuple)):
                    missing_events.extend([str(item) for item in start_ref])
                else:
                    missing_events.append(str(start_event or ""))
            if end_ms is None:
                if isinstance(end_ref, (list, tuple)):
                    missing_events.extend([str(item) for item in end_ref])
                else:
                    missing_events.append(str(end_event or ""))
            duration_ms = (
                None if start_ms is None or end_ms is None else max(0, int(end_ms) - int(start_ms))
            )
            status = "missing"
            if duration_ms is not None:
                status = "slow" if threshold_ms > 0 and duration_ms > threshold_ms else "passed"
            stages.append(
                {
                    "key": key,
                    "label": label,
                    "startEvent": start_event,
                    "endEvent": end_event,
                    "startMs": start_ms,
                    "endMs": end_ms,
                    "durationMs": duration_ms,
                    "thresholdMs": threshold_ms,
                    "status": status,
                }
            )
        classification = _classify_stages(
            stages=stages,
            missing_events=missing_events,
            classification_map={
                "launch_to_site_ready": "bridge/site bootstrap delayed",
                "site_ready_to_window_created": "browser launch / app-window creation delayed",
                "window_created_to_window_shown": "native reveal delayed",
                "window_shown_to_page_loaded": "desktop page load delayed",
            },
        )
        stage_statuses = [stage["status"] for stage in stages]
        return {
            "page": safe_page,
            "profileMode": mode,
            "events": events,
            "stages": stages,
            "firstUsableEvent": ready_event if ready_event in events else "",
            "firstUsableMs": events.get(ready_event),
            "classification": classification,
            "status": "passed"
            if stage_statuses and all(status == "passed" for status in stage_statuses)
            else "failed",
            "missingEvents": sorted({event for event in missing_events if event}),
        }

    render_event, render_ms = _pick_first(events, [f"{safe_page}_first_render"])
    interactive_event, interactive_ms = _pick_first(
        events, [f"{safe_page}_first_interactive", f"{safe_page}_ready"]
    )
    first_usable_candidates = [
        (name, value)
        for name, value in ((render_event, render_ms), (interactive_event, interactive_ms))
        if name and value is not None
    ]
    first_usable_event = None
    first_usable_ms = None
    if first_usable_candidates:
        first_usable_event, first_usable_ms = max(first_usable_candidates, key=lambda item: item[1])

    page_loaded_ref = (
        "desktop_page_loaded",
        "desktop_shell_loaded",
        f"{safe_page}_module_boot_start",
        f"{safe_page}_page_boot_start",
        f"{safe_page}_html_parse_start",
    )
    stage_defs = [
        (
            "launch_to_site_ready",
            "Launch -> Site Ready",
            "desktop_launch_start",
            "desktop_site_ready",
        ),
        (
            "site_ready_to_window_created",
            "Site Ready -> Window Created",
            "desktop_site_ready",
            "desktop_window_created",
        ),
        (
            "window_created_to_window_shown",
            "Window Created -> Window Shown",
            "desktop_window_created",
            ("desktop_window_shown", "desktop_shell_window_shown"),
        ),
        (
            "window_shown_to_page_loaded",
            "Window Shown -> Page Loaded",
            ("desktop_window_shown", "desktop_shell_window_shown"),
            page_loaded_ref,
        ),
        (
            "page_loaded_to_local_data_ready",
            "Page Loaded -> Local Data Ready",
            page_loaded_ref,
            f"{safe_page}_local_data_init_ready",
        ),
        (
            "local_data_ready_to_auth_ready",
            "Local Data Ready -> Auth Ready",
            f"{safe_page}_local_data_init_ready",
            f"{safe_page}_auth_ready",
        ),
        (
            "auth_ready_to_first_render",
            "Auth Ready -> First Render",
            f"{safe_page}_auth_ready",
            f"{safe_page}_first_render",
        ),
        (
            "first_render_to_first_interactive",
            "First Render -> First Interactive",
            f"{safe_page}_first_render",
            f"{safe_page}_first_interactive",
        ),
        (
            "total_launch_to_first_usable_ui",
            "Launch -> First Usable UI",
            "desktop_launch_start",
            first_usable_event or f"{safe_page}_first_interactive",
        ),
    ]

    stages: list[dict[str, Any]] = []
    missing_events: list[str] = []
    for key, label, start_ref, end_ref in stage_defs:
        start_event, start_ms = _resolve_stage_event(events, start_ref)
        end_event, end_ms = _resolve_stage_event(events, end_ref)
        threshold_ms = int(thresholds.get(key, 0))
        if start_ms is None:
            if isinstance(start_ref, (list, tuple)):
                missing_events.extend([str(item) for item in start_ref])
            else:
                missing_events.append(str(start_event or ""))
        if end_ms is None:
            if isinstance(end_ref, (list, tuple)):
                missing_events.extend([str(item) for item in end_ref])
            else:
                missing_events.append(str(end_event or ""))
        duration_ms = (
            None if start_ms is None or end_ms is None else max(0, int(end_ms) - int(start_ms))
        )
        status = "missing"
        if duration_ms is not None:
            status = "slow" if threshold_ms > 0 and duration_ms > threshold_ms else "passed"
        stages.append(
            {
                "key": key,
                "label": label,
                "startEvent": start_event,
                "endEvent": end_event,
                "startMs": start_ms,
                "endMs": end_ms,
                "durationMs": duration_ms,
                "thresholdMs": threshold_ms,
                "status": status,
            }
        )

    classification = _classify_stages(
        stages=stages,
        missing_events=missing_events,
        classification_map={
            "launch_to_site_ready": "bridge/site bootstrap delayed",
            "site_ready_to_window_created": "browser launch / app-window creation delayed",
            "window_created_to_window_shown": "native reveal delayed",
            "window_shown_to_page_loaded": "page boot delayed",
            "page_loaded_to_local_data_ready": "local data init delayed",
            "local_data_ready_to_auth_ready": "local auth bootstrap delayed",
            "auth_ready_to_first_render": f"{safe_page} render delayed",
            "first_render_to_first_interactive": f"{safe_page} interactive readiness delayed",
        },
    )

    stage_statuses = [stage["status"] for stage in stages]
    summary_status = (
        "passed"
        if stage_statuses and all(status == "passed" for status in stage_statuses)
        else "failed"
    )
    return {
        "page": safe_page,
        "profileMode": mode,
        "events": events,
        "stages": stages,
        "firstUsableEvent": first_usable_event or "",
        "firstUsableMs": first_usable_ms,
        "classification": classification,
        "status": summary_status,
        "missingEvents": sorted({event for event in missing_events if event}),
    }


def render_startup_summary(summary: dict[str, Any]) -> str:
    lines = [
        f"Startup Profile ({summary.get('profileMode', 'cold')}, page={summary.get('page', 'jobs')})",
        f"Bottleneck: {summary.get('classification', 'unknown')}",
    ]
    for stage in summary.get("stages") or []:
        duration = stage.get("durationMs")
        duration_label = "missing" if duration is None else f"{int(duration)}ms"
        threshold = stage.get("thresholdMs")
        threshold_label = (
            f"{int(threshold)}ms" if isinstance(threshold, int) and threshold > 0 else "-"
        )
        lines.append(
            f"- {stage.get('label')}: {duration_label} [{stage.get('status')}] threshold={threshold_label}"
        )
    return "\n".join(lines)


def write_startup_summary(path: Path, summary: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
