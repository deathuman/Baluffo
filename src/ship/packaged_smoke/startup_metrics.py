"""Startup metric helpers behind the packaged smoke root facade.

AI boundary owns: packaged smoke startup metric parsing, snapshots, and metric assertions.
AI boundary implement in: this file for startup metric evidence; smoke orchestration stays in orchestrator/runtime.
AI boundary search before contracts: packaged smoke runtime, startup profile, and startup metric tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused startup metric tests.
"""

from __future__ import annotations

from typing import Any

from src.ship.startup_probe_policy import startup_metric_fields
from src.ship.startup_telemetry import JOBS_REPORT_GATE_EVENT, JOBS_REPORT_GATE_SKIPPED_EVENT

root: Any | None = None

JOBS_REPORT_GATE_FULL_REPORT = "full-report"
# Both launcher halves consult the gate on a packaged start. The smoke requires a
# row from each, otherwise one process can lose its gate journal and the remaining
# row makes the launch look healthy.
JOBS_REPORT_GATE_PROCESSES = ("site", "bridge")


def _root() -> Any:
    if root is None:
        raise RuntimeError("packaged_smoke.startup_metrics.root is not configured")
    return root


def fetch_startup_metrics(bridge_base_url: str, limit: int = 1000) -> list[dict[str, Any]]:
    deps = _root()
    metrics_payload = deps.fetch_json(
        f"{bridge_base_url}/desktop-local-data/startup-metrics?limit={int(limit)}"
    )
    rows = metrics_payload.get("rows") if isinstance(metrics_payload.get("rows"), list) else []
    return [row for row in rows if isinstance(row, dict)]


def startup_metric_launch_mode(rows: list[dict[str, Any]]) -> str:
    for row in rows:
        if str(row.get("event") or "").strip() != "desktop_browser_launch_selected":
            continue
        fields = startup_metric_fields(row)
        return str(fields.get("mode") or "").strip().lower()
    return ""


def startup_metric_event_present(
    rows: list[dict[str, Any]],
    event: str,
    **expected_fields: object,
) -> bool:
    expected_event = str(event or "").strip()
    for row in rows:
        if str(row.get("event") or "").strip() != expected_event:
            continue
        fields = startup_metric_fields(row)
        matches = True
        for key, expected in expected_fields.items():
            actual = fields.get(str(key))
            if isinstance(expected, bool):
                if bool(actual) is not bool(expected):
                    matches = False
                    break
                continue
            if isinstance(expected, int):
                if int(actual or 0) != int(expected):
                    matches = False
                    break
                continue
            if str(actual or "").strip() != str(expected or "").strip():
                matches = False
                break
        if matches:
            return True
    return False


def jobs_report_gate_full_report_read(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    """First startup journal gate row that parsed the whole fetch report.

    The desktop launch journals ``desktop_launch_jobs_report_gate`` every time the
    jobs success gate runs (once per site/bridge process). ``reader`` is
    ``summary-sidecar`` on the bounded fast path and ``full-report`` when the
    sidecar was missing, unfinished, or described another run. A full-report read
    is a startup regression rather than a threshold: the packaged smoke fails on
    it so a sidecar break blocks a release instead of only showing up as latency.
    """
    for row in rows:
        if str(row.get("event") or "").strip() != JOBS_REPORT_GATE_EVENT:
            continue
        fields = startup_metric_fields(row)
        if str(fields.get("reader") or "").strip().lower() == JOBS_REPORT_GATE_FULL_REPORT:
            return fields
    return None


def jobs_report_gate_missing_processes(
    rows: list[dict[str, Any]], *, require_reader: bool = False
) -> list[str]:
    """Gate processes that never journaled a row in this launch.

    The site and bridge processes each consult the jobs success gate, so a launch
    where only one of them journals leaves the other half unobserved. A gate row
    means it read an artifact; a ``desktop_launch_jobs_report_gate_skipped`` row
    means it found nothing to read. Both prove the process reported; with
    ``require_reader`` only the former counts, which is what the seeded lane needs
    to know the fixture actually exercised the sidecar path. Rows written without
    a ``process`` field cannot be attributed and therefore count as missing.
    """
    expected_events = (
        {JOBS_REPORT_GATE_EVENT}
        if require_reader
        else {JOBS_REPORT_GATE_EVENT, JOBS_REPORT_GATE_SKIPPED_EVENT}
    )
    seen: set[str] = set()
    journaled = False
    for row in rows:
        if str(row.get("event") or "").strip() not in expected_events:
            continue
        journaled = True
        process = str(startup_metric_fields(row).get("process") or "").strip().lower()
        if process:
            seen.add(process)
    if not journaled and not require_reader:
        # No half journaled the gate at all: the launch never reached it, and the
        # startup checks own that failure. Comparing halves is only meaningful
        # once at least one process reported.
        return []
    return [process for process in JOBS_REPORT_GATE_PROCESSES if process not in seen]


def find_startup_metric_fields(
    rows: list[dict[str, Any]],
    event: str,
    **expected_fields: object,
) -> dict[str, Any] | None:
    expected_event = str(event or "").strip()
    for row in reversed(rows):
        if str(row.get("event") or "").strip() != expected_event:
            continue
        fields = startup_metric_fields(row)
        matches = True
        for key, expected in expected_fields.items():
            actual = fields.get(str(key))
            if isinstance(expected, bool):
                if bool(actual) is not bool(expected):
                    matches = False
                    break
                continue
            if isinstance(expected, int):
                if int(actual or 0) != int(expected):
                    matches = False
                    break
                continue
            if str(actual or "").strip() != str(expected or "").strip():
                matches = False
                break
        if matches:
            return fields
    return None
