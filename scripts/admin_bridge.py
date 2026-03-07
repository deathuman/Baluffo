#!/usr/bin/env python3
"""Local admin bridge for source discovery approval workflows."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
import uuid
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Dict, List, Tuple
from urllib.parse import parse_qs, urlparse

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.jobs_fetcher import DEFAULT_STUDIO_SOURCE_REGISTRY
from scripts.source_registry import (
    ACTIVE_PATH,
    APPROVAL_STATE_PATH,
    DISCOVERY_REPORT_PATH,
    PENDING_PATH,
    REJECTED_PATH,
    ensure_source_id,
    load_json_array,
    load_json_object,
    save_json_atomic,
    source_identity,
    unique_sources,
)

OPS_HISTORY_PATH = ROOT / "data" / "admin-run-history.json"
OPS_ALERT_STATE_PATH = ROOT / "data" / "admin-alert-state.json"
JOBS_FETCH_REPORT_PATH = ROOT / "data" / "jobs-fetch-report.json"
TASKS_CONFIG_PATH = ROOT / ".vscode" / "tasks.json"

MAX_HISTORY_ROWS = 240
STALE_FETCH_HOURS = 12
DEGRADED_FAILURE_RATIO = 0.25
OUTPUT_DROP_RATIO = 0.40
OPS_SCHEMA_VERSION = 1


def read_json_from_request(handler: BaseHTTPRequestHandler) -> Dict[str, Any]:
    length = int(handler.headers.get("Content-Length") or 0)
    if length <= 0:
        return {}
    raw = handler.rfile.read(length)
    try:
        payload = json.loads(raw.decode("utf-8"))
        return payload if isinstance(payload, dict) else {}
    except json.JSONDecodeError:
        return {}


def ensure_active_registry() -> List[Dict[str, Any]]:
    active = load_json_array(ACTIVE_PATH, [])
    if active:
        return active
    active = [dict(row) for row in DEFAULT_STUDIO_SOURCE_REGISTRY]
    save_json_atomic(ACTIVE_PATH, active)
    return active


def normalize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    # Precedence is explicit: active > pending > rejected.
    seen = set()
    normalized: Dict[str, List[Dict[str, Any]]] = {"active": [], "pending": [], "rejected": []}
    for bucket in ("active", "pending", "rejected"):
        for row in state.get(bucket, []):
            if not isinstance(row, dict):
                continue
            key = source_identity(row)
            if key in seen:
                continue
            seen.add(key)
            normalized[bucket].append(ensure_source_id(row))
    return normalized


def load_state() -> Dict[str, List[Dict[str, Any]]]:
    return normalize_state({
        "active": ensure_active_registry(),
        "pending": load_json_array(PENDING_PATH, []),
        "rejected": load_json_array(REJECTED_PATH, []),
    })


def summarize_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, int]:
    return {
        "activeCount": len(state["active"]),
        "pendingCount": len(state["pending"]),
        "rejectedCount": len(state["rejected"]),
    }


def persist_state(state: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
    normalized = normalize_state(state)
    save_json_atomic(ACTIVE_PATH, normalized["active"])
    save_json_atomic(PENDING_PATH, normalized["pending"])
    save_json_atomic(REJECTED_PATH, normalized["rejected"])
    return normalized


def move_entries(pending: List[Dict[str, Any]], selected_ids: List[str]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    selected = set(str(item) for item in selected_ids)
    moved: List[Dict[str, Any]] = []
    remaining: List[Dict[str, Any]] = []
    for row in pending:
        if source_identity(row) in selected:
            moved.append(row)
        else:
            remaining.append(row)
    return moved, remaining


def run_background_script(script_name: str, args: List[str] | None = None) -> None:
    command = [sys.executable, str(Path(__file__).resolve().parent / script_name)]
    command.extend(args or [])
    subprocess.Popen(command, cwd=str(Path(__file__).resolve().parents[1]))


def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def now_iso() -> str:
    return now_utc().isoformat()


def parse_iso(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def load_run_history() -> List[Dict[str, Any]]:
    rows = load_json_array(OPS_HISTORY_PATH, [])
    cleaned: List[Dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        if not row.get("type"):
            continue
        cleaned.append(dict(row))
    cleaned.sort(key=lambda item: str(item.get("finishedAt") or item.get("startedAt") or ""))
    return cleaned[-MAX_HISTORY_ROWS:]


def save_run_history(rows: List[Dict[str, Any]]) -> None:
    save_json_atomic(OPS_HISTORY_PATH, rows[-MAX_HISTORY_ROWS:])


def append_run_history(row: Dict[str, Any]) -> Dict[str, Any]:
    history = load_run_history()
    entry = dict(row)
    entry.setdefault("id", f"run_{uuid.uuid4().hex[:12]}")
    history.append(entry)
    save_run_history(history)
    return entry


def upsert_run_history(entry: Dict[str, Any], *, dedupe_fields: Tuple[str, ...]) -> Dict[str, Any]:
    history = load_run_history()
    match_idx = -1
    for idx, row in enumerate(history):
        if all(str(row.get(field) or "") == str(entry.get(field) or "") for field in dedupe_fields):
            match_idx = idx
            break
    if match_idx >= 0:
        merged = {**history[match_idx], **entry}
        merged.setdefault("id", history[match_idx].get("id") or f"run_{uuid.uuid4().hex[:12]}")
        history[match_idx] = merged
        save_run_history(history)
        return merged
    return append_run_history(entry)


def load_alert_state() -> Dict[str, Any]:
    state = load_json_object(OPS_ALERT_STATE_PATH, {})
    acked = state.get("acked")
    if not isinstance(acked, dict):
        acked = {}
    return {
        "schemaVersion": OPS_SCHEMA_VERSION,
        "acked": {str(k): str(v) for k, v in acked.items()},
    }


def save_alert_state(state: Dict[str, Any]) -> None:
    payload = {
        "schemaVersion": OPS_SCHEMA_VERSION,
        "acked": dict(state.get("acked") or {}),
        "updatedAt": now_iso(),
    }
    save_json_atomic(OPS_ALERT_STATE_PATH, payload)


def detect_task_interval_hours(task: Dict[str, Any]) -> float | None:
    text = " ".join([
        str(task.get("label") or ""),
        str(task.get("command") or ""),
        str(task.get("detail") or ""),
    ]).lower()
    match_hours = re.search(r"every\s+(\d+(?:\.\d+)?)\s*(h|hour|hours)\b", text)
    if match_hours:
        return max(0.1, float(match_hours.group(1)))
    match_minutes = re.search(r"every\s+(\d+(?:\.\d+)?)\s*(m|min|minute|minutes)\b", text)
    if match_minutes:
        return max(1.0, float(match_minutes.group(1))) / 60.0
    match_flag = re.search(r"--every-hours\s+(\d+(?:\.\d+)?)", text)
    if match_flag:
        return max(0.1, float(match_flag.group(1)))
    return None


def parse_schedule_metadata() -> Dict[str, Any]:
    fallback = {
        "fetcher": {"intervalHours": None, "nextRunAt": "", "note": "unknown"},
        "discovery": {"intervalHours": None, "nextRunAt": "", "note": "unknown"},
    }
    try:
        payload = json.loads(TASKS_CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return fallback
    tasks = payload.get("tasks")
    if not isinstance(tasks, list):
        return fallback

    by_type: Dict[str, Dict[str, Any]] = {
        "fetcher": dict(fallback["fetcher"]),
        "discovery": dict(fallback["discovery"]),
    }
    for task in tasks:
        if not isinstance(task, dict):
            continue
        command = str(task.get("command") or "").lower()
        label = str(task.get("label") or "").lower()
        interval = detect_task_interval_hours(task)
        if "jobs_fetcher.py" in command or "run jobs fetcher" in label:
            by_type["fetcher"]["intervalHours"] = interval
            by_type["fetcher"]["note"] = "inferred" if interval else "manual_task"
        if "source_discovery.py" in command or "run source discovery" in label:
            by_type["discovery"]["intervalHours"] = interval
            by_type["discovery"]["note"] = "inferred" if interval else "manual_task"
    return by_type


def median(values: List[float]) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    mid = len(ordered) // 2
    if len(ordered) % 2:
        return float(ordered[mid])
    return float((ordered[mid - 1] + ordered[mid]) / 2.0)


def summarize_fetch_report(report: Dict[str, Any]) -> Dict[str, Any]:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    output = int(summary.get("outputCount") or summary.get("uniqueOutputCount") or 0)
    failed = int(summary.get("failedSources") or 0)
    source_count = int(summary.get("sourceCount") or 0)
    duration_ms = 0
    sources = report.get("sources")
    if isinstance(sources, list):
        duration_ms = sum(int(item.get("durationMs") or 0) for item in sources if isinstance(item, dict))
    status = "ok"
    if source_count > 0 and failed >= source_count:
        status = "error"
    elif failed > 0:
        status = "warning"
    return {
        "outputCount": output,
        "failedSources": failed,
        "sourceCount": source_count,
        "durationMs": duration_ms,
        "failedRatio": (failed / source_count) if source_count > 0 else 0.0,
    }


def summarize_discovery_report(report: Dict[str, Any]) -> Dict[str, Any]:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    queued = int(summary.get("queuedCandidateCount") or summary.get("newCandidateCount") or 0)
    failed = int(summary.get("failedProbeCount") or 0)
    probed = int(summary.get("probedCandidateCount") or summary.get("probedCount") or 0)
    duration_ms = 0
    started = parse_iso(report.get("startedAt"))
    finished = parse_iso(report.get("finishedAt"))
    if started and finished:
        duration_ms = int(max(0.0, (finished - started).total_seconds() * 1000))
    status = "ok"
    if probed > 0 and failed >= probed:
        status = "error"
    elif failed > 0:
        status = "warning"
    return {
        "queuedCandidateCount": queued,
        "failedProbeCount": failed,
        "probedCandidateCount": probed,
        "durationMs": duration_ms,
    }, status


def sync_history_from_reports() -> List[Dict[str, Any]]:
    history = load_run_history()
    fetch_report = load_json_object(JOBS_FETCH_REPORT_PATH, {})
    if fetch_report.get("finishedAt"):
        fetch_summary = summarize_fetch_report(fetch_report)
        upsert_run_history({
            "type": "fetch",
            "status": "ok" if fetch_summary["failedSources"] == 0 else ("error" if fetch_summary["failedRatio"] >= 1 else "warning"),
            "startedAt": str(fetch_report.get("startedAt") or ""),
            "finishedAt": str(fetch_report.get("finishedAt") or ""),
            "durationMs": int(fetch_summary["durationMs"]),
            "summary": {
                "outputCount": int(fetch_summary["outputCount"]),
                "failedSources": int(fetch_summary["failedSources"]),
                "sourceCount": int(fetch_summary["sourceCount"]),
            },
        }, dedupe_fields=("type", "finishedAt"))
    discovery_report = load_json_object(DISCOVERY_REPORT_PATH, {})
    if discovery_report.get("finishedAt"):
        discovery_summary, status = summarize_discovery_report(discovery_report)
        upsert_run_history({
            "type": "discovery",
            "status": status,
            "startedAt": str(discovery_report.get("startedAt") or ""),
            "finishedAt": str(discovery_report.get("finishedAt") or ""),
            "durationMs": int(discovery_summary["durationMs"]),
            "summary": {
                "queuedCandidateCount": int(discovery_summary["queuedCandidateCount"]),
                "failedProbeCount": int(discovery_summary["failedProbeCount"]),
                "probedCandidateCount": int(discovery_summary["probedCandidateCount"]),
            },
        }, dedupe_fields=("type", "finishedAt"))
    return load_run_history()


def evaluate_alerts(*, history: List[Dict[str, Any]], latest_fetch_report: Dict[str, Any], pending_count: int) -> Dict[str, Any]:
    alert_state = load_alert_state()
    acked = dict(alert_state.get("acked") or {})
    active_conditions: List[Dict[str, Any]] = []
    now = now_utc()
    fetch_rows = [row for row in history if str(row.get("type")) == "fetch" and row.get("finishedAt")]
    latest_fetch = fetch_rows[-1] if fetch_rows else None
    last_success_fetch = next(
        (row for row in reversed(fetch_rows) if str(row.get("status")) in {"ok", "warning"}),
        None
    )
    stale_hours = None
    if last_success_fetch:
        finished = parse_iso(last_success_fetch.get("finishedAt"))
        if finished:
            stale_hours = (now - finished).total_seconds() / 3600.0
    if stale_hours is None or stale_hours > STALE_FETCH_HOURS:
        active_conditions.append({
            "id": "stale_fetch",
            "severity": "critical",
            "message": f"No successful fetch in the last {STALE_FETCH_HOURS}h.",
            "value": None if stale_hours is None else round(stale_hours, 2),
            "triggeredAt": now_iso(),
        })

    fetch_summary = summarize_fetch_report(latest_fetch_report)
    failed_ratio = float(fetch_summary["failedRatio"])
    if failed_ratio > DEGRADED_FAILURE_RATIO:
        active_conditions.append({
            "id": "degraded_reliability",
            "severity": "warning" if failed_ratio < 0.5 else "critical",
            "message": f"Failed source ratio is {failed_ratio:.0%} (threshold {DEGRADED_FAILURE_RATIO:.0%}).",
            "value": round(failed_ratio, 4),
            "triggeredAt": now_iso(),
        })

    outputs = [int((row.get("summary") or {}).get("outputCount") or 0) for row in fetch_rows if int((row.get("summary") or {}).get("outputCount") or 0) > 0]
    if len(outputs) >= 4 and latest_fetch:
        baseline_values = outputs[:-1] if len(outputs) > 1 else outputs
        baseline = median([float(v) for v in baseline_values[-10:]])
        latest_output = float(outputs[-1])
        if baseline > 0 and latest_output < baseline * (1.0 - OUTPUT_DROP_RATIO):
            drop_ratio = 1.0 - (latest_output / baseline)
            active_conditions.append({
                "id": "output_drop",
                "severity": "warning" if drop_ratio < 0.6 else "critical",
                "message": f"Output dropped {drop_ratio:.0%} vs rolling median.",
                "value": round(drop_ratio, 4),
                "triggeredAt": now_iso(),
            })

    # Clear ack for alerts no longer active.
    active_ids = {row["id"] for row in active_conditions}
    for key in list(acked.keys()):
        if key not in active_ids:
            acked.pop(key, None)

    visible_alerts = [row for row in active_conditions if row["id"] not in acked]
    save_alert_state({"acked": acked})
    return {
        "alerts": visible_alerts,
        "suppressedCount": max(0, len(active_conditions) - len(visible_alerts)),
        "pendingApprovals": int(pending_count),
    }


def format_age(finished_at: str) -> str:
    dt = parse_iso(finished_at)
    if not dt:
        return "unknown"
    delta = now_utc() - dt
    total_minutes = int(max(0.0, delta.total_seconds() // 60))
    if total_minutes < 60:
        return f"{total_minutes}m"
    hours = total_minutes // 60
    if hours < 48:
        return f"{hours}h"
    return f"{hours // 24}d"


def compute_ops_health() -> Dict[str, Any]:
    history = sync_history_from_reports()
    latest_fetch_report = load_json_object(JOBS_FETCH_REPORT_PATH, {})
    state = load_state()
    schedule = parse_schedule_metadata()
    alerts_meta = evaluate_alerts(history=history, latest_fetch_report=latest_fetch_report, pending_count=len(state["pending"]))

    now = now_utc()
    seven_days_ago = now - timedelta(days=7)
    fetch_rows = [row for row in history if str(row.get("type")) == "fetch" and row.get("finishedAt")]
    fetch_7d = [
        row for row in fetch_rows
        if (parse_iso(row.get("finishedAt")) or datetime.min.replace(tzinfo=timezone.utc)) >= seven_days_ago
    ]
    success_7d = [row for row in fetch_7d if str(row.get("status")) in {"ok", "warning"}]
    success_rate = (len(success_7d) / len(fetch_7d)) if fetch_7d else 0.0
    avg_duration = int(sum(int(row.get("durationMs") or 0) for row in fetch_7d) / len(fetch_7d)) if fetch_7d else 0

    latest_fetch = fetch_rows[-1] if fetch_rows else None
    last_success = next((row for row in reversed(fetch_rows) if str(row.get("status")) in {"ok", "warning"}), None)
    latest_fetch_summary = summarize_fetch_report(latest_fetch_report)
    failed_ratio_latest = latest_fetch_summary["failedRatio"]

    latest_run = history[-1] if history else {}
    for run_type, key in (("fetch", "fetcher"), ("discovery", "discovery")):
        interval_hours = schedule[key].get("intervalHours")
        if not interval_hours:
            schedule[key]["nextRunAt"] = ""
            continue
        last_type_row = next((row for row in reversed(history) if str(row.get("type")) == run_type and row.get("finishedAt")), None)
        last_finished = parse_iso(last_type_row.get("finishedAt")) if last_type_row else None
        if last_finished:
            schedule[key]["nextRunAt"] = (last_finished + timedelta(hours=float(interval_hours))).isoformat()
        else:
            schedule[key]["nextRunAt"] = ""

    severity = "healthy"
    if any(alert.get("severity") == "critical" for alert in alerts_meta["alerts"]):
        severity = "critical"
    elif alerts_meta["alerts"]:
        severity = "warning"

    return {
        "generatedAt": now_iso(),
        "status": severity,
        "kpis": {
            "lastSuccessfulFetchAge": format_age(last_success.get("finishedAt") if last_success else ""),
            "sevenDayFetchSuccessRate": round(success_rate, 4),
            "avgFetchDurationMs7d": avg_duration,
            "failedSourceRatioLatest": round(float(failed_ratio_latest), 4),
            "pendingApprovalsCount": len(state["pending"]),
            "lastRunResult": {
                "type": str(latest_run.get("type") or ""),
                "status": str(latest_run.get("status") or "unknown"),
                "finishedAt": str(latest_run.get("finishedAt") or latest_run.get("startedAt") or ""),
            },
        },
        "schedule": schedule,
        "alerts": alerts_meta["alerts"],
        "suppressedAlertsCount": int(alerts_meta["suppressedCount"]),
        "historyCount": len(history),
    }


class Handler(BaseHTTPRequestHandler):
    def _route_path(self) -> str:
        return urlparse(self.path).path

    def _route_query(self) -> Dict[str, List[str]]:
        return parse_qs(urlparse(self.path).query)

    def _send_json(self, payload: Any, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET,POST,OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_OPTIONS(self) -> None:  # noqa: N802
        self._send_json({"ok": True})

    def do_GET(self) -> None:  # noqa: N802
        path = self._route_path()
        query = self._route_query()
        if path == "/registry/active":
            state = load_state()
            self._send_json({"sources": state["active"], "summary": summarize_state(state)})
            return
        if path == "/registry/pending":
            state = load_state()
            self._send_json({"sources": state["pending"], "summary": summarize_state(state)})
            return
        if path == "/registry/rejected":
            state = load_state()
            self._send_json({"sources": state["rejected"], "summary": summarize_state(state)})
            return
        if path == "/discovery/report":
            report = load_json_object(DISCOVERY_REPORT_PATH, {})
            self._send_json(report or {"summary": {}, "candidates": [], "failures": []})
            return
        if path == "/registry/summary":
            state = load_state()
            self._send_json({"summary": summarize_state(state)})
            return
        if path == "/ops/health":
            self._send_json(compute_ops_health())
            return
        if path == "/ops/history":
            limit_raw = (query.get("limit") or ["30"])[0]
            try:
                limit = max(1, min(200, int(limit_raw)))
            except ValueError:
                limit = 30
            rows = load_run_history()
            self._send_json({"runs": rows[-limit:], "count": len(rows)})
            return
        self._send_json({"error": "Not found"}, status=404)

    def do_POST(self) -> None:  # noqa: N802
        path = self._route_path()
        payload = read_json_from_request(self)
        state = load_state()

        if path == "/registry/approve":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            moved, remaining = move_entries(state["pending"], [str(item) for item in ids])
            for row in moved:
                row["enabledByDefault"] = True
            state["pending"] = remaining
            state["active"] = unique_sources([*state["active"], *moved])
            state = persist_state(state)
            approval = load_json_object(APPROVAL_STATE_PATH, {"approvedSinceLastRun": 0})
            approval["approvedSinceLastRun"] = int(approval.get("approvedSinceLastRun") or 0) + len(moved)
            save_json_atomic(APPROVAL_STATE_PATH, approval)
            self._send_json({"approved": len(moved), "summary": summarize_state(state)})
            return

        if path == "/registry/reject":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            moved, remaining = move_entries(state["pending"], [str(item) for item in ids])
            state["pending"] = remaining
            state["rejected"] = unique_sources([*state["rejected"], *moved])
            state = persist_state(state)
            self._send_json({"rejected": len(moved), "summary": summarize_state(state)})
            return

        if path == "/registry/rollback":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            selected = set(str(item) for item in ids)
            moved: List[Dict[str, Any]] = []
            active_remaining: List[Dict[str, Any]] = []
            for row in state["active"]:
                if source_identity(row) in selected:
                    moved.append(row)
                else:
                    active_remaining.append(row)
            state["active"] = active_remaining
            state["pending"] = unique_sources([*state["pending"], *moved])
            state = persist_state(state)
            self._send_json({"rolledBack": len(moved), "summary": summarize_state(state)})
            return

        if path == "/registry/restore-rejected":
            ids = payload.get("ids") if isinstance(payload.get("ids"), list) else []
            moved, remaining = move_entries(state["rejected"], [str(item) for item in ids])
            state["rejected"] = remaining
            for row in moved:
                row["enabledByDefault"] = False
            state["pending"] = unique_sources([*state["pending"], *moved])
            state = persist_state(state)
            self._send_json({"restored": len(moved), "summary": summarize_state(state)})
            return

        if path == "/tasks/run-discovery":
            append_run_history({
                "type": "discovery",
                "status": "started",
                "startedAt": now_iso(),
                "finishedAt": "",
                "durationMs": 0,
                "summary": {},
            })
            run_background_script("source_discovery.py", ["--mode", "dynamic"])
            self._send_json({"started": True, "task": "source_discovery", "mode": "dynamic"})
            return

        if path == "/tasks/run-discovery-full":
            append_run_history({
                "type": "discovery",
                "status": "started",
                "startedAt": now_iso(),
                "finishedAt": "",
                "durationMs": 0,
                "summary": {},
            })
            run_background_script("source_discovery.py", ["--mode", "dynamic"])
            self._send_json({"started": True, "task": "source_discovery", "mode": "dynamic"})
            return

        if path == "/tasks/run-fetcher":
            append_run_history({
                "type": "fetch",
                "status": "started",
                "startedAt": now_iso(),
                "finishedAt": "",
                "durationMs": 0,
                "summary": {},
            })
            run_background_script("jobs_fetcher.py")
            approval = load_json_object(APPROVAL_STATE_PATH, {"approvedSinceLastRun": 0})
            approval["approvedSinceLastRun"] = 0
            save_json_atomic(APPROVAL_STATE_PATH, approval)
            self._send_json({"started": True, "task": "jobs_fetcher"})
            return

        if path == "/ops/alerts/ack":
            alert_id = str(payload.get("id") or "").strip()
            if not alert_id:
                self._send_json({"error": "Missing alert id"}, status=400)
                return
            state_alert = load_alert_state()
            acked = dict(state_alert.get("acked") or {})
            acked[alert_id] = now_iso()
            save_alert_state({"acked": acked})
            self._send_json({"acked": alert_id, "ok": True})
            return

        self._send_json({"error": "Not found"}, status=404)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run local admin bridge API.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8877)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    ensure_active_registry()
    server = ThreadingHTTPServer((args.host, args.port), Handler)
    print(f"Admin bridge listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
