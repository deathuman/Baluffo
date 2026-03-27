#!/usr/bin/env python3
"""Source registry utilities for discovery/approval workflows."""

from __future__ import annotations

import hashlib
import json
import os
import time
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse, urlunsplit

from src.baluffo_config import get_storage_defaults
from src.shared.utils import now_iso

_STORAGE_DEFAULTS = get_storage_defaults()
_DEFAULT_DATA_DIR = _STORAGE_DEFAULTS["data_dir"]
DATA_DIR = Path(os.getenv("BALUFFO_DATA_DIR") or _DEFAULT_DATA_DIR).expanduser().resolve()
ACTIVE_PATH = DATA_DIR / "source-registry-active.json"
PENDING_PATH = DATA_DIR / "source-registry-pending.json"
REJECTED_PATH = DATA_DIR / "source-registry-rejected.json"
DISCOVERY_REPORT_PATH = DATA_DIR / "source-discovery-report.json"
DISCOVERY_CANDIDATES_PATH = DATA_DIR / "source-discovery-candidates.json"
M5_STRATEGIC_BACKLOG_PATH = DATA_DIR / "m5-strategic-backlog.json"
URL_PATCH_MANIFEST_PATH = DATA_DIR / "url-patch-manifest.json"
APPROVAL_STATE_PATH = DATA_DIR / "source-approval-state.json"


def ensure_data_dir() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)


def load_json_array(
    path: Path, default: list[dict[str, Any]] | None = None
) -> list[dict[str, Any]]:
    fallback = default or []
    try:
        if not path.exists():
            return [dict(row) for row in fallback]
        payload = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(payload, list):
            return [dict(row) for row in fallback]
        return [row for row in payload if isinstance(row, dict)]
    except (OSError, json.JSONDecodeError):
        return [dict(row) for row in fallback]


def load_json_object(path: Path, default: dict[str, Any] | None = None) -> dict[str, Any]:
    fallback = dict(default or {})
    try:
        if not path.exists():
            return fallback
        payload = json.loads(path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def save_json_atomic(path: Path, payload: Any) -> None:
    ensure_data_dir()
    # Use a unique temp file per write to avoid collisions across threads/processes.
    tmp = path.with_suffix(path.suffix + f".{os.getpid()}.{time.time_ns()}.tmp")
    try:
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        last_error: Exception | None = None
        for attempt in range(18):
            try:
                os.replace(tmp, path)
                last_error = None
                break
            except PermissionError as exc:
                last_error = exc
                # Windows can transiently lock the destination while another thread replaces it.
                time.sleep(0.012 * (attempt + 1))
        if last_error is not None:
            raise last_error
    finally:
        try:
            if tmp.exists():
                tmp.unlink()
        except OSError:
            pass


def source_identity(row: dict[str, Any]) -> str:
    adapter = str(row.get("adapter") or "").strip().lower()
    explicit_id = str(row.get("id") or "").strip()
    if explicit_id:
        return explicit_id.lower()
    for key in (
        "id",
        "slug",
        "account",
        "company_id",
        "api_url",
        "feed_url",
        "board_url",
        "listing_url",
        "name",
    ):
        value = str(row.get(key) or "").strip().lower()
        if value:
            return f"{adapter}:{key}:{value}"
    digest = hashlib.sha1(
        json.dumps(row, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()
    return f"{adapter}:unknown:{digest}"


def ensure_source_id(row: dict[str, Any]) -> dict[str, Any]:
    normalized = dict(row)
    normalized["id"] = source_identity(normalized)
    return normalized


def normalize_source_url(raw_url: str) -> str:
    text = str(raw_url or "").strip()
    if not text:
        return ""
    try:
        parsed = urlparse(text)
    except ValueError:
        return ""
    scheme = (parsed.scheme or "").lower()
    host = (parsed.netloc or "").strip().lower()
    if scheme not in {"http", "https"} or not host:
        return ""
    path = (parsed.path or "").rstrip("/")
    return urlunsplit((scheme, host, path, "", ""))


def source_endpoint_url(row: dict[str, Any]) -> str:
    for key in ("api_url", "feed_url", "board_url", "listing_url"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    pages = row.get("pages")
    if isinstance(pages, list):
        for value in pages:
            text = str(value or "").strip()
            if text:
                return text
    return ""


def source_url_fingerprint(row: dict[str, Any]) -> str:
    return normalize_source_url(source_endpoint_url(row))


def unique_sources(rows: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen = set()
    for row in rows:
        if not isinstance(row, dict):
            continue
        key = source_identity(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(ensure_source_id(row))
    return out


def _normalize_discovery_health_status(value: Any) -> str:
    token = str(value or "").strip().lower()
    if token in {"healthy", "success"}:
        return "ok"
    if token in {"failed", "failure"}:
        return "error"
    return token


def _pending_row_is_auto_approvable(row: dict[str, Any]) -> bool:
    if not isinstance(row, dict):
        return False
    jobs_found = row.get("jobsFound")
    sample_count = row.get("sampleCount")
    jobs_count = 0
    for value in (jobs_found, sample_count):
        try:
            numeric = int(value or 0)
        except (TypeError, ValueError):
            numeric = 0
        if numeric > 0:
            jobs_count = numeric
            break
    if jobs_count <= 0:
        return False
    if str(row.get("lastProbeError") or "").strip():
        return False
    status = _normalize_discovery_health_status(row.get("_lastStatus") or row.get("status"))
    if status == "error":
        return False
    return True


def _queued_report_candidate_ids(report: dict[str, Any]) -> set[str]:
    candidates = report.get("candidates") if isinstance(report.get("candidates"), list) else []
    queued_ids: set[str] = set()
    for row in candidates:
        if not isinstance(row, dict) or bool(row.get("deferred")):
            continue
        queued_ids.add(source_identity(row))
    return queued_ids


def _stamp_live_transition(
    row: dict[str, Any], *, approved_by: str, approved_at: str
) -> dict[str, Any]:
    updated = dict(row)
    updated["enabledByDefault"] = True
    updated["candidateState"] = "live"
    updated["approvedAt"] = str(updated.get("approvedAt") or approved_at)
    updated["approvedBy"] = str(approved_by or updated.get("approvedBy") or "")
    updated["liveAt"] = str(updated.get("liveAt") or approved_at)
    updated["quarantinedAt"] = ""
    updated["quarantineReason"] = ""
    return updated


def apply_discovery_auto_approval(
    state: dict[str, list[dict[str, Any]]],
    report: dict[str, Any],
    *,
    auto_approve_enabled: bool,
    approval_state_path: Path = APPROVAL_STATE_PATH,
    now_iso_fn: Callable[[], str] | None = now_iso,
) -> tuple[dict[str, list[dict[str, Any]]], int]:
    normalized_state = {
        bucket: unique_sources(
            dict(row) for row in list(state.get(bucket) or []) if isinstance(row, dict)
        )
        for bucket in ("active", "pending", "rejected")
    }
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    runtime = report.get("runtime") if isinstance(report.get("runtime"), dict) else {}
    runtime_auto = (
        runtime.get("autoApproval") if isinstance(runtime.get("autoApproval"), dict) else {}
    )
    queued_ids = _queued_report_candidate_ids(report)
    approved_at = str(now_iso_fn() if callable(now_iso_fn) else now_iso())
    moved: list[dict[str, Any]] = []
    remaining: list[dict[str, Any]] = []

    if auto_approve_enabled:
        for row in normalized_state["pending"]:
            row_id = source_identity(row)
            if row_id in queued_ids or _pending_row_is_auto_approvable(row):
                moved.append(
                    _stamp_live_transition(
                        row, approved_by="discovery_auto_approve", approved_at=approved_at
                    )
                )
            else:
                remaining.append(dict(row))
        next_state = {
            "active": unique_sources([*normalized_state["active"], *moved]),
            "pending": unique_sources(remaining),
            "rejected": unique_sources(normalized_state["rejected"]),
        }
    else:
        next_state = normalized_state

    approved_count = max(int(summary.get("approvedCandidateCount") or 0), len(moved))
    summary["approvedCandidateCount"] = approved_count
    summary["liveCandidateCount"] = max(int(summary.get("liveCandidateCount") or 0), approved_count)
    report["summary"] = summary

    runtime_auto = dict(runtime_auto)
    runtime_auto["enabled"] = bool(auto_approve_enabled)
    runtime_auto["approvedCount"] = max(int(runtime_auto.get("approvedCount") or 0), approved_count)
    runtime = dict(runtime)
    runtime["autoApproval"] = runtime_auto
    report["runtime"] = runtime

    candidates = report.get("candidates") if isinstance(report.get("candidates"), list) else []
    if candidates:
        report["candidates"] = [
            _stamp_live_transition(
                row, approved_by="discovery_auto_approve", approved_at=approved_at
            )
            if isinstance(row, dict)
            and not bool(row.get("deferred"))
            and source_identity(row) in queued_ids
            else row
            for row in candidates
        ]

    if auto_approve_enabled and moved:
        approval_state = load_json_object(approval_state_path, {"approvedSinceLastRun": 0})
        approval_state["approvedSinceLastRun"] = int(
            approval_state.get("approvedSinceLastRun") or 0
        ) + len(moved)
        save_json_atomic(approval_state_path, approval_state)

    return next_state, approved_count
