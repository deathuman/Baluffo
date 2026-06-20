"""Fetch-report source-run helpers for GET routes.

AI boundary owns: fetch-report source-run payload hydration helpers.
AI boundary implement in: source runtime storage and fetch-report contract helpers.
AI boundary search before contracts: fetch-report route callers, source runtime storage, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused route helper tests.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Any, Protocol

from src.bridge.fetch_report_review_state import load_fetch_report_with_dedup_review_state
from src.bridge.routes.route_storage_metrics import (
    record_storage_read_metric,
    storage_metrics_data_dir,
)
from src.bridge.storage_health import get_storage_store, record_storage_diagnostic
from src.storage.source_runtime import SourceRuntimeStore


class FetchReportRouteApi(Protocol):
    DEDUP_REVIEW_STATE_PATH: Path
    JOBS_FETCH_REPORT_PATH: Path
    runtime_config: Any

    def normalize_fetch_report_contract(self, payload: Any) -> dict[str, Any]: ...


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return int(default)


def _record_source_run_diagnostic(
    api: FetchReportRouteApi,
    *,
    code: str,
    ok: bool,
    message: str = "",
    details: dict[str, Any] | None = None,
) -> None:
    record_storage_diagnostic(
        storage_metrics_data_dir(api),
        surface="sourceRuns",
        code=code,
        ok=ok,
        message=message,
        details=dict(details or {}),
    )


def _source_runtime_store(
    api: FetchReportRouteApi, *, row_limit: int = 500
) -> SourceRuntimeStore | None:
    try:
        return SourceRuntimeStore(
            get_storage_store(storage_metrics_data_dir(api)),
            row_limit=max(1, int(row_limit)),
        )
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        _record_source_run_diagnostic(
            api,
            code="source_runs_store_unavailable",
            ok=False,
            message=str(exc),
        )
        return None


def _source_runs_mode(runtime_store: SourceRuntimeStore) -> str:
    try:
        modes = runtime_store.store.get_authority_modes()
    except (OSError, RuntimeError, TypeError, ValueError):
        return "json"
    return str((modes or {}).get("sourceRuns") or "json").strip().lower()


def _rollback_source_runs_to_json(
    api: FetchReportRouteApi,
    runtime_store: SourceRuntimeStore,
    *,
    code: str,
    message: str,
    details: dict[str, Any] | None = None,
) -> None:
    try:
        runtime_store.store.set_authority_mode("sourceRuns", "json", reason=code)
    except (OSError, RuntimeError, TypeError, ValueError) as exc:
        message = f"{message}; rollback failed: {exc}"
    _record_source_run_diagnostic(
        api,
        code=code,
        ok=False,
        message=message,
        details=dict(details or {}),
    )


def _source_parity_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [
        {
            "name": _clean_text(row.get("name")),
            "status": _clean_text(row.get("status")).lower(),
            "adapter": _clean_text(row.get("adapter")),
            "fetchStrategy": _clean_text(row.get("fetchStrategy")),
            "studio": _clean_text(row.get("studio")),
            "fetchedCount": _safe_int(row.get("fetchedCount")),
            "keptCount": _safe_int(row.get("keptCount")),
            "lowConfidenceDropped": _safe_int(row.get("lowConfidenceDropped")),
            "error": _clean_text(row.get("error")),
            "durationMs": _safe_int(row.get("durationMs")),
        }
        for row in rows
        if isinstance(row, dict)
    ]


def _fetch_report_source_count(payload: dict[str, Any]) -> int:
    summary = _as_dict(payload.get("summary"))
    sources = _as_list(payload.get("sources"))
    return max(0, _safe_int(summary.get("sourceCount")), len(sources))


def hydrate_fetch_report_sources_from_sqlite(
    api: FetchReportRouteApi,
    payload: dict[str, Any],
) -> dict[str, Any]:
    run_id = _clean_text(payload.get("runId"))
    if not run_id:
        return payload
    expected_count = _fetch_report_source_count(payload)
    runtime_store = _source_runtime_store(api, row_limit=max(500, expected_count))
    if runtime_store is None or _source_runs_mode(runtime_store) != "sqlite":
        return payload
    rows = runtime_store.source_runs(run_id=run_id, limit=max(1, expected_count or 500))
    json_sources = [row for row in _as_list(payload.get("sources")) if isinstance(row, dict)]
    if expected_count and len(rows) != expected_count:
        _rollback_source_runs_to_json(
            api,
            runtime_store,
            code="source_runs_read_count_mismatch",
            message="SQLite source_runs count did not match fetch report",
            details={"expectedCount": expected_count, "sqliteCount": len(rows)},
        )
        return payload
    if json_sources and _source_parity_rows(rows) != _source_parity_rows(json_sources):
        _rollback_source_runs_to_json(
            api,
            runtime_store,
            code="source_runs_read_projection_mismatch",
            message="SQLite source_runs projection did not match fetch report",
            details={"jsonCount": len(json_sources), "sqliteCount": len(rows)},
        )
        return payload
    if rows:
        hydrated = dict(payload)
        hydrated["sources"] = rows
        _record_source_run_diagnostic(
            api,
            code="source_runs_read_projection_match",
            ok=True,
            details={"rowCount": len(rows)},
        )
        return hydrated
    return payload


def fetch_report_sources_payload(
    api: FetchReportRouteApi,
    query: dict[str, list[str]],
) -> dict[str, Any]:
    started_at = time.perf_counter()
    source = "json"
    rows: list[dict[str, Any]] = []
    failed = True
    try:
        report, warning = load_fetch_report_with_dedup_review_state(
            normalize_fetch_report_contract=api.normalize_fetch_report_contract,
            jobs_fetch_report_path=api.JOBS_FETCH_REPORT_PATH,
            dedup_review_state_path=api.DEDUP_REVIEW_STATE_PATH,
        )
        run_id = _clean_text((query.get("runId") or [""])[0]) or _clean_text(report.get("runId"))
        status = _clean_text((query.get("status") or [""])[0]).lower()
        limit = max(1, min(500, _safe_int((query.get("limit") or ["100"])[0], 100)))
        offset = max(0, _safe_int((query.get("offset") or ["0"])[0], 0))
        runtime_store = _source_runtime_store(api, row_limit=limit)
        if run_id and runtime_store is not None and _source_runs_mode(runtime_store) == "sqlite":
            rows = runtime_store.source_runs(
                run_id=run_id,
                status=status,
                limit=limit,
                offset=offset,
            )
            if rows:
                source = "sqlite"
            else:
                _rollback_source_runs_to_json(
                    api,
                    runtime_store,
                    code="source_runs_read_empty",
                    message="SQLite source_runs did not contain requested fetch source rows",
                    details={"runId": run_id},
                )
        if not rows:
            json_rows = [row for row in _as_list(report.get("sources")) if isinstance(row, dict)]
            if status:
                json_rows = [
                    row for row in json_rows if _clean_text(row.get("status")).lower() == status
                ]
            rows = json_rows[offset : offset + limit]
        payload = {
            "ok": True,
            "runId": run_id,
            "sources": rows,
            "count": len(rows),
            "limit": limit,
            "offset": offset,
            "source": source,
            "warning": warning,
        }
        failed = False
        return payload
    finally:
        record_storage_read_metric(
            api,
            surface="sourceRuns.reportSources",
            artifact="jobs-fetch-report.json",
            storage_kind=source,
            started_at=started_at,
            row_count=len(rows),
            failed=failed,
        )
