"""Fetch-report source-run helpers for GET routes.

AI boundary owns: fetch-report source-run payload hydration helpers.
AI boundary implement in: source runtime storage and fetch-report contract helpers.
AI boundary search before contracts: fetch-report route callers, source runtime storage, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused route helper tests.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from src.bridge.routes.route_storage_metrics import (
    storage_metrics_data_dir,
)
from src.bridge.storage_health import get_storage_store, record_storage_diagnostic
from src.shared.coerce import as_dict as _as_dict
from src.shared.coerce import as_list as _as_list
from src.shared.coerce import as_text as _clean_text
from src.shared.utils import int_or_default as _safe_int
from src.storage.evidence_archive import EvidenceArchiveStore
from src.storage.source_runtime import SourceRuntimeStore


class FetchReportRouteApi(Protocol):
    DEDUP_REVIEW_STATE_PATH: Path
    JOBS_FETCH_REPORT_PATH: Path
    runtime_config: Any

    def normalize_fetch_report_contract(self, payload: Any) -> dict[str, Any]: ...


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


def _source_details_archive_entry(
    payload: dict[str, Any], rows: list[dict[str, Any]]
) -> dict[str, Any]:
    source_runs = _as_dict(payload.get("sourceRuns"))
    entry = _as_dict(source_runs.get("sourceDetailsArchive"))
    if _clean_text(entry.get("path")):
        return entry
    for row in rows:
        evidence = _as_dict(row.get("evidenceRefs"))
        entry = _as_dict(evidence.get("sourceDetailsArchive"))
        if _clean_text(entry.get("path")):
            return entry
    return {}


def _source_merge_key(row: dict[str, Any]) -> str:
    return (
        _clean_text(row.get("sourceKey"))
        or _clean_text(row.get("sourceId"))
        or _clean_text(row.get("name"))
    )


def _hydrate_source_details_from_archive(
    api: FetchReportRouteApi,
    payload: dict[str, Any],
    rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Reattach details that live in the gzip archive, not in SQLite."""
    if not rows or all(row.get("details") for row in rows):
        return rows
    entry = _source_details_archive_entry(payload, rows)
    if not entry:
        return rows
    try:
        archive_payload = EvidenceArchiveStore(storage_metrics_data_dir(api)).read_archive(entry)
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        _record_source_run_diagnostic(
            api,
            code="source_details_archive_hydration_failed",
            ok=False,
            message=str(exc),
            details={"path": str(entry.get("path") or "")},
        )
        return rows
    archived_sources = [
        source for source in _as_list(archive_payload.get("sources")) if isinstance(source, dict)
    ]
    if not archived_sources:
        return rows
    by_index = {index: source for index, source in enumerate(archived_sources)}
    by_key = {key: source for source in archived_sources if (key := _source_merge_key(source))}
    hydrated: list[dict[str, Any]] = []
    for index, row in enumerate(rows):
        if row.get("details"):
            hydrated.append(row)
            continue
        archived = by_index.get(index)
        if archived is not None and (
            _clean_text(archived.get("name")) != _clean_text(row.get("name"))
            and _source_merge_key(archived) != _source_merge_key(row)
        ):
            archived = by_key.get(_source_merge_key(row))
        if archived is None or "details" not in archived:
            hydrated.append(row)
            continue
        hydrated.append({**row, "details": archived.get("details")})
    return hydrated


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
        hydrated["sources"] = _hydrate_source_details_from_archive(api, payload, rows)
        _record_source_run_diagnostic(
            api,
            code="source_runs_read_projection_match",
            ok=True,
            details={"rowCount": len(rows)},
        )
        return hydrated
    return payload
