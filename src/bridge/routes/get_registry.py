"""Registry GET route handlers.

AI boundary owns: registry summary/source/pending GET route response wiring only.
AI boundary implement in: registry services, discovery overlays, and source-state helpers.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Protocol

from src.bridge.performance_profile import time_operation
from src.bridge.registry_source_table import compact_registry_source_table_row
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.routes.route_storage_metrics import record_storage_read_metric
from src.source_registry import is_hidden_from_default
from src.source_registry_auto_approval import annotate_pending_auto_approval_rows
from src.source_registry_io import load_runtime_evidence_array


class _RegistryRouteApi(Protocol):
    DISCOVERY_CANDIDATES_PATH: Path
    DISCOVERY_REPORT_PATH: Path
    JOBS_FETCH_REPORT_PATH: Path
    runtime_config: Any

    def get_registry_exact_summary_payload(self) -> dict[str, Any]: ...

    def get_registry_summary_payload(self) -> dict[str, Any]: ...
    def get_registry_compact_table_payload(self, **kwargs: Any) -> dict[str, Any]: ...

    def load_json_object(self, path: Path, default: Any = None) -> dict[str, Any]: ...

    def load_state(self) -> dict[str, list[dict[str, Any]]]: ...

    def summarize_state(self, state: dict[str, list[dict[str, Any]]]) -> dict[str, Any]: ...


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _source_match_tokens(row: dict[str, Any]) -> set[str]:
    tokens: set[str] = set()
    for key in (
        "id",
        "sourceId",
        "url",
        "sourceUrl",
        "source_url",
        "listingUrl",
        "listing_url",
        "careersUrl",
        "careers_url",
        "feed_url",
        "board_url",
    ):
        value = str(row.get(key) or "").strip().lower().rstrip("/")
        if value:
            tokens.add(f"{key.lower()}:{value}")
            if key.endswith("url") or key.endswith("_url") or key in {"url", "sourceUrl"}:
                tokens.add(f"url:{value}")
    name = str(row.get("name") or "").strip().lower()
    studio = str(row.get("studio") or "").strip().lower()
    adapter = str(row.get("adapter") or "").strip().lower()
    if name and adapter:
        tokens.add(f"name_adapter:{name}|{adapter}")
    if studio and adapter:
        tokens.add(f"studio_adapter:{studio}|{adapter}")
    return tokens


def _read_discovery_candidate_rows(api: _RegistryRouteApi) -> list[dict[str, Any]]:
    candidates_path = getattr(api, "DISCOVERY_CANDIDATES_PATH", None)
    if candidates_path is None:
        return []
    return load_runtime_evidence_array(candidates_path, [])


def _read_discovery_report_candidate_rows(api: _RegistryRouteApi) -> list[dict[str, Any]]:
    report_path = getattr(api, "DISCOVERY_REPORT_PATH", None)
    if report_path is None:
        return []
    try:
        report = _as_dict(api.load_json_object(Path(report_path), {}))
    except (OSError, TypeError, ValueError):
        return []
    return [row for row in _as_list(report.get("candidates")) if isinstance(row, dict)]


def _overlay_discovery_candidate_fields(
    rows: list[dict[str, Any]],
    candidates: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    by_token: dict[str, dict[str, Any]] = {}
    for candidate in candidates:
        for token in _source_match_tokens(candidate):
            by_token.setdefault(token, candidate)

    evidence_fields = (
        "jobsFound",
        "sampleCount",
        "status",
        "lastProbeError",
        "error",
        "lastProbedAt",
        "deferred",
        "pendingReason",
        "deferReason",
        "quarantineReason",
        "weakSignal",
        "candidateState",
        "confidence",
        "rankScore",
        "rankReasons",
    )
    out: list[dict[str, Any]] = []
    for row in rows:
        match = next(
            (by_token[token] for token in _source_match_tokens(row) if token in by_token), None
        )
        if not match:
            out.append(row)
            continue
        merged = dict(row)
        for field in evidence_fields:
            if field in match:
                merged[field] = match[field]
        out.append(merged)
    return out


def _normalize_pending_discovery_job_counts(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized: list[dict[str, Any]] = []
    for row in rows:
        if row.get("jobsFound") is not None or row.get("sampleCount") is not None:
            normalized.append(row)
            continue
        updated = dict(row)
        updated["jobsFound"] = 0
        updated["sampleCount"] = 0
        normalized.append(updated)
    return normalized


def _include_hidden_registry_rows(query: dict[str, list[str]]) -> bool:
    return str((query.get("includeHidden") or [""])[0] or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _pending_registry_payload_from_state(
    api: _RegistryRouteApi,
    query: dict[str, list[str]],
    state: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    pending_rows = _normalize_pending_discovery_job_counts(
        _overlay_discovery_candidate_fields(
            state.get("pending") or [],
            _read_discovery_candidate_rows(api),
        )
    )
    hidden_pending_count = sum(1 for row in pending_rows if is_hidden_from_default(row))
    if not _include_hidden_registry_rows(query):
        pending_rows = [row for row in pending_rows if not is_hidden_from_default(row)]
    summary = api.summarize_state(state)
    summary["hiddenPendingCount"] = hidden_pending_count
    return {"sources": pending_rows, "summary": summary}


def _include_hidden_pending_registry_rows(query: dict[str, list[str]]) -> bool:
    return str((query.get("includeHiddenPending") or [""])[0] or "").strip().lower() in {
        "1",
        "true",
        "yes",
    }


def _requested_registry_source_buckets(query: dict[str, list[str]]) -> tuple[list[str], str]:
    raw = str((query.get("buckets") or ["pending,active,rejected"])[0] or "").strip()
    buckets = [item.strip().lower() for item in raw.split(",") if item.strip()]
    if not buckets:
        buckets = ["pending", "active", "rejected"]
    allowed = {"pending", "active", "rejected"}
    invalid = [bucket for bucket in buckets if bucket not in allowed]
    if invalid:
        return [], ", ".join(sorted(set(invalid)))
    deduped: list[str] = []
    for bucket in buckets:
        if bucket not in deduped:
            deduped.append(bucket)
    return deduped, ""


def _registry_table_limit_per_bucket(query: dict[str, list[str]]) -> int:
    raw = str((query.get("limitPerBucket") or query.get("limit") or [""])[0] or "").strip()
    if not raw:
        return 0
    try:
        value = int(raw)
    except ValueError:
        return 0
    if value <= 0:
        return 0
    return min(value, 500)


def _active_compact_registry_sources(query: dict[str, list[str]]) -> bool:
    raw = str((query.get("activeCompact") or query.get("compactActive") or [""])[0] or "")
    return raw.strip().lower() in {"1", "true", "yes"}


def _apply_registry_table_limit(
    sources: dict[str, list[dict[str, Any]]],
    limit_per_bucket: int,
) -> dict[str, dict[str, int]]:
    if limit_per_bucket <= 0:
        return {}
    truncated_buckets: dict[str, dict[str, int]] = {}
    for bucket, rows in list(sources.items()):
        total = len(rows)
        if total > limit_per_bucket:
            sources[bucket] = rows[:limit_per_bucket]
            truncated_buckets[bucket] = {"returned": limit_per_bucket, "total": total}
    return truncated_buckets


def _registry_authority_mode_from_summary(summary: dict[str, Any]) -> str:
    explicit = str(summary.get("authorityMode") or "").strip().lower()
    if explicit:
        return explicit
    reason = str(summary.get("reason") or "").strip().lower()
    if reason.startswith("sqlite"):
        return "sqlite"
    if reason.startswith("shadow"):
        return "shadow"
    if reason.startswith("json"):
        return "json"
    return ""


def _registry_summary_route_payload(
    api: _RegistryRouteApi, query: dict[str, list[str]]
) -> tuple[int, dict[str, Any]]:
    view = str((query.get("view") or [""])[0] or "").strip().lower()
    if view not in ("", "cheap", "storage", "exact"):
        return 400, {
            "ok": False,
            "error": "invalid registry summary view",
            "invalidView": view,
            "allowedViews": ["storage", "exact"],
        }
    started_at = time.perf_counter()
    failed = True
    try:
        if view == "exact":
            summary = _as_dict(api.get_registry_exact_summary_payload())
        else:
            summary = _as_dict(api.get_registry_summary_payload())
        failed = False
    finally:
        record_storage_read_metric(
            api,
            surface="registry.summaryExact" if view == "exact" else "registry.summary",
            artifact="source-registry",
            storage_kind="normalized" if view == "exact" else "storage",
            started_at=started_at,
            failed=failed,
        )
    generated_at = str(
        summary.get("updatedAt") or summary.get("publishedAt") or datetime.now(UTC).isoformat()
    )
    return 200, {
        "ok": True,
        "summary": summary,
        "authorityMode": _registry_authority_mode_from_summary(summary),
        "generatedAt": generated_at,
    }


def _registry_sources_payload(
    api: _RegistryRouteApi, query: dict[str, list[str]]
) -> tuple[int, dict[str, Any]]:
    view = str((query.get("view") or ["full"])[0] or "full").strip().lower()
    if view not in {"", "full", "table"}:
        return 400, {
            "ok": False,
            "error": "invalid registry sources view",
            "invalidView": view,
            "allowedViews": ["full", "table"],
        }
    table_view = view == "table"
    table_limit_per_bucket = _registry_table_limit_per_bucket(query) if table_view else 0
    buckets, invalid = _requested_registry_source_buckets(query)
    if invalid:
        return 400, {
            "ok": False,
            "error": "invalid registry bucket",
            "invalidBuckets": invalid,
        }
    # detail=summary on the table view implicitly selects the cheap path. We
    # keep the activeCompact flag as an alias for backward compat with the
    # existing Admin active-task lane.
    table_detail = _registry_sources_table_detail(query)
    if table_view and not table_detail:
        return 400, {
            "ok": False,
            "error": "invalid registry sources table detail",
            "allowedDetails": ["summary", "full"],
        }
    if table_view and (_active_compact_registry_sources(query) or table_detail == "summary"):
        return _active_compact_registry_sources_payload(
            api,
            query=query,
            buckets=buckets,
            limit_per_bucket=table_limit_per_bucket or 25,
        )
    started_at = time.perf_counter()
    failed = True
    row_count = 0
    storage_kind = "normalized"
    try:
        state = api.load_state()
        summary = {
            **api.summarize_state(state),
            "summaryExact": True,
            "countBasis": "normalized",
        }
        sources: dict[str, list[dict[str, Any]]] = {}
        if "pending" in buckets:
            pending_query = dict(query)
            pending_query["includeHidden"] = [
                "1" if _include_hidden_pending_registry_rows(query) else "0"
            ]
            pending_payload = _pending_registry_payload_from_state(api, pending_query, state)
            sources["pending"] = [
                dict(row)
                for row in _as_list(pending_payload.get("sources"))
                if isinstance(row, dict)
            ]
            summary = {**summary, **_as_dict(pending_payload.get("summary"))}
        if "active" in buckets:
            sources["active"] = [
                dict(row) for row in state.get("active") or [] if isinstance(row, dict)
            ]
        if "rejected" in buckets:
            sources["rejected"] = [
                dict(row) for row in state.get("rejected") or [] if isinstance(row, dict)
            ]
        if table_view:
            sources, summary = _registry_sources_table_view_parts(
                api,
                state=state,
                sources=sources,
                summary=summary,
                limit_per_bucket=table_limit_per_bucket,
                detail_mode=table_detail,
            )
        row_count = sum(len(rows) for rows in sources.values())
        storage_kind = str(summary.get("authorityMode") or "normalized")
        payload = {"ok": True, "sources": sources, "summary": summary}
        if table_view:
            payload["detailLevel"] = "table"
            payload["summaryView"] = True
        failed = False
        return 200, payload
    finally:
        record_storage_read_metric(
            api,
            surface="registry.sources",
            artifact="source-registry",
            storage_kind=storage_kind,
            started_at=started_at,
            row_count=row_count,
            failed=failed,
        )


def _active_compact_registry_sources_payload(
    api: _RegistryRouteApi,
    *,
    query: dict[str, list[str]],
    buckets: list[str],
    limit_per_bucket: int,
) -> tuple[int, dict[str, Any]]:
    payload = _as_dict(
        api.get_registry_compact_table_payload(
            buckets=buckets,
            limit_per_bucket=limit_per_bucket,
            include_hidden_pending=_include_hidden_pending_registry_rows(query),
        )
    )
    source_payload = _as_dict(payload.get("sources"))
    sources = {
        bucket: [
            compact_registry_source_table_row(dict(row))
            for row in _as_list(source_payload.get(bucket))
            if isinstance(row, dict)
        ]
        for bucket in buckets
    }
    return 200, {
        **payload,
        "ok": True,
        "sources": sources,
        "detailLevel": "table",
        "summaryView": True,
        "activeCompact": True,
    }


def _registry_sources_table_view_parts(
    api: _RegistryRouteApi,
    *,
    state: dict[str, list[dict[str, Any]]],
    sources: dict[str, list[dict[str, Any]]],
    summary: dict[str, Any],
    limit_per_bucket: int,
    detail_mode: str = "full",
) -> tuple[dict[str, list[dict[str, Any]]], dict[str, Any]]:
    if limit_per_bucket:
        truncated_buckets = _apply_registry_table_limit(sources, limit_per_bucket)
        summary["tableLimitPerBucket"] = limit_per_bucket
        summary["tableTruncatedBuckets"] = truncated_buckets
    # detail=summary skips the auto-approval annotation pass — that's the leg that
    # re-reads the discovery report and walks active aliases on every call. The
    # default detail=full keeps existing behavior; the cheap variant ships when
    # the Admin startup lane asks for it via ?view=table&detail=summary.
    if "pending" in sources and detail_mode == "full":
        annotated_pending, pending_approval_summary = annotate_pending_auto_approval_rows(
            sources.get("pending") or [],
            active_rows=[row for row in state.get("active") or [] if isinstance(row, dict)],
            report_candidates=_read_discovery_report_candidate_rows(api),
        )
        sources["pending"] = annotated_pending
        summary["pendingApproval"] = pending_approval_summary
        summary["pendingAutoApprovalEligibleCount"] = int(
            pending_approval_summary.get("autoApprovalEligibleCount") or 0
        )
    summary["detail"] = detail_mode
    return {
        bucket: [compact_registry_source_table_row(row) for row in rows]
        for bucket, rows in sources.items()
    }, summary


def _registry_sources_table_detail(query: dict[str, list[str]]) -> str:
    raw = str((query.get("detail") or [""])[0] or "").strip().lower()
    if raw in {"", "full"}:
        return "full"
    if raw == "summary":
        return "summary"
    return ""  # unknown detail mode


def handle_registry_routes(
    handler: BridgeResponseWriter,
    *,
    api: _RegistryRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    if path == "/registry/sources":
        with time_operation("registry.sources"):
            status, payload = _registry_sources_payload(api, query)
        handler.send_json(payload, status=status)
        return True

    if path == "/registry/summary":
        view = str((query.get("view") or [""])[0] or "").strip().lower()
        operation_name = "registry.summary.exact" if view == "exact" else "registry.summary.storage"
        with time_operation(operation_name):
            status, payload = _registry_summary_route_payload(api, query)
        handler.send_json(payload, status=status)
        return True

    return False
