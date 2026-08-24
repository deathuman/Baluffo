"""Registry GET route handlers.

AI boundary owns: registry summary/source GET route response wiring only.
AI boundary implement in: registry service compact-table payload; conflict
summaries stay in the conflicts route leaf.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

import time
from datetime import UTC, datetime
from typing import Any, Protocol

from src.bridge.performance_profile import time_operation
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.routes.route_storage_metrics import record_storage_read_metric


class _RegistryRouteApi(Protocol):
    def get_registry_compact_table_payload(self, **kwargs: Any) -> dict[str, Any]: ...


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


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
    deduped: list[str] = []
    for bucket in buckets:
        if bucket not in deduped:
            deduped.append(bucket)
    return deduped, ", ".join(sorted(set(invalid)))


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


def _removed_registry_sources_params(query: dict[str, list[str]]) -> list[str]:
    """Legacy modes removed from /registry/sources; any value means a caller
    still expects the old behavior and must get a loud error instead of a
    silently different payload."""
    removed: list[str] = []
    for key in ("detail", "activeCompact", "compactActive"):
        raw = str((query.get(key) or [""])[0] or "").strip()
        if raw:
            removed.append(f"{key}={raw}")
    return removed


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


def _registry_sources_payload(
    api: _RegistryRouteApi, query: dict[str, list[str]]
) -> tuple[int, dict[str, Any]]:
    view = str((query.get("view") or ["table"])[0] or "table").strip().lower()
    if view != "table":
        return 400, {
            "ok": False,
            "error": "invalid registry sources view",
            "invalidView": view,
            "allowedViews": ["table"],
        }
    removed = _removed_registry_sources_params(query)
    if removed:
        return 400, {
            "ok": False,
            "error": (
                "removed registry sources parameter(s): "
                f"{', '.join(removed)}; the endpoint now serves the single "
                "compact-table lane"
            ),
            "removedParams": removed,
        }
    buckets, invalid = _requested_registry_source_buckets(query)
    if invalid:
        return 400, {
            "ok": False,
            "error": "invalid registry bucket",
            "invalidBuckets": invalid,
        }
    limit_per_bucket = _registry_table_limit_per_bucket(query) or 25
    started_at = time.perf_counter()
    failed = True
    row_count = 0
    storage_kind = "normalized"
    try:
        payload = _as_dict(
            api.get_registry_compact_table_payload(
                buckets=buckets,
                limit_per_bucket=limit_per_bucket,
                include_hidden_pending=_include_hidden_pending_registry_rows(query),
            )
        )
        sources = _as_dict(payload.get("sources"))
        summary = _as_dict(payload.get("summary"))
        row_count = sum(len(rows) for rows in sources.values())
        storage_kind = str(summary.get("authorityMode") or storage_kind)
        response = {
            **payload,
            "ok": True,
            "detailLevel": "table",
            "summaryView": True,
        }
        failed = False
        return 200, response
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


def _registry_summary_route_payload(
    api: _RegistryRouteApi, query: dict[str, list[str]]
) -> tuple[int, dict[str, Any]]:
    view = str((query.get("view") or [""])[0] or "").strip().lower()
    if view not in ("", "exact"):
        return 400, {
            "ok": False,
            "error": "invalid registry summary view",
            "invalidView": view,
            "allowedViews": ["exact"],
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
        operation_name = "registry.summary.exact" if view == "exact" else "registry.summary"
        with time_operation(operation_name):
            status, payload = _registry_summary_route_payload(api, query)
        handler.send_json(payload, status=status)
        return True

    return False
