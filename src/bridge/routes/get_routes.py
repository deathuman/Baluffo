"""GET route surface for the admin bridge.

AI boundary owns: GET route dispatch and response wiring only.
AI boundary implement in: bridge services/leaves behind `BridgeApi`.
AI boundary search before contracts: frontend callers, route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET tests.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

from src.bridge.admin_bootstrap import get_admin_bootstrap_payload
from src.bridge.api import BridgeApi
from src.bridge.container_mode import is_container_runtime, send_container_unavailable
from src.bridge.performance_profile import time_operation
from src.bridge.routes.error_boundary import (
    send_json_boundary,
)
from src.bridge.routes.get_discovery import handle_discovery_routes
from src.bridge.routes.get_fetch_report import handle_fetch_report_routes
from src.bridge.routes.get_local_data import handle_local_data_get_routes
from src.bridge.routes.get_ops_diagnostics import handle_ops_diagnostic_routes
from src.bridge.routes.get_ops_status import handle_ops_status_routes
from src.bridge.routes.get_registry import handle_registry_routes
from src.bridge.routes.get_registry_conflicts import (
    handle_registry_conflict_routes,
    registry_conflicts_badge_from_exact_summary,
)
from src.bridge.routes.get_source_policy import handle_source_policy_routes
from src.bridge.routes.get_sync import handle_sync_routes
from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.routes.route_payload_helpers import (
    as_dict as _as_dict,
)
from src.bridge.routes.route_payload_helpers import (
    as_list as _as_list,
)
from src.bridge.routes.route_payload_helpers import (
    clean_text as _clean_text,
)
from src.bridge.routes.route_payload_helpers import (
    path_signature as _path_signature,
)
from src.jobs.common.contracts_source_policy_recommendations import (
    merge_source_policy_review_state_into_recommendations,
    read_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    read_source_policy_review_state_artifact,
)

_ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED = False


def _consume_admin_bootstrap_smoke_fail_once() -> bool:
    if str(os.getenv("BALUFFO_PACKAGED_SMOKE_RUNTIME") or "").strip() != "1":
        return False
    requested = str(os.getenv("BALUFFO_PACKAGED_SMOKE_ADMIN_BOOTSTRAP_FAIL_ONCE") or "")
    if requested.strip().lower() not in {"1", "true", "yes", "on"}:
        return False
    global _ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED
    if _ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED:
        return False
    _ADMIN_BOOTSTRAP_SMOKE_FAIL_ONCE_CONSUMED = True
    return True


def _ops_tab_badge(
    *,
    count: int = 0,
    tone: str = "neutral",
    title: str = "",
    loaded: bool = True,
    error: str = "",
) -> dict[str, Any]:
    return {
        "count": max(0, int(count or 0)),
        "tone": str(tone or "neutral"),
        "title": str(title or ""),
        "loaded": bool(loaded),
        "error": str(error or ""),
    }


def _source_policy_badge_from_artifacts(api: BridgeApi) -> dict[str, Any]:
    recommendations, recommendation_warning = read_source_policy_recommendations_artifact(
        api.SOURCE_POLICY_RECOMMENDATIONS_PATH
    )
    review_state, review_state_warning = read_source_policy_review_state_artifact(
        api.SOURCE_POLICY_REVIEW_STATE_PATH
    )
    payload = merge_source_policy_review_state_into_recommendations(
        recommendations_artifact=recommendations,
        review_state=review_state,
    )
    pairs = _as_list(_as_dict(payload).get("pairs"))
    needs_action = 0
    for row in pairs:
        row_obj = _as_dict(row)
        review_state_text = _clean_text(row_obj.get("reviewState") or "new").lower()
        if review_state_text in {"new", "acknowledged"}:
            needs_action += 1
    warnings = [warning for warning in (recommendation_warning, review_state_warning) if warning]
    tone = "warning" if needs_action > 0 or warnings else "neutral"
    title = (
        f"{needs_action} source policy review item{'' if needs_action == 1 else 's'}"
        if needs_action > 0
        else "No source policy review items"
    )
    if warnings and needs_action <= 0:
        title = "Source policy review summary has warnings"
    return _ops_tab_badge(count=needs_action, tone=tone, title=title)


def _discovery_review_badge_from_report(api: BridgeApi) -> dict[str, Any]:
    report_path = Path(api.DISCOVERY_REPORT_PATH)
    if _path_signature(report_path) is None:
        return _ops_tab_badge(
            loaded=False,
            title="Discovery review count unavailable",
            error="discovery_report_missing",
        )
    report = _as_dict(api.load_json_object(report_path, {}))
    if not report:
        return _ops_tab_badge(
            loaded=False,
            title="Discovery review count unavailable",
            error="discovery_report_empty",
        )
    normalized = _as_dict(api.normalize_discovery_report_contract(report))
    review = _as_dict(normalized.get("candidateReview"))
    summary = _as_dict(normalized.get("summary"))
    counts = _as_dict(normalized.get("counts"))
    count = _safe_int(
        review.get("totalCandidates")
        or summary.get("queuedCandidateCount")
        or summary.get("candidateCount")
        or summary.get("newCandidateCount")
        or counts.get("queuedCandidates")
        or counts.get("candidateCount")
        or counts.get("generatedCandidates")
        or 0,
        0,
    )
    return _ops_tab_badge(
        count=count,
        tone="warning" if count > 0 else "neutral",
        title=(
            f"{count} discovery review item{'' if count == 1 else 's'}"
            if count > 0
            else "No discovery review items"
        ),
    )


def _admin_ops_tab_counts_summary(api: BridgeApi) -> dict[str, Any]:
    badges: dict[str, dict[str, Any]] = {}

    try:
        health = _as_dict(api.compute_ops_dashboard_health_summary())
        alerts = _as_list(health.get("alerts"))
        critical_count = sum(
            1
            for alert in alerts
            if _clean_text(_as_dict(alert).get("severity")).lower() == "critical"
        )
        badges["overview"] = _ops_tab_badge(
            count=len(alerts),
            tone="critical" if critical_count else "warning" if alerts else "neutral",
            title=(
                f"{len(alerts)} active alert{'' if len(alerts) == 1 else 's'}"
                if alerts
                else "No active alerts"
            ),
        )
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        badges["overview"] = _ops_tab_badge(
            loaded=False,
            title="Overview count unavailable",
            error=str(exc),
        )

    try:
        badges["discovery"] = _discovery_review_badge_from_report(api)
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        badges["discovery"] = _ops_tab_badge(
            loaded=False,
            title="Discovery review count unavailable",
            error=str(exc),
        )

    try:
        badges["source-policy"] = _source_policy_badge_from_artifacts(api)
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        badges["source-policy"] = _ops_tab_badge(
            loaded=False,
            title="Source policy review count unavailable",
            error=str(exc),
        )

    try:
        badges["registry-conflicts"] = registry_conflicts_badge_from_exact_summary(api)
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        badges["registry-conflicts"] = _ops_tab_badge(
            loaded=False,
            title="Registry conflict count unavailable",
            error=str(exc),
        )

    badges["dedup"] = _ops_tab_badge(
        loaded=False,
        title="Dedup count loads with dedup diagnostics",
    )

    return {
        "ok": True,
        "summaryView": True,
        "detailLevel": "summary",
        "generatedAt": api.now_iso(),
        "badges": badges,
    }


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value or default)
    except (TypeError, ValueError):
        return int(default)


def _json_error(exc: Exception) -> dict[str, Any]:
    return {"ok": False, "error": str(exc)}


def handle_get(
    handler: BridgeResponseWriter, *, api: BridgeApi, path: str, query: dict[str, list[str]]
) -> bool:
    """Handle GET routes for the admin bridge.

    Important: `api` must be the currently running BridgeApi instance.
    """

    if path == "/app/ready":
        with time_operation("app.ready.route_payload"):
            handler.send_json(api.compute_ops_health_ready())
        return True

    if path == "/admin/bootstrap":
        with time_operation("admin.bootstrap.route_payload"):
            if _consume_admin_bootstrap_smoke_fail_once():
                handler.send_json(
                    {
                        "ok": False,
                        "error": "packaged smoke forced admin bootstrap timeout",
                        "smokeFailure": True,
                    },
                    status=504,
                )
                return True
            handler.send_json(get_admin_bootstrap_payload(api))
        return True

    if path == "/admin/ops-tab-counts":
        view = str((query.get("view") or ["summary"])[0] or "summary").strip().lower()
        if view not in {"", "summary"}:
            handler.send_json(
                {"ok": False, "error": f"unsupported ops-tab-counts view: {view}"},
                status=400,
            )
            return True
        with time_operation("admin.ops_tab_counts.summary.route_payload"):
            handler.send_json(_admin_ops_tab_counts_summary(api))
        return True

    if handle_discovery_routes(handler, api=api, path=path, query=query):
        return True

    if handle_local_data_get_routes(handler, api=api, path=path, query=query):
        return True

    if path == "/app/update-status":
        if is_container_runtime(api):
            send_container_unavailable(handler)
            return True
        send_json_boundary(
            handler,
            api.get_update_status_payload,
            error_status=500,
            error_payload=_json_error,
        )
        return True

    if handle_registry_routes(handler, api=api, path=path, query=query):
        return True

    if handle_fetch_report_routes(handler, api=api, path=path, query=query):
        return True

    if handle_ops_status_routes(handler, api=api, path=path, query=query):
        return True

    if handle_ops_diagnostic_routes(handler, api=api, path=path, query=query):
        return True

    if handle_source_policy_routes(handler, api=api, path=path, query=query):
        return True

    if handle_registry_conflict_routes(handler, api=api, path=path, query=query):
        return True

    if handle_sync_routes(handler, api=api, path=path, query=query):
        return True

    if path == "/tasks/jobs-pipeline-schedule":
        handler.send_json(api.get_jobs_pipeline_schedule_payload())
        return True

    if path == "/tasks/run-jobs-pipeline-status":
        handler.send_json(api.get_jobs_pipeline_status_payload())
        return True

    return False
