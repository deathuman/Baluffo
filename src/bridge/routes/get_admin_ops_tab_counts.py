"""Admin Ops tab-count GET route wiring."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from src.bridge.performance_profile import time_operation
from src.bridge.routes.get_registry_conflicts import (
    registry_conflicts_badge_from_exact_summary,
)
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


class _AdminOpsTabCountsRouteApi(Protocol):
    DISCOVERY_REPORT_PATH: Path
    JOBS_FETCH_REPORT_PATH: Path
    SOURCE_POLICY_RECOMMENDATIONS_PATH: Path
    SOURCE_POLICY_REVIEW_STATE_PATH: Path

    def compute_ops_dashboard_health_summary(self) -> dict[str, Any]: ...

    def get_registry_auto_heal_report(self) -> dict[str, Any]: ...

    def get_registry_summary_payload(self) -> dict[str, Any]: ...

    def load_json_object(self, path: Path, default: Any = None) -> dict[str, Any]: ...

    def load_registry_conflict_adjudication(self) -> dict[str, Any]: ...

    def load_state(self) -> dict[str, Any]: ...

    def normalize_discovery_report_contract(self, payload: Any) -> dict[str, Any]: ...

    def now_iso(self) -> str: ...

    def summarize_state(self, state: dict[str, Any]) -> dict[str, Any]: ...


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


def _source_policy_badge_from_artifacts(api: _AdminOpsTabCountsRouteApi) -> dict[str, Any]:
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


def _discovery_review_badge_from_report(api: _AdminOpsTabCountsRouteApi) -> dict[str, Any]:
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


def _admin_ops_tab_counts_summary(api: _AdminOpsTabCountsRouteApi) -> dict[str, Any]:
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


def handle_admin_ops_tab_counts_routes(
    handler: BridgeResponseWriter,
    *,
    api: _AdminOpsTabCountsRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    """Handle Admin Ops tab-count GET routes."""
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

    return False
