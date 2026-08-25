"""Admin Ops tab-count GET route wiring.

AI boundary owns: `/admin/ops-tab-counts` GET route response wiring only.
AI boundary implement in: source-policy, discovery, and registry artifact helpers.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

import json
import math
import os
import threading
import time
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

# ponytail: 30-second freshness window. Admin's own poll cadence is ~5 s; the
# badge strip can be a hair stale without anyone noticing. The mtime key still
# catches real file changes within the TTL on the next refresh.
OPS_TAB_COUNTS_CACHE_TTL_S = 30.0


class _AdminOpsTabCountsRouteApi(Protocol):
    DISCOVERY_REPORT_PATH: Path
    JOBS_FETCH_REPORT_PATH: Path
    SOURCE_POLICY_RECOMMENDATIONS_PATH: Path
    SOURCE_POLICY_REVIEW_STATE_PATH: Path

    def compute_ops_dashboard_health_summary(self) -> dict[str, Any]: ...

    def get_registry_auto_heal_report(self) -> dict[str, Any]: ...

    def get_registry_summary_payload(self) -> dict[str, Any]: ...

    def load_json_object(self, path: Path, default: Any) -> dict[str, Any]: ...

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
    tone = "warning" if needs_action > 0 else "neutral"
    title = (
        f"{needs_action} source policy review item{'' if needs_action == 1 else 's'}"
        if needs_action > 0
        else "No source policy review items"
    )
    if warnings:
        suffix = "; artifact warnings present" if needs_action <= 0 else "; artifact warnings"
        title = f"{title}{suffix}"
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


def _dedup_badge_from_fetch_report(api: _AdminOpsTabCountsRouteApi) -> dict[str, Any]:
    """Mirror frontend toDedupBadgeState so the badge loads without opening the tab."""
    report = _as_dict(api.load_json_object(Path(api.JOBS_FETCH_REPORT_PATH), {}))
    # Live artifacts carry top-level dedupEvidence; legacy/task-shaped reports
    # nest it under latestRun. Accept both so the badge survives either writer.
    evidence = _as_dict(report.get("dedupEvidence")) if report else {}
    if not evidence:
        evidence = (
            _as_dict(_as_dict(report.get("latestRun")).get("dedupEvidence")) if report else {}
        )
    if not evidence:
        return _ops_tab_badge(
            loaded=False,
            title="Dedup count unavailable",
            error="fetch_report_missing_or_empty" if not report else "dedup_evidence_missing",
        )
    gate = _as_dict(evidence.get("dedupAuditGate"))
    review_rows = sum(
        len(_as_list(evidence.get(key)))
        for key in (
            "reviewQueue",
            "providerStaticDisagreementExamples",
            "providerStaticTitleCompanyCollisionExamples",
        )
    )
    non_primary = _as_dict(gate.get("currentRunNonPrimaryMergeCounts"))
    blocking_count = sum(
        max(0, _safe_int(gate.get(key), 0))
        for key in (
            "currentRunBlockingReviewQueueCount",
            "carriedBlockingReviewQueueCount",
            "providerStaticDisagreementBlockedCount",
        )
    ) + max(0, _safe_int(non_primary.get("blocking"), 0))
    gate_flag_count = len(_as_list(gate.get("blockers"))) + len(_as_list(gate.get("warnings")))
    count = max(review_rows, blocking_count, gate_flag_count)
    blocked = (
        _clean_text(gate.get("status")).lower() == "blocked"
        or len(_as_list(gate.get("blockers"))) > 0
    )
    return _ops_tab_badge(
        count=count,
        tone="critical" if blocked else "warning" if count > 0 else "neutral",
        title=(
            f"{count} dedup review item{'' if count == 1 else 's'}"
            if count > 0
            else "No dedup review items"
        ),
    )


def _ops_tab_counts_cache_path(api: _AdminOpsTabCountsRouteApi) -> Path:
    """Cache file sits alongside jobs-source-state.json (data dir, not _out/)."""
    return Path(api.JOBS_FETCH_REPORT_PATH).with_name("ops-tab-counts.json")


def _ops_tab_counts_cache_key(api: _AdminOpsTabCountsRouteApi) -> list[list[Any]]:
    """mtime_ns for every stable file the badge computation reads.

    Files that don't exist contribute `None`, which is fine — an absent file
    is also a stable signal. Overview badge depends on `compute_ops_dashboard_health_summary`
    which has no single backing file to mtime-check; the TTL bound absorbs that.
    jobs-source-state.json is size-keyed only (see below): mtime invalidation
    fired on every heartbeat rewrite during runs, exactly while Admin was open.
    """
    paths = (
        Path(api.DISCOVERY_REPORT_PATH),
        Path(api.SOURCE_POLICY_RECOMMENDATIONS_PATH),
        Path(api.SOURCE_POLICY_REVIEW_STATE_PATH),
        Path(api.JOBS_FETCH_REPORT_PATH).with_name("registry-conflict-adjudication.json"),
        Path(api.JOBS_FETCH_REPORT_PATH),
    )
    signature: list[list[Any]] = []
    for path in paths:
        try:
            signature.append([str(path), path.stat().st_mtime_ns])
        except OSError:
            signature.append([str(path), None])
    # jobs-source-state.json gets a size-only signature: heartbeats rewrite it
    # with stable size during runs (cache holds), while real merges/finalize
    # change the row set and therefore its size (cache invalidates).
    source_state_path = Path(api.JOBS_FETCH_REPORT_PATH).with_name("jobs-source-state.json")
    try:
        signature.append([f"{source_state_path}:size", int(source_state_path.stat().st_size)])
    except OSError:
        signature.append([f"{source_state_path}:size", None])
    return signature


def _read_ops_tab_counts_cache(path: Path, key: list[list[Any]]) -> dict[str, Any] | None:
    try:
        cached = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None
    if not isinstance(cached, dict):
        return None
    try:
        cached_at = float(cached.get("cachedAtUnix") or 0.0)
    except (TypeError, ValueError, OverflowError):
        # ponytail: a corrupt envelope must degrade to recompute, never to a
        # route 500; inf/nan timestamps from bad JSON land here too.
        return None
    if not math.isfinite(cached_at) or cached_at <= 0.0:
        return None
    if (time.time() - cached_at) > OPS_TAB_COUNTS_CACHE_TTL_S:
        # Even with matching mtimes, refuse to serve a stale envelope. The TTL is a
        # safety net for cases where a backing file gets rewritten without an mtime
        # bump visible to us (network fs, copy truncation windows); 30s is short
        # enough that badge drift stays invisible on admin.
        return None
    if cached.get("cacheKey") != key:
        return None
    payload = cached.get("payload")
    if not isinstance(payload, dict) or not payload.get("ok"):
        return None
    # Stamp freshness for observability, not caching semantics.
    payload["cachedResponse"] = True
    payload["cachedAgeS"] = round(time.time() - cached_at, 2)
    return payload


def _write_ops_tab_counts_cache(path: Path, key: list[list[Any]], payload: dict[str, Any]) -> None:
    envelope = {
        "cacheKey": key,
        "cachedAtUnix": time.time(),
        "payload": payload,
    }
    # Unique tmp name: concurrent ThreadingHTTPServer writers must not clobber
    # each other's staging file before the atomic replace.
    tmp_path = path.with_suffix(f"{path.suffix}.{os.getpid()}.{threading.get_ident()}.tmp")
    try:
        tmp_path.write_text(
            json.dumps(envelope, separators=(",", ":"), sort_keys=True),
            encoding="utf-8",
        )
        tmp_path.replace(path)
    except OSError:
        # Cache write failure is not user-visible; skip and pay full cost next time.
        try:
            tmp_path.unlink(missing_ok=True)
        except OSError:
            pass


def _admin_ops_tab_counts_summary(api: _AdminOpsTabCountsRouteApi) -> dict[str, Any]:
    cache_path = _ops_tab_counts_cache_path(api)
    cache_key = _ops_tab_counts_cache_key(api)
    with time_operation("admin.ops_tab_counts.summary.cache_read"):
        cached = _read_ops_tab_counts_cache(cache_path, cache_key)
    if cached is not None:
        return cached

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

    try:
        badges["dedup"] = _dedup_badge_from_fetch_report(api)
    except (OSError, RuntimeError, TypeError, ValueError, json.JSONDecodeError) as exc:
        badges["dedup"] = _ops_tab_badge(
            loaded=False,
            title="Dedup count unavailable",
            error=str(exc),
        )

    payload = {
        "ok": True,
        "summaryView": True,
        "detailLevel": "summary",
        "generatedAt": api.now_iso(),
        "badges": badges,
    }
    with time_operation("admin.ops_tab_counts.summary.cache_write"):
        _write_ops_tab_counts_cache(cache_path, cache_key, payload)
    return payload


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
