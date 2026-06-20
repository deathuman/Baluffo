"""Source-policy GET route handlers.

AI boundary owns: `/source-policy/recommendations` GET route response wiring only.
AI boundary implement in: source-policy contracts, review state, and link-backfill helpers.
AI boundary search before contracts: frontend callers, bridge route inventory, API docs.
AI boundary verify: `npm run lint:repo-guardrails` plus focused GET route tests.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Protocol

from src.bridge.routes.response_writer import BridgeResponseWriter
from src.bridge.routes.route_payload_helpers import (
    as_dict as _as_dict,
)
from src.bridge.routes.route_payload_helpers import (
    as_list as _as_list,
)
from src.bridge.source_policy_link_backfill import (
    enrich_provider_coverage_link_backfill,
    load_provider_coverage_link_backfill,
    source_policy_soak_report_path,
)
from src.jobs.common.contracts_source_policy_recommendations import (
    merge_source_policy_review_state_into_recommendations,
    read_source_policy_recommendations_artifact,
)
from src.jobs.common.contracts_source_policy_review_state import (
    read_source_policy_review_state_artifact,
)


class _SourcePolicyRouteApi(Protocol):
    JOBS_FETCH_REPORT_PATH: Path
    SOURCE_POLICY_RECOMMENDATIONS_PATH: Path
    SOURCE_POLICY_REVIEW_STATE_PATH: Path

    def load_state(self) -> dict[str, Any]: ...

    def source_identity(self, row: dict[str, Any]) -> str: ...


def _empty_suppression_eligibility_payload() -> dict[str, Any]:
    return {
        "readyLinkedProviderCount": 0,
        "selectedLinkedStaticCount": 0,
        "missingLinkedStaticCount": 0,
        "suppressedLinkedStaticCount": 0,
        "missingLinkedStaticRows": [],
    }


def _load_suppression_eligibility(api: _SourcePolicyRouteApi) -> tuple[dict[str, Any], str]:
    path = source_policy_soak_report_path(api)
    empty_payload = _empty_suppression_eligibility_payload()
    if not path.exists():
        return empty_payload, ""
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return empty_payload, f"source_policy_soak_report_unreadable: {exc}"
    section = _as_dict(_as_dict(payload.get("sections")).get("suppressionEligibility"))
    if not section:
        return empty_payload, ""
    result = {
        key: section.get(key, empty_payload[key])
        for key in (
            "readyLinkedProviderCount",
            "selectedLinkedStaticCount",
            "missingLinkedStaticCount",
            "suppressedLinkedStaticCount",
        )
    }
    result["missingLinkedStaticRows"] = [
        dict(row)
        for row in _as_list(section.get("missingLinkedStaticRows"))
        if isinstance(row, dict)
    ]
    return result, ""


def handle_source_policy_routes(
    handler: BridgeResponseWriter,
    *,
    api: _SourcePolicyRouteApi,
    path: str,
    query: dict[str, list[str]],
) -> bool:
    del query
    if path == "/source-policy/recommendations":
        recommendations, recommendation_warning = read_source_policy_recommendations_artifact(
            api.SOURCE_POLICY_RECOMMENDATIONS_PATH
        )
        review_state, review_state_warning = read_source_policy_review_state_artifact(
            api.SOURCE_POLICY_REVIEW_STATE_PATH
        )
        link_backfill, link_backfill_warning = load_provider_coverage_link_backfill(api)
        suppression_eligibility, suppression_eligibility_warning = _load_suppression_eligibility(
            api
        )
        link_backfill = enrich_provider_coverage_link_backfill(api, link_backfill)
        payload = merge_source_policy_review_state_into_recommendations(
            recommendations_artifact=recommendations,
            review_state=review_state,
        )
        handler.send_json(
            {
                "ok": True,
                "recommendations": payload,
                "reviewState": review_state,
                "providerCoverageLinkBackfill": link_backfill,
                "suppressionEligibility": suppression_eligibility,
                "warnings": [
                    warning
                    for warning in (
                        recommendation_warning,
                        review_state_warning,
                        link_backfill_warning,
                        suppression_eligibility_warning,
                    )
                    if warning
                ],
            }
        )
        return True

    return False
