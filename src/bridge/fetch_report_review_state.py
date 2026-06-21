"""Fetch-report review-state merge helpers.

AI boundary owns: dedup review-state hydration into fetch-report payloads.
AI boundary implement in: this file for review-state merging only; report normalization stays in shared/bridge normalizers.
AI boundary search before contracts: fetch-report routes, jobs review-state contracts, and source health tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused fetch-report review-state tests.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.jobs.common.contracts_dedup_review_state import (
    merge_dedup_review_state_into_dedup_evidence,
    read_dedup_review_state_artifact,
)
from src.jobs.reporting_dedup_evidence import build_dedup_audit_gate
from src.source_registry_io import load_runtime_evidence


def merge_dedup_review_state_into_fetch_report(
    fetch_report: dict[str, Any], review_state: Any
) -> dict[str, Any]:
    payload = dict(fetch_report or {})
    dedup_evidence = payload.get("dedupEvidence")
    if not isinstance(dedup_evidence, dict):
        return payload
    merged_dedup_evidence = merge_dedup_review_state_into_dedup_evidence(
        dedup_evidence, review_state
    )
    merged_dedup_evidence["dedupAuditGate"] = build_dedup_audit_gate(merged_dedup_evidence)
    payload["dedupEvidence"] = merged_dedup_evidence
    return payload


def load_fetch_report_with_dedup_review_state(
    *,
    normalize_fetch_report_contract: Callable[[dict[str, Any]], dict[str, Any]],
    jobs_fetch_report_path: Path,
    dedup_review_state_path: Path,
) -> tuple[dict[str, Any], str]:
    payload = normalize_fetch_report_contract(load_runtime_evidence(jobs_fetch_report_path, {}))
    review_state, warning = read_dedup_review_state_artifact(dedup_review_state_path)
    return merge_dedup_review_state_into_fetch_report(payload, review_state), warning
