"""Source-check orchestration helpers extracted from admin_bridge composition root."""

from __future__ import annotations

import re
import uuid
from collections.abc import Callable
from typing import Any

from src.bridge.registry_tombstones import is_tombstoned, load_tombstones
from src.source_discovery.provider_migration_advisory import (
    stage_provider_candidates_from_advisories,
)
from src.source_registry_identity import provider_fields_from_row_identity
from src.source_registry_state import transition_registry_to_pending


def normalize_manual_static_studio_fields(
    row: dict[str, Any],
    *,
    normalize_source_url: Callable[[str], str],
    infer_studio_name_from_host: Callable[[str], str],
) -> dict[str, Any]:
    normalized = dict(row)
    source_url = normalize_source_url(
        str(normalized.get("listing_url") or "")
    ) or normalize_source_url(
        str(
            (normalized.get("pages") or [""])[0]
            if isinstance(normalized.get("pages"), list)
            else ""
        )
    )
    if not source_url:
        return normalized
    inferred = infer_studio_name_from_host(source_url)
    current_studio = str(normalized.get("studio") or "").strip().lower()
    if current_studio in {"", "www", "w", "manual source"} or bool(
        re.search(r"\b(?:game|studio)\s+s\b", current_studio)
    ):
        normalized["studio"] = inferred
        normalized["company"] = inferred
        normalized["name"] = f"{inferred} (Manual Website)"
    return normalized


def _build_static_success_result(
    *,
    run_id: str,
    updated: dict[str, Any],
    jobs_found: int,
    weak_signal: bool,
    probe_meta: dict[str, Any],
    source_identity: Callable[[dict[str, Any]], str],
) -> dict[str, Any]:
    result = {
        "started": True,
        "runId": run_id,
        "sourceId": source_identity(updated),
        "ok": True,
        "jobsFound": int(jobs_found),
        "weakSignal": bool(weak_signal),
        "browserFallbackAttempted": bool((probe_meta or {}).get("browserFallbackAttempted")),
        "browserFallbackUsed": bool((probe_meta or {}).get("browserFallbackUsed")),
    }
    staged = (probe_meta or {}).get("stagedProviderCandidate")
    if isinstance(staged, dict):
        result["stagedProviderCandidate"] = staged
    return result


def _stage_provider_candidate_from_static_check(
    *,
    state: dict[str, Any],
    updated: dict[str, Any],
    source_identity: Callable[[dict[str, Any]], str],
    probe_meta: dict[str, Any],
    at: str,
) -> dict[str, Any]:
    links = probe_meta.get("providerEvidenceLinks") if isinstance(probe_meta, dict) else None
    if not isinstance(links, list) or not links:
        return {}
    evidence = dict(updated)
    evidence["sourceIdentity"] = source_identity(updated)
    evidence["atsLinks"] = [str(link) for link in links[:5] if str(link or "").strip()]
    staged = stage_provider_candidates_from_advisories(
        [evidence],
        active_rows=state.get("active", []),
        pending_rows=state.get("pending", []),
        at=at,
    )
    if not staged:
        return {}
    pending = transition_registry_to_pending(
        staged[0],
        reason="provider_migration_candidate",
        actor="provider_migration_advisory",
        at=at,
    )
    pending["candidateState"] = "staged_provider_candidate"
    pending_rows = state.get("pending", [])
    if isinstance(pending_rows, list):
        pending_rows.append(pending)
        state["pending"] = pending_rows
    return {
        "sourceId": source_identity(pending),
        "name": str(pending.get("name") or ""),
        "adapter": str(pending.get("adapter") or ""),
        "detectedProviderFamily": str(pending.get("detectedProviderFamily") or ""),
        "detectedProviderUrl": str(pending.get("detectedProviderUrl") or ""),
        "migrationSourceIdentity": str(pending.get("migrationSourceIdentity") or ""),
    }


def _build_failure_result(
    *,
    run_id: str,
    updated: dict[str, Any],
    error: str,
    failure_details: dict[str, Any],
    source_identity: Callable[[dict[str, Any]], str],
    include_browser_used: bool = False,
    probe_meta: dict[str, Any] | None = None,
) -> dict[str, Any]:
    result = {
        "started": True,
        "runId": run_id,
        "sourceId": source_identity(updated),
        "ok": False,
        "error": str(error or "probe failed"),
        "errorCode": str(failure_details.get("errorCode") or "probe_failed"),
        "suggestedUrls": failure_details.get("suggestedUrls") or [],
    }
    if "browserFallbackAttempted" in failure_details:
        result["browserFallbackAttempted"] = bool(failure_details.get("browserFallbackAttempted"))
    if include_browser_used:
        result["browserFallbackUsed"] = bool((probe_meta or {}).get("browserFallbackUsed"))
    return result


def _record_static_probe_success(
    *,
    run_id: str,
    state: dict[str, Any],
    rows: list[dict[str, Any]],
    idx: int,
    row: dict[str, Any],
    jobs_found: int,
    weak_signal: bool,
    probe_meta: dict[str, Any],
    source_identity: Callable[[dict[str, Any]], str],
    compute_candidate_score: Callable[[dict[str, Any], int], tuple[int, list[str]]],
    persist_state_and_auto_sync: Callable[..., dict[str, Any]],
    now_iso: Callable[[], str],
) -> dict[str, Any]:
    score, reasons = compute_candidate_score(row, jobs_found)
    updated = dict(row)
    probed_at = now_iso()
    updated["lastProbedAt"] = probed_at
    updated["jobsFound"] = int(jobs_found)
    updated["sampleCount"] = int(jobs_found)
    updated["score"] = int(score)
    updated["reasons"] = reasons
    updated["confidence"] = "high" if jobs_found >= 10 else ("medium" if jobs_found >= 1 else "low")
    updated.pop("lastProbeError", None)
    updated["lastProbeWeakSignal"] = bool(weak_signal)
    rows[idx] = updated
    staged_provider = _stage_provider_candidate_from_static_check(
        state=state,
        updated=updated,
        source_identity=source_identity,
        probe_meta=probe_meta,
        at=probed_at,
    )
    if staged_provider:
        probe_meta = dict(probe_meta)
        probe_meta["stagedProviderCandidate"] = staged_provider
    persist_state_and_auto_sync(state, reason="source_check_updated")
    return _build_static_success_result(
        run_id=run_id,
        updated=updated,
        jobs_found=jobs_found,
        weak_signal=weak_signal,
        probe_meta=probe_meta,
        source_identity=source_identity,
    )


def _record_static_probe_failure(
    *,
    run_id: str,
    state: dict[str, Any],
    rows: list[dict[str, Any]],
    idx: int,
    row: dict[str, Any],
    error: str,
    probe_meta: dict[str, Any],
    source_identity: Callable[[dict[str, Any]], str],
    persist_state_and_auto_sync: Callable[..., dict[str, Any]],
    normalize_source_url: Callable[[str], str],
    build_check_failure_details: Callable[..., dict[str, Any]],
    now_iso: Callable[[], str],
) -> dict[str, Any]:
    updated = dict(row)
    updated["lastProbedAt"] = now_iso()
    updated["lastProbeError"] = str(error or "probe failed")
    rows[idx] = updated
    persist_state_and_auto_sync(state, reason="source_check_updated")
    source_url = normalize_source_url(
        str(updated.get("listing_url") or "")
    ) or normalize_source_url(
        str((updated.get("pages") or [""])[0] if isinstance(updated.get("pages"), list) else "")
    )
    failure_details = build_check_failure_details(
        str(error or "probe failed"),
        source_url or "",
        browser_fallback_attempted=bool((probe_meta or {}).get("browserFallbackAttempted")),
    )
    return _build_failure_result(
        run_id=run_id,
        updated=updated,
        error=str(error or "probe failed"),
        failure_details=failure_details,
        source_identity=source_identity,
        include_browser_used=True,
        probe_meta=probe_meta,
    )


def _trigger_static_source_check(
    *,
    run_id: str,
    state: dict[str, Any],
    rows: list[dict[str, Any]],
    idx: int,
    row: dict[str, Any],
    timeout_s: int,
    source_identity: Callable[[dict[str, Any]], str],
    normalize_manual_static_studio_fields_fn: Callable[[dict[str, Any]], dict[str, Any]],
    check_static_source_fn: Callable[
        [dict[str, Any], int], tuple[bool, int, str, bool, dict[str, Any]]
    ],
    now_iso: Callable[[], str],
    compute_candidate_score: Callable[[dict[str, Any], int], tuple[int, list[str]]],
    persist_state_and_auto_sync: Callable[..., dict[str, Any]],
    normalize_source_url: Callable[[str], str],
    build_check_failure_details: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    row = normalize_manual_static_studio_fields_fn(row)
    ok, jobs_found, error, weak_signal, probe_meta = check_static_source_fn(row, timeout_s)
    if ok:
        return _record_static_probe_success(
            run_id=run_id,
            state=state,
            rows=rows,
            idx=idx,
            row=row,
            jobs_found=jobs_found,
            weak_signal=weak_signal,
            probe_meta=probe_meta,
            source_identity=source_identity,
            compute_candidate_score=compute_candidate_score,
            persist_state_and_auto_sync=persist_state_and_auto_sync,
            now_iso=now_iso,
        )
    return _record_static_probe_failure(
        run_id=run_id,
        state=state,
        rows=rows,
        idx=idx,
        row=row,
        error=error,
        probe_meta=probe_meta,
        source_identity=source_identity,
        persist_state_and_auto_sync=persist_state_and_auto_sync,
        normalize_source_url=normalize_source_url,
        build_check_failure_details=build_check_failure_details,
        now_iso=now_iso,
    )


def _reconstruct_probe_candidate(row: dict[str, Any]) -> dict[str, Any]:
    reconstructed = dict(row)
    for key, value in provider_fields_from_row_identity(reconstructed).items():
        reconstructed.setdefault(key, value)
    adapter = str(reconstructed.get("adapter") or "").strip().lower()
    if adapter == "greenhouse" and not reconstructed.get("api_url") and reconstructed.get("slug"):
        reconstructed["api_url"] = (
            f"https://boards-api.greenhouse.io/v1/boards/{reconstructed.get('slug')}/jobs"
        )
    elif adapter == "lever" and not reconstructed.get("api_url") and reconstructed.get("account"):
        reconstructed["api_url"] = (
            f"https://api.lever.co/v0/postings/{reconstructed.get('account')}?mode=json"
        )
    elif (
        adapter == "workable" and not reconstructed.get("api_url") and reconstructed.get("account")
    ):
        reconstructed["api_url"] = (
            f"https://apply.workable.com/api/v1/widget/accounts/{reconstructed.get('account')}?details=true"
        )
    elif (
        adapter == "smartrecruiters"
        and not reconstructed.get("api_url")
        and reconstructed.get("company_id")
    ):
        reconstructed["api_url"] = (
            f"https://api.smartrecruiters.com/v1/companies/{reconstructed.get('company_id')}/postings"
        )
    return reconstructed


def _candidate_endpoint_url(row: dict[str, Any]) -> str:
    return str(
        row.get("listing_url")
        or row.get("api_url")
        or row.get("feed_url")
        or row.get("board_url")
        or ""
    )


def trigger_source_check(
    source_id: str,
    *,
    timeout_s: int = 12,
    load_state: Callable[[], dict[str, Any]],
    source_identity: Callable[[dict[str, Any]], str],
    normalize_manual_static_studio_fields_fn: Callable[[dict[str, Any]], dict[str, Any]],
    check_static_source_fn: Callable[
        [dict[str, Any], int], tuple[bool, int, str, bool, dict[str, Any]]
    ],
    now_iso: Callable[[], str],
    compute_candidate_score: Callable[[dict[str, Any], int], tuple[int, list[str]]],
    normalize_candidate: Callable[..., dict[str, Any]],
    probe_candidate: Callable[..., tuple[bool, int, str]],
    persist_state_and_auto_sync: Callable[..., dict[str, Any]],
    normalize_source_url: Callable[[str], str],
    build_check_failure_details: Callable[..., dict[str, Any]],
) -> dict[str, Any]:
    token = str(source_id or "").strip().lower()
    if not token:
        return {"started": False, "error": "Missing sourceId."}

    state = load_state()
    if is_tombstoned(token, load_tombstones()):
        return {"started": False, "error": "Source is deleted locally. Restore it first."}
    run_id = f"check_{uuid.uuid4().hex[:12]}"
    for bucket in ("active", "pending", "rejected"):
        rows = state.get(bucket, [])
        for idx, row in enumerate(rows):
            if source_identity(row) != token:
                continue
            if str(row.get("adapter") or "").strip().lower() == "static":
                state[bucket] = rows
                return _trigger_static_source_check(
                    run_id=run_id,
                    state=state,
                    rows=rows,
                    idx=idx,
                    row=row,
                    timeout_s=timeout_s,
                    source_identity=source_identity,
                    normalize_manual_static_studio_fields_fn=normalize_manual_static_studio_fields_fn,
                    check_static_source_fn=check_static_source_fn,
                    now_iso=now_iso,
                    compute_candidate_score=compute_candidate_score,
                    persist_state_and_auto_sync=persist_state_and_auto_sync,
                    normalize_source_url=normalize_source_url,
                    build_check_failure_details=build_check_failure_details,
                )

            ok, jobs_found, error = probe_candidate(row, timeout_s=timeout_s)
            if not ok and str(error or "").strip().lower() == "missing adapter or url":
                ok, jobs_found, error = probe_candidate(
                    _reconstruct_probe_candidate(row), timeout_s=timeout_s
                )
            if ok:
                score, reasons = compute_candidate_score(row, jobs_found)
                updated = normalize_candidate(row, score, reasons, jobs_found, probed_at=now_iso())
                updated["enabledByDefault"] = bool(row.get("enabledByDefault"))
                updated.pop("lastProbeError", None)
                if row.get("manualAddedAt"):
                    updated["manualAddedAt"] = row.get("manualAddedAt")
                rows[idx] = updated
                state[bucket] = rows
                persist_state_and_auto_sync(state, reason="source_check_updated")
                return {
                    "started": True,
                    "runId": run_id,
                    "sourceId": source_identity(updated),
                    "ok": True,
                    "jobsFound": int(jobs_found),
                }

            updated = dict(row)
            updated["lastProbedAt"] = now_iso()
            updated["lastProbeError"] = str(error or "probe failed")
            rows[idx] = updated
            state[bucket] = rows
            persist_state_and_auto_sync(state, reason="source_check_updated")
            source_url = (
                normalize_source_url(endpoint_url := _candidate_endpoint_url(row)) or endpoint_url
            )
            failure_details = build_check_failure_details(
                str(error or "probe failed"), str(source_url or "")
            )
            return _build_failure_result(
                run_id=run_id,
                updated=updated,
                error=str(error or "probe failed"),
                failure_details=failure_details,
                source_identity=source_identity,
            )

    return {"started": False, "error": "Source not found."}
