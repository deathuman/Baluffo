"""Registry conflict adjudication task helpers.

AI boundary owns: background adjudication launch, progress, and conflict decision plumbing.
AI boundary implement in: this file for adjudication task flow; conflict row derivation stays in registry conflict leaves.
AI boundary search before contracts: registry conflict routes, post admin routes, and adjudication tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused registry adjudication tests.
"""

from __future__ import annotations

import json
import re
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any
from urllib.parse import urlparse

from src.bridge.registry_conflicts import (
    build_registry_conflicts_summary_cache_key,
    derive_registry_conflict_queue,
    summarize_registry_conflicts_payload,
    write_registry_conflicts_summary_cache,
)
from src.bridge.source_check_http import try_fetch_with_playwright
from src.bridge.source_probe_evidence import probe_source_evidence
from src.jobs.adapters.html_parsers import parse_jobpostings_from_html
from src.jobs.adapters.parsers.json_payloads import (
    parse_greenhouse_jobs_payload,
    parse_lever_jobs_payload,
    parse_pinpoint_jobs_payload,
    parse_recruitee_jobs_payload,
    parse_smartrecruiters_jobs_payload,
    parse_workable_jobs_payload,
)
from src.jobs.adapters.parsers.provider_html import parse_jazzhr_jobs_html
from src.jobs.text_utils import clean_text, norm_text, normalize_url
from src.source_discovery.probe import static_probe_evidence
from src.source_registry import source_identity
from src.source_registry_identity import provider_fields_from_row_identity
from src.source_registry_state import transition_registry_to_pending

ADJUDICATION_REASON = "registry_conflict_adjudication_auto_demote"
ADJUDICATION_PATH_NAME = "registry-conflict-adjudication.json"
_ADJUDICATION_LOCK = threading.RLock()
_ADJUDICATION_JOB_LOCK = threading.Lock()
_ADJUDICATION_JOB_THREAD: threading.Thread | None = None
_RECENT_PROGRESS_EVENT_LIMIT = 20
_DEFAULT_PROGRESS_THROTTLE_SECONDS = 1.0
_PROVIDER_PAYLOAD_PARSERS = {
    "greenhouse": parse_greenhouse_jobs_payload,
    "lever": parse_lever_jobs_payload,
    "smartrecruiters": parse_smartrecruiters_jobs_payload,
    "workable": parse_workable_jobs_payload,
    "recruitee": parse_recruitee_jobs_payload,
    "pinpoint": parse_pinpoint_jobs_payload,
}


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _artifact_path(api: Any) -> Any:
    return getattr(api, "REGISTRY_CONFLICT_ADJUDICATION_PATH", None) or (
        api.JOBS_FETCH_REPORT_PATH.with_name(ADJUDICATION_PATH_NAME)
    )


def load_registry_conflict_adjudication(api: Any) -> dict[str, Any]:
    return _as_dict(api.load_json_object(_artifact_path(api), {}))


def _summary_payload() -> dict[str, int]:
    return {
        "autoDemoteApplied": 0,
        "recommendedDemotion": 0,
        "keepBoth": 0,
        "needsReview": 0,
        "probeFailed": 0,
    }


def _task_progress_payload(
    *,
    active: bool,
    phase_key: str,
    phase_label: str,
    ratio: float,
    counts: dict[str, int],
    updated_at: str,
    target_label: str = "",
    target_url: str = "",
) -> dict[str, Any]:
    mode = "determinate" if counts.get("totalSources", 0) > 0 else "indeterminate"
    return {
        "active": active,
        "phaseKey": phase_key,
        "phaseLabel": phase_label,
        "mode": mode,
        "ratio": max(0.0, min(1.0, ratio)),
        "counts": counts,
        "targetLabel": target_label,
        "targetUrl": target_url,
        "updatedAt": updated_at,
    }


def _base_progress_payload(now: str) -> dict[str, Any]:
    return {
        "totalFamilyCount": 0,
        "checkedFamilyCount": 0,
        "totalSourceCount": 0,
        "checkedSourceCount": 0,
        "currentFamilyKey": "",
        "currentFamilyIndex": 0,
        "currentSourceId": "",
        "currentSourceName": "",
        "currentAdapter": "",
        "currentEndpointUrl": "",
        "lastProgressAt": now,
        "recentEvents": [],
    }


def _progress_counts(progress: dict[str, Any]) -> dict[str, int]:
    return {
        "checkedFamilies": int(progress.get("checkedFamilyCount") or 0),
        "totalFamilies": int(progress.get("totalFamilyCount") or 0),
        "checkedSources": int(progress.get("checkedSourceCount") or 0),
        "totalSources": int(progress.get("totalSourceCount") or 0),
    }


def _progress_ratio(progress: dict[str, Any]) -> float:
    total = int(progress.get("totalSourceCount") or 0)
    if total <= 0:
        return 0.0
    checked = int(progress.get("checkedSourceCount") or 0)
    return checked / total


def _progress_throttle_seconds(payload: dict[str, Any]) -> float:
    try:
        return max(
            0.0,
            float(payload.get("progressThrottleSeconds") or _DEFAULT_PROGRESS_THROTTLE_SECONDS),
        )
    except (TypeError, ValueError):
        return _DEFAULT_PROGRESS_THROTTLE_SECONDS


def _progress_event(
    event: str,
    *,
    timestamp: str,
    family_key: str = "",
    source_id: str = "",
    source_name: str = "",
    adapter: str = "",
    jobs_found: int | None = None,
    ok: bool | None = None,
) -> dict[str, Any]:
    row: dict[str, Any] = {
        "event": event,
        "timestamp": timestamp,
    }
    if family_key:
        row["familyKey"] = family_key
    if source_id:
        row["sourceId"] = source_id
    if source_name:
        row["sourceName"] = source_name
    if adapter:
        row["adapter"] = adapter
    if jobs_found is not None:
        row["jobsFound"] = jobs_found
    if ok is not None:
        row["ok"] = ok
    return row


def _running_adjudication_payload(
    payload: dict[str, Any], *, run_id: str, started_at: str
) -> dict[str, Any]:
    now = _now_iso()
    progress = _base_progress_payload(now)
    return {
        "ok": True,
        "status": "running",
        "runId": run_id,
        "startedAt": started_at,
        "heartbeatAt": now,
        "applyAutopilot": bool(payload.get("applyAutopilot")),
        "trigger": _clean(payload.get("trigger")),
        "checkedFamilyCount": 0,
        "checkedSourceCount": 0,
        "demoted": 0,
        "appliedIds": [],
        "families": [],
        "taskProgress": _task_progress_payload(
            active=True,
            phase_key="building_queue",
            phase_label="Building conflict queue",
            ratio=0.0,
            counts=_progress_counts(progress),
            updated_at=now,
        ),
        "progress": progress,
        "summary": _summary_payload(),
    }


def _failed_adjudication_payload(
    payload: dict[str, Any],
    *,
    run_id: str,
    started_at: str,
    error: str,
    current: dict[str, Any] | None = None,
) -> dict[str, Any]:
    now = _now_iso()
    current_payload = _as_dict(current)
    progress = _as_dict(current_payload.get("progress")) or _base_progress_payload(now)
    progress["lastProgressAt"] = now
    counts = _progress_counts(progress)
    return {
        **_running_adjudication_payload(payload, run_id=run_id, started_at=started_at),
        "status": "failed",
        "finishedAt": now,
        "heartbeatAt": now,
        "checkedFamilyCount": int(progress.get("checkedFamilyCount") or 0),
        "checkedSourceCount": int(progress.get("checkedSourceCount") or 0),
        "taskProgress": _task_progress_payload(
            active=False,
            phase_key="failed",
            phase_label="Conflict source check failed",
            ratio=_progress_ratio(progress),
            counts=counts,
            updated_at=now,
            target_label=_clean(progress.get("currentSourceName")),
            target_url=_clean(progress.get("currentEndpointUrl")),
        ),
        "progress": progress,
        "error": error,
    }


def _row_id(row: dict[str, Any]) -> str:
    return _clean(row.get("id") or row.get("sourceId") or source_identity(row))


def _row_state(row: dict[str, Any]) -> str:
    return _clean(row.get("registryState") or row.get("candidateState")).lower()


def _row_adapter(row: dict[str, Any]) -> str:
    adapter = _clean(row.get("adapter") or row.get("sourceType")).lower()
    if adapter:
        return adapter
    row_id = _row_id(row).lower()
    return row_id.split(":", 1)[0] if ":" in row_id else ""


def _urls_from_row(row: dict[str, Any]) -> list[str]:
    values = [
        row.get(key)
        for key in (
            "api_url",
            "feed_url",
            "board_url",
            "listing_url",
            "careersUrl",
            "url",
            "sourceUrl",
            "id",
            "sourceId",
        )
    ]
    urls: list[str] = []
    for value in values:
        for match in re.findall(r"https?://[^\s|]+", _clean(value)):
            url = match.rstrip("),.;'\"")
            if url and url not in urls:
                urls.append(url)
    return urls


def _adapter_token(row: dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = _clean(row.get(key))
        if value:
            return value
    identity_fields = provider_fields_from_row_identity(row)
    for key in keys:
        value = _clean(identity_fields.get(key))
        if value:
            return value
    for url in _urls_from_row(row):
        host = urlparse(url).netloc.lower()
        if host:
            return host.split(".", 1)[0]
    return ""


def _endpoint_url(row: dict[str, Any]) -> str:
    for url in _urls_from_row(row):
        return url
    adapter = _row_adapter(row)
    if adapter == "greenhouse":
        slug = _adapter_token(row, "slug", "account", "company_id")
        return (
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true" if slug else ""
        )
    if adapter == "lever":
        account = _adapter_token(row, "account", "slug", "company_id")
        return f"https://api.lever.co/v0/postings/{account}?mode=json" if account else ""
    if adapter == "workable":
        account = _adapter_token(row, "account", "slug", "company_id")
        return (
            f"https://apply.workable.com/api/v1/widget/accounts/{account}?details=true"
            if account
            else ""
        )
    if adapter == "smartrecruiters":
        company_id = _adapter_token(row, "company_id", "account", "slug")
        return (
            f"https://api.smartrecruiters.com/v1/companies/{company_id}/postings"
            if company_id
            else ""
        )
    if adapter == "jazzhr":
        board_url = _adapter_token(row, "board_url")
        return board_url if board_url else ""
    return ""


def _json_payload(text: str) -> Any:
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return None


def _parse_jobs(
    row: dict[str, Any], text: str, final_url: str
) -> tuple[bool, list[dict[str, Any]]]:
    adapter = _row_adapter(row)
    if adapter == "static":
        evidence = static_probe_evidence(text, final_url or _endpoint_url(row))
        jobs = [
            {
                "sourceJobId": f"static:{_row_id(row)}:{idx}",
                "company": _clean(row.get("company") or row.get("studio") or row.get("name")),
                "jobLink": url,
            }
            for idx, url in enumerate(evidence.sample_urls, start=1)
        ]
        while len(jobs) < evidence.count:
            idx = len(jobs) + 1
            jobs.append(
                {
                    "sourceJobId": f"static:{_row_id(row)}:{idx}",
                    "company": _clean(row.get("company") or row.get("studio") or row.get("name")),
                }
            )
        return bool(text), jobs
    payload = _json_payload(text)
    fallback_company = _clean(row.get("company") or row.get("studio") or row.get("name"))
    token = _adapter_token(row, "slug", "account", "company_id")
    parser = _PROVIDER_PAYLOAD_PARSERS.get(adapter)
    if parser:
        jobs = parser(payload, token, fallback_company)
    elif adapter == "jazzhr":
        jobs = parse_jazzhr_jobs_html(text, final_url or _endpoint_url(row), fallback_company)
    elif adapter == "ubisoft_algolia":
        hits = _as_list(_as_dict(payload).get("hits"))
        jobs = [
            {
                "sourceJobId": (
                    "ubisoft_algolia:"
                    f"{clean_text(hit.get('objectID') or hit.get('refNumber') or hit.get('slug'))}"
                ),
                "title": clean_text(hit.get("title")),
                "company": fallback_company or "Ubisoft",
                "city": clean_text(hit.get("city")),
                "country": clean_text(hit.get("countryCode")).upper(),
                "jobLink": normalize_url(hit.get("link") or hit.get("referralUrl")),
                "postedAt": clean_text(hit.get("createdAt")),
            }
            for hit in hits
            if isinstance(hit, dict) and clean_text(hit.get("title"))
        ]
    else:
        jobs = parse_jobpostings_from_html(
            text,
            base_url=final_url or _endpoint_url(row),
            fallback_company=fallback_company,
            fallback_source_id_prefix=_row_id(row),
        )
    valid_payload = payload is not None if adapter != "static" else bool(text)
    return bool(valid_payload or jobs), [dict(job) for job in jobs if isinstance(job, dict)]


def _job_location(job: dict[str, Any]) -> str:
    summary = clean_text(job.get("locationSummary"))
    if summary:
        return summary
    return ", ".join(
        part for part in (clean_text(job.get("city")), clean_text(job.get("country"))) if part
    )


def _job_key(job: dict[str, Any]) -> str:
    return "|".join(
        (
            norm_text(job.get("title")),
            norm_text(job.get("company")),
            norm_text(_job_location(job)),
        )
    )


def _job_sample(job: dict[str, Any]) -> dict[str, str]:
    return {
        "sourceJobId": clean_text(job.get("sourceJobId")),
        "title": clean_text(job.get("title")),
        "company": clean_text(job.get("company")),
        "location": _job_location(job),
        "jobLink": normalize_url(job.get("jobLink")),
        "postedAt": clean_text(job.get("postedAt")),
    }


def _newest_job_date(jobs: list[dict[str, Any]]) -> str:
    values = sorted(
        clean_text(job.get("postedAt")) for job in jobs if clean_text(job.get("postedAt"))
    )
    return values[-1] if values else ""


def _probe_row(row: dict[str, Any], timeout_s: int) -> dict[str, Any]:
    source_id = _row_id(row)
    evidence = probe_source_evidence(
        row,
        timeout_s,
        try_playwright=try_fetch_with_playwright,
    )
    endpoint = evidence.endpoint_url or _endpoint_url(row)
    final_url = evidence.final_url or endpoint
    text = evidence.response_text
    jobs: list[dict[str, Any]] = []
    valid_payload = False
    parse_error = ""
    if text:
        try:
            parse_row = row
            if evidence.payload_adapter:
                parse_row = {
                    **row,
                    **(evidence.payload_fields or {}),
                    "adapter": evidence.payload_adapter,
                }
            valid_payload, jobs = _parse_jobs(parse_row, text, final_url)
        except (TypeError, ValueError, KeyError) as exc:
            parse_error = str(exc)
    provider_jobs_found = int(evidence.jobs_found or 0)
    jobs_found = len(jobs) if valid_payload or jobs else provider_jobs_found
    if evidence.adapter != "static" or evidence.payload_adapter:
        jobs_found = max(jobs_found, provider_jobs_found)
    ok = bool(evidence.ok and not parse_error and (valid_payload or jobs or jobs_found == 0))
    return {
        "sourceId": source_id,
        "name": _clean(row.get("name")),
        "adapter": evidence.adapter or _row_adapter(row),
        "endpointUrl": endpoint,
        "finalUrl": final_url,
        "httpStatus": int(evidence.http_status or 0),
        "ok": ok,
        "error": evidence.error or parse_error,
        "validPayload": bool(valid_payload),
        "jobsFound": jobs_found,
        "countConfidence": evidence.count_confidence,
        "countReason": evidence.count_reason,
        "sampleUrls": list(evidence.sample_urls),
        "browserFallbackRecommended": bool(evidence.browser_fallback_recommended),
        "browserFallbackUsed": bool(evidence.browser_fallback_used),
        "newestJobDate": _newest_job_date(jobs),
        "sampleJobs": [_job_sample(job) for job in jobs[:5]],
        "_jobIds": sorted(
            {
                clean_text(job.get("sourceJobId")).lower()
                for job in jobs
                if clean_text(job.get("sourceJobId"))
            }
        ),
        "_jobLinks": sorted(
            {
                normalize_url(job.get("jobLink")).lower()
                for job in jobs
                if normalize_url(job.get("jobLink"))
            }
        ),
        "_jobKeys": sorted({_job_key(job) for job in jobs if _job_key(job)}),
    }


def _public_probe(probe: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in probe.items() if not str(key).startswith("_")}


def _overlap(left: dict[str, Any], right: dict[str, Any]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    ratios: list[float] = []
    for key, label in (
        ("_jobIds", "sourceJobIds"),
        ("_jobLinks", "jobLinks"),
        ("_jobKeys", "jobFingerprints"),
    ):
        left_values = set(_as_list(left.get(key)))
        right_values = set(_as_list(right.get(key)))
        shared = left_values & right_values
        counts[label] = len(shared)
        denominator = min(len(left_values), len(right_values))
        if denominator:
            ratios.append(len(shared) / denominator)
    ratio = max(ratios) if ratios else 0.0
    return {
        "ratio": round(ratio, 3),
        "sharedSourceJobIds": counts.get("sourceJobIds", 0),
        "sharedJobLinks": counts.get("jobLinks", 0),
        "sharedJobFingerprints": counts.get("jobFingerprints", 0),
    }


def _probe_score(probe: dict[str, Any]) -> tuple[int, int]:
    return (
        1 if bool(probe.get("ok")) else 0,
        int(probe.get("jobsFound") or 0),
    )


def _canonical_host_score(probe: dict[str, Any]) -> int:
    final = urlparse(_clean(probe.get("finalUrl") or probe.get("endpointUrl")))
    endpoint = urlparse(_clean(probe.get("endpointUrl")))
    host = final.netloc.lower()
    score = 0
    if host and not any(token in host for token in ("homeinteractive", "old", "legacy")):
        score += 1
    if (
        final.scheme
        and endpoint.scheme
        and final.netloc.lower() == endpoint.netloc.lower()
        and final.path.rstrip("/") == endpoint.path.rstrip("/")
    ):
        score += 2
    if "focusentertainment" in host:
        score += 2
    return score


def _best_probe(probes: list[dict[str, Any]]) -> dict[str, Any]:
    return max(
        probes,
        key=lambda probe: (
            *_probe_score(probe),
            _canonical_host_score(probe),
            _clean(probe.get("newestJobDate")),
        ),
    )


def _classify_loser(
    best: dict[str, Any], loser: dict[str, Any]
) -> tuple[str, str, str, dict[str, Any]]:
    overlap = _overlap(best, loser)
    best_jobs = int(best.get("jobsFound") or 0)
    loser_jobs = int(loser.get("jobsFound") or 0)
    same_final = normalize_url(best.get("finalUrl")) == normalize_url(loser.get("finalUrl"))
    if bool(best.get("ok")) and best_jobs > 0 and not bool(loser.get("ok")):
        return (
            "auto_demote_applied",
            "high",
            "winner has live jobs while loser failed probe",
            overlap,
        )
    if same_final and bool(best.get("ok")) and best_jobs >= loser_jobs:
        return "auto_demote_applied", "high", "sources resolve to the same final URL", overlap
    if (
        bool(best.get("ok"))
        and best_jobs > 0
        and overlap["ratio"] >= 0.8
        and best_jobs >= loser_jobs
    ):
        loser_newer = _clean(loser.get("newestJobDate")) > _clean(best.get("newestJobDate"))
        if not loser_newer:
            return "auto_demote_applied", "high", "sources return the same job set", overlap
    if bool(best.get("ok")) and bool(loser.get("ok")) and best_jobs > 0 and loser_jobs == 0:
        return (
            "auto_demote_applied",
            "high",
            "winner has live jobs while loser returned zero jobs",
            overlap,
        )
    if bool(best.get("ok")) and bool(loser.get("ok")) and best_jobs > 0 and loser_jobs > 0:
        if overlap["ratio"] < 0.5:
            return "keep_both", "medium", "both sources are live and job sets differ", overlap
        return (
            "recommended_demotion",
            "medium",
            "sources overlap but evidence is not strict enough for autopilot",
            overlap,
        )
    if not bool(best.get("ok")) and not bool(loser.get("ok")):
        return "probe_failed", "low", "both sources failed probe", overlap
    return "needs_review", "low", "insufficient live evidence for safe demotion", overlap


def _selected_conflicts(
    conflict_payload: dict[str, Any], payload: dict[str, Any]
) -> list[dict[str, Any]]:
    family_filter = {
        _clean(item).lower() for item in _as_list(payload.get("familyKeys")) if _clean(item)
    }
    source_filter = {
        _clean(item).lower() for item in _as_list(payload.get("sourceIds")) if _clean(item)
    }
    conflicts = []
    for card in _as_list(conflict_payload.get("conflicts")):
        if not isinstance(card, dict):
            continue
        family_key = _clean(card.get("familyKey"))
        rows = [_as_dict(row) for row in _as_list(card.get("rows"))]
        active_rows = [row for row in rows if _row_state(row) == "active"]
        if len(active_rows) < 2:
            continue
        if family_filter and family_key.lower() not in family_filter:
            continue
        if source_filter:
            active_rows = [row for row in active_rows if _row_id(row).lower() in source_filter]
            if len(active_rows) < 2:
                continue
        next_card = dict(card)
        next_card["rows"] = active_rows
        conflicts.append(next_card)
    return conflicts


def _demote_ids(
    state: dict[str, list[dict[str, Any]]], target_ids: set[str], now: str
) -> tuple[dict[str, list[dict[str, Any]]], list[str]]:
    moved: list[dict[str, Any]] = []
    active: list[dict[str, Any]] = []
    applied: list[str] = []
    for row in state.get("active") or []:
        row_id = source_identity(row)
        if row_id in target_ids:
            moved.append(
                transition_registry_to_pending(
                    row,
                    reason=ADJUDICATION_REASON,
                    actor=ADJUDICATION_REASON,
                    at=now,
                )
            )
            applied.append(row_id)
        else:
            active.append(row)
    next_state = {
        "active": active,
        "pending": list(state.get("pending") or []) + moved,
        "rejected": list(state.get("rejected") or []),
    }
    return next_state, applied


def _decision_status(status: str, apply_autopilot: bool) -> str:
    if apply_autopilot or status != "auto_demote_applied":
        return status
    return "recommended_demotion"


def _family_status(decisions: list[dict[str, Any]]) -> str:
    ordered_statuses = (
        "auto_demote_applied",
        "recommended_demotion",
        "keep_both",
        "probe_failed",
    )
    statuses = {str(decision.get("status") or "") for decision in decisions}
    return next((status for status in ordered_statuses if status in statuses), "needs_review")


def _summary_from_families(families: list[dict[str, Any]]) -> dict[str, int]:
    return {
        "autoDemoteApplied": sum(1 for row in families if row["status"] == "auto_demote_applied"),
        "recommendedDemotion": sum(
            1 for row in families if row["status"] == "recommended_demotion"
        ),
        "keepBoth": sum(1 for row in families if row["status"] == "keep_both"),
        "needsReview": sum(1 for row in families if row["status"] == "needs_review"),
        "probeFailed": sum(1 for row in families if row["status"] == "probe_failed"),
    }


class _AdjudicationProgress:
    def __init__(
        self,
        api: Any,
        payload: dict[str, Any],
        *,
        run_id: str,
        started_at: str,
        throttle_s: float,
    ) -> None:
        self._api = api
        self._payload = dict(payload)
        self._run_id = run_id
        self._started_at = started_at
        self._throttle_s = max(0.0, throttle_s)
        self._last_write_monotonic = 0.0
        self._phase_key = "building_queue"
        self._phase_label = "Building conflict queue"
        self._progress = _base_progress_payload(_now_iso())
        self._recent_events: list[dict[str, Any]] = []
        self._completed_source_ids: set[str] = set()

    def _append_event(self, event: dict[str, Any]) -> None:
        self._recent_events.append(event)
        self._recent_events = self._recent_events[-_RECENT_PROGRESS_EVENT_LIMIT:]
        self._progress["recentEvents"] = list(self._recent_events)

    def write(self, *, force: bool = False) -> None:
        monotonic_now = time.monotonic()
        if (
            not force
            and self._last_write_monotonic
            and monotonic_now - self._last_write_monotonic < self._throttle_s
        ):
            return
        self._last_write_monotonic = monotonic_now
        now = _now_iso()
        self._progress["lastProgressAt"] = now
        counts = _progress_counts(self._progress)
        payload = {
            **_running_adjudication_payload(
                self._payload, run_id=self._run_id, started_at=self._started_at
            ),
            "heartbeatAt": now,
            "checkedFamilyCount": counts["checkedFamilies"],
            "checkedSourceCount": counts["checkedSources"],
            "taskProgress": _task_progress_payload(
                active=True,
                phase_key=self._phase_key,
                phase_label=self._phase_label,
                ratio=_progress_ratio(self._progress),
                counts=counts,
                updated_at=now,
                target_label=_clean(self._progress.get("currentSourceName")),
                target_url=_clean(self._progress.get("currentEndpointUrl")),
            ),
            "progress": dict(self._progress),
        }
        self._api.save_json_atomic(_artifact_path(self._api), payload)

    def phase(self, phase_key: str, phase_label: str) -> None:
        self._phase_key = phase_key
        self._phase_label = phase_label
        self.write(force=True)

    def set_totals(self, *, total_family_count: int, total_source_count: int) -> None:
        self._progress["totalFamilyCount"] = max(0, int(total_family_count))
        self._progress["totalSourceCount"] = max(0, int(total_source_count))
        self.write(force=True)

    def source_started(self, row: dict[str, Any], *, family_key: str, family_index: int) -> None:
        now = _now_iso()
        self._progress.update(
            {
                "currentFamilyKey": family_key,
                "currentFamilyIndex": family_index,
                "currentSourceId": _row_id(row),
                "currentSourceName": _clean(row.get("name")),
                "currentAdapter": _row_adapter(row),
                "currentEndpointUrl": _endpoint_url(row),
            }
        )
        self._append_event(
            _progress_event(
                "source_started",
                timestamp=now,
                family_key=family_key,
                source_id=_row_id(row),
                source_name=_clean(row.get("name")),
                adapter=_row_adapter(row),
            )
        )
        self.write(force=True)

    def source_finished(self, row: dict[str, Any], probe: dict[str, Any]) -> None:
        now = _now_iso()
        source_id = _row_id(row)
        if source_id:
            self._completed_source_ids.add(source_id)
        self._progress["checkedSourceCount"] = len(self._completed_source_ids)
        self._append_event(
            _progress_event(
                "source_finished",
                timestamp=now,
                family_key=_clean(self._progress.get("currentFamilyKey")),
                source_id=source_id,
                source_name=_clean(row.get("name")),
                adapter=_row_adapter(row),
                jobs_found=int(probe.get("jobsFound") or 0),
                ok=bool(probe.get("ok")),
            )
        )
        self.write(force=True)

    def family_finished(self, family_key: str, family_index: int) -> None:
        self._progress["checkedFamilyCount"] = max(
            int(self._progress.get("checkedFamilyCount") or 0),
            family_index,
        )
        self._append_event(
            _progress_event(
                "family_finished",
                timestamp=_now_iso(),
                family_key=family_key,
            )
        )
        self.write(force=True)


def _build_family_adjudication(
    card: dict[str, Any],
    *,
    timeout_s: int,
    apply_autopilot: bool,
    progress_callback: Callable[[str, dict[str, Any], dict[str, Any] | None], None] | None = None,
) -> tuple[dict[str, Any] | None, set[str]]:
    probes = []
    for row in [_as_dict(item) for item in _as_list(card.get("rows"))]:
        if progress_callback:
            progress_callback("source_started", row, None)
        probe = _probe_row(row, timeout_s)
        probes.append(probe)
        if progress_callback:
            progress_callback("source_finished", row, probe)
    if not probes:
        return None, set()
    best = _best_probe(probes)
    target_ids: set[str] = set()
    decisions = []
    for probe in probes:
        if probe.get("sourceId") == best.get("sourceId"):
            continue
        status, confidence, reason, overlap = _classify_loser(best, probe)
        if status == "auto_demote_applied":
            target_ids.add(_clean(probe.get("sourceId")))
        decisions.append(
            {
                "sourceId": _clean(probe.get("sourceId")),
                "status": _decision_status(status, apply_autopilot),
                "confidence": confidence,
                "reason": reason,
                "overlap": overlap,
            }
        )
    return (
        {
            "familyKey": _clean(card.get("familyKey")),
            "status": _family_status(decisions),
            "winnerSourceId": _clean(best.get("sourceId")),
            "checkedSourceIds": [_clean(probe.get("sourceId")) for probe in probes],
            "probes": [_public_probe(probe) for probe in probes],
            "decisions": decisions,
        },
        target_ids,
    )


def run_registry_conflict_adjudication(
    api: Any, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    data = _as_dict(payload)
    apply_autopilot = bool(data.get("applyAutopilot"))
    timeout_s = max(3, min(20, int(data.get("timeoutSeconds") or 8)))
    throttle_s = _progress_throttle_seconds(data)
    run_id = _clean(data.get("runId")) or f"conflict_check_{uuid.uuid4().hex[:10]}"
    started_at = _clean(data.get("startedAt")) or _now_iso()
    progress = _AdjudicationProgress(
        api,
        data,
        run_id=run_id,
        started_at=started_at,
        throttle_s=throttle_s,
    )
    with _ADJUDICATION_LOCK:
        progress.phase("loading_registry", "Loading registry state")
        state = api.load_state()
        progress.phase("building_queue", "Building conflict queue")
        source_state_path = api.JOBS_FETCH_REPORT_PATH.with_name("jobs-source-state.json")
        source_state_payload = api.load_json_object(source_state_path, {})
        conflict_payload = derive_registry_conflict_queue(state, source_state_payload)
        selected = _selected_conflicts(conflict_payload, data)
        selected_source_ids = {
            _row_id(_as_dict(row))
            for card in selected
            for row in _as_list(card.get("rows"))
            if _row_id(_as_dict(row))
        }
        progress.set_totals(
            total_family_count=len(selected),
            total_source_count=len(selected_source_ids),
        )
        progress.phase("probing_sources", "Checking conflicting sources")
        families: list[dict[str, Any]] = []
        target_ids: set[str] = set()

        for family_index, card in enumerate(selected, start=1):
            family_key = _clean(card.get("familyKey"))

            def _record_progress(
                event: str,
                row: dict[str, Any],
                probe: dict[str, Any] | None = None,
                *,
                current_family_key: str = family_key,
                current_family_index: int = family_index,
            ) -> None:
                if event == "source_started":
                    progress.source_started(
                        row,
                        family_key=current_family_key,
                        family_index=current_family_index,
                    )
                elif event == "source_finished" and probe is not None:
                    progress.source_finished(row, probe)

            family, family_target_ids = _build_family_adjudication(
                card,
                timeout_s=timeout_s,
                apply_autopilot=apply_autopilot,
                progress_callback=_record_progress,
            )
            progress.family_finished(family_key, family_index)
            if not family:
                continue
            families.append(family)
            target_ids.update(family_target_ids)
        applied_ids: list[str] = []
        if apply_autopilot and target_ids:
            progress.phase("applying_autopilot", "Applying high-confidence recommendations")
            state, applied_ids = _demote_ids(state, target_ids, _now_iso())
            if applied_ids:
                state = api.persist_state_and_auto_sync(state, reason=ADJUDICATION_REASON)
        finished_at = _now_iso()
        summary = _summary_from_families(families)
        checked_source_count = len(
            {source_id for row in families for source_id in row["checkedSourceIds"]}
        )
        terminal_counts = {
            "checkedFamilies": len(families),
            "totalFamilies": len(selected),
            "checkedSources": checked_source_count,
            "totalSources": len(selected_source_ids),
        }
        result = {
            "ok": True,
            "status": "succeeded",
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": finished_at,
            "heartbeatAt": finished_at,
            "applyAutopilot": apply_autopilot,
            "checkedFamilyCount": len(families),
            "checkedSourceCount": checked_source_count,
            "demoted": len(applied_ids),
            "appliedIds": applied_ids,
            "families": families,
            "taskProgress": _task_progress_payload(
                active=False,
                phase_key="succeeded",
                phase_label="Conflict source check finished",
                ratio=1.0,
                counts=terminal_counts,
                updated_at=finished_at,
            ),
            "progress": {
                **progress._progress,
                "checkedFamilyCount": len(families),
                "checkedSourceCount": checked_source_count,
                "lastProgressAt": finished_at,
            },
            "summary": summary,
        }
        api.save_json_atomic(_artifact_path(api), result)
        try:
            registry_summary = api.get_registry_summary_payload()
            source_state_path = api.JOBS_FETCH_REPORT_PATH.with_name("jobs-source-state.json")
            cache_payload = overlay_adjudication(
                {
                    **conflict_payload,
                    "registrySummary": api.summarize_state(state),
                    "registryAutoHeal": api.get_registry_auto_heal_report(),
                    "ok": True,
                },
                result,
            )
            cache_key = build_registry_conflicts_summary_cache_key(
                registry_summary=registry_summary,
                source_state_path=source_state_path,
                adjudication_payload=result,
            )
            write_registry_conflicts_summary_cache(
                source_state_path=source_state_path,
                cache_key=cache_key,
                payload=summarize_registry_conflicts_payload(cache_payload),
            )
        except (AttributeError, OSError, TypeError, ValueError):
            pass
        return result


def start_registry_conflict_adjudication(
    api: Any, payload: dict[str, Any] | None = None
) -> dict[str, Any]:
    global _ADJUDICATION_JOB_THREAD

    data = _as_dict(payload)
    with _ADJUDICATION_JOB_LOCK:
        if _ADJUDICATION_JOB_THREAD is not None and _ADJUDICATION_JOB_THREAD.is_alive():
            current = load_registry_conflict_adjudication(api)
            if current.get("status") == "running":
                return {**current, "started": False, "alreadyRunning": True}
            return {
                "ok": True,
                "status": "running",
                "started": False,
                "alreadyRunning": True,
            }

        run_id = f"conflict_check_{uuid.uuid4().hex[:10]}"
        started_at = _now_iso()
        running = _running_adjudication_payload(data, run_id=run_id, started_at=started_at)
        api.save_json_atomic(_artifact_path(api), running)

        def _worker() -> None:
            try:
                run_registry_conflict_adjudication(
                    api,
                    {
                        **data,
                        "runId": run_id,
                        "startedAt": started_at,
                    },
                )
            except (KeyError, OSError, RuntimeError, TypeError, ValueError) as exc:
                current = load_registry_conflict_adjudication(api)
                api.save_json_atomic(
                    _artifact_path(api),
                    _failed_adjudication_payload(
                        data,
                        run_id=run_id,
                        started_at=started_at,
                        error=str(exc),
                        current=current,
                    ),
                )

        _ADJUDICATION_JOB_THREAD = threading.Thread(
            target=_worker,
            name=f"registry-conflict-adjudication-{run_id}",
            daemon=True,
        )
        _ADJUDICATION_JOB_THREAD.start()
        return {**running, "started": True, "alreadyRunning": False}


def overlay_adjudication(
    conflict_payload: dict[str, Any], adjudication: dict[str, Any]
) -> dict[str, Any]:
    if not adjudication:
        return conflict_payload
    by_family = {
        _clean(row.get("familyKey")): row
        for row in _as_list(adjudication.get("families"))
        if isinstance(row, dict)
    }
    payload = dict(conflict_payload)
    conflicts = []
    for card in _as_list(payload.get("conflicts")):
        if not isinstance(card, dict):
            continue
        next_card = dict(card)
        family = by_family.get(_clean(card.get("familyKey")))
        if family:
            next_card["adjudication"] = family
        conflicts.append(next_card)
    payload["conflicts"] = conflicts
    payload["adjudication"] = {
        key: value for key, value in adjudication.items() if key != "families"
    }
    return payload


__all__ = [
    "ADJUDICATION_REASON",
    "load_registry_conflict_adjudication",
    "overlay_adjudication",
    "run_registry_conflict_adjudication",
    "start_registry_conflict_adjudication",
]
