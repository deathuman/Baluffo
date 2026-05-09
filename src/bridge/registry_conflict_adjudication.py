from __future__ import annotations

import json
import re
import threading
import uuid
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from src.bridge.registry_conflicts import derive_registry_conflict_queue
from src.bridge.source_check_http import try_fetch_with_playwright
from src.jobs.adapters.html_parsers import parse_jobpostings_from_html
from src.jobs.adapters.parsers.json_payloads import (
    parse_greenhouse_jobs_payload,
    parse_lever_jobs_payload,
    parse_pinpoint_jobs_payload,
    parse_recruitee_jobs_payload,
    parse_smartrecruiters_jobs_payload,
    parse_workable_jobs_payload,
)
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


def _running_adjudication_payload(
    payload: dict[str, Any], *, run_id: str, started_at: str
) -> dict[str, Any]:
    return {
        "ok": True,
        "status": "running",
        "runId": run_id,
        "startedAt": started_at,
        "applyAutopilot": bool(payload.get("applyAutopilot")),
        "trigger": _clean(payload.get("trigger")),
        "checkedFamilyCount": 0,
        "checkedSourceCount": 0,
        "demoted": 0,
        "appliedIds": [],
        "families": [],
        "summary": {
            "autoDemoteApplied": 0,
            "recommendedDemotion": 0,
            "keepBoth": 0,
            "needsReview": 0,
            "probeFailed": 0,
        },
    }


def _failed_adjudication_payload(
    payload: dict[str, Any], *, run_id: str, started_at: str, error: str
) -> dict[str, Any]:
    return {
        **_running_adjudication_payload(payload, run_id=run_id, started_at=started_at),
        "status": "failed",
        "finishedAt": _now_iso(),
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
    return ""


def _fetch_url(url: str, timeout_s: int) -> tuple[int, str, str, str]:
    if not url:
        return 0, "", "", "missing endpoint URL"
    try:
        request = Request(url, headers={"User-Agent": "Baluffo Admin Conflict Check"})
        with urlopen(request, timeout=timeout_s) as response:
            status = int(getattr(response, "status", 0) or response.getcode() or 0)
            final_url = _clean(response.geturl()) or url
            body = response.read().decode("utf-8", errors="replace")
            return status, final_url, body, ""
    except (HTTPError, TimeoutError, URLError, OSError) as exc:
        return 0, "", "", str(exc)


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
    if adapter == "greenhouse":
        jobs = parse_greenhouse_jobs_payload(payload, token, fallback_company)
    elif adapter == "lever":
        jobs = parse_lever_jobs_payload(payload, token, fallback_company)
    elif adapter == "smartrecruiters":
        jobs = parse_smartrecruiters_jobs_payload(payload, token, fallback_company)
    elif adapter == "workable":
        jobs = parse_workable_jobs_payload(payload, token, fallback_company)
    elif adapter == "recruitee":
        jobs = parse_recruitee_jobs_payload(payload, token, fallback_company)
    elif adapter == "pinpoint":
        jobs = parse_pinpoint_jobs_payload(payload, token, fallback_company)
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
    endpoint = _endpoint_url(row)
    status, final_url, text, error = _fetch_url(endpoint, timeout_s)
    jobs: list[dict[str, Any]] = []
    valid_payload = False
    parse_error = ""
    if text:
        try:
            valid_payload, jobs = _parse_jobs(row, text, final_url or endpoint)
        except (TypeError, ValueError, KeyError) as exc:
            parse_error = str(exc)
    if _row_adapter(row) == "static" and status and status < 400 and not jobs:
        browser_html, browser_error = try_fetch_with_playwright(final_url or endpoint, timeout_s)
        if browser_html:
            try:
                valid_payload, jobs = _parse_jobs(row, browser_html, final_url or endpoint)
                parse_error = ""
            except (TypeError, ValueError, KeyError) as exc:
                parse_error = str(exc)
        elif browser_error and not parse_error:
            parse_error = browser_error
    ok = bool(status and status < 400 and not error and (valid_payload or jobs))
    return {
        "sourceId": source_id,
        "name": _clean(row.get("name")),
        "adapter": _row_adapter(row),
        "endpointUrl": endpoint,
        "finalUrl": final_url or endpoint,
        "httpStatus": int(status or 0),
        "ok": ok,
        "error": error or parse_error,
        "validPayload": bool(valid_payload),
        "jobsFound": len(jobs),
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


def _build_family_adjudication(
    card: dict[str, Any],
    *,
    timeout_s: int,
    apply_autopilot: bool,
) -> tuple[dict[str, Any] | None, set[str]]:
    probes = [_probe_row(row, timeout_s) for row in _as_list(card.get("rows"))]
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
    run_id = _clean(data.get("runId")) or f"conflict_check_{uuid.uuid4().hex[:10]}"
    started_at = _clean(data.get("startedAt")) or _now_iso()
    with _ADJUDICATION_LOCK:
        state = api.load_state()
        source_state_path = api.JOBS_FETCH_REPORT_PATH.with_name("jobs-source-state.json")
        source_state_payload = api.load_json_object(source_state_path, {})
        conflict_payload = derive_registry_conflict_queue(state, source_state_payload)
        selected = _selected_conflicts(conflict_payload, data)
        families: list[dict[str, Any]] = []
        target_ids: set[str] = set()
        for card in selected:
            family, family_target_ids = _build_family_adjudication(
                card,
                timeout_s=timeout_s,
                apply_autopilot=apply_autopilot,
            )
            if not family:
                continue
            families.append(family)
            target_ids.update(family_target_ids)
        applied_ids: list[str] = []
        if apply_autopilot and target_ids:
            state, applied_ids = _demote_ids(state, target_ids, _now_iso())
            if applied_ids:
                state = api.persist_state_and_auto_sync(state, reason=ADJUDICATION_REASON)
        result = {
            "ok": True,
            "status": "succeeded",
            "runId": run_id,
            "startedAt": started_at,
            "finishedAt": _now_iso(),
            "applyAutopilot": apply_autopilot,
            "checkedFamilyCount": len(families),
            "checkedSourceCount": len(
                {source_id for row in families for source_id in row["checkedSourceIds"]}
            ),
            "demoted": len(applied_ids),
            "appliedIds": applied_ids,
            "families": families,
            "summary": {
                "autoDemoteApplied": sum(
                    1 for row in families if row["status"] == "auto_demote_applied"
                ),
                "recommendedDemotion": sum(
                    1 for row in families if row["status"] == "recommended_demotion"
                ),
                "keepBoth": sum(1 for row in families if row["status"] == "keep_both"),
                "needsReview": sum(1 for row in families if row["status"] == "needs_review"),
                "probeFailed": sum(1 for row in families if row["status"] == "probe_failed"),
            },
        }
        api.save_json_atomic(_artifact_path(api), result)
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
                api.save_json_atomic(
                    _artifact_path(api),
                    _failed_adjudication_payload(
                        data,
                        run_id=run_id,
                        started_at=started_at,
                        error=str(exc),
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
