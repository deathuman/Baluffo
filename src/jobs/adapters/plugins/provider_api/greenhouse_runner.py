"""Greenhouse provider runner."""

from __future__ import annotations

import json
from collections.abc import Callable

from src.exceptions import AdapterValidationError
from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.common.config import GREENHOUSE_JOBS_URL_TEMPLATE
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.state import get_incremental_cache_decision
from src.jobs.text_utils import clean_text
from src.jobs.transport import conditional_revalidate_url


def _as_dict(value: object) -> dict[str, object]:
    return value if isinstance(value, dict) else {}


def _run_greenhouse_boards(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, object]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    provider_url = ""
    for board in registry_entries("greenhouse"):
        slug = clean_text(board.get("slug"))
        if not slug:
            continue
        label = clean_text(board.get("name")) or clean_text(board.get("studio")) or slug
        entry_name = clean_text(board.get("name")) or slug
        url = GREENHOUSE_JOBS_URL_TEMPLATE.format(slug=slug)
        entry_report = {
            "adapter": "greenhouse",
            "studio": clean_text(board.get("studio")) or label,
            "name": entry_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        cache_decision = get_incremental_cache_decision(
            entry_name,
            source_state_rows or {},
            adapter="greenhouse",
            force_refresh_all=force_refresh_all,
        )
        entry_report["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
        entry_report["cacheDecisionReason"] = (
            clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
        )
        if entry_report["cacheDecision"] in {"skip_fresh", "cooldown_skip"}:
            entry_report["status"] = "excluded"
            entry_report["error"] = entry_report["cacheDecisionReason"]
            entry_report["exclusionReason"] = f"cache_{entry_report['cacheDecisionReason']}"
            details.append(entry_report)
            continue
        if entry_report["cacheDecision"] == "revalidate_only":
            state_entry = _as_dict((source_state_rows or {}).get(entry_name))
            revalidate = conditional_revalidate_url(
                url,
                timeout_s,
                etag=clean_text(state_entry.get("lastHttpEtag")),
                last_modified=clean_text(state_entry.get("lastHttpLastModified")),
            )
            entry_report["httpStatus"] = int(revalidate.get("statusCode") or 0)
            if clean_text(revalidate.get("etag")):
                entry_report["httpEtag"] = clean_text(revalidate.get("etag"))
            if clean_text(revalidate.get("lastModified")):
                entry_report["httpLastModified"] = clean_text(revalidate.get("lastModified"))
            if bool(revalidate.get("notModified")):
                entry_report["status"] = "excluded"
                entry_report["error"] = "not_modified_304"
                entry_report["exclusionReason"] = "cache_not_modified_304"
                entry_report["cacheDecisionReason"] = "not_modified_304"
                details.append(entry_report)
                continue
        try:
            text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = _provider_parsers.parse_greenhouse_jobs_payload(
                payload, slug, fallback_company=label
            )
            for row in parsed:
                row["adapter"] = "greenhouse"
                row["studio"] = clean_text(board.get("studio")) or label
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            if not provider_url:
                provider_url = url
            errors.append(f"greenhouse:{slug}: {exc}")
        details.append(entry_report)
    set_source_diagnostics(
        "greenhouse_boards",
        adapter="greenhouse",
        studio="multiple",
        provider_url=provider_url,
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []
