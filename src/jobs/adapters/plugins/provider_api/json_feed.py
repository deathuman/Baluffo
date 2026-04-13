"""JSON feed factory and runner for provider APIs (lever, workable, smartrecruiters, recruitee, pinpoint)."""

from __future__ import annotations

import json
from collections.abc import Callable

from src.exceptions import AdapterValidationError
from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.adapters.plugins.types import SimpleAdapterPlugin
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.state import get_incremental_cache_decision
from src.jobs.text_utils import clean_text
from src.jobs.transport import conditional_revalidate_url


def _run_json_feed_sources(
    *,
    adapter_name: str,
    registry_adapter: str,
    default_error: str,
    parse_payload,
    build_url,
    payload_count,
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
    for source in registry_entries(registry_adapter):
        source_name = clean_text(source.get("name")) or f"{registry_adapter}_source"
        studio = clean_text(source.get("studio")) or source_name
        endpoint = build_url(source)
        entry_report = {
            "adapter": adapter_name,
            "studio": studio,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        cache_decision = get_incremental_cache_decision(
            source_name,
            source_state_rows or {},
            adapter=adapter_name,
            force_refresh_all=force_refresh_all,
        )
        entry_report["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
        entry_report["cacheDecisionReason"] = (
            clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
        )
        if not endpoint:
            entry_report["status"] = "error"
            entry_report["error"] = default_error
            details.append(entry_report)
            continue
        if entry_report["cacheDecision"] in {"skip_fresh", "cooldown_skip"}:
            entry_report["status"] = "excluded"
            entry_report["error"] = entry_report["cacheDecisionReason"]
            entry_report["exclusionReason"] = f"cache_{entry_report['cacheDecisionReason']}"
            details.append(entry_report)
            continue
        if entry_report["cacheDecision"] == "revalidate_only":
            state_entry = (
                (source_state_rows or {}).get(source_name)
                if isinstance(source_state_rows, dict)
                else {}
            )
            revalidate = conditional_revalidate_url(
                endpoint,
                timeout_s,
                etag=clean_text((state_entry or {}).get("lastHttpEtag")),
                last_modified=clean_text((state_entry or {}).get("lastHttpLastModified")),
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
            text = fetch_with_retries(endpoint, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed = parse_payload(source, payload, studio)
            entry_report["fetchedCount"] = payload_count(payload, parsed)
            entry_report["keptCount"] = len(parsed)
            for row in parsed:
                row["adapter"] = adapter_name
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            if not provider_url:
                provider_url = endpoint
            errors.append(f"{registry_adapter}:{source_name}: {exc}")
        details.append(entry_report)

    set_source_diagnostics(
        f"{registry_adapter}_sources",
        adapter=adapter_name,
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


def _json_feed_plugin(adapter_name: str) -> SimpleAdapterPlugin:
    registry_adapter = adapter_name
    if adapter_name == "smartrecruiters":
        default_error = "missing company_id/api_url"
        parse_payload = lambda source, payload, studio: (
            _provider_parsers.parse_smartrecruiters_jobs_payload(
                payload, clean_text(source.get("company_id")), fallback_company=studio
            )
        )
        build_url = lambda source: (
            clean_text(source.get("api_url"))
            or (
                f"https://api.smartrecruiters.com/v1/companies/{clean_text(source.get('company_id'))}/postings"
                if clean_text(source.get("company_id"))
                else ""
            )
        )
        payload_count = lambda payload, parsed: (
            len(payload.get("content", [])) if isinstance(payload, dict) else len(parsed)
        )
    elif adapter_name == "workable":
        default_error = "missing account/api_url"
        parse_payload = lambda source, payload, studio: (
            _provider_parsers.parse_workable_jobs_payload(
                payload, clean_text(source.get("account")), fallback_company=studio
            )
        )
        build_url = lambda source: (
            clean_text(source.get("api_url"))
            or (
                f"https://apply.workable.com/api/v1/widget/accounts/{clean_text(source.get('account'))}?details=true"
                if clean_text(source.get("account"))
                else ""
            )
        )
        payload_count = lambda payload, parsed: (
            len(payload.get("jobs", [])) if isinstance(payload, dict) else len(parsed)
        )
    elif adapter_name == "recruitee":
        default_error = "missing subdomain/api_url"
        parse_payload = lambda source, payload, studio: (
            _provider_parsers.parse_recruitee_jobs_payload(
                payload, clean_text(source.get("subdomain")), fallback_company=studio
            )
        )
        build_url = lambda source: (
            clean_text(source.get("api_url"))
            or (
                f"https://{clean_text(source.get('subdomain'))}/api/offers/"
                if "." in clean_text(source.get("subdomain"))
                else (
                    f"https://{clean_text(source.get('subdomain'))}.recruitee.com/api/offers/"
                    if clean_text(source.get("subdomain"))
                    else ""
                )
            )
        )
        payload_count = lambda payload, parsed: (
            len(payload.get("offers", [])) if isinstance(payload, dict) else len(parsed)
        )
    elif adapter_name == "pinpoint":
        default_error = "missing subdomain/api_url"
        parse_payload = lambda source, payload, studio: (
            _provider_parsers.parse_pinpoint_jobs_payload(
                payload, clean_text(source.get("subdomain")), fallback_company=studio
            )
        )
        build_url = lambda source: (
            clean_text(source.get("api_url"))
            or (
                f"https://{clean_text(source.get('subdomain'))}/postings.json"
                if "." in clean_text(source.get("subdomain"))
                else (
                    f"https://{clean_text(source.get('subdomain'))}.pinpointhq.com/postings.json"
                    if clean_text(source.get("subdomain"))
                    else ""
                )
            )
        )
        payload_count = lambda payload, parsed: (
            len(payload.get("data", [])) if isinstance(payload, dict) else len(parsed)
        )
    else:
        # lever
        default_error = "missing account/api_url"
        parse_payload = lambda source, payload, studio: _provider_parsers.parse_lever_jobs_payload(
            payload, clean_text(source.get("account")), fallback_company=studio
        )
        build_url = lambda source: (
            clean_text(source.get("api_url"))
            or (
                f"https://api.lever.co/v0/postings/{clean_text(source.get('account'))}?mode=json"
                if clean_text(source.get("account"))
                else ""
            )
        )
        payload_count = lambda payload, parsed: (
            len(payload) if isinstance(payload, list) else len(parsed)
        )

    return SimpleAdapterPlugin(
        name=f"{adapter_name}_sources",
        family="provider_api",
        priority=50,
        can_handle_fn=lambda ctx: (
            ctx.family == "provider_api" and ctx.adapter_key == f"{adapter_name}_sources"
        ),
        run_fn=lambda **kwargs: _run_json_feed_sources(
            adapter_name=adapter_name,
            registry_adapter=registry_adapter,
            default_error=default_error,
            parse_payload=parse_payload,
            build_url=build_url,
            payload_count=payload_count,
            **kwargs,
        ),
    )
