"""JSON feed factory and runner for provider APIs (lever, workable, smartrecruiters, recruitee, pinpoint)."""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from src.exceptions import AdapterValidationError
from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.adapters.plugins.types import AdapterPluginContext, SimpleAdapterPlugin
from src.jobs.common.diagnostics import set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.registry import registry_entries
from src.jobs.state import get_incremental_cache_decision
from src.jobs.text_utils import clean_text
from src.jobs.transport import conditional_revalidate_url

ParsePayload = Callable[[dict[str, object], Any, str], list[RawJob]]
BuildUrl = Callable[[dict[str, object]], str]
PayloadCount = Callable[[Any, list[RawJob]], int]
ProviderParser = Callable[[Any, str, str], list[RawJob]]


@dataclass(frozen=True)
class JsonFeedSpec:
    default_error: str
    source_key: str
    url_template: str
    parser: ProviderParser
    payload_key: str = ""
    dotted_url_template: str = ""
    list_payload: bool = False


def _build_json_feed_url(source: dict[str, object], spec: JsonFeedSpec) -> str:
    api_url = clean_text(source.get("api_url"))
    if api_url:
        return api_url
    source_value = clean_text(source.get(spec.source_key))
    if not source_value:
        return ""
    if spec.dotted_url_template and "." in source_value:
        return spec.dotted_url_template.format(value=source_value)
    return spec.url_template.format(value=source_value)


def _parse_json_feed_payload(
    source: dict[str, object], payload: Any, studio: str, spec: JsonFeedSpec
) -> list[RawJob]:
    return spec.parser(payload, clean_text(source.get(spec.source_key)), studio)


def _json_feed_payload_count(payload: Any, parsed: list[RawJob], spec: JsonFeedSpec) -> int:
    if spec.list_payload and isinstance(payload, list):
        return len(payload)
    if spec.payload_key and isinstance(payload, dict):
        return len(payload.get(spec.payload_key, []))
    return len(parsed)


JSON_FEED_SPECS: dict[str, JsonFeedSpec] = {
    "smartrecruiters": JsonFeedSpec(
        default_error="missing company_id/api_url",
        source_key="company_id",
        url_template="https://api.smartrecruiters.com/v1/companies/{value}/postings",
        parser=_provider_parsers.parse_smartrecruiters_jobs_payload,
        payload_key="content",
    ),
    "workable": JsonFeedSpec(
        default_error="missing account/api_url",
        source_key="account",
        url_template="https://apply.workable.com/api/v1/widget/accounts/{value}?details=true",
        parser=_provider_parsers.parse_workable_jobs_payload,
        payload_key="jobs",
    ),
    "recruitee": JsonFeedSpec(
        default_error="missing subdomain/api_url",
        source_key="subdomain",
        url_template="https://{value}.recruitee.com/api/offers/",
        dotted_url_template="https://{value}/api/offers/",
        parser=_provider_parsers.parse_recruitee_jobs_payload,
        payload_key="offers",
    ),
    "pinpoint": JsonFeedSpec(
        default_error="missing subdomain/api_url",
        source_key="subdomain",
        url_template="https://{value}.pinpointhq.com/postings.json",
        dotted_url_template="https://{value}/postings.json",
        parser=_provider_parsers.parse_pinpoint_jobs_payload,
        payload_key="data",
    ),
    "lever": JsonFeedSpec(
        default_error="missing account/api_url",
        source_key="account",
        url_template="https://api.lever.co/v0/postings/{value}?mode=json",
        parser=_provider_parsers.parse_lever_jobs_payload,
        list_payload=True,
    ),
}


def _json_feed_source_identity(source: dict[str, object], registry_adapter: str) -> tuple[str, str]:
    source_name = clean_text(source.get("name")) or f"{registry_adapter}_source"
    studio = clean_text(source.get("studio")) or source_name
    return source_name, studio


def _json_feed_entry_report(
    *, adapter_name: str, studio: str, source_name: str
) -> dict[str, object]:
    return {
        "adapter": adapter_name,
        "studio": studio,
        "name": source_name,
        "status": "ok",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "",
    }


def _apply_json_feed_cache_decision(
    *,
    entry_report: dict[str, object],
    source_name: str,
    adapter_name: str,
    source_state_rows: dict[str, dict[str, object]] | None,
    force_refresh_all: bool,
) -> None:
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


def _skip_json_feed_for_cache(entry_report: dict[str, object]) -> bool:
    if entry_report["cacheDecision"] not in {"skip_fresh", "cooldown_skip"}:
        return False
    entry_report["status"] = "excluded"
    entry_report["error"] = entry_report["cacheDecisionReason"]
    entry_report["exclusionReason"] = f"cache_{entry_report['cacheDecisionReason']}"
    return True


def _json_feed_revalidate_not_modified(
    *,
    endpoint: str,
    entry_report: dict[str, object],
    source_name: str,
    source_state_rows: dict[str, dict[str, object]] | None,
    timeout_s: int,
) -> bool:
    if entry_report["cacheDecision"] != "revalidate_only":
        return False
    state_entry = (
        (source_state_rows or {}).get(source_name) if isinstance(source_state_rows, dict) else {}
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
    if not bool(revalidate.get("notModified")):
        return False
    entry_report["status"] = "excluded"
    entry_report["error"] = "not_modified_304"
    entry_report["exclusionReason"] = "cache_not_modified_304"
    entry_report["cacheDecisionReason"] = "not_modified_304"
    return True


def _run_json_feed_sources(
    *,
    adapter_name: str,
    registry_adapter: str,
    default_error: str,
    parse_payload: ParsePayload,
    build_url: BuildUrl,
    payload_count: PayloadCount,
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
        source_name, studio = _json_feed_source_identity(source, registry_adapter)
        endpoint = build_url(source)
        entry_report = _json_feed_entry_report(
            adapter_name=adapter_name,
            studio=studio,
            source_name=source_name,
        )
        _apply_json_feed_cache_decision(
            entry_report=entry_report,
            source_name=source_name,
            adapter_name=adapter_name,
            source_state_rows=source_state_rows,
            force_refresh_all=force_refresh_all,
        )
        if not endpoint:
            entry_report["status"] = "error"
            entry_report["error"] = default_error
            details.append(entry_report)
            continue
        if _skip_json_feed_for_cache(entry_report):
            details.append(entry_report)
            continue
        if _json_feed_revalidate_not_modified(
            endpoint=endpoint,
            entry_report=entry_report,
            source_name=source_name,
            source_state_rows=source_state_rows,
            timeout_s=timeout_s,
        ):
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
    spec = JSON_FEED_SPECS.get(adapter_name, JSON_FEED_SPECS["lever"])
    registry_adapter = adapter_name

    adapter_key = f"{adapter_name}_sources"

    def can_handle(ctx: AdapterPluginContext) -> bool:
        return ctx.family == "provider_api" and ctx.adapter_key == adapter_key

    def run_plugin(**kwargs: Any) -> list[RawJob]:
        return _run_json_feed_sources(
            adapter_name=adapter_name,
            registry_adapter=registry_adapter,
            default_error=spec.default_error,
            parse_payload=lambda source, payload, studio: _parse_json_feed_payload(
                source, payload, studio, spec
            ),
            build_url=lambda source: _build_json_feed_url(source, spec),
            payload_count=lambda payload, parsed: _json_feed_payload_count(payload, parsed, spec),
            **kwargs,
        )

    return SimpleAdapterPlugin(
        name=adapter_key,
        family="provider_api",
        priority=50,
        can_handle_fn=can_handle,
        run_fn=run_plugin,
    )
