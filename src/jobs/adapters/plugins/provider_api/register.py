from __future__ import annotations

import json
from collections.abc import Callable
from urllib.parse import urlparse

from src.exceptions import AdapterValidationError
from src.jobs.adapters import _runtime as runtime_deps
from src.jobs.adapters import provider_migration as _provider_migration
from src.jobs.adapters import provider_parsers as _provider_parsers
from src.jobs.adapters import provider_personio as _provider_personio
from src.jobs.adapters.html_parsers import (
    parse_jobpostings_from_html,
    parse_teamtailor_listing_links,
)
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import SimpleAdapterPlugin
from src.jobs.common.config import GREENHOUSE_JOBS_URL_TEMPLATE
from src.jobs.models import RawJob
from src.jobs.state import get_incremental_cache_decision
from src.jobs.text_utils import clean_text
from src.jobs.transport import conditional_revalidate_url

_REGISTERED = False


def _normalize_ashby_board_url(url: str) -> str:
    text = clean_text(url)
    if not text:
        return ""
    parsed = urlparse(text)
    path = parsed.path.rstrip("/")
    if path.lower().endswith("/jobs"):
        normalized = parsed._replace(path=path[:-5] or "/", query="", fragment="")
        return normalized.geturl().rstrip("/")
    return text


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
    deps = runtime_deps.facade()
    for board in deps.registry_entries("greenhouse"):
        slug = clean_text(board.get("slug"))
        if not slug:
            continue
        label = clean_text(board.get("name")) or clean_text(board.get("studio")) or slug
        url = GREENHOUSE_JOBS_URL_TEMPLATE.format(slug=slug)
        entry_report = {
            "adapter": "greenhouse",
            "studio": clean_text(board.get("studio")) or label,
            "name": clean_text(board.get("name")) or slug,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        cache_decision = get_incremental_cache_decision(
            entry_report["name"],
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
            state_entry = (
                (source_state_rows or {}).get(entry_report["name"])
                if isinstance(source_state_rows, dict)
                else {}
            )
            revalidate = conditional_revalidate_url(
                url,
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
            text = deps.fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
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
            errors.append(f"greenhouse:{slug}: {exc}")
        details.append(entry_report)
    deps.set_source_diagnostics(
        "greenhouse_boards",
        adapter="greenhouse",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def _run_teamtailor_sources(
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
    seen_links = set()
    details: list[dict[str, object]] = []
    deps = runtime_deps.facade()
    parse_listing_links = getattr(
        deps, "parse_teamtailor_listing_links", parse_teamtailor_listing_links
    )
    parse_jobpostings_from_html_impl = getattr(
        deps, "parse_jobpostings_from_html", parse_jobpostings_from_html
    )
    for source in deps.registry_entries("teamtailor"):
        source_name = clean_text(source.get("name")) or "teamtailor_source"
        listing_url = clean_text(source.get("listing_url"))
        base_url = clean_text(source.get("base_url")) or listing_url
        fallback_company = clean_text(source.get("company"))
        entry_report = {
            "adapter": "teamtailor",
            "studio": clean_text(source.get("studio")) or fallback_company or source_name,
            "name": source_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        cache_decision = get_incremental_cache_decision(
            source_name,
            source_state_rows or {},
            adapter="teamtailor",
            force_refresh_all=force_refresh_all,
        )
        entry_report["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
        entry_report["cacheDecisionReason"] = (
            clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
        )
        if not listing_url:
            entry_report["status"] = "error"
            entry_report["error"] = "missing listing_url"
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
                listing_url,
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
            listing_html = deps.fetch_with_retries(
                listing_url, fetch_text, timeout_s, retries, backoff_s
            )
            job_links = parse_listing_links(listing_html, base_url=base_url)
            entry_report["fetchedCount"] = len(job_links)
            kept_before = len(jobs)
            for idx, job_link in enumerate(job_links, start=1):
                if job_link in seen_links:
                    continue
                seen_links.add(job_link)
                try:
                    detail_html = deps.fetch_with_retries(
                        job_link, fetch_text, timeout_s, retries, backoff_s
                    )
                    parsed = parse_jobpostings_from_html_impl(
                        detail_html,
                        base_url=job_link,
                        fallback_company=fallback_company,
                        fallback_source_id_prefix=f"teamtailor:{source_name}:{idx}",
                    )
                    if parsed:
                        for row in parsed:
                            row["adapter"] = "teamtailor"
                            row["studio"] = (
                                clean_text(source.get("studio")) or fallback_company or source_name
                            )
                        jobs.extend(parsed)
                    else:
                        slug = urlparse(job_link).path.rstrip("/").split("/")[-1]
                        title = slug.replace("-", " ").strip()
                        if title:
                            jobs.append(
                                {
                                    "sourceJobId": f"teamtailor:{source_name}:{slug}",
                                    "title": title,
                                    "company": fallback_company or "Unknown",
                                    "city": "",
                                    "country": "Unknown",
                                    "workType": "",
                                    "contractType": "",
                                    "jobLink": job_link,
                                    "sector": "Game",
                                    "postedAt": "",
                                    "adapter": "teamtailor",
                                    "studio": clean_text(source.get("studio"))
                                    or fallback_company
                                    or source_name,
                                }
                            )
                except Exception as exc:  # noqa: BLE001
                    errors.append(f"teamtailor:{source_name}:{job_link}: {exc}")
            entry_report["keptCount"] = max(0, len(jobs) - kept_before)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"teamtailor:{source_name}:{listing_url}: {exc}")
        details.append(entry_report)

    deps.set_source_diagnostics(
        "teamtailor_sources",
        adapter="teamtailor",
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


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
    deps = runtime_deps.facade()
    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    for source in deps.registry_entries(registry_adapter):
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
            text = deps.fetch_with_retries(endpoint, fetch_text, timeout_s, retries, backoff_s)
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
            errors.append(f"{registry_adapter}:{source_name}: {exc}")
        details.append(entry_report)

    deps.set_source_diagnostics(
        f"{registry_adapter}_sources",
        adapter=adapter_name,
        studio="multiple",
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


def _run_html_board_sources(
    *,
    adapter_name: str,
    registry_adapter: str,
    default_error: str,
    parse_html,
    build_url,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    source_state_rows: dict[str, dict[str, object]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    deps = runtime_deps.facade()
    jobs: list[RawJob] = []
    errors: list[str] = []
    details: list[dict[str, object]] = []
    for source in deps.registry_entries(registry_adapter):
        source_name = clean_text(source.get("name")) or f"{registry_adapter}_source"
        studio = clean_text(source.get("studio")) or source_name
        board_url = build_url(source)
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
        if not board_url:
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
                board_url,
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
            candidate_urls = (
                _iter_ashby_candidate_urls(source) if adapter_name == "ashby" else [board_url]
            )
            parsed = []
            last_text = ""
            for candidate_url in candidate_urls:
                last_text = deps.fetch_with_retries(
                    candidate_url, fetch_text, timeout_s, retries, backoff_s
                )
                parsed = parse_html(last_text, candidate_url, studio)
                if parsed:
                    break
            entry_report["fetchedCount"] = len(parsed)
            entry_report["keptCount"] = len(parsed)
            if not parsed:
                entry_report["status"] = "error"
                if adapter_name == "ashby" and "page not found" in clean_text(last_text).lower():
                    entry_report["classification"] = "dead_listing_page"
                entry_report["error"] = f"no jobs extracted from {adapter_name} board html"
            for row in parsed:
                row["adapter"] = adapter_name
                row["studio"] = studio
            jobs.extend(parsed)
        except Exception as exc:  # noqa: BLE001
            entry_report["status"] = "error"
            entry_report["error"] = str(exc)
            errors.append(f"{registry_adapter}:{source_name}: {exc}")
        details.append(entry_report)

    deps.set_source_diagnostics(
        f"{registry_adapter}_sources",
        adapter=adapter_name,
        studio="multiple",
        details=details,
        partial_errors=errors,
    )
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def _html_board_plugin(adapter_name: str) -> SimpleAdapterPlugin:
    registry_adapter = adapter_name
    if adapter_name == "breezy":
        default_error = "missing board_url"
        parse_html = _provider_parsers.parse_breezy_jobs_html
        build_url = lambda source: clean_text(source.get("board_url"))
    elif adapter_name == "ashby":
        default_error = "missing board_url"
        parse_html = _provider_parsers.parse_ashby_jobs_from_html
        build_url = lambda source: _normalize_ashby_board_url(clean_text(source.get("board_url")))
    else:
        default_error = "missing board_url"
        parse_html = _provider_parsers.parse_jazzhr_jobs_html
        build_url = lambda source: clean_text(source.get("board_url"))

    return SimpleAdapterPlugin(
        name=f"{adapter_name}_sources",
        family="provider_api",
        priority=55,
        can_handle_fn=lambda ctx: (
            ctx.family == "provider_api" and ctx.adapter_key == f"{adapter_name}_sources"
        ),
        run_fn=lambda **kwargs: _run_html_board_sources(
            adapter_name=adapter_name,
            registry_adapter=registry_adapter,
            default_error=default_error,
            parse_html=parse_html,
            build_url=build_url,
            **kwargs,
        ),
    )


def _normalize_ashby_candidate_url(url: str) -> str:
    text = clean_text(url)
    if not text:
        return ""
    if text.rstrip("/").endswith("/jobs"):
        return text.rstrip("/")[:-5].rstrip("/")
    return text


def _iter_ashby_candidate_urls(source: dict[str, object]) -> list[str]:
    candidates = [
        clean_text(source.get("board_url")),
        _normalize_ashby_candidate_url(clean_text(source.get("board_url"))),
        clean_text(source.get("careersUrl")),
        clean_text(source.get("sourceDirectoryEntryUrl")),
    ]
    rows: list[str] = []
    seen: set[str] = set()
    for item in candidates:
        if not item or item in seen:
            continue
        seen.add(item)
        rows.append(item)
    return rows


def ensure_registered() -> None:
    global _REGISTERED
    if _REGISTERED:
        return
    _REGISTERED = True
    default_registry.register(
        SimpleAdapterPlugin(
            name="greenhouse_boards",
            family="provider_api",
            priority=10,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "greenhouse_boards"
            ),
            run_fn=_run_greenhouse_boards,
        )
    )
    default_registry.register(
        SimpleAdapterPlugin(
            name="teamtailor_sources",
            family="provider_api",
            priority=20,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "teamtailor_sources"
            ),
            run_fn=_run_teamtailor_sources,
        )
    )
    default_registry.register(_json_feed_plugin("lever"))
    default_registry.register(_json_feed_plugin("workable"))
    default_registry.register(_json_feed_plugin("smartrecruiters"))
    default_registry.register(_json_feed_plugin("recruitee"))
    default_registry.register(_json_feed_plugin("pinpoint"))
    default_registry.register(
        SimpleAdapterPlugin(
            name="personio_sources",
            family="provider_api",
            priority=55,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "personio_sources"
            ),
            run_fn=_provider_personio.run_personio_sources_source,
        )
    )
    default_registry.register(
        SimpleAdapterPlugin(
            name="bamboohr_sources",
            family="provider_api",
            priority=56,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "bamboohr_sources"
            ),
            run_fn=_provider_migration.run_bamboohr_sources_source,
        )
    )
    default_registry.register(
        SimpleAdapterPlugin(
            name="workday_sources",
            family="provider_api",
            priority=56,
            can_handle_fn=lambda ctx: (
                ctx.family == "provider_api" and ctx.adapter_key == "workday_sources"
            ),
            run_fn=_provider_migration.run_workday_sources_source,
        )
    )
    default_registry.register(_html_board_plugin("breezy"))
    default_registry.register(_html_board_plugin("jazzhr"))
    default_registry.register(_html_board_plugin("ashby"))
