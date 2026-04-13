"""Social-source adapters extracted from the legacy fetcher."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import Any
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from src.exceptions import AdapterValidationError
from src.jobs.adapters import social_parsers as _social_parsers
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.social.register import ensure_registered as ensure_social_plugins
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.common.diagnostics import SOURCE_DIAGNOSTICS, set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.state import get_incremental_cache_decision
from src.jobs.text_utils import clean_text

from ..common import config as common_config


def _request_json_with_headers(
    url: str, *, timeout_s: int, headers: dict[str, str] | None = None
) -> dict[str, Any]:
    req = Request(url=url, headers=headers or {})
    with urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read().decode(resp.headers.get_content_charset() or "utf-8", errors="replace")
        parsed = json.loads(raw) if raw else {}
        return parsed if isinstance(parsed, dict) else {}


def run_social_reddit_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
    heartbeat_callback: Callable[[], None] | None = None,
) -> list[RawJob]:
    cfg = social_config.get("reddit") if isinstance(social_config.get("reddit"), dict) else {}
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        set_source_diagnostics(
            "social_reddit", adapter="social", studio="reddit", details=[], partial_errors=[]
        )
        return []

    ensure_social_plugins(social_config=social_config)
    plugin, _selection = default_registry.select(
        AdapterPluginContext(family="social", adapter_key="social_reddit")
    )
    subs = [clean_text(item) for item in (cfg.get("subreddits") or []) if clean_text(item)]
    if not subs:
        set_source_diagnostics(
            "social_reddit", adapter="social", studio="reddit", details=[], partial_errors=[]
        )
        return []

    details: list[dict[str, Any]] = []
    errors: list[str] = []
    rows: list[RawJob] = []

    def tick() -> None:
        if heartbeat_callback:
            heartbeat_callback()

    for sub in subs:
        entry = {
            "adapter": "social",
            "studio": f"reddit/{sub}",
            "name": f"reddit:r/{sub}",
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        cache_decision = get_incremental_cache_decision(
            entry["name"],
            source_state_rows or {},
            adapter="social",
            force_refresh_all=force_refresh_all,
        )
        entry["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
        entry["cacheDecisionReason"] = (
            clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
        )
        if entry["cacheDecision"] in {"skip_fresh", "cooldown_skip"}:
            entry["status"] = "excluded"
            entry["error"] = entry["cacheDecisionReason"]
            entry["exclusionReason"] = f"cache_{entry['cacheDecisionReason']}"
            details.append(entry)
            continue
        try:
            tick()
            sub_rows = plugin.run(
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                subreddits=[sub],
                heartbeat_callback=heartbeat_callback,
            )
            entry["fetchedCount"] = len(sub_rows)
            entry["keptCount"] = len(sub_rows)
            rows.extend(sub_rows)
            tick()
        except Exception as exc:  # noqa: BLE001
            entry["status"] = "error"
            entry["error"] = str(exc)
            errors.append(f"reddit:{sub}: {exc}")
        details.append(entry)

    plugin_diag = (
        SOURCE_DIAGNOSTICS.get("social_reddit")
        if isinstance(SOURCE_DIAGNOSTICS.get("social_reddit"), dict)
        else {}
    )
    plugin_detail_by_name = {
        clean_text(item.get("name")): item
        for item in (plugin_diag.get("details") or [])
        if isinstance(item, dict) and clean_text(item.get("name"))
    }
    for entry in details:
        plugin_detail = plugin_detail_by_name.get(clean_text(entry.get("name"))) or {}
        reject_reason_counts = plugin_detail.get("rejectReasonCounts")
        if isinstance(reject_reason_counts, dict) and reject_reason_counts:
            entry["rejectReasonCounts"] = dict(reject_reason_counts)

    set_source_diagnostics(
        "social_reddit", adapter="social", studio="reddit", details=details, partial_errors=errors
    )
    SOURCE_DIAGNOSTICS["social_reddit"]["lowConfidenceDropped"] = int(
        plugin_diag.get("lowConfidenceDropped") or 0
    )
    if rows:
        return rows
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_social_x_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    cfg = social_config.get("x") if isinstance(social_config.get("x"), dict) else {}
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        set_source_diagnostics(
            "social_x", adapter="social", studio="x", details=[], partial_errors=[]
        )
        return []
    queries = [clean_text(item) for item in (cfg.get("queries") or []) if clean_text(item)]
    if not queries:
        return []
    max_posts = max(1, int(cfg.get("maxPostsPerQuery") or 25))
    min_conf = max(
        0,
        min(
            100,
            int(
                cfg.get("minConfidence")
                or social_config.get("minConfidence")
                or common_config.DEFAULT_SOCIAL_MIN_CONFIDENCE
            ),
        ),
    )
    reject_for_hire = bool(social_config.get("rejectForHirePosts", True))
    api_cfg = cfg.get("api") if isinstance(cfg.get("api"), dict) else {}
    scraper_cfg = cfg.get("scraperFallback") if isinstance(cfg.get("scraperFallback"), dict) else {}
    rss_cfg = cfg.get("rssFallback") if isinstance(cfg.get("rssFallback"), dict) else {}
    bearer_env = clean_text(api_cfg.get("bearerTokenEnv") or "BALUFFO_X_BEARER_TOKEN")
    bearer = clean_text(os.environ.get(bearer_env))
    endpoint = clean_text(api_cfg.get("endpoint"))
    scraper_endpoint = clean_text(scraper_cfg.get("endpoint"))
    rss_instances = [
        clean_text(item).rstrip("/")
        for item in (rss_cfg.get("instances") or [])
        if clean_text(item)
    ]

    details: list[dict[str, Any]] = []
    errors: list[str] = []
    jobs: list[RawJob] = []
    low_conf_total = 0

    for query in queries:
        entry = {
            "adapter": "social",
            "studio": "x",
            "name": f"x:{query}",
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        cache_decision = get_incremental_cache_decision(
            entry["name"],
            source_state_rows or {},
            adapter="social",
            force_refresh_all=force_refresh_all,
        )
        entry["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
        entry["cacheDecisionReason"] = (
            clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
        )
        parsed_rows: list[RawJob] = []
        low_conf_query = 0
        reject_reason_counts: dict[str, int] = {}
        if entry["cacheDecision"] in {"skip_fresh", "cooldown_skip"}:
            entry["status"] = "excluded"
            entry["error"] = entry["cacheDecisionReason"]
            entry["exclusionReason"] = f"cache_{entry['cacheDecisionReason']}"
            details.append(entry)
            continue
        try:
            payload: Any = {}
            if bool(api_cfg.get("enabled", True)) and bearer and endpoint:
                url = f"{endpoint}?query={quote(query, safe='')}&max_results={max_posts}&tweet.fields=created_at,entities"
                payload = _request_json_with_headers(
                    url,
                    timeout_s=timeout_s,
                    headers={"Authorization": f"Bearer {bearer}", "Accept": "application/json"},
                )
            elif bool(scraper_cfg.get("enabled")) and scraper_endpoint:
                url = f"{scraper_endpoint}?q={quote(query, safe='')}&limit={max_posts}"
                text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
                payload = json.loads(text)
            elif bool(rss_cfg.get("enabled", True)) and rss_instances:
                rss_errors: list[str] = []
                rss_payload_text = ""
                for instance in rss_instances:
                    rss_url = f"{instance}/search/rss?f=tweets&q={quote(query, safe='')}"
                    try:
                        rss_payload_text = fetch_with_retries(
                            rss_url, fetch_text, timeout_s, retries, backoff_s
                        )
                        break
                    except Exception as rss_exc:  # noqa: BLE001
                        rss_errors.append(f"{instance}: {rss_exc}")
                if not rss_payload_text:
                    raise (
                        AdapterValidationError.from_errors(rss_errors)
                        if rss_errors
                        else AdapterValidationError("x rss fallback failed")
                    )
                parsed_rows, low_conf_query = _social_parsers.parse_x_rss_payload(
                    rss_payload_text,
                    query_label=query,
                    min_confidence=min_conf,
                    reject_for_hire_posts=reject_for_hire,
                    reject_reasons=reject_reason_counts,
                )
                entry["fetchedCount"] = len(parsed_rows) + int(low_conf_query)
                entry["keptCount"] = len(parsed_rows)
                if reject_reason_counts:
                    entry["rejectReasonCounts"] = reject_reason_counts
                low_conf_total += int(low_conf_query)
                jobs.extend(parsed_rows)
                details.append(entry)
                continue
            else:
                entry["status"] = "error"
                entry["error"] = "missing x api credentials and fallbacks disabled"
                errors.append(f"x:{query}: {entry['error']}")
                details.append(entry)
                continue

            parsed_rows, low_conf_query = _social_parsers.parse_x_payload(
                payload,
                query_label=query,
                min_confidence=min_conf,
                reject_for_hire_posts=reject_for_hire,
                reject_reasons=reject_reason_counts,
            )
            if isinstance(payload, dict) and isinstance(payload.get("data"), list):
                entry["fetchedCount"] = len(payload.get("data") or [])
            else:
                entry["fetchedCount"] = len(parsed_rows) + int(low_conf_query)
        except Exception as exc:  # noqa: BLE001
            entry["status"] = "error"
            entry["error"] = str(exc)
            errors.append(f"x:{query}: {exc}")
        entry["keptCount"] = len(parsed_rows)
        if reject_reason_counts:
            entry["rejectReasonCounts"] = reject_reason_counts
        low_conf_total += int(low_conf_query)
        jobs.extend(parsed_rows)
        details.append(entry)

    set_source_diagnostics(
        "social_x", adapter="social", studio="x", details=details, partial_errors=errors
    )
    SOURCE_DIAGNOSTICS["social_x"]["lowConfidenceDropped"] = int(low_conf_total)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def run_social_mastodon_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
) -> list[RawJob]:
    cfg = social_config.get("mastodon") if isinstance(social_config.get("mastodon"), dict) else {}
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        set_source_diagnostics(
            "social_mastodon",
            adapter="social",
            studio="mastodon",
            details=[],
            partial_errors=[],
        )
        return []
    instances = [
        clean_text(item).rstrip("/") for item in (cfg.get("instances") or []) if clean_text(item)
    ]
    tags = [
        clean_text(item).lstrip("#") for item in (cfg.get("hashtags") or []) if clean_text(item)
    ]
    max_posts = max(1, int(cfg.get("maxPostsPerTag") or 40))
    min_conf = max(
        0,
        min(
            100,
            int(social_config.get("minConfidence") or common_config.DEFAULT_SOCIAL_MIN_CONFIDENCE),
        ),
    )
    reject_for_hire = bool(social_config.get("rejectForHirePosts", True))
    details: list[dict[str, Any]] = []
    errors: list[str] = []
    jobs: list[RawJob] = []
    low_conf_total = 0

    for instance in instances:
        for tag in tags:
            entry = {
                "adapter": "social",
                "studio": f"mastodon/{clean_text(urlparse(instance).netloc)}",
                "name": f"mastodon:{clean_text(urlparse(instance).netloc)}:#{tag}",
                "status": "ok",
                "fetchedCount": 0,
                "keptCount": 0,
                "error": "",
            }
            reject_reason_counts: dict[str, int] = {}
            cache_decision = get_incremental_cache_decision(
                entry["name"],
                source_state_rows or {},
                adapter="social",
                force_refresh_all=force_refresh_all,
            )
            entry["cacheDecision"] = clean_text(cache_decision.get("cacheDecision")) or "run_now"
            entry["cacheDecisionReason"] = (
                clean_text(cache_decision.get("cacheDecisionReason")) or "run_now"
            )
            if entry["cacheDecision"] in {"skip_fresh", "cooldown_skip"}:
                entry["status"] = "excluded"
                entry["error"] = entry["cacheDecisionReason"]
                entry["exclusionReason"] = f"cache_{entry['cacheDecisionReason']}"
                details.append(entry)
                continue
            try:
                url = f"{instance}/api/v1/timelines/tag/{quote(tag, safe='')}?limit={max_posts}"
                text = fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
                payload = json.loads(text)
                parsed_rows, low_conf_tag = _social_parsers.parse_mastodon_payload(
                    payload,
                    instance=instance,
                    tag=tag,
                    min_confidence=min_conf,
                    reject_for_hire_posts=reject_for_hire,
                    reject_reasons=reject_reason_counts,
                )
                entry["fetchedCount"] = (
                    len(payload)
                    if isinstance(payload, list)
                    else len(parsed_rows) + int(low_conf_tag)
                )
                entry["keptCount"] = len(parsed_rows)
                if reject_reason_counts:
                    entry["rejectReasonCounts"] = reject_reason_counts
                low_conf_total += int(low_conf_tag)
                jobs.extend(parsed_rows)
            except Exception as exc:  # noqa: BLE001
                entry["status"] = "error"
                entry["error"] = str(exc)
                errors.append(f"mastodon:{instance}:#{tag}: {exc}")
            details.append(entry)

    set_source_diagnostics(
        "social_mastodon",
        adapter="social",
        studio="mastodon",
        details=details,
        partial_errors=errors,
    )
    SOURCE_DIAGNOSTICS["social_mastodon"]["lowConfidenceDropped"] = int(low_conf_total)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []
