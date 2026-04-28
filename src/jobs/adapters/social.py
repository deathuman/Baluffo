"""Social-source adapters extracted from the legacy fetcher."""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from typing import Any, cast
from urllib.parse import quote, urlparse
from urllib.request import Request, urlopen

from src.exceptions import AdapterValidationError
from src.jobs.adapters import social_parsers as _social_parsers
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.social.register import ensure_registered as ensure_social_plugins
from src.jobs.adapters.plugins.types import AdapterPluginContext
from src.jobs.adapters.recovery import run_recoverable_adapter_attempt
from src.jobs.common.diagnostics import SOURCE_DIAGNOSTICS, set_source_diagnostics
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.state import get_incremental_cache_decision
from src.jobs.text_utils import clean_text

from ..common import config as common_config


def _as_dict(value: object) -> dict[str, Any]:
    return cast(dict[str, Any], value) if isinstance(value, dict) else {}


def _as_list(value: object) -> list[Any]:
    return cast(list[Any], value) if isinstance(value, list) else []


def _text_items(value: object) -> list[str]:
    return [text for item in _as_list(value) if (text := clean_text(item))]


def _diag_rows(value: object) -> list[dict[str, Any]]:
    return [row for row in _as_list(value) if isinstance(row, dict)]


def _diag_payload(source_name: str) -> dict[str, Any]:
    return _as_dict(SOURCE_DIAGNOSTICS.get(source_name))


def _coerce_int(value: object, default: int = 0) -> int:
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        text = clean_text(value)
        if not text:
            return default
        try:
            return int(text)
        except ValueError:
            return default
    return default


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
    progress_callback: Callable[..., None] | None = None,
) -> list[RawJob]:
    cfg = _as_dict(social_config.get("reddit"))
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        set_source_diagnostics(
            "social_reddit", adapter="social", studio="reddit", details=[], partial_errors=[]
        )
        return []

    ensure_social_plugins(social_config=social_config)
    plugin, _selection = default_registry.select(
        AdapterPluginContext(family="social", adapter_key="social_reddit")
    )
    subs = _text_items(cfg.get("subreddits"))
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

    def emit_progress(
        *,
        phase_key: str,
        phase_label: str,
        target_label: str = "",
        target_url: str = "",
        counts: dict[str, Any] | None = None,
        event_level: str = "muted",
        message: str = "",
    ) -> None:
        if progress_callback is None:
            return
        progress_callback(
            phase_key=phase_key,
            phase_label=phase_label,
            target_label=target_label,
            target_url=target_url,
            counts=counts,
            event_level=event_level,
            message=message,
        )

    for sub in subs:
        subreddit_label = f"reddit/r/{sub}"
        subreddit_url = f"https://www.reddit.com/r/{sub}/new.json"
        entry_name = f"reddit:r/{sub}"
        entry = {
            "adapter": "social",
            "studio": f"reddit/{sub}",
            "name": entry_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        cache_decision = get_incremental_cache_decision(
            entry_name,
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
            emit_progress(
                phase_key="scanning_subsource",
                phase_label="Scanning subsource",
                target_label=subreddit_label,
                target_url=subreddit_url,
                message=f"Scanning {subreddit_label}.",
            )
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
            emit_progress(
                phase_key="subsource_loaded",
                phase_label="Subsource loaded",
                target_label=subreddit_label,
                target_url=subreddit_url,
                counts={"fetchedCount": len(sub_rows), "keptCount": len(sub_rows)},
                message=f"Loaded {len(sub_rows)} row(s) from {subreddit_label}.",
            )
            tick()
        except Exception as exc:  # noqa: BLE001
            entry["status"] = "error"
            entry["error"] = str(exc)
            errors.append(f"reddit:{sub}: {exc}")
            emit_progress(
                phase_key="subsource_error",
                phase_label="Subsource error",
                target_label=subreddit_label,
                target_url=subreddit_url,
                event_level="warn",
                message=f"{subreddit_label} failed: {exc}",
            )
        details.append(entry)

    plugin_diag = _diag_payload("social_reddit")
    plugin_detail_by_name = {
        clean_text(item.get("name")): item
        for item in _diag_rows(plugin_diag.get("details"))
        if clean_text(item.get("name"))
    }
    for entry in details:
        plugin_detail = plugin_detail_by_name.get(clean_text(entry.get("name"))) or {}
        reject_reason_counts = plugin_detail.get("rejectReasonCounts")
        if isinstance(reject_reason_counts, dict) and reject_reason_counts:
            entry["rejectReasonCounts"] = dict(reject_reason_counts)

    set_source_diagnostics(
        "social_reddit", adapter="social", studio="reddit", details=details, partial_errors=errors
    )
    SOURCE_DIAGNOSTICS["social_reddit"]["lowConfidenceDropped"] = _coerce_int(
        plugin_diag.get("lowConfidenceDropped")
    )
    if rows:
        return rows
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def _load_x_query_payload(
    *,
    query: str,
    max_posts: int,
    api_cfg: dict[str, Any],
    scraper_cfg: dict[str, Any],
    rss_cfg: dict[str, Any],
    bearer: str,
    endpoint: str,
    scraper_endpoint: str,
    rss_instances: list[str],
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None,
) -> tuple[str, Any]:
    if bool(api_cfg.get("enabled", True)) and bearer and endpoint:
        url = f"{endpoint}?query={quote(query, safe='')}&max_results={max_posts}&tweet.fields=created_at,entities"
        if heartbeat_callback:
            heartbeat_callback()
        return (
            "api",
            _request_json_with_headers(
                url,
                timeout_s=timeout_s,
                headers={"Authorization": f"Bearer {bearer}", "Accept": "application/json"},
            ),
        )
    if bool(scraper_cfg.get("enabled")) and scraper_endpoint:
        url = f"{scraper_endpoint}?q={quote(query, safe='')}&limit={max_posts}"
        text = fetch_with_retries(
            url,
            fetch_text,
            timeout_s,
            retries,
            backoff_s,
            heartbeat_callback=heartbeat_callback,
        )
        return "api", json.loads(text)
    if bool(rss_cfg.get("enabled", True)) and rss_instances:
        rss_errors: list[str] = []
        for instance in rss_instances:
            rss_url = f"{instance}/search/rss?f=tweets&q={quote(query, safe='')}"

            def _fetch_rss(rss_url: str = rss_url) -> tuple[str, str]:
                return (
                    "rss",
                    fetch_with_retries(
                        rss_url,
                        fetch_text,
                        timeout_s,
                        retries,
                        backoff_s,
                        heartbeat_callback=heartbeat_callback,
                    ),
                )

            def _record_rss_error(exc: Exception, instance: str = instance) -> None:
                rss_errors.append(f"{instance}: {exc}")

            result = run_recoverable_adapter_attempt(_fetch_rss, _record_rss_error)
            if result is not None:
                return result
        raise AdapterValidationError.from_errors(rss_errors)
    return "missing", {}


def run_social_x_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: dict[str, Any],
    source_state_rows: dict[str, dict[str, Any]] | None = None,
    force_refresh_all: bool = False,
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> list[RawJob]:
    cfg = _as_dict(social_config.get("x"))
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        set_source_diagnostics(
            "social_x", adapter="social", studio="x", details=[], partial_errors=[]
        )
        return []
    queries = _text_items(cfg.get("queries"))
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
    api_cfg = _as_dict(cfg.get("api"))
    scraper_cfg = _as_dict(cfg.get("scraperFallback"))
    rss_cfg = _as_dict(cfg.get("rssFallback"))
    bearer_env = clean_text(api_cfg.get("bearerTokenEnv") or "BALUFFO_X_BEARER_TOKEN")
    bearer = clean_text(os.environ.get(bearer_env))
    endpoint = clean_text(api_cfg.get("endpoint"))
    scraper_endpoint = clean_text(scraper_cfg.get("endpoint"))
    rss_instances = [item.rstrip("/") for item in _text_items(rss_cfg.get("instances"))]

    details: list[dict[str, Any]] = []
    errors: list[str] = []
    jobs: list[RawJob] = []
    low_conf_total = 0

    def emit_progress(
        *,
        phase_key: str,
        phase_label: str,
        target_label: str = "",
        target_url: str = "",
        counts: dict[str, Any] | None = None,
        event_level: str = "muted",
        message: str = "",
    ) -> None:
        if progress_callback is None:
            return
        progress_callback(
            phase_key=phase_key,
            phase_label=phase_label,
            target_label=target_label,
            target_url=target_url,
            counts=counts,
            event_level=event_level,
            message=message,
        )

    for query in queries:
        query_label = f"x:{query}"
        entry_name = f"x:{query}"
        entry = {
            "adapter": "social",
            "studio": "x",
            "name": entry_name,
            "status": "ok",
            "fetchedCount": 0,
            "keptCount": 0,
            "error": "",
        }
        cache_decision = get_incremental_cache_decision(
            entry_name,
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
            emit_progress(
                phase_key="scanning_subsource",
                phase_label="Scanning subsource",
                target_label=query_label,
                counts={"maxPosts": max_posts},
                message=f"Scanning {query_label}.",
            )
            payload_kind, payload = _load_x_query_payload(
                query=query,
                max_posts=max_posts,
                api_cfg=api_cfg,
                scraper_cfg=scraper_cfg,
                rss_cfg=rss_cfg,
                bearer=bearer,
                endpoint=endpoint,
                scraper_endpoint=scraper_endpoint,
                rss_instances=rss_instances,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                heartbeat_callback=heartbeat_callback,
            )
            if payload_kind == "rss":
                parsed_rows, low_conf_query = _social_parsers.parse_x_rss_payload(
                    str(payload),
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
                emit_progress(
                    phase_key="subsource_loaded",
                    phase_label="Subsource loaded",
                    target_label=query_label,
                    counts={
                        "fetchedCount": len(parsed_rows) + int(low_conf_query),
                        "keptCount": len(parsed_rows),
                    },
                    message=f"Loaded {len(parsed_rows)} row(s) from {query_label}.",
                )
                details.append(entry)
                continue
            if payload_kind == "missing":
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
            payload_data = _as_list(_as_dict(payload).get("data"))
            if payload_data:
                entry["fetchedCount"] = len(payload_data)
            else:
                entry["fetchedCount"] = len(parsed_rows) + int(low_conf_query)
        except Exception as exc:  # noqa: BLE001
            entry["status"] = "error"
            entry["error"] = str(exc)
            errors.append(f"x:{query}: {exc}")
            emit_progress(
                phase_key="subsource_error",
                phase_label="Subsource error",
                target_label=query_label,
                event_level="warn",
                message=f"{query_label} failed: {exc}",
            )
        entry["keptCount"] = len(parsed_rows)
        if reject_reason_counts:
            entry["rejectReasonCounts"] = reject_reason_counts
        low_conf_total += int(low_conf_query)
        jobs.extend(parsed_rows)
        if entry["status"] == "ok":
            emit_progress(
                phase_key="subsource_loaded",
                phase_label="Subsource loaded",
                target_label=query_label,
                counts={"fetchedCount": entry["fetchedCount"], "keptCount": len(parsed_rows)},
                message=f"Loaded {len(parsed_rows)} row(s) from {query_label}.",
            )
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
    heartbeat_callback: Callable[[], None] | None = None,
    progress_callback: Callable[..., None] | None = None,
) -> list[RawJob]:
    cfg = _as_dict(social_config.get("mastodon"))
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        set_source_diagnostics(
            "social_mastodon",
            adapter="social",
            studio="mastodon",
            details=[],
            partial_errors=[],
        )
        return []
    instances = [item.rstrip("/") for item in _text_items(cfg.get("instances"))]
    tags = [item.lstrip("#") for item in _text_items(cfg.get("hashtags"))]
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

    def tick() -> None:
        if heartbeat_callback:
            heartbeat_callback()

    def emit_progress(
        *,
        phase_key: str,
        phase_label: str,
        target_label: str = "",
        target_url: str = "",
        counts: dict[str, Any] | None = None,
        event_level: str = "muted",
        message: str = "",
    ) -> None:
        if progress_callback is None:
            return
        progress_callback(
            phase_key=phase_key,
            phase_label=phase_label,
            target_label=target_label,
            target_url=target_url,
            counts=counts,
            event_level=event_level,
            message=message,
        )

    for instance in instances:
        for tag in tags:
            timeline_url = (
                f"{instance}/api/v1/timelines/tag/{quote(tag, safe='')}?limit={max_posts}"
            )
            target_host = clean_text(urlparse(instance).netloc)
            target_label = f"mastodon:{target_host}:#{tag}"
            entry_name = target_label
            entry = {
                "adapter": "social",
                "studio": f"mastodon/{target_host}",
                "name": entry_name,
                "status": "ok",
                "fetchedCount": 0,
                "keptCount": 0,
                "error": "",
            }
            reject_reason_counts: dict[str, int] = {}
            cache_decision = get_incremental_cache_decision(
                entry_name,
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
                emit_progress(
                    phase_key="scanning_subsource",
                    phase_label="Scanning subsource",
                    target_label=target_label,
                    target_url=timeline_url,
                    counts={"maxPosts": max_posts},
                    message=f"Scanning {target_label}.",
                )
                tick()
                text = fetch_with_retries(timeline_url, fetch_text, timeout_s, retries, backoff_s)
                payload = json.loads(text)
                parsed_rows, low_conf_tag = _social_parsers.parse_mastodon_payload(
                    payload,
                    instance=instance,
                    tag=tag,
                    min_confidence=min_conf,
                    reject_for_hire_posts=reject_for_hire,
                    reject_reasons=reject_reason_counts,
                )
                payload_rows = _as_list(payload)
                entry["fetchedCount"] = (
                    len(payload_rows) if payload_rows else len(parsed_rows) + int(low_conf_tag)
                )
                entry["keptCount"] = len(parsed_rows)
                if reject_reason_counts:
                    entry["rejectReasonCounts"] = reject_reason_counts
                low_conf_total += int(low_conf_tag)
                jobs.extend(parsed_rows)
                emit_progress(
                    phase_key="subsource_loaded",
                    phase_label="Subsource loaded",
                    target_label=target_label,
                    target_url=timeline_url,
                    counts={"fetchedCount": entry["fetchedCount"], "keptCount": len(parsed_rows)},
                    message=f"Loaded {len(parsed_rows)} row(s) from {target_label}.",
                )
                tick()
            except Exception as exc:  # noqa: BLE001
                entry["status"] = "error"
                entry["error"] = str(exc)
                errors.append(f"mastodon:{instance}:#{tag}: {exc}")
                emit_progress(
                    phase_key="subsource_error",
                    phase_label="Subsource error",
                    target_label=target_label,
                    target_url=timeline_url,
                    event_level="warn",
                    message=f"{target_label} failed: {exc}",
                )
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
