from __future__ import annotations

import json
import time
from collections.abc import Callable
from typing import Any, cast
from urllib.parse import quote
from xml.etree import ElementTree as ET

from src.exceptions import AdapterValidationError
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import SimpleAdapterPlugin
from src.jobs.adapters.recovery import run_recoverable_adapter_attempt
from src.jobs.adapters.social_parser import reddit_parser as _social_parsers
from src.jobs.common.config import DEFAULT_SOCIAL_MIN_CONFIDENCE, SOURCE_DIAGNOSTICS
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.models import RawJob
from src.jobs.text_utils import clean_text


def _as_dict(value: object) -> dict[str, object]:
    return cast(dict[str, object], value) if isinstance(value, dict) else {}


def _as_list(value: object) -> list[object]:
    return cast(list[object], value) if isinstance(value, list) else []


def _bool_value(value: object, default: bool = False) -> bool:
    return default if value is None else bool(value)


def _int_value(value: object, default: int) -> int:
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


def _append_adapter_error(errors: list[str], label: str, exc: Exception) -> None:
    errors.append(f"{label}: {exc}")


def _float_value(value: object, default: float) -> float:
    if isinstance(value, bool):
        return float(value)
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        text = clean_text(value)
        if not text:
            return default
        try:
            return float(text)
        except ValueError:
            return default
    return default


def _text_items(value: object) -> list[str]:
    return [text for item in _as_list(value) if (text := clean_text(item))]


def _social_subsource_entry(*, studio: str, name: str) -> dict[str, Any]:
    return {
        "adapter": "social",
        "studio": studio,
        "name": name,
        "status": "ok",
        "fetchedCount": 0,
        "keptCount": 0,
        "error": "",
    }


def set_source_diagnostics(
    source_name: str,
    *,
    adapter: str,
    studio: str,
    details: list[dict[str, Any]] | None = None,
    partial_errors: list[str] | None = None,
) -> None:
    SOURCE_DIAGNOSTICS[source_name] = {
        "adapter": clean_text(adapter) or "unknown",
        "studio": clean_text(studio) or "multiple",
        "details": details or [],
        "partialErrors": partial_errors or [],
    }


def _finish_social_plugin_source(
    source_name: str,
    *,
    studio: str,
    details: list[dict[str, Any]],
    errors: list[str],
    jobs: list[RawJob],
    low_confidence_dropped: int,
) -> list[RawJob]:
    set_source_diagnostics(
        source_name,
        adapter="social",
        studio=studio,
        details=details,
        partial_errors=errors,
    )
    SOURCE_DIAGNOSTICS[source_name]["lowConfidenceDropped"] = int(low_confidence_dropped)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def _finish_social_plugin_entry(
    entry: dict[str, Any],
    *,
    parsed_rows: list[RawJob],
    reject_reason_counts: dict[str, int],
    error_messages: list[str],
    error_prefix: str,
    errors: list[str],
) -> None:
    if error_messages and not parsed_rows:
        entry["status"] = "error"
        entry["error"] = "; ".join(error_messages)
        errors.append(f"{error_prefix}: {entry['error']}")
    entry["keptCount"] = len(parsed_rows)
    if reject_reason_counts:
        entry["rejectReasonCounts"] = reject_reason_counts


_REGISTERED = False
_SOCIAL_CONFIG: dict[str, object] = {}


def _parse_reddit_json_result(
    text: str,
    *,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reason_counts: dict[str, int],
    entry: dict[str, object],
) -> tuple[list[RawJob], int, str]:
    try:
        payload = json.loads(text)
    except json.JSONDecodeError as json_exc:
        return [], 0, f"JSON decode error: {json_exc}"
    parsed_rows, low_conf_sub = _social_parsers.parse_reddit_json_payload(
        payload,
        subreddit=subreddit,
        min_confidence=min_confidence,
        reject_for_hire_posts=reject_for_hire_posts,
        reject_reasons=reject_reason_counts,
    )
    entry["fetchedCount"] = len(
        (
            ((payload.get("data") or {}).get("children"))
            if isinstance(payload, dict) and isinstance(payload.get("data"), dict)
            else []
        )
        or []
    )
    return parsed_rows, int(low_conf_sub), ""


def _parse_reddit_rss_result(
    text: str,
    *,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reason_counts: dict[str, int],
) -> tuple[list[RawJob], int, str]:
    try:
        parsed_rows, low_conf_sub = _social_parsers.parse_reddit_rss_payload(
            text,
            subreddit=subreddit,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            reject_reasons=reject_reason_counts,
        )
    except ET.ParseError as rss_exc:
        return [], 0, f"RSS parse error: {rss_exc}"
    return parsed_rows, int(low_conf_sub), ""


def _run_reddit_html_fallback(
    *,
    subreddit: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reason_counts: dict[str, int],
    error_messages: list[str],
    tick: Callable[[], None],
) -> tuple[list[RawJob], int, bool]:
    def _html_attempt() -> tuple[list[RawJob], int]:
        html_url = f"https://old.reddit.com/r/{quote(subreddit, safe='')}/new/"
        tick()
        html_text = fetch_with_retries(
            html_url,
            fetch_text,
            timeout_s,
            retries,
            backoff_s,
            heartbeat_callback=heartbeat_callback,
        )
        parsed, low_conf = _social_parsers.parse_reddit_html_payload(
            html_text,
            subreddit=subreddit,
            min_confidence=min_confidence,
            reject_for_hire_posts=reject_for_hire_posts,
            reject_reasons=reject_reason_counts,
        )
        return parsed, int(low_conf)

    def _record_html_error(exc: Exception) -> None:
        _append_adapter_error(error_messages, "HTML fetch error", exc)

    html_result = run_recoverable_adapter_attempt(_html_attempt, _record_html_error)
    if html_result is None:
        return [], 0, False
    parsed_rows, low_conf_sub = html_result
    return parsed_rows, low_conf_sub, True


def _tick(heartbeat_callback: Callable[[], None] | None) -> None:
    if heartbeat_callback:
        heartbeat_callback()


def _run_reddit_json_api(
    *,
    json_url: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reason_counts: dict[str, int],
    entry: dict[str, Any],
    error_messages: list[str],
) -> tuple[list[RawJob], int]:
    def _fetch_json() -> str:
        return fetch_with_retries(
            json_url,
            fetch_text,
            timeout_s,
            retries,
            backoff_s,
            heartbeat_callback=heartbeat_callback,
        )

    def _record_json_error(exc: Exception) -> None:
        _append_adapter_error(error_messages, "JSON API error", exc)

    text = run_recoverable_adapter_attempt(_fetch_json, _record_json_error)
    if text is None:
        return [], 0
    _tick(heartbeat_callback)
    parsed_rows, low_conf_sub, parse_error = _parse_reddit_json_result(
        text,
        subreddit=subreddit,
        min_confidence=min_confidence,
        reject_for_hire_posts=reject_for_hire_posts,
        reject_reason_counts=reject_reason_counts,
        entry=entry,
    )
    if parse_error:
        error_messages.append(parse_error)
    else:
        _tick(heartbeat_callback)
    return parsed_rows, low_conf_sub


def _run_reddit_rss_fallback(
    *,
    rss_url: str,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    heartbeat_callback: Callable[[], None] | None,
    subreddit: str,
    min_confidence: int,
    reject_for_hire_posts: bool,
    reject_reason_counts: dict[str, int],
    entry: dict[str, Any],
    error_messages: list[str],
) -> tuple[list[RawJob], int]:
    def _fetch_rss() -> str:
        return fetch_with_retries(
            rss_url,
            fetch_text,
            timeout_s,
            retries,
            backoff_s,
            heartbeat_callback=heartbeat_callback,
        )

    def _record_rss_error(exc: Exception) -> None:
        _append_adapter_error(error_messages, "RSS fetch error", exc)

    rss_text = run_recoverable_adapter_attempt(_fetch_rss, _record_rss_error)
    if rss_text is None:
        return [], 0
    _tick(heartbeat_callback)
    parsed_rows, low_conf_sub, parse_error = _parse_reddit_rss_result(
        rss_text,
        subreddit=subreddit,
        min_confidence=min_confidence,
        reject_for_hire_posts=reject_for_hire_posts,
        reject_reason_counts=reject_reason_counts,
    )
    if parse_error:
        error_messages.append(parse_error)
    else:
        entry["fetchedCount"] = len(parsed_rows) + int(low_conf_sub)
        _tick(heartbeat_callback)
    return parsed_rows, low_conf_sub


def _run_reddit(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    subreddits: list[str] | None = None,
    heartbeat_callback: Callable[[], None] | None = None,
) -> list[RawJob]:
    social_config = _as_dict(_SOCIAL_CONFIG)
    cfg = _as_dict(social_config.get("reddit"))
    if not _bool_value(social_config.get("enabled")) or not _bool_value(cfg.get("enabled"), True):
        set_source_diagnostics(
            "social_reddit", adapter="social", studio="reddit", details=[], partial_errors=[]
        )
        return []
    subs_source: object = subreddits if subreddits is not None else cfg.get("subreddits")
    subs = _text_items(subs_source)
    max_posts = max(1, _int_value(cfg.get("maxPostsPerSubreddit"), 50))
    min_conf = max(
        0, min(100, _int_value(social_config.get("minConfidence"), DEFAULT_SOCIAL_MIN_CONFIDENCE))
    )
    reject_for_hire = _bool_value(social_config.get("rejectForHirePosts"), True)

    # Enhanced Reddit-specific settings
    rss_fallback = _bool_value(cfg.get("rssFallback"), True)
    html_fallback = _bool_value(cfg.get("htmlFallback"), True)
    rate_limit_delay = _float_value(cfg.get("rateLimitDelay"), 2.0)
    details: list[dict[str, Any]] = []
    errors: list[str] = []
    jobs: list[RawJob] = []
    low_conf_total = 0

    for i, sub in enumerate(subs):
        source_name = f"reddit:r/{sub}"
        json_url = f"https://www.reddit.com/r/{quote(sub, safe='')}/new.json?limit={max_posts}"
        rss_url = f"https://www.reddit.com/r/{quote(sub, safe='')}/new.rss"
        entry = _social_subsource_entry(studio=f"reddit/{sub}", name=source_name)
        parsed_rows: list[RawJob] = []
        low_conf_sub = 0
        reject_reason_counts: dict[str, int] = {}
        error_messages: list[str] = []

        # Add delay between requests to avoid rate limiting
        if i > 0:
            time.sleep(rate_limit_delay)

        parsed_rows, low_conf_sub = _run_reddit_json_api(
            json_url=json_url,
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            heartbeat_callback=heartbeat_callback,
            subreddit=sub,
            min_confidence=min_conf,
            reject_for_hire_posts=reject_for_hire,
            reject_reason_counts=reject_reason_counts,
            entry=entry,
            error_messages=error_messages,
        )

        if not parsed_rows and rss_fallback:
            parsed_rows, low_conf_sub = _run_reddit_rss_fallback(
                rss_url=rss_url,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                heartbeat_callback=heartbeat_callback,
                subreddit=sub,
                min_confidence=min_conf,
                reject_for_hire_posts=reject_for_hire,
                reject_reason_counts=reject_reason_counts,
                entry=entry,
                error_messages=error_messages,
            )

        if not parsed_rows and html_fallback:
            parsed_rows, low_conf_sub, html_parsed = _run_reddit_html_fallback(
                subreddit=sub,
                fetch_text=fetch_text,
                timeout_s=timeout_s,
                retries=retries,
                backoff_s=backoff_s,
                heartbeat_callback=heartbeat_callback,
                min_confidence=min_conf,
                reject_for_hire_posts=reject_for_hire,
                reject_reason_counts=reject_reason_counts,
                error_messages=error_messages,
                tick=lambda: _tick(heartbeat_callback),
            )
            if html_parsed:
                entry["fetchedCount"] = len(parsed_rows) + int(low_conf_sub)
                _tick(heartbeat_callback)

        _finish_social_plugin_entry(
            entry,
            parsed_rows=parsed_rows,
            reject_reason_counts=reject_reason_counts,
            error_messages=error_messages,
            error_prefix=f"reddit:{sub}",
            errors=errors,
        )
        low_conf_total += int(low_conf_sub)
        jobs.extend(parsed_rows)
        details.append(entry)

    return _finish_social_plugin_source(
        "social_reddit",
        studio="reddit",
        details=details,
        errors=errors,
        jobs=jobs,
        low_confidence_dropped=low_conf_total,
    )


def _run_x(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    from src.jobs.adapters import social as social_adapter

    return social_adapter.run_social_x_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        social_config=dict(_SOCIAL_CONFIG),
    )


def _run_mastodon(
    *, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float
) -> list[RawJob]:
    from src.jobs.adapters import social as social_adapter

    return social_adapter.run_social_mastodon_source(
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
        social_config=dict(_SOCIAL_CONFIG),
    )


def ensure_registered(*, social_config: dict[str, Any]) -> None:
    global _REGISTERED
    global _SOCIAL_CONFIG
    _SOCIAL_CONFIG = {str(key): value for key, value in dict(social_config or {}).items()}
    if _REGISTERED:
        return
    _REGISTERED = True

    default_registry.register(
        SimpleAdapterPlugin(
            name="social_reddit",
            family="social",
            priority=10,
            can_handle_fn=lambda ctx: ctx.family == "social" and ctx.adapter_key == "social_reddit",
            run_fn=_run_reddit,
        )
    )
    default_registry.register(
        SimpleAdapterPlugin(
            name="social_x",
            family="social",
            priority=10,
            can_handle_fn=lambda ctx: ctx.family == "social" and ctx.adapter_key == "social_x",
            run_fn=_run_x,
        )
    )
    default_registry.register(
        SimpleAdapterPlugin(
            name="social_mastodon",
            family="social",
            priority=10,
            can_handle_fn=lambda ctx: (
                ctx.family == "social" and ctx.adapter_key == "social_mastodon"
            ),
            run_fn=_run_mastodon,
        )
    )
    # Additional social plugins can be registered here incrementally (x, mastodon, etc).
