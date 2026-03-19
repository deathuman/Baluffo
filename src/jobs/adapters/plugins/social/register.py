from __future__ import annotations

import json
from typing import Any, Callable, Dict, List
from urllib.parse import quote

from src.exceptions import AdapterValidationError
from src.jobs.adapters import social_parsers as _social_parsers
from src.jobs.adapters.plugins import default_registry
from src.jobs.adapters.plugins.types import SimpleAdapterPlugin
from src.jobs.models import RawJob
from src.jobs.common.config import SOURCE_DIAGNOSTICS
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.common.config import DEFAULT_SOCIAL_MIN_CONFIDENCE
from src.jobs.text_utils import clean_text


def set_source_diagnostics(
    source_name: str,
    *,
    adapter: str,
    studio: str,
    details: List[Dict[str, Any]] | None = None,
    partial_errors: List[str] | None = None,
) -> None:
    SOURCE_DIAGNOSTICS[source_name] = {
        "adapter": clean_text(adapter) or "unknown",
        "studio": clean_text(studio) or "multiple",
        "details": details or [],
        "partialErrors": partial_errors or [],
    }

_REGISTERED = False
_SOCIAL_CONFIG: Dict[str, Any] = {}


def _run_reddit(*, fetch_text: Callable[[str, int], str], timeout_s: int, retries: int, backoff_s: float) -> List[RawJob]:
    social_config = _SOCIAL_CONFIG if isinstance(_SOCIAL_CONFIG, dict) else {}
    cfg = social_config.get("reddit") if isinstance(social_config.get("reddit"), dict) else {}
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        set_source_diagnostics("social_reddit", adapter="social", studio="reddit", details=[], partial_errors=[])
        return []
    subs = [clean_text(item) for item in (cfg.get("subreddits") or []) if clean_text(item)]
    max_posts = max(1, int(cfg.get("maxPostsPerSubreddit") or 50))
    min_conf = max(0, min(100, int(social_config.get("minConfidence") or DEFAULT_SOCIAL_MIN_CONFIDENCE)))
    reject_for_hire = bool(social_config.get("rejectForHirePosts", True))
    details: List[Dict[str, Any]] = []
    errors: List[str] = []
    jobs: List[RawJob] = []
    low_conf_total = 0

    for sub in subs:
        source_name = f"reddit:r/{sub}"
        json_url = f"https://www.reddit.com/r/{quote(sub, safe='')}/new.json?limit={max_posts}"
        rss_url = f"https://www.reddit.com/r/{quote(sub, safe='')}/new.rss"
        entry = {"adapter": "social", "studio": f"reddit/{sub}", "name": source_name, "status": "ok", "fetchedCount": 0, "keptCount": 0, "error": ""}
        parsed_rows: List[RawJob] = []
        low_conf_sub = 0
        try:
            text = fetch_with_retries(json_url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed_rows, low_conf_sub = _social_parsers.parse_reddit_json_payload(
                payload,
                subreddit=sub,
                min_confidence=min_conf,
                reject_for_hire_posts=reject_for_hire,
            )
            entry["fetchedCount"] = len((((payload.get("data") or {}).get("children")) if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else []) or [])
        except Exception as exc:  # noqa: BLE001
            if bool(cfg.get("rssFallback", True)):
                try:
                    rss_text = fetch_with_retries(rss_url, fetch_text, timeout_s, retries, backoff_s)
                    parsed_rows, low_conf_sub = _social_parsers.parse_reddit_rss_payload(
                        rss_text,
                        subreddit=sub,
                        min_confidence=min_conf,
                        reject_for_hire_posts=reject_for_hire,
                    )
                    entry["fetchedCount"] = len(parsed_rows) + int(low_conf_sub)
                except Exception as rss_exc:  # noqa: BLE001
                    entry["status"] = "error"
                    entry["error"] = f"{exc}; {rss_exc}"
                    errors.append(f"reddit:{sub}: {exc}; {rss_exc}")
            else:
                entry["status"] = "error"
                entry["error"] = str(exc)
                errors.append(f"reddit:{sub}: {exc}")
        entry["keptCount"] = len(parsed_rows)
        low_conf_total += int(low_conf_sub)
        jobs.extend(parsed_rows)
        details.append(entry)

    set_source_diagnostics("social_reddit", adapter="social", studio="reddit", details=details, partial_errors=errors)
    SOURCE_DIAGNOSTICS["social_reddit"]["lowConfidenceDropped"] = int(low_conf_total)
    if jobs:
        return jobs
    if errors:
        raise AdapterValidationError.from_errors(errors)
    return []


def ensure_registered(*, social_config: Dict[str, Any]) -> None:
    global _REGISTERED
    global _SOCIAL_CONFIG
    _SOCIAL_CONFIG = dict(social_config or {})
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
    # Additional social plugins can be registered here incrementally (x, mastodon, etc).

