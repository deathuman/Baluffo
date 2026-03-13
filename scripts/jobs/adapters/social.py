"""Social-source adapters extracted from the legacy fetcher."""

from __future__ import annotations

import json
import os
from typing import Any, Callable, Dict, List, Optional
from urllib.parse import quote, urlparse
from urllib.request import Request

from scripts.jobs import common
from scripts.jobs.adapters import _runtime
from scripts.jobs.models import RawJob


def _request_json_with_headers(url: str, *, timeout_s: int, headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    deps = _runtime.facade()
    req = Request(url=url, headers=headers or {})
    with deps.urlopen(req, timeout=timeout_s) as resp:
        raw = resp.read().decode(resp.headers.get_content_charset() or "utf-8", errors="replace")
        parsed = json.loads(raw) if raw else {}
        return parsed if isinstance(parsed, dict) else {}


def run_social_reddit_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: Dict[str, Any],
) -> List[RawJob]:
    deps = _runtime.facade()
    cfg = social_config.get("reddit") if isinstance(social_config.get("reddit"), dict) else {}
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        deps.set_source_diagnostics("social_reddit", adapter="social", studio="reddit", details=[], partial_errors=[])
        return []
    subs = [common.clean_text(item) for item in (cfg.get("subreddits") or []) if common.clean_text(item)]
    max_posts = max(1, int(cfg.get("maxPostsPerSubreddit") or 50))
    min_conf = max(0, min(100, int(social_config.get("minConfidence") or common.DEFAULT_SOCIAL_MIN_CONFIDENCE)))
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
            text = deps.fetch_with_retries(json_url, fetch_text, timeout_s, retries, backoff_s)
            payload = json.loads(text)
            parsed_rows, low_conf_sub = common.parse_reddit_json_payload(
                payload,
                subreddit=sub,
                min_confidence=min_conf,
                reject_for_hire_posts=reject_for_hire,
            )
            entry["fetchedCount"] = len((((payload.get("data") or {}).get("children")) if isinstance(payload, dict) and isinstance(payload.get("data"), dict) else []) or [])
        except Exception as exc:  # noqa: BLE001
            if bool(cfg.get("rssFallback", True)):
                try:
                    rss_text = deps.fetch_with_retries(rss_url, fetch_text, timeout_s, retries, backoff_s)
                    parsed_rows, low_conf_sub = common.parse_reddit_rss_payload(
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

    deps.set_source_diagnostics("social_reddit", adapter="social", studio="reddit", details=details, partial_errors=errors)
    deps.SOURCE_DIAGNOSTICS["social_reddit"]["lowConfidenceDropped"] = int(low_conf_total)
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_social_x_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: Dict[str, Any],
) -> List[RawJob]:
    deps = _runtime.facade()
    cfg = social_config.get("x") if isinstance(social_config.get("x"), dict) else {}
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        deps.set_source_diagnostics("social_x", adapter="social", studio="x", details=[], partial_errors=[])
        return []
    queries = [common.clean_text(item) for item in (cfg.get("queries") or []) if common.clean_text(item)]
    if not queries:
        return []
    max_posts = max(1, int(cfg.get("maxPostsPerQuery") or 25))
    min_conf = max(0, min(100, int(cfg.get("minConfidence") or social_config.get("minConfidence") or common.DEFAULT_SOCIAL_MIN_CONFIDENCE)))
    reject_for_hire = bool(social_config.get("rejectForHirePosts", True))
    api_cfg = cfg.get("api") if isinstance(cfg.get("api"), dict) else {}
    scraper_cfg = cfg.get("scraperFallback") if isinstance(cfg.get("scraperFallback"), dict) else {}
    rss_cfg = cfg.get("rssFallback") if isinstance(cfg.get("rssFallback"), dict) else {}
    bearer_env = common.clean_text(api_cfg.get("bearerTokenEnv") or "BALUFFO_X_BEARER_TOKEN")
    bearer = common.clean_text(os.environ.get(bearer_env))
    endpoint = common.clean_text(api_cfg.get("endpoint"))
    scraper_endpoint = common.clean_text(scraper_cfg.get("endpoint"))
    rss_instances = [common.clean_text(item).rstrip("/") for item in (rss_cfg.get("instances") or []) if common.clean_text(item)]

    details: List[Dict[str, Any]] = []
    errors: List[str] = []
    jobs: List[RawJob] = []
    low_conf_total = 0

    for query in queries:
        entry = {"adapter": "social", "studio": "x", "name": f"x:{query}", "status": "ok", "fetchedCount": 0, "keptCount": 0, "error": ""}
        parsed_rows: List[RawJob] = []
        low_conf_query = 0
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
                text = deps.fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
                payload = json.loads(text)
            elif bool(rss_cfg.get("enabled", True)) and rss_instances:
                rss_errors: List[str] = []
                rss_payload_text = ""
                for instance in rss_instances:
                    rss_url = f"{instance}/search/rss?f=tweets&q={quote(query, safe='')}"
                    try:
                        rss_payload_text = deps.fetch_with_retries(rss_url, fetch_text, timeout_s, retries, backoff_s)
                        break
                    except Exception as rss_exc:  # noqa: BLE001
                        rss_errors.append(f"{instance}: {rss_exc}")
                if not rss_payload_text:
                    raise RuntimeError("; ".join(rss_errors) if rss_errors else "x rss fallback failed")
                parsed_rows, low_conf_query = common.parse_x_rss_payload(
                    rss_payload_text,
                    query_label=query,
                    min_confidence=min_conf,
                    reject_for_hire_posts=reject_for_hire,
                )
                entry["fetchedCount"] = len(parsed_rows) + int(low_conf_query)
                entry["keptCount"] = len(parsed_rows)
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

            parsed_rows, low_conf_query = common.parse_x_payload(
                payload,
                query_label=query,
                min_confidence=min_conf,
                reject_for_hire_posts=reject_for_hire,
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
        low_conf_total += int(low_conf_query)
        jobs.extend(parsed_rows)
        details.append(entry)

    deps.set_source_diagnostics("social_x", adapter="social", studio="x", details=details, partial_errors=errors)
    deps.SOURCE_DIAGNOSTICS["social_x"]["lowConfidenceDropped"] = int(low_conf_total)
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []


def run_social_mastodon_source(
    *,
    fetch_text: Callable[[str, int], str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
    social_config: Dict[str, Any],
) -> List[RawJob]:
    deps = _runtime.facade()
    cfg = social_config.get("mastodon") if isinstance(social_config.get("mastodon"), dict) else {}
    if not bool(social_config.get("enabled")) or not bool(cfg.get("enabled", True)):
        deps.set_source_diagnostics("social_mastodon", adapter="social", studio="mastodon", details=[], partial_errors=[])
        return []
    instances = [common.clean_text(item).rstrip("/") for item in (cfg.get("instances") or []) if common.clean_text(item)]
    tags = [common.clean_text(item).lstrip("#") for item in (cfg.get("hashtags") or []) if common.clean_text(item)]
    max_posts = max(1, int(cfg.get("maxPostsPerTag") or 40))
    min_conf = max(0, min(100, int(social_config.get("minConfidence") or common.DEFAULT_SOCIAL_MIN_CONFIDENCE)))
    reject_for_hire = bool(social_config.get("rejectForHirePosts", True))
    details: List[Dict[str, Any]] = []
    errors: List[str] = []
    jobs: List[RawJob] = []
    low_conf_total = 0

    for instance in instances:
        for tag in tags:
            entry = {
                "adapter": "social",
                "studio": f"mastodon/{common.clean_text(urlparse(instance).netloc)}",
                "name": f"mastodon:{common.clean_text(urlparse(instance).netloc)}:#{tag}",
                "status": "ok",
                "fetchedCount": 0,
                "keptCount": 0,
                "error": "",
            }
            try:
                url = f"{instance}/api/v1/timelines/tag/{quote(tag, safe='')}?limit={max_posts}"
                text = deps.fetch_with_retries(url, fetch_text, timeout_s, retries, backoff_s)
                payload = json.loads(text)
                parsed_rows, low_conf_tag = common.parse_mastodon_payload(
                    payload,
                    instance=instance,
                    tag=tag,
                    min_confidence=min_conf,
                    reject_for_hire_posts=reject_for_hire,
                )
                entry["fetchedCount"] = len(payload) if isinstance(payload, list) else len(parsed_rows) + int(low_conf_tag)
                entry["keptCount"] = len(parsed_rows)
                low_conf_total += int(low_conf_tag)
                jobs.extend(parsed_rows)
            except Exception as exc:  # noqa: BLE001
                entry["status"] = "error"
                entry["error"] = str(exc)
                errors.append(f"mastodon:{instance}:#{tag}: {exc}")
            details.append(entry)

    deps.set_source_diagnostics("social_mastodon", adapter="social", studio="mastodon", details=details, partial_errors=errors)
    deps.SOURCE_DIAGNOSTICS["social_mastodon"]["lowConfidenceDropped"] = int(low_conf_total)
    if jobs:
        return jobs
    if errors:
        raise RuntimeError("; ".join(errors))
    return []

