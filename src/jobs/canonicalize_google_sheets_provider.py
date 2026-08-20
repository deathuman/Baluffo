"""Google Sheets provider title lookup and link-status resolution.

AI boundary owns: provider feed title maps (greenhouse/lever/workable/ashby) and the two
resolver classes (provider title hydration, category-link status) used during canonicalization.
AI boundary implement in: this leaf for provider lookup/resolvers; category detection comes
from ``canonicalize_google_sheets_category.py``.
"""

from __future__ import annotations

import json
import threading
import time
import urllib.error
import urllib.request
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any
from urllib.parse import quote, unquote, urlparse

from src.jobs.adapters.parsers.provider_html import parse_ashby_jobs_from_html
from src.jobs.canonicalize_google_sheets_category import _is_google_sheets_category_label
from src.jobs.common.fetch import fetch_with_retries
from src.jobs.text_utils import (
    clean_text,
    normalize_url,
    sanitize_public_text,
)

_GOOGLE_SHEETS_TITLE_HYDRATION_STAT_KEYS = (
    "title_hydration_candidates",
    "title_hydration_feed_fetches",
    "title_hydration_cache_hits",
    "title_hydration_repaired",
    "title_hydration_missed",
    "title_hydration_errors",
    "title_hydration_ms",
)
_GOOGLE_SHEETS_CATEGORY_LINK_STAT_KEYS = (
    "category_link_status_candidates",
    "category_link_status_checked",
    "category_link_status_cache_hits",
    "category_link_status_stale_dropped",
    "category_link_status_errors",
    "category_link_status_ms",
)
_GOOGLE_SHEETS_GREENHOUSE_HOSTS = frozenset(
    {
        "boards.greenhouse.io",
        "job-boards.greenhouse.io",
        "job-boards.eu.greenhouse.io",
    }
)
_GOOGLE_SHEETS_LEVER_HOSTS = frozenset({"jobs.lever.co", "jobs.eu.lever.co"})
_GOOGLE_SHEETS_WORKABLE_HOSTS = frozenset({"apply.workable.com"})
_GOOGLE_SHEETS_ASHBY_HOSTS = frozenset({"jobs.ashbyhq.com"})


def _google_sheets_provider_title_target(
    job_link: str,
) -> tuple[str, str, str, tuple[str, ...]] | None:
    parsed = urlparse(clean_text(job_link) or "")
    host = parsed.netloc.lower().removeprefix("www.")
    parts = [unquote(part).strip() for part in parsed.path.split("/") if part.strip()]
    normalized_link = normalize_url(job_link)
    if host in _GOOGLE_SHEETS_GREENHOUSE_HOSTS and len(parts) >= 3 and parts[1].lower() == "jobs":
        board_slug = clean_text(parts[0])
        job_id = clean_text(parts[2])
        if not board_slug or not job_id:
            return None
        feed_url = (
            "https://boards-api.greenhouse.io/v1/boards/"
            f"{quote(board_slug, safe='')}/jobs?content=true"
        )
        return "greenhouse", board_slug, feed_url, (f"id:{job_id}", f"url:{normalized_link}")
    if host in _GOOGLE_SHEETS_LEVER_HOSTS and len(parts) >= 2:
        account = clean_text(parts[0])
        posting_id = clean_text(parts[-1])
        if not account or not posting_id or posting_id.lower() == "jobs":
            return None
        feed_url = f"https://api.lever.co/v0/postings/{quote(account, safe='')}?mode=json"
        return "lever", account, feed_url, (f"id:{posting_id}", f"url:{normalized_link}")
    if host in _GOOGLE_SHEETS_WORKABLE_HOSTS and len(parts) >= 3 and parts[1].lower() == "j":
        account = clean_text(parts[0])
        shortcode = clean_text(parts[2])
        if not account or not shortcode:
            return None
        feed_url = (
            "https://apply.workable.com/api/v1/widget/accounts/"
            f"{quote(account, safe='')}?details=true"
        )
        return "workable", account, feed_url, (f"id:{shortcode}", f"url:{normalized_link}")
    if host in _GOOGLE_SHEETS_ASHBY_HOSTS and len(parts) >= 2:
        board = clean_text(parts[0])
        posting_id = clean_text(parts[-1])
        if not board or not posting_id or posting_id.lower() == "jobs":
            return None
        feed_url = f"https://jobs.ashbyhq.com/{quote(board, safe='')}"
        return "ashby", board, feed_url, (f"id:{posting_id}", f"url:{normalized_link}")
    return None


def _google_sheets_provider_title_lookup_keys(provider: str, row: dict[str, Any]) -> set[str]:
    keys: set[str] = set()
    for value in (
        row.get("id"),
        row.get("internal_job_id"),
        row.get("requisitionCode"),
        row.get("shortcode"),
    ):
        text = clean_text(value)
        if text:
            keys.add(f"id:{text}")
    urls: tuple[Any, ...]
    if provider == "greenhouse":
        urls = (row.get("absolute_url"), row.get("url"))
    elif provider == "workable":
        urls = (row.get("url"), row.get("shortlink"))
    else:
        urls = (row.get("hostedUrl"), row.get("applyUrl"), row.get("url"))
    for value in urls:
        normalized = normalize_url(value)
        if normalized:
            keys.add(f"url:{normalized}")
    return keys


def _google_sheets_provider_title_from_row(provider: str, row: dict[str, Any]) -> str:
    if provider == "greenhouse":
        return sanitize_public_text(row.get("title") or row.get("text"))
    return sanitize_public_text(row.get("text") or row.get("title"))


def _google_sheets_provider_title_map(provider: str, payload: Any) -> dict[str, str]:
    if provider == "greenhouse":
        rows = payload.get("jobs") if isinstance(payload, dict) else None
    elif provider == "workable":
        rows = payload.get("jobs") if isinstance(payload, dict) else None
    else:
        rows = payload if isinstance(payload, list) else None
    if not isinstance(rows, list):
        return {}
    title_by_key: dict[str, str] = {}
    for row_value in rows:
        if not isinstance(row_value, dict):
            continue
        title = _google_sheets_provider_title_from_row(provider, row_value)
        if not title:
            continue
        for key in _google_sheets_provider_title_lookup_keys(provider, row_value):
            title_by_key.setdefault(key, title)
    return title_by_key


def _google_sheets_ashby_title_map(feed_url: str, html_text: str) -> dict[str, str]:
    rows = parse_ashby_jobs_from_html(html_text, feed_url, "")
    title_by_key: dict[str, str] = {}
    for row in rows:
        title = sanitize_public_text(row.get("title"))
        if not title:
            continue
        source_job_id = clean_text(row.get("sourceJobId"))
        if source_job_id.startswith("ashby:"):
            title_by_key.setdefault(f"id:{source_job_id.removeprefix('ashby:')}", title)
        normalized = normalize_url(row.get("jobLink"))
        if normalized:
            title_by_key.setdefault(f"url:{normalized}", title)
    return title_by_key


class GoogleSheetsProviderTitleResolver:
    """Per-run Google Sheets title resolver backed by provider JSON feeds."""

    def __init__(
        self,
        *,
        fetch_text: Callable[[str, int], str],
        timeout_s: int,
        retries: int,
        backoff_s: float,
    ) -> None:
        self._fetch_text = fetch_text
        self._timeout_s = max(1, int(timeout_s or 1))
        self._retries = max(0, int(retries or 0))
        self._backoff_s = max(0.0, float(backoff_s or 0.0))
        self._cache: dict[tuple[str, str], dict[str, str]] = {}
        self._row_feed_uses: set[tuple[str, str]] = set()
        self._stats: Counter[str] = Counter()
        self._lock = threading.Lock()

    def supports(self, job_link: str) -> bool:
        return _google_sheets_provider_title_target(job_link) is not None

    def prefetch(
        self,
        job_links: Sequence[str],
        *,
        concurrency: int = 1,
        progress_callback: Callable[..., Any] | None = None,
    ) -> None:
        targets = [
            target
            for link in job_links
            if (target := _google_sheets_provider_title_target(link)) is not None
        ]
        feed_targets: dict[tuple[str, str], tuple[str, str, str, tuple[str, ...]]] = {
            (provider, feed_key): target
            for target in targets
            for provider, feed_key, _feed_url, _lookup_keys in (target,)
        }
        pending = [target for key, target in feed_targets.items() if key not in self._cache]
        if not pending:
            return
        max_workers = max(1, min(int(concurrency or 1), len(pending)))

        def emit_title_progress(completed: int, *, force: bool = False) -> None:
            if progress_callback is None:
                return
            if not force and completed < len(pending):
                return
            stats = self.snapshot_stats()
            progress_callback(
                phase_key="hydrating_sheet_titles",
                phase_label="Hydrating sheet titles",
                counts={
                    "titleHydrationCandidates": len(targets),
                    "titleHydrationFeedsPending": len(pending),
                    "titleHydrationFeedsCompleted": completed,
                    "titleHydrationFeedFetches": int(
                        stats.get("title_hydration_feed_fetches") or 0
                    ),
                    "titleHydrationErrors": int(stats.get("title_hydration_errors") or 0),
                },
                message=(
                    "Hydrating Google Sheets provider titles: "
                    f"{completed} of {len(pending)} feeds checked."
                ),
            )

        emit_title_progress(0, force=True)
        if max_workers <= 1:
            for completed, (provider, feed_key, feed_url, _lookup_keys) in enumerate(
                pending, start=1
            ):
                self._ensure_feed(provider, feed_key, feed_url)
                emit_title_progress(completed)
            return
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._ensure_feed, provider, feed_key, feed_url)
                for provider, feed_key, feed_url, _lookup_keys in pending
            ]
            for completed, future in enumerate(as_completed(futures), start=1):
                future.result()
                emit_title_progress(completed)

    def resolve_title(self, job_link: str) -> str:
        target = _google_sheets_provider_title_target(job_link)
        if target is None:
            return ""
        provider, feed_key, feed_url, lookup_keys = target
        cache_key = (provider, feed_key)
        with self._lock:
            self._stats["title_hydration_candidates"] += 1
            if cache_key in self._row_feed_uses:
                self._stats["title_hydration_cache_hits"] += 1
            else:
                self._row_feed_uses.add(cache_key)
        title_by_key = self._ensure_feed(provider, feed_key, feed_url)
        for lookup_key in lookup_keys:
            title = sanitize_public_text(title_by_key.get(lookup_key))
            if title and not _is_google_sheets_category_label(title):
                with self._lock:
                    self._stats["title_hydration_repaired"] += 1
                return title
        with self._lock:
            self._stats["title_hydration_missed"] += 1
        return ""

    def snapshot_stats(self) -> dict[str, int]:
        with self._lock:
            return {
                key: int(self._stats.get(key, 0))
                for key in _GOOGLE_SHEETS_TITLE_HYDRATION_STAT_KEYS
            }

    def _ensure_feed(self, provider: str, feed_key: str, feed_url: str) -> dict[str, str]:
        cache_key = (provider, feed_key)
        with self._lock:
            cached = self._cache.get(cache_key)
            if cached is not None:
                return cached
        started = time.perf_counter()
        title_by_key: dict[str, str] = {}
        try:
            with self._lock:
                self._stats["title_hydration_feed_fetches"] += 1
            text = fetch_with_retries(
                feed_url,
                self._fetch_text,
                self._timeout_s,
                self._retries,
                self._backoff_s,
            )
            if provider == "ashby":
                title_by_key = _google_sheets_ashby_title_map(feed_url, text)
            else:
                title_by_key = _google_sheets_provider_title_map(provider, json.loads(text))
        except (RuntimeError, OSError, ValueError):
            with self._lock:
                self._stats["title_hydration_errors"] += 1
            title_by_key = {}
        finally:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            with self._lock:
                self._stats["title_hydration_ms"] += elapsed_ms
                self._cache[cache_key] = title_by_key
        return title_by_key


class GoogleSheetsCategoryLinkStatusResolver:
    """Per-run liveness resolver for suspicious Google Sheets category-title rows."""

    def __init__(
        self,
        *,
        timeout_s: int,
        fetch_status: Callable[[str, int], int] | None = None,
    ) -> None:
        self._timeout_s = max(1, int(timeout_s or 1))
        self._fetch_status = fetch_status or self._default_fetch_status
        self._cache: dict[str, int] = {}
        self._stats: Counter[str] = Counter()
        self._lock = threading.Lock()

    def prefetch(
        self,
        job_links: Sequence[str],
        *,
        concurrency: int = 1,
        progress_callback: Callable[..., Any] | None = None,
    ) -> None:
        normalized_links = [normalize_url(link) for link in job_links if normalize_url(link)]
        unique_links = list(dict.fromkeys(normalized_links))
        with self._lock:
            self._stats["category_link_status_candidates"] += len(normalized_links)
            pending = [link for link in unique_links if link not in self._cache]
            self._stats["category_link_status_cache_hits"] += len(unique_links) - len(pending)
        if not pending:
            return
        started = time.perf_counter()
        last_progress_at = started

        def emit_status_progress(completed: int, *, force: bool = False) -> None:
            nonlocal last_progress_at
            if progress_callback is None:
                return
            now = time.perf_counter()
            if not force and completed < len(pending) and now - last_progress_at < 2.0:
                return
            last_progress_at = now
            stats = self.snapshot_stats()
            progress_callback(
                phase_key="checking_category_links",
                phase_label="Checking category links",
                counts={
                    "categoryLinkStatusCandidates": len(normalized_links),
                    "categoryLinkStatusChecked": int(
                        stats.get("category_link_status_checked") or 0
                    ),
                    "categoryLinkStatusCompleted": completed,
                    "categoryLinkStatusStaleDropped": int(
                        stats.get("category_link_status_stale_dropped") or 0
                    ),
                    "categoryLinkStatusErrors": int(stats.get("category_link_status_errors") or 0),
                },
                message=(
                    "Checking repaired Google Sheets category links: "
                    f"{completed} of {len(pending)} checked."
                ),
            )

        emit_status_progress(0, force=True)
        max_workers = max(1, min(int(concurrency or 1), len(pending)))
        try:
            if max_workers <= 1:
                for completed, link in enumerate(pending, start=1):
                    self._ensure_status(link, track_ms=False)
                    emit_status_progress(completed)
                return
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = [
                    executor.submit(self._ensure_status, link, track_ms=False) for link in pending
                ]
                for completed, future in enumerate(as_completed(futures), start=1):
                    future.result()
                    emit_status_progress(completed)
        finally:
            elapsed_ms = int((time.perf_counter() - started) * 1000)
            with self._lock:
                self._stats["category_link_status_ms"] += elapsed_ms
            emit_status_progress(len(pending), force=True)

    def is_stale(self, job_link: str) -> bool:
        normalized_link = normalize_url(job_link)
        if not normalized_link:
            return False
        return self._ensure_status(normalized_link) in {404, 410}

    def note_stale_drop(self) -> None:
        with self._lock:
            self._stats["category_link_status_stale_dropped"] += 1

    def snapshot_stats(self) -> dict[str, int]:
        with self._lock:
            return {
                key: int(self._stats.get(key, 0)) for key in _GOOGLE_SHEETS_CATEGORY_LINK_STAT_KEYS
            }

    def _ensure_status(self, job_link: str, *, track_ms: bool = True) -> int:
        normalized_link = normalize_url(job_link)
        with self._lock:
            cached = self._cache.get(normalized_link)
            if cached is not None:
                return cached
        started = time.perf_counter() if track_ms else 0.0
        status = 0
        try:
            status = int(self._fetch_status(normalized_link, self._timeout_s) or 0)
            with self._lock:
                self._stats["category_link_status_checked"] += 1
        except (RuntimeError, OSError, ValueError):
            with self._lock:
                self._stats["category_link_status_errors"] += 1
            status = 0
        finally:
            with self._lock:
                if track_ms:
                    self._stats["category_link_status_ms"] += int(
                        (time.perf_counter() - started) * 1000
                    )
                self._cache[normalized_link] = status
        return status

    @staticmethod
    def _default_fetch_status(url: str, timeout_s: int) -> int:
        last_error: Exception | None = None
        for method in ("HEAD", "GET"):
            try:
                request = urllib.request.Request(
                    url,
                    headers={"User-Agent": "Mozilla/5.0"},
                    method=method,
                )
                with urllib.request.urlopen(request, timeout=max(1, int(timeout_s or 1))) as resp:
                    return int(getattr(resp, "status", 0) or 0)
            except urllib.error.HTTPError as exc:
                code = int(getattr(exc, "code", 0) or 0)
                if method == "HEAD" and code in {400, 403, 405, 429, 500, 501, 503}:
                    last_error = exc
                    continue
                return code
            except (OSError, ValueError) as exc:
                last_error = exc
                if method == "HEAD":
                    continue
                break
        _ = last_error
        return 0
