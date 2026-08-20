"""Google Sheets redirect resolution and row canonicalization pipeline.

AI boundary owns: Google Sheets row redirect resolution, title-hydration candidate link
selection, category link-status filtering, and the sheet-level canonicalization entry point.
AI boundary implement in: this leaf for the sheet redirect pipeline; the canonical job
build and google_sheets repair stay in their own leaves.
AI boundary search before contracts: DATA_CONTRACT.md, CanonicalJob models, adapter parsers, and jobs quality tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused canonicalization/quality tests.
"""

from __future__ import annotations

import time
from collections import Counter
from collections.abc import Callable, Sequence
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

from src.jobs.adapters import community
from src.jobs.common.heuristics import (
    normalize_company_value,
)
from src.jobs.common.url import is_supported_redirect_url
from src.jobs.models import CanonicalJob, RawJob
from src.jobs.page_gating import looks_like_source_specific_static_noise_row
from src.jobs.text_utils import (
    clean_text,
    normalize_url,
    sanitize_public_text,
)
from src.jobs.transport import PooledRedirectResolver

from .canonicalize_google_sheets import (
    _GOOGLE_SHEETS_CATEGORY_LINK_STAT_KEYS,
    _GOOGLE_SHEETS_TITLE_HYDRATION_STAT_KEYS,
    GoogleSheetsCategoryLinkStatusResolver,
    GoogleSheetsProviderTitleResolver,
    _derive_google_sheets_title_from_url,
    _is_google_sheets_category_label,
    _is_google_sheets_repairable_broad_title,
    _looks_like_google_sheets_category_row_noise,
)
from .canonicalize_locations import canonicalize_job_with_reason

DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY = community.DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY
GOOGLE_SHEETS_CATEGORY_LINK_STATUS_CONCURRENCY = 32


def _google_sheet_redirect_candidates(raw_rows: Sequence[RawJob]) -> list[tuple[int, str]]:
    redirect_candidates: list[tuple[int, str]] = []
    for idx, raw in enumerate(raw_rows):
        normalized_link = normalize_url((raw or {}).get("jobLink"))
        if normalized_link and is_supported_redirect_url(normalized_link):
            redirect_candidates.append((idx, normalized_link))
    return redirect_candidates


def _resolve_google_sheet_redirects(
    *,
    redirect_candidates: list[tuple[int, str]],
    redirect_resolver: PooledRedirectResolver | None,
    redirect_concurrency: int,
    progress_callback: Callable[..., Any] | None = None,
) -> tuple[dict[int, str], dict[str, Any], dict[str, Any], int]:
    snapshot_stats = getattr(redirect_resolver, "snapshot_stats", None)
    resolver_stats_before = snapshot_stats() if callable(snapshot_stats) else {}
    redirect_started = time.perf_counter()
    resolve_fn = getattr(redirect_resolver, "resolve", None)
    resolved_links: dict[int, str] = {}
    if redirect_candidates and callable(resolve_fn):
        if redirect_concurrency <= 1 or len(redirect_candidates) <= 1:
            resolved_links = _resolve_redirects_serial(
                redirect_candidates=redirect_candidates,
                resolve_fn=resolve_fn,
                progress_callback=progress_callback,
            )
        else:
            resolved_links = _resolve_redirects_parallel(
                redirect_candidates=redirect_candidates,
                resolve_fn=resolve_fn,
                redirect_concurrency=redirect_concurrency,
                progress_callback=progress_callback,
            )
    redirect_resolve_ms = int((time.perf_counter() - redirect_started) * 1000)
    resolver_stats_after = snapshot_stats() if callable(snapshot_stats) else {}
    return resolved_links, resolver_stats_before, resolver_stats_after, redirect_resolve_ms


def _resolve_redirects_serial(
    *,
    redirect_candidates: list[tuple[int, str]],
    resolve_fn: Callable[[str], str],
    progress_callback: Callable[..., Any] | None = None,
) -> dict[int, str]:
    resolved_links: dict[int, str] = {}
    for completed, (row_idx, url) in enumerate(redirect_candidates, start=1):
        resolved_links[row_idx] = resolve_fn(url)
        _emit_redirect_progress(
            progress_callback,
            redirect_candidates=redirect_candidates,
            resolved_links=resolved_links,
            completed=completed,
        )
    return resolved_links


def _resolve_redirects_parallel(
    *,
    redirect_candidates: list[tuple[int, str]],
    resolve_fn: Callable[[str], str],
    redirect_concurrency: int,
    progress_callback: Callable[..., Any] | None = None,
) -> dict[int, str]:
    resolved_links: dict[int, str] = {}
    with ThreadPoolExecutor(
        max_workers=min(redirect_concurrency, len(redirect_candidates))
    ) as executor:
        future_map = {
            executor.submit(resolve_fn, url): row_idx for row_idx, url in redirect_candidates
        }
        for completed, future in enumerate(as_completed(future_map), start=1):
            resolved_links[future_map[future]] = future.result()
            _emit_redirect_progress(
                progress_callback,
                redirect_candidates=redirect_candidates,
                resolved_links=resolved_links,
                completed=completed,
            )
    return resolved_links


def _emit_redirect_progress(
    progress_callback: Callable[..., Any] | None,
    *,
    redirect_candidates: list[tuple[int, str]],
    resolved_links: dict[int, str],
    completed: int,
) -> None:
    if progress_callback is None:
        return
    total = len(redirect_candidates)
    if completed < total and completed % 100 != 0:
        return
    resolved_count = sum(
        1
        for idx, original in redirect_candidates
        if normalize_url(resolved_links.get(idx))
        and normalize_url(resolved_links.get(idx)) != normalize_url(original)
    )
    progress_callback(
        phase_key="resolving_sheet_redirects",
        phase_label="Resolving sheet redirects",
        counts={
            "redirectCandidates": total,
            "redirectCompleted": completed,
            "redirectResolved": int(resolved_count),
        },
        message=f"Resolving Google Sheets redirects: {completed} of {total} checked.",
    )


def _google_sheet_final_link(raw: RawJob, idx: int, resolved_links: dict[int, str]) -> str:
    return normalize_url(resolved_links.get(idx)) or normalize_url((raw or {}).get("jobLink"))


def _google_sheet_title_hydration_candidate_link(
    *,
    raw: RawJob,
    idx: int,
    source: str,
    resolved_links: dict[int, str],
    title_hydration_resolver: GoogleSheetsProviderTitleResolver,
) -> str:
    if not isinstance(raw, dict):
        return ""
    title = sanitize_public_text(raw.get("title"))
    company = normalize_company_value(sanitize_public_text(raw.get("company")))
    normalized_link = _google_sheet_final_link(raw, idx, resolved_links)
    if not title or not company or not normalized_link:
        return ""
    if (
        not clean_text(source).startswith("google_sheets")
        or (
            not _is_google_sheets_category_label(title)
            and not _is_google_sheets_repairable_broad_title(title)
        )
        or looks_like_source_specific_static_noise_row(
            title=title,
            job_link=normalized_link,
            source_name=source,
        )
        or _looks_like_google_sheets_category_row_noise(
            source=source,
            title=title,
            company=company,
            job_link=normalized_link,
        )
        or _derive_google_sheets_title_from_url(
            source=source,
            title=title,
            company=company,
            job_link=normalized_link,
        )
    ):
        return ""
    if not title_hydration_resolver.supports(normalized_link):
        return ""
    return normalized_link


def _google_sheet_title_hydration_candidate_links(
    *,
    raw_rows: Sequence[RawJob],
    source: str,
    resolved_links: dict[int, str],
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None,
) -> list[str]:
    if title_hydration_resolver is None:
        return []
    candidate_links: list[str] = []
    for idx, raw in enumerate(raw_rows):
        candidate_link = _google_sheet_title_hydration_candidate_link(
            raw=raw,
            idx=idx,
            source=source,
            resolved_links=resolved_links,
            title_hydration_resolver=title_hydration_resolver,
        )
        if candidate_link:
            candidate_links.append(candidate_link)
    return candidate_links


def _is_surviving_google_sheets_category_link_status_candidate(
    *,
    raw: RawJob,
    source: str,
    normalized: CanonicalJob,
) -> bool:
    if not clean_text(source).startswith("google_sheets"):
        return False
    if not isinstance(raw, dict):
        return False
    raw_title = sanitize_public_text(raw.get("title"))
    if not _is_google_sheets_category_label(raw_title):
        return False
    if _is_google_sheets_category_label(normalized.title):
        return False
    return bool(normalize_url(normalized.jobLink))


def _emit_google_sheets_progress(
    progress_callback: Callable[..., Any] | None,
    *,
    phase_key: str,
    phase_label: str,
    counts: dict[str, Any],
    message: str,
) -> None:
    if progress_callback is None:
        return
    progress_callback(
        phase_key=phase_key,
        phase_label=phase_label,
        counts=counts,
        message=message,
    )


def _filter_stale_google_sheet_category_links(
    *,
    canonical_batch: list[CanonicalJob],
    candidate_indexes: list[int],
    category_link_status_resolver: GoogleSheetsCategoryLinkStatusResolver | None,
    drop_reasons: Counter[str],
    redirect_concurrency: int,
    progress_callback: Callable[..., Any] | None,
) -> list[CanonicalJob]:
    if category_link_status_resolver is None or not candidate_indexes:
        return canonical_batch
    candidate_links = [
        canonical_batch[idx].jobLink
        for idx in candidate_indexes
        if 0 <= idx < len(canonical_batch) and normalize_url(canonical_batch[idx].jobLink)
    ]
    _emit_google_sheets_progress(
        progress_callback,
        phase_key="checking_category_links",
        phase_label="Checking category links",
        counts={
            "categoryLinkStatusCandidates": len(candidate_links),
            "categoryLinkStatusChecked": 0,
            "categoryLinkStatusStaleDropped": 0,
            "categoryLinkStatusErrors": 0,
        },
        message=f"Checking {len(candidate_links)} repaired Google Sheets category links.",
    )
    category_link_status_resolver.prefetch(
        candidate_links,
        concurrency=max(
            redirect_concurrency,
            min(GOOGLE_SHEETS_CATEGORY_LINK_STATUS_CONCURRENCY, len(candidate_links) or 1),
        ),
        progress_callback=progress_callback,
    )
    stale_indexes = {
        idx
        for idx in candidate_indexes
        if 0 <= idx < len(canonical_batch)
        and category_link_status_resolver.is_stale(canonical_batch[idx].jobLink)
    }
    if not stale_indexes:
        return canonical_batch
    for _idx in stale_indexes:
        category_link_status_resolver.note_stale_drop()
    drop_reasons["google_sheets_category_row"] += len(stale_indexes)
    stats = category_link_status_resolver.snapshot_stats()
    _emit_google_sheets_progress(
        progress_callback,
        phase_key="checking_category_links",
        phase_label="Checking category links",
        counts={
            "categoryLinkStatusCandidates": len(candidate_links),
            "categoryLinkStatusChecked": int(stats.get("category_link_status_checked") or 0),
            "categoryLinkStatusStaleDropped": int(
                stats.get("category_link_status_stale_dropped") or 0
            ),
            "categoryLinkStatusErrors": int(stats.get("category_link_status_errors") or 0),
        },
        message=(
            "Checked repaired Google Sheets category links; "
            f"dropped {len(stale_indexes)} stale rows."
        ),
    )
    return [row for idx, row in enumerate(canonical_batch) if idx not in stale_indexes]


def _canonicalize_google_sheet_rows_with_resolved_links(
    *,
    raw_rows: Sequence[RawJob],
    source: str,
    fetched_at: str,
    resolved_links: dict[int, str],
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None,
    category_link_status_resolver: GoogleSheetsCategoryLinkStatusResolver | None,
    redirect_concurrency: int,
    progress_callback: Callable[..., Any] | None,
) -> tuple[list[CanonicalJob], Counter[str], int]:
    canonical_started = time.perf_counter()
    last_progress_at = canonical_started
    canonical_batch: list[CanonicalJob] = []
    category_link_candidate_indexes: list[int] = []
    drop_reasons: Counter[str] = Counter()
    for idx, raw in enumerate(raw_rows):
        normalized, drop_reason = canonicalize_job_with_reason(
            raw,
            source=source,
            fetched_at=fetched_at,
            resolved_job_link=resolved_links.get(idx),
            title_hydration_resolver=title_hydration_resolver,
        )
        if normalized:
            if _is_surviving_google_sheets_category_link_status_candidate(
                raw=raw,
                source=source,
                normalized=normalized,
            ):
                category_link_candidate_indexes.append(len(canonical_batch))
            canonical_batch.append(normalized)
        elif drop_reason:
            drop_reasons[drop_reason] += 1
        now = time.perf_counter()
        if ((idx + 1) % 1000 == 0) or (now - last_progress_at >= 2.0):
            last_progress_at = now
            _emit_google_sheets_progress(
                progress_callback,
                phase_key="normalizing_rows",
                phase_label="Normalizing rows",
                counts={
                    "fetchedCount": len(raw_rows),
                    "normalizedCount": idx + 1,
                    "keptCount": len(canonical_batch),
                    "canonicalDropped": sum(drop_reasons.values()),
                },
                message=(
                    f"Normalizing Google Sheets rows: {idx + 1} of {len(raw_rows)} processed."
                ),
            )
    canonical_batch = _filter_stale_google_sheet_category_links(
        canonical_batch=canonical_batch,
        candidate_indexes=category_link_candidate_indexes,
        category_link_status_resolver=category_link_status_resolver,
        drop_reasons=drop_reasons,
        redirect_concurrency=redirect_concurrency,
        progress_callback=progress_callback,
    )
    canonicalize_ms = int((time.perf_counter() - canonical_started) * 1000)
    return canonical_batch, drop_reasons, canonicalize_ms


def _google_sheet_redirect_stats(
    *,
    redirect_candidates: list[tuple[int, str]],
    resolved_links: dict[int, str],
    resolver_stats_before: dict[str, Any],
    resolver_stats_after: dict[str, Any],
    redirect_resolve_ms: int,
    canonicalize_ms: int,
    title_hydration_stats: dict[str, int] | None = None,
    category_link_status_stats: dict[str, int] | None = None,
) -> dict[str, int]:
    redirect_resolved = sum(
        1
        for idx, original in redirect_candidates
        if normalize_url(resolved_links.get(idx))
        and normalize_url(resolved_links.get(idx)) != normalize_url(original)
    )
    stats = {
        "redirect_candidates": len(redirect_candidates),
        "redirect_resolved": int(redirect_resolved),
        "redirect_cache_hits": max(
            0,
            int(resolver_stats_after.get("cacheHits", 0))
            - int(resolver_stats_before.get("cacheHits", 0)),
        ),
        "redirect_resolve_ms": int(redirect_resolve_ms),
        "canonicalize_ms": int(canonicalize_ms),
    }
    for key in _GOOGLE_SHEETS_TITLE_HYDRATION_STAT_KEYS:
        stats[key] = int((title_hydration_stats or {}).get(key) or 0)
    for key in _GOOGLE_SHEETS_CATEGORY_LINK_STAT_KEYS:
        stats[key] = int((category_link_status_stats or {}).get(key) or 0)
    return stats


def canonicalize_google_sheets_rows(
    raw_rows: Sequence[RawJob],
    *,
    source: str,
    fetched_at: str,
    redirect_resolver: PooledRedirectResolver | None = None,
    redirect_concurrency: int = DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY,
    title_hydration_resolver: GoogleSheetsProviderTitleResolver | None = None,
    category_link_status_resolver: GoogleSheetsCategoryLinkStatusResolver | None = None,
    progress_callback: Callable[..., Any] | None = None,
) -> tuple[list[CanonicalJob], Counter, dict[str, int]]:
    redirect_concurrency = max(
        1, int(redirect_concurrency or DEFAULT_GOOGLE_SHEETS_REDIRECT_CONCURRENCY)
    )
    redirect_candidates = _google_sheet_redirect_candidates(raw_rows)
    _emit_google_sheets_progress(
        progress_callback,
        phase_key="resolving_sheet_redirects",
        phase_label="Resolving sheet redirects",
        counts={
            "redirectCandidates": len(redirect_candidates),
            "redirectCompleted": 0,
            "redirectResolved": 0,
        },
        message=f"Resolving {len(redirect_candidates)} Google Sheets redirect links.",
    )
    (
        resolved_links,
        resolver_stats_before,
        resolver_stats_after,
        redirect_resolve_ms,
    ) = _resolve_google_sheet_redirects(
        redirect_candidates=redirect_candidates,
        redirect_resolver=redirect_resolver,
        redirect_concurrency=redirect_concurrency,
        progress_callback=progress_callback,
    )
    if title_hydration_resolver is not None:
        title_hydration_resolver.prefetch(
            _google_sheet_title_hydration_candidate_links(
                raw_rows=raw_rows,
                source=source,
                resolved_links=resolved_links,
                title_hydration_resolver=title_hydration_resolver,
            ),
            concurrency=redirect_concurrency,
            progress_callback=progress_callback,
        )
    canonical_batch, drop_reasons, canonicalize_ms = (
        _canonicalize_google_sheet_rows_with_resolved_links(
            raw_rows=raw_rows,
            source=source,
            fetched_at=fetched_at,
            resolved_links=resolved_links,
            title_hydration_resolver=title_hydration_resolver,
            category_link_status_resolver=category_link_status_resolver,
            redirect_concurrency=redirect_concurrency,
            progress_callback=progress_callback,
        )
    )
    return (
        canonical_batch,
        drop_reasons,
        _google_sheet_redirect_stats(
            redirect_candidates=redirect_candidates,
            resolved_links=resolved_links,
            resolver_stats_before=resolver_stats_before,
            resolver_stats_after=resolver_stats_after,
            redirect_resolve_ms=redirect_resolve_ms,
            canonicalize_ms=canonicalize_ms,
            title_hydration_stats=(
                title_hydration_resolver.snapshot_stats()
                if title_hydration_resolver is not None
                else None
            ),
            category_link_status_stats=(
                category_link_status_resolver.snapshot_stats()
                if category_link_status_resolver is not None
                else None
            ),
        ),
    )
