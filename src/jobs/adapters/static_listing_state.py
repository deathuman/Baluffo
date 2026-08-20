"""Static listing per-source state and context helpers.

AI boundary owns: provisional artifact row checks, detail candidate accumulation, stage-state
bookkeeping, detail-location resolution needs, one-man-studio cleanup, and early-completion
helpers shared by the flow and plugin leaves.
AI boundary implement in: this leaf for state/context helpers; shared primitives come from
``static_listing_common.py``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from threading import Lock
from typing import Any

from src.jobs.adapters.location_rules import classify_city_garbage
from src.jobs.adapters.static_detail_heuristics import (
    _is_one_man_studio_noise_city,
    source_detail_limit_for,
)
from src.jobs.adapters.static_listing_common import StaticDetailCandidate
from src.jobs.adapters.static_runtime_support import (
    update_source_detail_taxonomy,
)
from src.jobs.common.exact_category_titles import has_static_container_artifact_evidence
from src.jobs.text_utils import clean_text, normalize_url, sanitize_location_text

from .static_runtime import StaticSourceContext


def _is_provisional_static_artifact_row(row: dict[str, Any]) -> bool:
    return has_static_container_artifact_evidence(row.get("title"), row.get("jobLink"))


def _append_detail_candidate(
    detail_links: list[StaticDetailCandidate],
    detail_seen: set[str],
    seen_links: set[str],
    *,
    candidate_url: str,
    anchor_text: str,
    depth: int,
    parent_url: str,
) -> bool:
    absolute = normalize_url(candidate_url)
    if not absolute or absolute in detail_seen or absolute in seen_links:
        return False
    detail_seen.add(absolute)
    detail_links.append(
        StaticDetailCandidate(
            url=absolute,
            title=clean_text(anchor_text),
            depth=max(0, int(depth or 0)),
            parent_url=clean_text(parent_url),
        )
    )
    return True


def _source_detail_key(ctx: StaticSourceContext) -> str:
    if ctx.selected_source_count == 1:
        return ctx.run_deps.diagnostics_name
    return ctx.source_name


def _nested_detail_limit_for(ctx: StaticSourceContext, discovered_links: int) -> int:
    return source_detail_limit_for(
        _source_detail_key(ctx),
        source_state_rows=ctx.run_deps.source_state_rows,
        discovered_links=discovered_links,
        listing_jobs_found=0,
        low_yield_detail_cap=ctx.runtime_config.low_yield_detail_cap,
        very_low_yield_detail_cap=ctx.runtime_config.very_low_yield_detail_cap,
        uncapped_deep_static=ctx.runtime_config.uncapped_deep_static,
    )


@dataclass
class StaticListingStageState:
    browser_fallbacks: int = 0
    terminal_reason: str = ""
    batch_meta: dict[str, dict[str, Any]] = field(default_factory=dict)
    lock: Lock = field(default_factory=Lock)

    # pure helper
    def note_terminal_reason(self, reason: str, ctx: StaticSourceContext) -> None:
        clean_reason = clean_text(reason)
        if not clean_reason:
            return
        with self.lock:
            if not clean_text(self.terminal_reason):
                self.terminal_reason = clean_reason
                ctx.stats["listing_terminal_reason"] = clean_reason

    # pure helper
    def record_batch_meta(self, url: str, **payload: Any) -> None:
        with self.lock:
            self.batch_meta[url] = {
                **(self.batch_meta.get(url) or {}),
                **payload,
            }

    # pure helper
    def clear_batch_meta(self) -> None:
        with self.lock:
            self.batch_meta.clear()

    # pure helper
    def increment_browser_fallbacks(self) -> None:
        with self.lock:
            self.browser_fallbacks = int(self.browser_fallbacks or 0) + 1


# pure — classifier
def _needs_detail_location_resolution(
    row: dict[str, Any], link: str = "", location_hint: str = ""
) -> bool:
    del link
    city = clean_text(row.get("city"))
    if city:
        sanitized_city, city_reason = sanitize_location_text(city, field_name="city")
        if city_reason or classify_city_garbage(city) or not sanitized_city:
            return True
    hint = clean_text(location_hint)
    return not city and bool(hint)


# mutation — modifies in-place state
def _apply_one_man_studio_cleanup(
    ctx: StaticSourceContext, plugin_jobs: list[dict[str, Any]]
) -> None:
    if (
        "theonemanstudio" not in ctx.source_name.lower()
        and "one man studio" not in ctx.company.lower()
    ):
        return
    for row in plugin_jobs:
        if not isinstance(row, dict):
            continue
        row_city, _ = sanitize_location_text(row.get("city"), field_name="city")
        row_country, _ = sanitize_location_text(row.get("country"), field_name="country")
        if row_country == "Remote":
            row_country = ""
        if row_city == "Remote":
            row_city = ""
        if row_country in {"", "Unknown"} and _is_one_man_studio_noise_city(
            row_city,
            source_name=ctx.source_name,
            source=ctx.source,
        ):
            row_city = ""
        row["city"] = row_city
        row["country"] = row_country
        if row_city or row_country not in {"", "Unknown"}:
            row["locations"] = [
                {
                    "city": row_city,
                    "country": row_country if row_country != "Unknown" else "",
                }
            ]
            row["locationSummary"] = ", ".join(
                part for part in [row_city, row_country if row_country != "Unknown" else ""] if part
            )
        else:
            row["locations"] = []
            row["locationSummary"] = ""


# mutation — registry-facing artifact update
def _complete_source_without_generic_flow(ctx: StaticSourceContext) -> None:
    update_source_detail_taxonomy(ctx.entry_report)
    ctx.details.append(ctx.entry_report)


# mutation — finalizes stats on ctx
