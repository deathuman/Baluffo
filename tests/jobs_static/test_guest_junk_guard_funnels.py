"""Funnel integration for the origin-aware guest-junk guard.

Inbound: a non-LinkedIn static origin's rows flow and detail-candidate
accumulation must drop guest-view rows (Fusebox harvest shape) instead of
emitting rows / queueing detail fetches LinkedIn answers with HTTP 999.
Lifecycle: a failing source's stranded verification_overdue junk rows drain
through the failed-source shield with auditable evidence instead of being
preserved forever.
"""

from __future__ import annotations

from typing import Any

import pytest

from src.jobs.adapters import static_listing, static_listing_rows
from src.jobs.adapters.static_listing_state import _append_detail_candidate
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
)
from src.jobs.models import CanonicalJob
from src.jobs.state_lifecycle import apply_job_lifecycle_state

FUSEBOX_GUEST_ROW = (
    "https://www.linkedin.com/jobs/production-specialist-jobs?trk=organization_guest_linkster_link"
)
FUSEBOX_SOURCE_ID = "static_source::static:listing_url:https://fuseboxgames.com/careers/"


def _make_context(source_id: str = FUSEBOX_SOURCE_ID) -> StaticSourceContext:
    source_name = "Fusebox Games (Nazara) (GameDevMap)"
    source: dict[str, Any] = {
        "name": source_name,
        "company": source_name,
        "studio": "Fusebox Games",
        "pages": ["https://fuseboxgames.com/careers/"],
        "id": source_id,
    }
    run_deps = StaticRunDeps(
        fetch_text=lambda _url, _timeout: "",
        timeout_s=5,
        retries=0,
        backoff_s=0,
    )
    runtime_config = StaticSourceRuntimeConfig(
        static_profile="standard",
        static_detail_concurrency=1,
        static_source_time_budget_s=30,
        low_yield_detail_cap=10,
        very_low_yield_detail_cap=3,
        uncapped_deep_static=False,
        listing_only_hosts=[],
        default_path_tokens=[],
        default_query_keys=[],
    )
    return StaticSourceContext(
        run_deps=run_deps,
        runtime_config=runtime_config,
        html_fetcher=StaticHtmlFetcher(
            fetch_text=run_deps.fetch_text,
            timeout_s=run_deps.timeout_s,
            retries=run_deps.retries,
            backoff_s=run_deps.backoff_s,
        ),
        source=source,
        source_name=source_name,
        company=source_name,
        pages=list(source["pages"]),
        entry_report=build_static_entry_report(
            source=source,
            source_name=source_name,
            pages=list(source["pages"]),
            company=source_name,
        ),
        state_entry={},
        selected_source_count=1,
        jobs=[],
        warnings=[],
        errors=[],
        details=[],
    )


# --- inbound rows flow -------------------------------------------------------


def test_rows_flow_drops_guest_junk_row(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _make_context()

    def parse_jobpostings_from_html(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "title": "Production Specialist",
                "jobLink": FUSEBOX_GUEST_ROW,
                "company": "Fusebox Games",
            },
            {
                "title": "Real Engineer",
                "jobLink": "https://fuseboxgames.com/careers/real-engineer",
                "company": "Fusebox Games",
            },
        ]

    monkeypatch.setattr(static_listing, "parse_jobpostings_from_html", parse_jobpostings_from_html)
    emitted, provisional = static_listing_rows._append_parsed_listing_rows(
        ctx,
        "<html></html>",
        "https://fuseboxgames.com/careers/",
        [],
        set(),
    )
    assert emitted == 1
    assert provisional == 0
    assert [row["title"] for row in ctx.jobs] == ["Real Engineer"]
    assert ctx.stats["junk_provenance_rows_dropped"] == 1


def test_rows_flow_keeps_sanctioned_linkedin_origin_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_context(
        source_id="static_source::static:listing_url:https://www.linkedin.com/jobs/search/?f_C=1"
    )

    def parse_jobpostings_from_html(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "title": "Front of House Administrator",
                "jobLink": "https://bg.linkedin.com/jobs/view/front-of-house-at-sega-4455769546",
                "company": "SEGA Europe",
            }
        ]

    monkeypatch.setattr(static_listing, "parse_jobpostings_from_html", parse_jobpostings_from_html)
    emitted, _provisional = static_listing_rows._append_parsed_listing_rows(
        ctx,
        "<html></html>",
        "https://www.linkedin.com/jobs/search/?f_C=1",
        [],
        set(),
    )
    assert emitted == 1
    assert ctx.jobs and ctx.stats.get("junk_provenance_rows_dropped", 0) == 0


# --- inbound detail candidates ------------------------------------------------


def test_detail_candidate_flow_drops_guest_junk_url() -> None:
    ctx = _make_context()
    detail_links: list[Any] = []
    added = _append_detail_candidate(
        detail_links,
        set(),
        set(),
        candidate_url=FUSEBOX_GUEST_ROW,
        anchor_text="Production Specialist",
        depth=0,
        parent_url="https://fuseboxgames.com/careers/",
        ctx=ctx,
    )
    assert not added
    assert not detail_links
    assert ctx.stats["junk_provenance_candidates_dropped"] == 1


# --- detail-page revival funnel (2026-09-14 fix) -------------------------------


def test_detail_page_extraction_drops_guest_junk_rows() -> None:
    """The revival loop: re-verifying junk rows fetches their LinkedIn guest
    search pages; detail-page extraction used to re-emit the same junk as
    fresh rows, reviving what the rows flow dropped (Fusebox 09-13 pass:
    86 rows re-harvested, stock 41 -> 48)."""
    from src.jobs.adapters.static_detail_heuristics_entry import process_detail_html

    search_page_html = (
        "<html><body>"
        '<script type="application/ld+json">['
        '{"@type": "JobPosting", "title": "Production Specialist jobs", '
        '"url": "https://www.linkedin.com/jobs/production-specialist-jobs?trk=organization_guest", '
        '"hiringOrganization": {"name": "Fusebox Games"}},'
        '{"@type": "JobPosting", "title": "Executive Producer jobs", '
        '"url": "https://uk.linkedin.com/jobs/executive-producer-jobs?trk=organization_guest-browse_jobs", '
        '"hiringOrganization": {"name": "Fusebox Games"}}'
        "]</script>"
        "</body></html>"
    )
    result = process_detail_html(
        detail="https://www.linkedin.com/jobs/production-specialist-jobs?trk=organization_guest",
        detail_title="Production Specialist jobs",
        detail_html=search_page_html,
        fetch_ms=10,
        cache_hit=False,
        company="Fusebox Games",
        source_name="Fusebox Games (Nazara) (GameDevMap)",
        source={
            "name": "Fusebox Games (Nazara) (GameDevMap)",
            "id": FUSEBOX_SOURCE_ID,
            "pages": ["https://fuseboxgames.com/careers/"],
        },
        ignored_link_titles=set(),
    )
    assert result["rows"] == []
    assert result["junkProvenanceRowsDropped"] == 2


def test_detail_page_extraction_keeps_sanctioned_origin_rows() -> None:
    from src.jobs.adapters.static_detail_heuristics_entry import process_detail_html

    search_page_html = (
        "<html><body>"
        '<script type="application/ld+json">['
        '{"@type": "JobPosting", "title": "Front of House Administrator", '
        '"url": "https://bg.linkedin.com/jobs/view/front-of-house-at-sega-4455769546", '
        '"hiringOrganization": {"name": "SEGA Europe"}}'
        "]</script>"
        "</body></html>"
    )
    result = process_detail_html(
        detail="https://www.linkedin.com/jobs/search/?f_C=1&geoId=1",
        detail_title="Jobs",
        detail_html=search_page_html,
        fetch_ms=10,
        cache_hit=False,
        company="SEGA Europe",
        source_name="SEGA (LinkedIn)",
        source={
            "name": "SEGA (LinkedIn)",
            "id": "static_source::static:listing_url:https://www.linkedin.com/jobs/search/?f_C=1",
            "pages": ["https://www.linkedin.com/jobs/search/?f_C=1"],
        },
        ignored_link_titles=set(),
    )
    assert len(result["rows"]) == 1
    assert result["rows"][0]["jobLink"].startswith("https://bg.linkedin.com/jobs/view/")
    assert result["junkProvenanceRowsDropped"] == 0


# --- lifecycle drain ----------------------------------------------------------


def _lifecycle_row(source: str) -> dict[str, Any]:
    return {
        "status": "active",
        "firstSeenAt": "2026-09-06T10:46:47+00:00",
        "lastSeenAt": "2026-09-06T10:46:47+00:00",
        "title": "Production Specialist",
        "company": "Fusebox Games",
        "city": "London",
        "country": "United Kingdom",
        "jobLink": FUSEBOX_GUEST_ROW,
        "source": source,
        "sourceJobId": "fusebox-1",
        "postedAt": "2026-09-01T00:00:00+00:00",
        "availabilityId": "availability-fusebox-1",
        "availabilityStatus": "verification_overdue",
        "availabilityVerifiedAt": "2026-09-01T10:00:00+00:00",
        "consecutiveAvailabilityFailures": 13,
    }


def _apply(
    lifecycle_row: dict[str, Any],
) -> tuple[list[CanonicalJob], dict[str, Any], dict[str, Any], dict[str, int]]:
    rows, lifecycle_rows, _archive, summary = apply_job_lifecycle_state(
        deduped_rows=[],
        lifecycle_rows={"fusebox-1": lifecycle_row},
        finished_at="2026-09-13T15:00:00+00:00",
        allow_mark_missing=False,
        eligible_missing_sources=set(),
        source_evidence={
            "eligibleMissingSources": set(),
            "failedMissingSources": {FUSEBOX_SOURCE_ID},
            "skippedMissingSources": set(),
        },
        known_missing_evidence_sources={FUSEBOX_SOURCE_ID},
    )
    entry = lifecycle_rows["fusebox-1"]
    return rows, entry, lifecycle_rows, summary


def test_failing_source_guest_junk_row_drains() -> None:
    rows, entry, _lifecycle, summary = _apply(_lifecycle_row(FUSEBOX_SOURCE_ID))
    assert entry["availabilityStatus"] == "unavailable"
    assert entry["status"] == "likely_removed"
    assert entry["availabilityEvidence"]["kind"] == "guest_junk_provenance"
    assert entry["availabilityClosureOrigin"] == "guest_junk_provenance"
    assert entry["consecutiveAvailabilityFailures"] == 0
    assert rows == []  # drained rows are not projected into the feed
    assert summary["guestJunkDrained"] == 1
    assert summary["preservedBecauseSourceFailed"] == 0


def test_failing_source_non_junk_row_still_preserved() -> None:
    row = _lifecycle_row(FUSEBOX_SOURCE_ID)
    row["jobLink"] = "https://fuseboxgames.com/careers/real-role"
    _rows, entry, _lifecycle, summary = _apply(row)
    assert entry["availabilityStatus"] == "verification_overdue"
    assert entry["lifecycleEvent"] == "preserved"
    assert entry["lifecycleReason"] == "source_failed"
    assert summary["guestJunkDrained"] == 0
    assert summary["preservedBecauseSourceFailed"] == 1


def test_guest_junk_drain_idempotent_once_unavailable() -> None:
    row = _lifecycle_row(FUSEBOX_SOURCE_ID)
    row["availabilityStatus"] = "unavailable"
    row["status"] = "likely_removed"
    row["removedAt"] = "2026-09-13T11:51:46+00:00"
    row["availabilityClosureOrigin"] = "guest_junk_provenance"
    row["availabilityEvidence"] = {
        "kind": "guest_junk_provenance",
        "confidence": "definitive",
        "checkedAt": "2026-09-13T11:51:46+00:00",
        "source": FUSEBOX_SOURCE_ID,
    }
    _rows, entry, _lifecycle, summary = _apply(row)
    # Already unavailable: nothing is re-stamped and the drain counter stays
    # at zero — guestJunkDrainedCount must reflect fresh drains only, never
    # re-count Skybound-scale already-drained stock every failed pass
    # (pass #2 reported 1,541 while only 124 rows carried fresh markers).
    assert entry["availabilityStatus"] == "unavailable"
    assert entry["availabilityClosureOrigin"] == "guest_junk_provenance"
    assert entry["availabilityEvidence"]["checkedAt"] == "2026-09-13T11:51:46+00:00"
    assert summary["guestJunkDrained"] == 0


def test_registry_form_source_id_guards_inbound_rows() -> None:
    """The 2026-09-14 no-op regression: extraction ctx carries the registry
    spelling (``static:listing_url:…``, no ``static_source::`` prefix); the
    rows flow must still drop guest junk for those sources."""
    ctx = _make_context(source_id="static:listing_url:https://fuseboxgames.com/careers/")

    def parse_jobpostings_from_html(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "title": "Production Specialist",
                "jobLink": FUSEBOX_GUEST_ROW,
                "company": "Fusebox Games",
            }
        ]

    monkeypatch = pytest.MonkeyPatch()
    try:
        monkeypatch.setattr(
            static_listing, "parse_jobpostings_from_html", parse_jobpostings_from_html
        )
        emitted, _provisional = static_listing_rows._append_parsed_listing_rows(
            ctx,
            "<html></html>",
            "https://fuseboxgames.com/careers/",
            [],
            set(),
        )
    finally:
        monkeypatch.undo()
    assert emitted == 0
    assert ctx.stats["junk_provenance_rows_dropped"] == 1


def test_active_junk_class_summary_flags_preserved_active_stock() -> None:
    """The zero-active-stock invariant (2026-09-14 widget survey): a source
    that merely skips its pass leaves its active junk row in the store, and
    the post-pass summary must flag it — nonzero is the alert."""
    row = _lifecycle_row(FUSEBOX_SOURCE_ID)
    row["status"] = "active"
    _rows, _entry, _lifecycle, summary = apply_job_lifecycle_state(
        deduped_rows=[],
        lifecycle_rows={"fusebox-1": row},
        finished_at="2026-09-14T12:00:00+00:00",
        allow_mark_missing=False,
        eligible_missing_sources=set(),
        source_evidence={
            "eligibleMissingSources": set(),
            "failedMissingSources": set(),
            "skippedMissingSources": {FUSEBOX_SOURCE_ID},
        },
        known_missing_evidence_sources={FUSEBOX_SOURCE_ID},
    )
    assert summary["activeJunkClass"] == 1


def test_active_junk_class_summary_zero_once_drained() -> None:
    """When the failed-source shield drains the junk row, the monitor's
    count returns to zero in the same pass — the flag is self-clearing."""
    row = _lifecycle_row(FUSEBOX_SOURCE_ID)
    row["status"] = "active"
    _rows, entry, _lifecycle, summary = _apply(row)
    assert entry["availabilityClosureOrigin"] == "guest_junk_provenance"
    assert summary["guestJunkDrained"] == 1
    assert summary["activeJunkClass"] == 0
