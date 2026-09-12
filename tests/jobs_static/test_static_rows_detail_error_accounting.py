"""Rows-flow detail-failure accounting for the details_broken signal.

The generic rendered-rows flow re-verifies rendered cards against their detail
URLs; the first failing detail (Mundfish's HTTP 500 on /careers/…) used to abort
the source with all-zero stats, so the taxonomy's details_broken signal could
never see the listing-live/details-dead evidence (hold-tail repair plan, Wave-0
verification finding 2026-09-10).
"""

from __future__ import annotations

from typing import Any

import pytest

from src.jobs.adapters import static_listing, static_listing_rows
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
    update_source_detail_taxonomy,
)
from src.jobs.pipeline_source_results import _apply_static_detail_evidence_to_report
from src.shared.fetch_report_normalization import normalize_jobs_fetch_report_detail_item


def _make_static_context(*, pages: list[str] | None = None) -> StaticSourceContext:
    source_name = "Mundfish (Sheet)"
    source: dict[str, Any] = {
        "name": source_name,
        "company": source_name,
        "studio": "Mundfish",
        "pages": pages if pages is not None else ["https://mundfish.com/en/careers"],
        "id": "static:listing_url:https://mundfish.com/en/careers",
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


def test_rows_flow_detail_failure_counts_attempt_before_reraise(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    ctx = _make_static_context()

    def process_detail_link(**_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("HTTP 500 for https://mundfish.com/careers/technical-game-designer")

    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)

    with pytest.raises(RuntimeError, match="HTTP 500"):
        static_listing_rows._fetch_rendered_detail_rows(
            ctx,
            "https://mundfish.com/careers/technical-game-designer",
            "Technical Game Designer",
            30,
        )

    assert ctx.stats["detail_pages_visited"] == 1
    assert ctx.stats["detail_fetch_failed"] == 1
    assert ctx.errors == [
        "static:Mundfish (Sheet):https://mundfish.com/careers/technical-game-designer: "
        "HTTP 500 for https://mundfish.com/careers/technical-game-designer"
    ]


def test_rows_flow_detail_failure_is_not_swallowed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The accounting wraps the failure; the abort-on-first-failure semantics
    (and the caller's per-page error recording) must be unchanged."""
    ctx = _make_static_context()

    def extract_rendered_card_jobs(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        # _locationHint forces the detail-verification lane (needs_lookup), the
        # production Mundfish shape: location-incomplete rendered cards get
        # re-verified against their detail URLs.
        return [
            {
                "title": "Senior Producer",
                "jobLink": "https://mundfish.com/careers/technical-game-designer",
                "_locationHint": "Kyiv",
            }
        ]

    def process_detail_link(**_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("HTTP 500 for https://mundfish.com/careers/technical-game-designer")

    monkeypatch.setattr(static_listing, "extract_rendered_card_jobs", extract_rendered_card_jobs)
    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)

    with pytest.raises(RuntimeError, match="HTTP 500"):
        static_listing_rows._append_rendered_card_rows(
            ctx,
            "<html></html>",
            "https://mundfish.com/en/careers",
            30,
            [],
            set(),
        )


def test_rows_flow_non_fetch_detail_bug_does_not_count_as_detail_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Unexpected runtime bugs keep the exception ratchet: no fetch-failure
    accounting, no swallow."""
    ctx = _make_static_context()

    def process_detail_link(**_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError("unexpected rows-flow bug")

    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)

    with pytest.raises(RuntimeError, match="unexpected rows-flow bug"):
        static_listing_rows._fetch_rendered_detail_rows(
            ctx,
            "https://mundfish.com/careers/technical-game-designer",
            "Technical Game Designer",
            30,
        )

    assert ctx.stats["detail_pages_visited"] == 0
    assert ctx.stats["detail_fetch_failed"] == 0
    assert ctx.errors == []


def test_rows_flow_stamps_listing_jobs_evidence_before_row_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The visible-card count is stamped before per-row verification: a
    first-row detail failure aborts the loop, and the details_broken signal
    needs the listing-live evidence to classify the split."""
    ctx = _make_static_context()
    # Real role titles: enumeration titles ("Role 0") are filtered upstream as
    # parser noise and would never reach the detail-verification lane.
    # _locationHint forces that lane on (needs_lookup), the production shape.
    rendered_rows = [
        {
            "title": f"Senior Game Designer {index}",
            "jobLink": f"https://mundfish.com/careers/role-{index}",
            "_locationHint": "Kyiv",
        }
        for index in range(12)
    ]

    def extract_rendered_card_jobs(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return rendered_rows

    def process_detail_link(**kwargs: Any) -> dict[str, Any]:
        raise RuntimeError(f"HTTP 500 for {kwargs.get('detail')}")

    monkeypatch.setattr(static_listing, "extract_rendered_card_jobs", extract_rendered_card_jobs)
    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)

    with pytest.raises(RuntimeError, match="HTTP 500"):
        static_listing_rows._append_rendered_card_rows(
            ctx,
            "<html></html>",
            "https://mundfish.com/en/careers",
            30,
            [],
            set(),
        )

    assert ctx.entry_report["listingJobsFound"] == 12
    assert ctx.stats["detail_pages_visited"] == 1
    assert ctx.stats["detail_fetch_failed"] == 1


def _mundfish_abort_source_detail() -> dict[str, Any]:
    """Post-abort entry report shape from the 2026-09-10 targeted pass, plus the
    accounting this module adds (detail counters + listingJobsFound)."""
    return {
        "name": "Mundfish (Sheet)",
        "status": "error",
        "keptCount": 0,
        "error": (
            "static:Mundfish (Sheet):https://mundfish.com/en/careers: "
            "HTTP 500 for https://mundfish.com/careers/technical-game-designer; "
            "static:Mundfish (Sheet): no jobs extracted from source pages"
        ),
        "classification": "",
        "listingJobsFound": 12,
        "stats": {
            "candidate_links_found": 0,
            "detail_pages_visited": 1,
            "detail_fetch_failed": 1,
        },
    }


def test_mundfish_rows_abort_shape_reports_details_broken() -> None:
    """End-to-end through the real taxonomy recompute chain: the rows-flow
    abort shape (one attempted detail, mass failure, visible board) must stamp
    details_broken instead of the needs_review fall-through."""
    report = update_source_detail_taxonomy(_mundfish_abort_source_detail())
    assert report["failureBucket"] == "details_broken"


def test_rows_abort_without_listing_evidence_stays_js_required() -> None:
    """Without visible-card evidence the signal must not fire: one failed
    detail with an invisible board is not a listing-live/details-dead split,
    and the shape degrades to the pre-S1 behavior (the Sheet-registration
    text rule's js_required) — the evidence stamp is the separator."""
    detail = _mundfish_abort_source_detail()
    detail.pop("listingJobsFound")
    report = update_source_detail_taxonomy(detail)
    assert report["failureBucket"] == "js_required"


def test_finalize_projection_surfaces_evidence_on_source_report() -> None:
    """The source-level zero-kept classification runs on the report only, so
    the adapter-stamped detail evidence must be projected onto it — otherwise
    the source-level bucket stayed js_required while the detail-level bucket
    was honestly details_broken (observed in the 2026-09-10 targeted pass)."""
    report = {
        "name": "Mundfish (Sheet)",
        "adapter": "static",
        "status": "error",
        "keptCount": 0,
        "error": "no jobs extracted from source pages",
        "failureBucket": "js_required",
    }
    _apply_static_detail_evidence_to_report(
        report=report,
        detail_rows=[_mundfish_abort_source_detail()],
    )
    assert report["detailPagesVisited"] == 1
    assert report["detailFetchFailedCount"] == 1
    assert report["listingJobsFound"] == 12
    assert report["failureBucket"] == "details_broken"


def test_finalize_projection_is_inert_without_evidence() -> None:
    """No evidence, no new keys, no bucket rewrite — healthy static reports
    must not gain surface noise."""
    report = {"name": "Healthy", "adapter": "static", "status": "ok", "keptCount": 7}
    _apply_static_detail_evidence_to_report(
        report=report,
        detail_rows=[{"name": "Healthy", "stats": {"detail_pages_visited": 0}}],
    )
    assert set(report) == {"name", "adapter", "status", "keptCount"}
    non_static = {"name": "X", "adapter": "greenhouse", "status": "error"}
    _apply_static_detail_evidence_to_report(
        report=non_static,
        detail_rows=[_mundfish_abort_source_detail()],
    )
    assert set(non_static) == {"name", "adapter", "status"}


def test_detail_item_normalizer_preserves_rows_flow_evidence() -> None:
    """The persisted-report detail stats are whitelisted; the new counters and
    the listingJobsFound evidence must survive the round-trip."""
    item = normalize_jobs_fetch_report_detail_item(_mundfish_abort_source_detail())
    assert item["listingJobsFound"] == 12
    assert item["stats"]["detail_fetch_failed"] == 1
    assert item["stats"]["detail_pages_visited"] == 1
