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

from src.jobs.adapters import static_listing, static_listing_rows, static_listing_traversal
from src.jobs.adapters.static_listing_flow import _finish_generic_source
from src.jobs.adapters.static_listing_state import StaticListingStageState
from src.jobs.adapters.static_listing_traversal import (
    StaticDetailTraversalPlan,
    _StaticDetailTraversalState,
)
from src.jobs.adapters.static_runtime import StaticRunDeps, StaticSourceContext
from src.jobs.adapters.static_runtime_support import (
    StaticHtmlFetcher,
    StaticSourceRuntimeConfig,
    build_static_entry_report,
    static_deadline_kind,
    update_source_detail_taxonomy,
)
from src.jobs.common.contracts_source_reports import normalize_source_report_row
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


def test_rows_flow_stops_verifying_once_the_source_budget_is_spent(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A spent budget must stop the row loop, not just the next detail call.

    Lanterns 2026-10-23: the source recorded `time budget exceeded (90s)` and
    still spent 588 s in candidate extraction, because the only budget check sat
    inside process_detail_link and fired on its *next* invocation. Each loop
    iteration must re-check before starting another verification.
    """
    import time

    ctx = _make_static_context()
    ctx.source_deadline = time.perf_counter() - 1.0

    def extract_rendered_card_jobs(*_args: Any, **_kwargs: Any) -> list[dict[str, Any]]:
        return [
            {
                "title": f"Senior Game Designer {index}",
                "jobLink": f"https://mundfish.com/careers/role-{index}",
                "_locationHint": "Kyiv",
            }
            for index in range(3)
        ]

    def process_detail_link(**_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("verification ran after the budget was spent")

    monkeypatch.setattr(static_listing, "extract_rendered_card_jobs", extract_rendered_card_jobs)
    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)

    static_listing_rows._append_rendered_card_rows(
        ctx,
        "<html></html>",
        "https://mundfish.com/en/careers",
        30,
        [],
        set(),
    )

    assert ctx.stats["detail_pages_visited"] == 0
    assert "time budget exceeded" in str(ctx.entry_report.get("error") or "")
    assert any("time_budget_exceeded" in warning for warning in ctx.warnings)


@pytest.mark.parametrize(
    "detail",
    ["request deadline exceeded (15s)", "total batch deadline exceeded (2s)"],
)
def test_hard_deadline_failure_is_recorded_as_time_budget_exceeded(detail: str) -> None:
    """Lanterns 2026-10-23: the 20 s arm filed the request deadline text while
    the 8 s arm filed ``time_budget_exceeded``, so the same truncated board
    carried a different error depending on which deadline fired first.
    """

    ctx = _make_static_context()
    ctx.record_static_fetch_failure(
        target_url="https://mundfish.com/en/careers",
        exc=detail,
    )

    recorded = " | ".join(
        [
            str(ctx.entry_report.get("error") or ""),
            *map(str, ctx.errors),
            *map(str, ctx.warnings),
        ]
    )
    assert "time_budget_exceeded" in recorded
    assert "deadline exceeded" not in recorded


def test_hard_deadline_does_not_queue_browser_fallback() -> None:
    ctx = _make_static_context()
    ctx.entry_report["browserFallbackRecommended"] = True
    ctx.record_static_fetch_failure(
        target_url="https://mundfish.com/en/careers",
        exc="request deadline exceeded (15s)",
    )

    assert ctx.entry_report["browserFallbackRecommended"] is False


@pytest.mark.parametrize(
    ("detail", "expected_kind"),
    [
        ("request deadline exceeded (15s)", "request_cap"),
        ("total batch deadline exceeded (2s)", "batch_deadline"),
    ],
)
def test_hard_deadline_kind_survives_the_real_source_recording_path(
    detail: str, expected_kind: str
) -> None:
    """Recording a listing failure must keep the clock, not just the token.

    The recorder normalizes the raw exception before finalization, which used
    to make both hard deadlines indistinguishable from the source budget.
    """

    ctx = _make_static_context()
    ctx.record_static_fetch_failure(
        target_url="https://mundfish.com/en/careers",
        exc=detail,
    )

    _finish_generic_source(ctx, StaticListingStageState())

    report = ctx.details[-1]
    assert report["error"] == "time_budget_exceeded"
    assert report["deadlineKind"] == expected_kind
    assert report["status"] == "error"


@pytest.mark.parametrize(
    ("detail", "expected_kind"),
    [
        ("request deadline exceeded (15s)", "request_cap"),
        ("total batch deadline exceeded (2s)", "batch_deadline"),
    ],
)
def test_partial_source_kept_jobs_stays_ok_when_a_hard_deadline_cuts_listing(
    detail: str, expected_kind: str
) -> None:
    """A listing deadline after a kept job records the cut without failing the source.

    The source-budget path deliberately keeps partial boards ``ok``; a request or
    batch deadline on the listing lane must not promote them to top-level failures.
    """

    ctx = _make_static_context()
    ctx.jobs.append(
        {
            "title": "Senior Game Designer",
            "company": "Mundfish",
            "jobLink": "https://mundfish.com/careers/senior-game-designer",
        }
    )
    ctx.record_static_fetch_failure(
        target_url="https://mundfish.com/en/careers",
        exc=detail,
    )

    _finish_generic_source(ctx, StaticListingStageState())

    report = ctx.details[-1]
    assert report["status"] == "ok"
    assert report["keptCount"] == 1
    assert report["classification"] == "timeout"
    assert report["error"] == "time_budget_exceeded"
    assert report["deadlineKind"] == expected_kind


@pytest.mark.parametrize(
    ("detail", "expected_kind"),
    [
        ("request deadline exceeded (15s)", "request_cap"),
        ("total batch deadline exceeded (2s)", "batch_deadline"),
    ],
)
def test_hard_deadline_kind_survives_the_real_detail_recording_path(
    detail: str, expected_kind: str
) -> None:
    """The detail recorder follows the same normalize-then-finalize flow."""

    ctx = _make_static_context()
    plan = StaticDetailTraversalPlan(
        page_url="https://mundfish.com/en/careers",
        detail_links=[],
        detail_concurrency=1,
        detail_retries=0,
        source_budget_s=30,
    )
    static_listing_traversal._record_detail_fetch_error(
        ctx,
        plan,
        _StaticDetailTraversalState(),
        detail="https://mundfish.com/careers/technical-game-designer",
        msg=detail,
    )

    _finish_generic_source(ctx, StaticListingStageState())

    report = ctx.details[-1]
    assert report["error"] == "time_budget_exceeded"
    assert report["deadlineKind"] == expected_kind
    assert report["status"] == "error"


@pytest.mark.parametrize(
    ("detail", "expected_kind", "expected_error"),
    [
        ("request deadline exceeded (15s)", "request_cap", "time_budget_exceeded"),
        ("total batch deadline exceeded (2s)", "batch_deadline", "time_budget_exceeded"),
        ("time budget exceeded (90s)", "source_budget", "time budget exceeded (90s)"),
    ],
)
def test_final_source_report_collapses_a_hard_deadline_error(
    detail: str, expected_kind: str, expected_error: str
) -> None:
    ctx = _make_static_context()
    ctx.entry_report["status"] = "error"
    ctx.entry_report["classification"] = "timeout"
    ctx.entry_report["error"] = detail

    _finish_generic_source(ctx, StaticListingStageState())

    assert ctx.details[-1]["error"] == expected_error
    assert ctx.details[-1]["deadlineKind"] == expected_kind


def test_static_deadline_kind_names_the_clock_that_fired() -> None:
    assert static_deadline_kind("request deadline exceeded (15s)") == "request_cap"
    assert static_deadline_kind("total batch deadline exceeded (2s)") == "batch_deadline"
    assert static_deadline_kind("time_budget_exceeded") == "source_budget"
    assert static_deadline_kind("time budget exceeded (90s)") == "source_budget"
    assert static_deadline_kind("failure budget exceeded (60s)") == ""
    assert static_deadline_kind("HTTP 500 for https://example.com") == ""


def test_source_budget_exhaustion_stamps_source_budget_kind() -> None:
    ctx = _make_static_context()
    ctx.stop_for_budget_exhaustion(target_url="https://mundfish.com/en/careers")

    _finish_generic_source(ctx, StaticListingStageState())

    assert "time budget exceeded" in ctx.details[-1]["error"]
    assert ctx.details[-1]["deadlineKind"] == "source_budget"


def test_deadline_kind_survives_normalization_and_source_projection() -> None:
    detail = {
        "name": "Slow Board",
        "adapter": "static",
        "status": "error",
        "keptCount": 0,
        "error": "time_budget_exceeded",
        "deadlineKind": "batch_deadline",
    }
    normalized_detail = normalize_jobs_fetch_report_detail_item(detail)
    assert normalized_detail["deadlineKind"] == "batch_deadline"

    report = {
        "name": "Slow Board",
        "adapter": "static",
        "status": "error",
        "keptCount": 0,
        "error": "time_budget_exceeded",
    }
    _apply_static_detail_evidence_to_report(report=report, detail_rows=[detail])
    assert report["deadlineKind"] == "batch_deadline"

    normalized_report = normalize_source_report_row(report)
    assert normalized_report["deadlineKind"] == "batch_deadline"


def test_healthy_source_report_has_no_deadline_kind() -> None:
    report = normalize_source_report_row(
        {"name": "Healthy", "adapter": "static", "status": "ok", "keptCount": 7}
    )
    assert "deadlineKind" not in report
