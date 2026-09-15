"""Provisional-artifact rows get the same detail-verification + card fallback
as non-provisional rendered rows.

Bandai trace remediation (2), 2026-09-14: ``_append_rendered_row`` demoted
provisional rows to detail candidates unconditionally — the card's posting
evidence died whenever the queued-candidate conversion stage never ran (the
bandai 38×0.65s budget death). The demotion now verifies inline against the
card's own detail URL first (probable-detail URLs) and falls back to the card
row on empty, matching the non-provisional lane's ``if row:`` arm.

Fixtures use a title that still flags under the narrowed classifier (the bandai
CJK shapes were un-collateralized by remediation 3) — a language-switch widget
shape landing on a job card is exactly what the demotion is for.
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
)

# A genuinely flagged title under the NARROWED classifier (the bandai CJK
# shapes stopped flagging when the language-switch fallback was fixed — that
# was the point): a language-switch widget shape misfiring onto a job card,
# riding a probable-detail path token (/jobs/).
PROVISIONAL_TITLE = "English ( Inglese )"
PROVISIONAL_LINK = "https://hrmos.co/pages/bandainamcostudios/jobs/050_0040_en010_0500_0010"
IMPROBABLE_LINK = "https://hrmos.co/pages/bandainamcostudios/about"


def _make_static_context() -> StaticSourceContext:
    source_name = "Bandai Namco Studios (GameDevMap)"
    source: dict[str, Any] = {
        "name": source_name,
        "company": source_name,
        "studio": "Bandai Namco Studios",
        "pages": ["https://www.bandainamcostudios.com/recruit/career"],
        "id": "static:listing_url:https://www.bandainamcostudios.com/recruit/career",
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
        default_path_tokens=["/job/", "/jobs/", "/jobdetail/"],
        default_query_keys=["job_id"],
    )
    return StaticSourceContext(
        run_deps=run_deps,
        runtime_config=runtime_config,
        html_fetcher=StaticHtmlFetcher(
            fetch_text=run_deps.fetch_text,
            timeout_s=run_deps.timeout_s,
            retries=run_deps.retries,
            backoff_s=0,
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


def _provisional_row(link: str | None = PROVISIONAL_LINK) -> dict[str, Any]:
    return {"title": PROVISIONAL_TITLE, "jobLink": link}


def _patch_detail_rows(
    monkeypatch: pytest.MonkeyPatch, rows_by_url: dict[str, list[dict[str, Any]]]
) -> list[str]:
    fetched: list[str] = []

    def process_detail_link(**kwargs: Any) -> dict[str, Any]:
        detail = kwargs["detail"]
        fetched.append(detail)
        return {
            "rows": rows_by_url.get(detail, []),
            "junkProvenanceRowsDropped": 0,
            "nestedDetailLinks": [],
            "parseEmpty": False,
            "fetchMs": 12,
            "parseMs": 1,
            "cacheHit": False,
            "rejectedClassification": "needs_review",
            "rejectedExample": detail,
        }

    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)
    return fetched


def test_provisional_row_detail_rows_win(monkeypatch: pytest.MonkeyPatch) -> None:
    """When the inline verification yields detail rows, they are emitted and
    the card is not queued."""
    ctx = _make_static_context()
    detail_rows = [
        {
            "title": PROVISIONAL_TITLE,
            "jobLink": PROVISIONAL_LINK,
            "city": "Tokyo",
        }
    ]
    fetched = _patch_detail_rows(monkeypatch, {PROVISIONAL_LINK: detail_rows})

    detail_links: list[Any] = []
    emitted, job_like, provisional = static_listing_rows._append_rendered_row(
        ctx,
        _provisional_row(),
        30,
        "https://www.bandainamcostudios.com/recruit/career",
        detail_links,
        set(),
    )

    assert fetched == [PROVISIONAL_LINK]
    assert emitted == 1
    assert job_like is True
    assert provisional == 0
    assert detail_links == []
    assert ctx.jobs and ctx.jobs[0]["jobLink"] == PROVISIONAL_LINK
    assert ctx.jobs[0]["source"].endswith("(GameDevMap)")
    assert PROVISIONAL_LINK in ctx.seen_links
    assert "provisional_card_fallback_emitted" not in ctx.stats


def test_provisional_row_empty_detail_falls_back_to_card_row(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Empty verification → the card row itself is emitted (fallback parity),
    stamped and counted."""
    ctx = _make_static_context()
    fetched = _patch_detail_rows(monkeypatch, {PROVISIONAL_LINK: []})

    detail_links: list[Any] = []
    emitted, job_like, provisional = static_listing_rows._append_rendered_row(
        ctx,
        _provisional_row(),
        30,
        "https://www.bandainamcostudios.com/recruit/career",
        detail_links,
        set(),
    )

    assert fetched == [PROVISIONAL_LINK]
    assert emitted == 1
    assert job_like is False
    assert provisional == 0
    assert detail_links == []
    assert ctx.jobs and ctx.jobs[0]["jobLink"] == PROVISIONAL_LINK
    assert ctx.jobs[0]["provisionalCardFallback"] is True
    assert ctx.stats["provisional_card_fallback_emitted"] == 1
    assert PROVISIONAL_LINK in ctx.seen_links


def test_provisional_row_improbable_url_still_queues_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Non-probable detail URLs keep the queue-only demotion: the candidate
    fan-out bounds them by cap instead of verifying inline."""
    ctx = _make_static_context()
    fetched = _patch_detail_rows(monkeypatch, {})

    detail_links: list[Any] = []
    detail_seen: set[str] = set()
    emitted, _job_like, provisional = static_listing_rows._append_rendered_row(
        ctx,
        _provisional_row(IMPROBABLE_LINK),
        30,
        "https://www.bandainamcostudios.com/recruit/career",
        detail_links,
        detail_seen,
    )

    assert fetched == []
    assert emitted == 0
    assert provisional == 1
    assert ctx.jobs == []
    assert [c.url for c in detail_links] == [IMPROBABLE_LINK]
    assert IMPROBABLE_LINK in detail_seen
    assert IMPROBABLE_LINK not in ctx.seen_links


def test_provisional_row_without_link_drops(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _make_static_context()
    fetched = _patch_detail_rows(monkeypatch, {})

    detail_links: list[Any] = []
    emitted, _job_like, provisional = static_listing_rows._append_rendered_row(
        ctx,
        _provisional_row(None),
        30,
        "https://www.bandainamcostudios.com/recruit/career",
        detail_links,
        set(),
    )

    assert fetched == []
    assert emitted == 0
    assert provisional == 1
    assert ctx.jobs == []
    assert detail_links == []


def test_provisional_row_non_job_like_title_still_verifies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """No job-like gate, deliberately: the real bandai shape is non-job-like
    under the same language-shape misclassification this fallback protects, and
    the non-provisional lane already verifies non-job-like titles first
    (``needs_lookup = not job_like or …``) — parity requires the same here."""
    ctx = _make_static_context()
    monkeypatch.setattr(static_listing_rows, "looks_like_job_title_candidate", lambda _t: False)
    fetched = _patch_detail_rows(monkeypatch, {PROVISIONAL_LINK: []})

    detail_links: list[Any] = []
    emitted, job_like, provisional = static_listing_rows._append_rendered_row(
        ctx,
        _provisional_row(),
        30,
        "https://www.bandainamcostudios.com/recruit/career",
        detail_links,
        set(),
    )

    assert fetched == [PROVISIONAL_LINK]
    assert emitted == 1
    assert job_like is False
    assert provisional == 0
    assert ctx.jobs and ctx.jobs[0]["provisionalCardFallback"] is True


def test_provisional_fallback_kill_switch_restores_queue_only(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """BALUFFO_PROVISIONAL_CARD_FALLBACK=0 restores the exact pre-fix
    behavior: queue-only demotion, no inline verification."""
    ctx = _make_static_context()
    monkeypatch.setattr(static_listing_rows, "PROVISIONAL_CARD_FALLBACK_ENABLED", False)
    fetched = _patch_detail_rows(monkeypatch, {})

    detail_links: list[Any] = []
    detail_seen: set[str] = set()
    emitted, _job_like, provisional = static_listing_rows._append_rendered_row(
        ctx,
        _provisional_row(),
        30,
        "https://www.bandainamcostudios.com/recruit/career",
        detail_links,
        detail_seen,
    )

    assert fetched == []
    assert emitted == 0
    assert provisional == 1
    assert [c.url for c in detail_links] == [PROVISIONAL_LINK]
    assert ctx.jobs == []


def test_provisional_fallback_fetch_failure_keeps_ratchet(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The inline verification inherits the rows-flow accounting semantics:
    an expected fetch failure is counted and re-raised (abort-on-first-failure
    unchanged)."""
    ctx = _make_static_context()

    def process_detail_link(**_kwargs: Any) -> dict[str, Any]:
        raise RuntimeError(f"HTTP 500 for {PROVISIONAL_LINK}")

    monkeypatch.setattr(static_listing, "process_detail_link", process_detail_link)

    detail_links: list[Any] = []
    with pytest.raises(RuntimeError, match="HTTP 500"):
        static_listing_rows._append_rendered_row(
            ctx,
            _provisional_row(),
            30,
            "https://www.bandainamcostudios.com/recruit/career",
            detail_links,
            set(),
        )

    assert ctx.stats["detail_pages_visited"] == 1
    assert ctx.stats["detail_fetch_failed"] == 1
    assert ctx.errors and "HTTP 500" in ctx.errors[0]
    assert detail_links == []
