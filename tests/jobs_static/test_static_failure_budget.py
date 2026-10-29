"""Per-source failure budget (2026-10-29).

The counterbalanced fetch block measured where a slow arm's wall goes: the same
522-532 sources failed in every arm, but 56-76 of them stalled past 60 s in the
slow arms against 12 in the fast one, and error seconds ran 5.8-16.2 ks. The
per-source *time* budget does not bound that -- pagination earns up to ``6 x base``
on discovery and it is never revoked, so a board that stops making progress can
sit at ~540 s (the capture contains 526 s / 409 s / 345 s stalls).

This budget is narrower on purpose: it bounds only time spent in *failed*
attempts, and only while the source has kept no job. The moment a board keeps a
job it returns to the normal path, which is what makes "no kept board is lost"
structural rather than a hope. A detail-page read is deliberately not an
exemption -- see ``source_has_kept_jobs``.
"""

from __future__ import annotations

import time
from dataclasses import replace

import pytest

from src.jobs.adapters.static_listing_traversal import _note_detail_batch_failure
from src.jobs.adapters.static_runtime_support import (
    STATIC_FAILURE_BUDGET_DEFAULT_S,
    build_static_source_runtime_config,
)
from tests.jobs_static.test_static_pagination_follow import (
    _BASE,
    _listing_html,
    _make_runner,
    _pagination_pages_html,
)

# --- config -------------------------------------------------------------------


def test_default_failure_budget_is_sixty_seconds(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BALUFFO_STATIC_FAILURE_BUDGET_S", raising=False)
    config = build_static_source_runtime_config(static_detail_concurrency=4)
    assert STATIC_FAILURE_BUDGET_DEFAULT_S == 60
    assert config.static_failure_budget_s == 60


def test_env_override_and_zero_disable(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BALUFFO_STATIC_FAILURE_BUDGET_S", "0")
    assert build_static_source_runtime_config(4).static_failure_budget_s == 0
    monkeypatch.setenv("BALUFFO_STATIC_FAILURE_BUDGET_S", "90")
    assert build_static_source_runtime_config(4).static_failure_budget_s == 90
    # Unparseable falls back to the default rather than raising mid-run.
    monkeypatch.setenv("BALUFFO_STATIC_FAILURE_BUDGET_S", "not-a-number")
    assert build_static_source_runtime_config(4).static_failure_budget_s == 60


# --- context arithmetic -------------------------------------------------------


def _ctx():
    _runner, ctx, _log = _make_runner({_BASE: _pagination_pages_html()})
    return ctx


def test_budget_accrues_and_trips_at_the_threshold() -> None:
    ctx = _ctx()
    ctx.record_failed_attempt(59.9)
    assert not ctx.failure_budget_exhausted()
    ctx.record_failed_attempt(0.1)
    assert ctx.failure_budget_exhausted()


def test_negative_seconds_do_not_credit_the_source() -> None:
    ctx = _ctx()
    ctx.record_failed_attempt(-30)
    assert ctx.failure_elapsed_s == 0.0


def test_a_kept_job_disables_the_budget() -> None:
    ctx = _ctx()
    ctx.record_failed_attempt(120)
    ctx.jobs.append({"title": "Engineer"})
    assert not ctx.failure_budget_exhausted()


def test_a_detail_page_read_does_not_exempt() -> None:
    # Only a kept job exempts. The capture has 24-27 error rows per slow arm that read a
    # detail page and still kept nothing; exempting those would protect no job and leave
    # ~40% of the failure tail in place.
    ctx = _ctx()
    ctx.record_failed_attempt(120)
    ctx.stats["detail_pages_visited"] = 4
    assert ctx.failure_budget_exhausted()


def test_zero_budget_never_trips(monkeypatch: pytest.MonkeyPatch) -> None:
    ctx = _ctx()
    ctx.runtime_config = replace(ctx.runtime_config, static_failure_budget_s=0)
    ctx.record_failed_attempt(10_000)
    assert not ctx.failure_budget_exhausted()


def test_stop_records_a_distinct_reason() -> None:
    ctx = _ctx()
    ctx.record_failed_attempt(60)
    assert ctx.failure_budget_exhausted()
    ctx.stop_for_failure_budget(target_url=_BASE)
    assert ctx.entry_report["status"] == "error"
    assert ctx.entry_report["classification"] == "timeout"
    assert ctx.entry_report["error"] == "failure budget exceeded (60s)"
    assert ctx.warnings[-1].endswith("failure_budget_exceeded")


# --- batch accounting ---------------------------------------------------------


def test_wholly_failed_batch_is_charged() -> None:
    runner, ctx, _log = _make_runner({_BASE: _pagination_pages_html()})
    started = time.perf_counter() - 0.5
    runner._note_batch_failure([{"ok": False}, {"ok": False}], started)
    assert ctx.failure_elapsed_s >= 0.5


def test_partial_success_is_free() -> None:
    # One ok row proves the board is live; a slow-but-live fanout must not be charged.
    runner, ctx, _log = _make_runner({_BASE: _pagination_pages_html()})
    started = time.perf_counter() - 0.5
    runner._note_batch_failure([{"ok": False}, {"ok": True}], started)
    assert ctx.failure_elapsed_s == 0.0


def test_detail_helper_charges_only_wholly_failed_batches() -> None:
    ctx = _ctx()
    started = time.perf_counter() - 0.5
    _note_detail_batch_failure(ctx, [{"ok": True}], started)
    assert ctx.failure_elapsed_s == 0.0
    _note_detail_batch_failure(ctx, [{"ok": False}], started)
    assert ctx.failure_elapsed_s >= 0.5


# --- runner wiring ------------------------------------------------------------


def test_runner_stops_before_following_pagination_when_budget_is_spent() -> None:
    """The stalled-board shape: pages discovered, nothing kept, clock burnt.

    Page 1 parses (so pagination is discovered) but the source has already spent
    its failure budget, so the runner must stop instead of fetching ``?page=2`` --
    which is the 540 s path the capture recorded.
    """
    pages_by_url = {
        _BASE: _pagination_pages_html(2),
        f"{_BASE}?page=2": _pagination_pages_html(),
    }
    runner, ctx, fetch_log = _make_runner(pages_by_url)
    ctx.record_failed_attempt(60)

    runner.run()

    assert fetch_log == [_BASE]
    assert ctx.entry_report["error"] == "failure budget exceeded (60s)"
    assert runner.pagination_discovered_count == 1
    assert runner.stop_source is True


def test_runner_does_not_charge_a_healthy_batch() -> None:
    pages_by_url = {
        _BASE: _listing_html(1, job_ids=["0001"], extra_pages=[2]),
        f"{_BASE}?page=2": _pagination_pages_html(),
        f"{_BASE}/00001": "<html><body>Engineer 1 detail</body></html>",
    }
    runner, ctx, fetch_log = _make_runner(pages_by_url)

    runner.run()

    assert f"{_BASE}?page=2" in fetch_log
    assert ctx.failure_elapsed_s == 0.0
    assert "failure budget" not in str(ctx.entry_report.get("error") or "")
