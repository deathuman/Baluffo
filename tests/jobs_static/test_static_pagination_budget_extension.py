"""Adaptive pagination budget (2026-09-15).

The per-source time budget predates pagination-follow: a 7-page board cannot
finish inside the pre-pagination 25s budget, so runs truncate and rely on the
lifecycle preserve shield. Discovered pagination pages now earn bounded extra
budget seconds (one base-budget unit per discovered page, capped by the same
page count the follow cap allows) via ``extend_source_deadline_for_pagination``.
"""

from __future__ import annotations

import time

import pytest

from src.jobs.adapters.static_runtime_support import (
    STATIC_PAGINATION_BUDGET_EXTENSION_MAX_PAGES,
    pagination_budget_extension_s,
    static_pagination_budget_extension_enabled,
)
from tests.jobs_static.test_static_pagination_follow import (
    _BASE,
    _listing_html,
    _make_runner,
    _pagination_pages_html,
)

# --- extension math ---------------------------------------------------------


def test_extension_math_per_page_and_cap() -> None:
    assert STATIC_PAGINATION_BUDGET_EXTENSION_MAX_PAGES == 6  # documented bound
    assert pagination_budget_extension_s(pages_discovered=0, base_budget_s=25) == 0
    assert pagination_budget_extension_s(pages_discovered=2, base_budget_s=25) == 50
    assert pagination_budget_extension_s(pages_discovered=6, base_budget_s=25) == 150
    # capped by the follow cap's page count, not by page count alone
    assert pagination_budget_extension_s(pages_discovered=9, base_budget_s=25) == 150


def test_extension_scales_with_operator_budget() -> None:
    assert pagination_budget_extension_s(pages_discovered=6, base_budget_s=300) == 1800
    assert pagination_budget_extension_s(pages_discovered=3, base_budget_s=10) == 30


def test_extension_kill_switch(monkeypatch: pytest.MonkeyPatch) -> None:
    assert static_pagination_budget_extension_enabled()  # default on
    monkeypatch.setenv("BALUFFO_STATIC_PAGINATION_BUDGET_EXTENSION", "0")
    assert not static_pagination_budget_extension_enabled()
    assert pagination_budget_extension_s(pages_discovered=6, base_budget_s=25) == 0
    monkeypatch.delenv("BALUFFO_STATIC_PAGINATION_BUDGET_EXTENSION")
    assert static_pagination_budget_extension_enabled()


# --- ctx deadline semantics -------------------------------------------------


def _make_ctx(base_budget_s: int = 30):
    _runner, ctx, _log = _make_runner({_BASE: _pagination_pages_html()})
    assert ctx.runtime_config.static_source_time_budget_s == base_budget_s
    return ctx


def test_sync_lowens_floor_and_deadline() -> None:
    ctx = _make_ctx()
    started = ctx.source_started
    ctx.sync_source_deadline(15)
    assert ctx.pagination_budget_floor_s == 15
    assert ctx.source_deadline == pytest.approx(started + 15, abs=1e-6)


def test_extension_raises_deadline_beyond_floor() -> None:
    ctx = _make_ctx()
    started = ctx.source_started
    ctx.sync_source_deadline(15)
    earned = ctx.extend_source_deadline_for_pagination(pages_discovered=3)
    assert earned == 90  # 3 pages x base 30
    assert ctx.effective_source_budget_s() == 105  # floor 15 + extension 90
    assert ctx.source_deadline == pytest.approx(started + 105, abs=1e-6)


def test_semantics_byte_equivalent_without_extension() -> None:
    ctx = _make_ctx()
    started = ctx.source_started
    for budget in (30, 15, 20, 15):
        ctx.sync_source_deadline(budget)
    # legacy: min over build_static_source_deadline calls
    assert ctx.pagination_budget_floor_s == 15
    assert ctx.source_deadline == pytest.approx(started + 15, abs=1e-6)
    assert ctx.effective_source_budget_s() == 15


def test_extension_is_idempotent_for_same_page_count() -> None:
    ctx = _make_ctx()
    ctx.extend_source_deadline_for_pagination(pages_discovered=2)
    first = ctx.effective_source_budget_s()
    ctx.extend_source_deadline_for_pagination(pages_discovered=2)
    assert ctx.effective_source_budget_s() == first


# --- runner wiring ----------------------------------------------------------


def test_runner_earns_extension_when_discovering_pages() -> None:
    pages_by_url = {
        _BASE: _listing_html(1, job_ids=["0001"], extra_pages=[2, 3]),
        f"{_BASE}?page=2": _pagination_pages_html(),
        f"{_BASE}?page=3": _pagination_pages_html(),
        f"{_BASE}/00001": "<html><body>Engineer 1 detail</body></html>",
    }
    runner, ctx, fetch_log = _make_runner(pages_by_url)
    started = ctx.source_started

    runner.run()

    assert f"{_BASE}?page=2" in fetch_log
    assert f"{_BASE}?page=3" in fetch_log
    assert runner.pagination_discovered_count == 2
    assert ctx.pagination_budget_extension_s == 60  # 2 pages x base 30
    assert ctx.effective_source_budget_s() == 90
    assert ctx.source_deadline >= started + 89


def test_runner_extension_capped_at_follow_cap() -> None:
    pages_by_url = {
        _BASE: _listing_html(1, job_ids=["0001"], extra_pages=[2, 3, 4, 5, 6, 7, 8]),
    }
    for n in range(2, 8):
        pages_by_url[f"{_BASE}?page={n}"] = _pagination_pages_html()
    pages_by_url[f"{_BASE}/00001"] = "<html><body>Engineer 1 detail</body></html>"
    runner, ctx, _fetch_log = _make_runner(pages_by_url)

    runner.run()

    assert runner.pagination_discovered_count == 6
    assert ctx.pagination_budget_extension_s == 180  # 6 pages x base 30 (the cap)
    assert ctx.effective_source_budget_s() == 210


def test_runner_no_extension_without_pagination() -> None:
    pages_by_url = {_BASE: _pagination_pages_html()}
    runner, ctx, fetch_log = _make_runner(pages_by_url)
    started = ctx.source_started

    runner.run()

    assert runner.pagination_discovered_count == 0
    assert ctx.pagination_budget_extension_s == 0
    assert ctx.source_deadline == pytest.approx(started + 30, abs=1e-6)
    assert fetch_log == [_BASE]


def test_runner_kill_switch_disables_extension(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("BALUFFO_STATIC_PAGINATION_BUDGET_EXTENSION", "0")
    pages_by_url = {
        _BASE: _listing_html(1, job_ids=["0001"], extra_pages=[2, 3]),
        f"{_BASE}?page=2": _pagination_pages_html(),
        f"{_BASE}?page=3": _pagination_pages_html(),
        f"{_BASE}/00001": "<html><body>Engineer 1 detail</body></html>",
    }
    runner, ctx, _fetch_log = _make_runner(pages_by_url)
    started = ctx.source_started

    runner.run()

    assert runner.pagination_discovered_count == 2  # discovery still happens
    assert ctx.pagination_budget_extension_s == 0  # but no extra budget
    assert ctx.source_deadline == pytest.approx(started + 30, abs=1e-6)


def test_extension_keeps_deadline_monotonic_relative_to_wall_clock() -> None:
    # The rebuilt deadline is anchored to source_started, so the effective
    # budget always spans the whole run — never "now + budget".
    ctx = _make_ctx()
    time.sleep(0.01)
    ctx.extend_source_deadline_for_pagination(pages_discovered=6)
    assert ctx.source_deadline == pytest.approx(ctx.source_started + 210, abs=1e-6)


def test_detail_flow_receives_effective_budget_not_base() -> None:
    # PS 2026-09-15: process_detail_link anchors per-detail deadlines at
    # source_started + the budget it is handed. The runner must pass the
    # effective budget (floor + pagination extension), or detail verifications
    # raise TimeoutError after the base window and abort each page's card
    # loop (~12 rows/page instead of ~50).
    _runner, ctx, _log = _make_runner({_BASE: _pagination_pages_html()})
    ctx.extend_source_deadline_for_pagination(pages_discovered=3)
    from src.jobs.adapters.static_listing_runner import StaticFetchRunner

    runner = StaticFetchRunner(ctx)
    page_url, _profile, budget = runner._listing_result_context(
        {"url": _BASE, "payload": {"sourceBudgetS": 30}}
    )
    assert page_url == _BASE
    assert budget == ctx.effective_source_budget_s()
    assert budget == 120  # floor 30 + 3 pages x 30
