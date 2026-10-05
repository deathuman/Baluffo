"""The per-source static time budget is a coverage default, not a tuning knob (2026-10-06).

``BALUFFO_STATIC_SOURCE_TIME_BUDGET_S`` defaulted to **25 seconds**, and boards that were
mid-fetch when that clock expired were filed as *failures* — ``time_budget_exceeded`` — so a
configured limit read as 134 broken boards on a live run. Every one of those errors named the
budget it hit, which is what made it diagnosable.

Raising it to 90s, changing nothing else, moved ``time_budget`` errors from **14 to 1** on an
isolated run and converted **11 boards to real collections**; the other 6 stopped timing out
and confirmed empty.

Two things this pins, because both are silent when wrong:

- the default is a **ceiling, not a wait** — a board with nothing to offer still returns as
  soon as its pages are exhausted, so raising it costs nothing for the 40% of boards that are
  genuinely empty;
- 90s brings the default path in line with what the project **already shipped**: the
  ``uncapped`` preset has run at 180s via ``bridge/task_launch_fetcher_args.py`` since before
  this change, so 25s was the outlier rather than 90s being a new limit.
"""

from __future__ import annotations

import pytest

from src.jobs.adapters.static_runtime_support import (
    STATIC_SOURCE_TIME_BUDGET_DEFAULT_S,
    build_static_source_deadline,
    build_static_source_runtime_config,
)

# The runtime's own named constant, so the assertion and the code cannot drift apart while
# both still read "90". The previous value is a literal here on purpose: it is only ever read
# by this test, and a `src/` constant nothing in `src/` reads is dead weight that the
# pre-push Vulture hook is right to flag (it scans `src/` + `whitelist.py`, never `tests/`).
PREVIOUS_DEFAULT_BUDGET_S = 25
CURRENT_DEFAULT_BUDGET_S = STATIC_SOURCE_TIME_BUDGET_DEFAULT_S


def test_default_budget_is_ninety_seconds(monkeypatch):
    monkeypatch.delenv("BALUFFO_STATIC_SOURCE_TIME_BUDGET_S", raising=False)
    config = build_static_source_runtime_config(static_detail_concurrency=4)
    assert config.static_source_time_budget_s == CURRENT_DEFAULT_BUDGET_S


def test_default_budget_is_not_the_value_that_starved_boards(monkeypatch):
    """The regression, stated directly.

    A test that only asserted "some budget" would pass at 25s and at 5s. The number is the
    point: it is the difference between a board that finishes and one reported as broken.
    """
    monkeypatch.delenv("BALUFFO_STATIC_SOURCE_TIME_BUDGET_S", raising=False)
    config = build_static_source_runtime_config(static_detail_concurrency=4)
    assert config.static_source_time_budget_s > PREVIOUS_DEFAULT_BUDGET_S


def test_env_override_still_wins(monkeypatch):
    """The escape hatch must survive a default change, or operators lose the dial."""
    monkeypatch.setenv("BALUFFO_STATIC_SOURCE_TIME_BUDGET_S", "180")
    config = build_static_source_runtime_config(static_detail_concurrency=4)
    assert config.static_source_time_budget_s == 180


@pytest.mark.parametrize("raw", ["1", "0", "-5", "", "not-a-number"])
def test_degenerate_values_are_floored_rather_than_crashing(monkeypatch, raw):
    """A budget below the 5s floor, or unparseable, must not reach the deadline maths.

    ``build_static_source_deadline`` adds this value to a timestamp, so a negative or
    unparseable budget would either truncate every board instantly or raise mid-run.
    """
    monkeypatch.setenv("BALUFFO_STATIC_SOURCE_TIME_BUDGET_S", raw)
    config = build_static_source_runtime_config(static_detail_concurrency=4)
    assert config.static_source_time_budget_s >= 5
    assert (
        float(
            build_static_source_deadline(
                source_started=0.0, source_budget_s=config.static_source_time_budget_s
            )
        )
        >= 1.0
    )
