"""D: the silent zeros are `unknown` -- not empty, and not failures.

The runtime files `ok` + kept 0 + "latest fetch kept no jobs" (168 rows in the v7 run) and
the "no jobs extracted from source pages" errors (23 rows) as `needs_review`, never
`legit_empty`: the pipeline does not know whether those boards have openings. Reading either
as "asked and empty" claims a conclusion the runtime refuses to draw, and counting the
second as a failure feeds the one number the error-kind split exists to break apart.

`failedSources` still counts the second group as failures -- that divergence is deliberate,
and this file pins the harness side of it.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tools"))


def _load():
    spec = importlib.util.spec_from_file_location(
        "coverage_drain_unknown_state_under_test", _ROOT / "tools" / "coverage_drain.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


cd = _load()


def _source(**fields):
    row = {
        "adapter": "static",
        "durationMs": 0,
        "fetchedCount": 0,
        "keptCount": 0,
        "details": [],
        "error": "",
        "name": "static_source::static:listing_url:https://example.test",
    }
    row.update(fields)
    return row


def test_a_needs_review_zero_kept_row_is_unknown_not_empty():
    """`ok` + kept 0 + "latest fetch kept no jobs" is the runtime's `needs_review` shape.

    168 boards in the v7 run carried it, all bucketed needs_review with 0 legit_empty -- the
    pipeline itself does not know whether they have openings, so "asked and empty" is a
    conclusion it refuses to draw.
    """
    row = _source(
        status="ok",
        durationMs=3985,
        fetchedCount=0,
        keptCount=0,
        healthReason="latest fetch kept no jobs",
        failureBucket="needs_review",
        details=[{"cacheDecision": "run_now"}],
    )
    assert cd._classify_source(row)["state"] == "unknown"


def test_a_no_openings_error_row_is_unknown_not_a_failure():
    """The same state via `error`: 23 rows in v7, also bucketed needs_review.

    Counting these as failures feeds the one number the kind split exists to break apart;
    the state says undecidable, and the kind stays for the breakdown.
    """
    row = _source(
        status="error",
        durationMs=900,
        error="static:x (static): no jobs extracted from source pages",
        fetchedCount=0,
        keptCount=0,
    )
    classified = cd._classify_source(row)
    assert classified["state"] == "unknown"
    assert classified["error_kind"] == "no_openings"


def test_a_no_openings_error_that_kept_jobs_stays_an_error():
    """The split must not swallow a row that kept jobs despite the message."""
    row = _source(
        durationMs=900,
        error="static:x (static): no jobs extracted from source pages",
        fetchedCount=3,
        keptCount=3,
    )
    assert cd._classify_source(row)["state"] == "error"


def test_the_health_reason_is_what_makes_the_zero_unknown():
    """Without the runtime's own verdict a zero-kept row is still `fetched_empty`."""
    row = _source(
        durationMs=3610,
        fetchedCount=0,
        keptCount=0,
        details=[{"cacheDecision": "run_now"}],
    )
    assert cd._classify_source(row)["state"] == "fetched_empty"


def test_the_unknown_state_is_reported_in_the_summary_states():
    assert "unknown" in cd.FETCH_STATES
