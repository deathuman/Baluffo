"""A zero kept count is not a coverage finding until the fetch evidence is beside it.

Every wrong number this harness produced ended the same way: a board kept zero jobs, the
zero was written into a table, and nobody asked whether the board had been asked at all.
`fetchedCount == 0` on its own cannot tell those apart.

The tells, from `static_listing_flow._handle_skip_and_revalidation`:

- a **skip decision** (`skip_fresh`, `cooldown_skip`) or no time spent and nothing fetched
  means never asked;
- a `run_now` decision that spent real milliseconds and fetched nothing means **asked, and
  the board had nothing**.

Both look identical in a kept-count column. The 199 static boards measured in the v6 drain
were the second kind -- 420-5,884 ms spent, `fetchedCount: 0` -- which is the opposite
conclusion from the one "unexplained zero" invites.

A third case matters as much: a **provider board's evidence does not exist**. The adapter
fetches every board it serves under one rollup row, so the rollup keeping jobs says nothing
about whether this particular board is among them. Those boards must not inherit the
rollup's state, because that is a false green wearing the same clothes as the false alarm.
"""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_ROOT))
sys.path.insert(0, str(_ROOT / "tools"))


def _load():
    spec = importlib.util.spec_from_file_location(
        "coverage_drain_evidence_under_test", _ROOT / "tools" / "coverage_drain.py"
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


def _report(*rows, registered: bool = True):
    """A registration report whose boards are registered, unless asked otherwise.

    Registered matters: `fetch_evidence` skips unregistered boards, because a board with no
    registry row has nothing to fetch and therefore no evidence to classify.
    """
    if not registered:
        return cd.registration_report(curated=list(rows), active=[], pending=[])
    active = []
    for row in rows:
        adapter = str(row.get("adapter") or "").lower()
        if adapter in {"static", "scrapy_static"}:
            active.append({"id": f"static:listing_url:{row['listing_url']}", "adapter": adapter})
        else:
            field = cd.ADAPTER_ID_FORMAT.get(adapter, "listing_url")
            value = str(row.get(field) or row.get("listing_url") or "")
            active.append({"id": f"{adapter}:{field}:{value}", "adapter": adapter})
    return cd.registration_report(curated=list(rows), active=active, pending=[])


def _board(url, adapter="static", **fields):
    row = {"adapter": adapter, "coverageAuditOpenings": 1, "listing_url": url, "name": url}
    row.update(fields)
    return row


# --- the distinction the whole module exists for --------------------------------------


def test_asked_and_found_nothing_is_not_never_asked():
    row = _source(
        durationMs=3610,
        fetchedCount=0,
        keptCount=0,
        details=[{"cacheDecision": "run_now"}],
    )
    assert cd._classify_source(row)["state"] == "fetched_empty"


@pytest.mark.parametrize("decision", ["skip_fresh", "cooldown_skip", "skip_revalidate"])
def test_a_skip_decision_means_never_asked(decision):
    row = _source(durationMs=0, fetchedCount=0, keptCount=0, details=[{"cacheDecision": decision}])
    assert cd._classify_source(row)["state"] == "not_selected"


def test_no_time_and_no_fetch_means_never_asked_even_without_a_decision():
    """A cache decision is not always recorded; the corroborating tells still hold."""
    assert cd._classify_source(_source())["state"] == "not_selected"


def test_a_missing_source_row_means_never_selected():
    assert cd._classify_source(None)["state"] == "not_selected"


def test_a_row_that_kept_jobs_is_collected_whatever_else_it_says():
    row = _source(
        durationMs=12, fetchedCount=1, keptCount=1, details=[{"cacheDecision": "run_now"}]
    )
    assert cd._classify_source(row)["state"] == "collected"


def test_an_error_is_an_error_not_an_empty_board():
    """An error and an empty board are different findings and must not share a state.

    Recording a failed fetch as `fetched_empty` is precisely how a real defect becomes
    indistinguishable from a board with nothing to offer.
    """
    row = _source(durationMs=900, error="HTTP 403", fetchedCount=0, keptCount=0)
    classified = cd._classify_source(row)
    assert classified["state"] == "error"
    assert "403" in classified["error"]


def test_error_outranks_a_stale_kept_count():
    """A row that errored did not keep anything, whatever a previous run left behind."""
    row = _source(durationMs=900, error="timeout", keptCount=0, fetchedCount=0)
    assert cd._classify_source(row)["state"] == "error"


# --- the error kinds: an empty board and a broken connection are opposite findings ------


def test_a_board_that_found_nothing_is_not_a_transport_failure():
    """The distinction that makes the 87 error rows actionable.

    `failedSources` counts both. 27 of the 87 rows in the v6 run were boards that fetched
    cleanly and extracted nothing -- a shape `reporting_breakdowns` already buckets as
    `needs_review`. Reading them as failures is how 27 empty boards became 27 broken ones.
    """
    empty = cd.classify_error_kind("static:x (static): no jobs extracted from source pages")
    broken = cd.classify_error_kind("static:x (static): Connection reset by peer")
    assert empty == "no_openings"
    assert broken == "transport"
    assert empty != broken


def test_time_budget_is_not_a_board_failure():
    """14 rows and ~752 openings stopped mid-fetch on a 25-second per-source budget.

    That is a configured limit, not a broken board, and `BALUFFO_STATIC_SOURCE_TIME_BUDGET_S`
    is a knob. Filing it as a failure buries it under boards that need real work.
    """
    assert cd.classify_error_kind("static:x: time budget exceeded (25s)") == "time_budget"
    assert cd.classify_error_kind("static:asus (static): time_budget_exceeded") == "time_budget"


def test_no_openings_wins_over_a_timeout_mention_inside_its_own_message():
    """The message is `... : no jobs extracted from source pages`; a stray 'timeout'
    word in a source name must not reclassify it as a transport failure."""
    assert (
        cd.classify_error_kind(
            "static_source::static:listing_url:https://timeouts.test: "
            "static:x: no jobs extracted from source pages"
        )
        == "no_openings"
    )


@pytest.mark.parametrize(
    ("error", "kind"),
    [
        ("HTTP 403 Forbidden", "blocked"),
        ("captcha challenge served", "blocked"),
        ("SSL certificate verify failed", "transport"),
        ("dns resolution failed", "transport"),
        ("Unsafe static redirect", "redirect"),
        ("HTML contains workday signature - consider adapter reclassification", "adapter_mismatch"),
        ("something nobody has a name for", "fetch_failed"),
    ],
)
def test_error_kinds(error, kind):
    assert cd.classify_error_kind(error) == kind


def test_every_error_kind_is_one_the_summary_prints():
    known = set(cd.ERROR_KINDS)
    assert known >= {
        "no_openings",
        "time_budget",
        "blocked",
        "transport",
        "redirect",
        "adapter_mismatch",
        "fetch_failed",
    }


def test_a_collected_board_carries_no_error_kind():
    row = _source(durationMs=5, keptCount=2, fetchedCount=2)
    assert cd._classify_source(row)["error_kind"] == ""


def test_an_unknown_source_row_carries_no_error_kind():
    assert cd._classify_source(None)["error_kind"] == ""
    assert cd.classify_error_kind("") == ""


# --- the evidence carries the numbers the state was derived from ---------------------


def test_evidence_records_the_numbers_not_just_the_verdict():
    row = _source(
        durationMs=1171,
        fetchedCount=0,
        keptCount=0,
        details=[{"cacheDecision": "run_now"}],
    )
    classified = cd._classify_source(row)
    assert classified["duration_ms"] == 1171
    assert classified["fetched_count"] == 0
    assert classified["cache_decision"] == "run_now"


def test_every_state_is_one_the_summary_prints():
    """A state the summary does not list is a state nobody sees."""
    known = set(cd.FETCH_STATES)
    for row in (
        _source(durationMs=5, keptCount=2),
        _source(durationMs=5),
        _source(error="x"),
        _source(details=[{"cacheDecision": "skip_fresh"}]),
        None,
    ):
        assert cd._classify_source(row)["state"] in known


# --- a provider board's evidence does not exist, and must not be faked ----------------


def test_provider_board_reports_rollup_only_not_the_rollups_state(tmp_path):
    """A rollup that kept jobs must not mark every board on the platform as collected.

    `greenhouse_boards` keeping 600 openings says the adapter ran. It does not say that any
    particular studio's board is among them, and reporting it as `collected` would be the
    98%-while-delivering-nothing failure in its purest form.
    """
    report = _report(_board("https://boards.greenhouse.io/studioghibli", adapter="workable"))
    fetch_report = {
        "sources": [
            _source(
                adapter="workable",
                name="workable_sources",
                durationMs=9000,
                fetchedCount=600,
                keptCount=600,
                details=[{"cacheDecision": "run_now"}],
            )
        ]
    }
    (tmp_path / cd.FETCH_REPORT).write_text(json.dumps(fetch_report), encoding="utf-8")
    evidence = cd.fetch_evidence(tmp_path, report)
    row = evidence[cd.report_identity(report[0])]
    assert row["state"] == "rollup_only"
    assert row["rollup"] == "workable_sources"


def test_provider_board_with_no_rollup_row_reports_no_report(tmp_path):
    report = _report(_board("https://boards.greenhouse.io/studioghibli", adapter="workable"))
    (tmp_path / cd.FETCH_REPORT).write_text(json.dumps({"sources": []}), encoding="utf-8")
    evidence = cd.fetch_evidence(tmp_path, report)
    assert evidence[cd.report_identity(report[0])]["state"] == "no_report"


def test_static_board_is_matched_by_its_own_source_row_not_a_rollup(tmp_path):
    url = "https://careers.ea.com/jobs"
    report = _report(_board(url))
    fetch_report = {
        "sources": [
            _source(
                name=cd.static_source_name(url),
                durationMs=420,
                fetchedCount=0,
                keptCount=0,
                details=[{"cacheDecision": "run_now"}],
            )
        ]
    }
    (tmp_path / cd.FETCH_REPORT).write_text(json.dumps(fetch_report), encoding="utf-8")
    row = cd.fetch_evidence(tmp_path, report)[cd.report_identity(report[0])]
    assert row["state"] == "fetched_empty", "a static board has its own evidence"


def test_missing_fetch_report_yields_no_evidence_rather_than_all_empty(tmp_path):
    """No report must not read as "every board is empty"."""
    report = _report(_board("https://careers.ea.com/jobs"))
    assert cd.fetch_evidence(tmp_path, report) == {}


def test_unregistered_and_unidentifiable_boards_get_no_evidence(tmp_path):
    (tmp_path / cd.FETCH_REPORT).write_text(json.dumps({"sources": []}), encoding="utf-8")
    report = _report(
        _board("https://careers.ea.com/jobs"), _board("https://careers.ea.com/jobs", name="dupe")
    )
    # Same board twice: one registry row, so the second curated board cannot register.
    report[1]["registered"] = False
    report.append(_report(_board(""), registered=False)[0])
    report[-1]["registered"] = False
    evidence = cd.fetch_evidence(tmp_path, report)
    assert list(evidence) == [cd.report_identity(report[0])]


# --- evidence and attribution must agree on which source speaks for a board -----------


def test_evidence_and_attribution_share_one_source_index(tmp_path):
    """Two derivations of "which row is this board" is how they drifted apart before.

    The evidence path must not conclude a board was never asked while the attribution path
    is reading its jobs off the same row.
    """
    url = "https://careers.ea.com/jobs"
    report = _report(_board(url))
    report[0]["registered"] = True
    by_source, prefix_index, rollup_of, tenant_index = cd._source_key_index(report)
    assert by_source[cd.static_source_name(url)] == cd.report_identity(report[0])
    assert rollup_of == {}
    assert prefix_index == []
    # A static board is one board per host, so it has no tenant to index by.
    assert tenant_index == {}
