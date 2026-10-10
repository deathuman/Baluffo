"""The delta-referee certification: which truncation findings refuse an epoch as a replay referee.

``truncation.py`` measures the work a bounded phase dropped; this module pins what the record *does*
about it. A redirect-pool bound or a source clock bound is one a replay walks into -- replay removes
both -- so the record refuses to certify the epoch as a delta referee (exit 3, the knob named in the
refusal). A declared detail cap is count-driven, so a replay of the same bytes drops the same work:
it is carried as a note and refuses nothing. The two end-to-end halves stay with their families
(``test_corpus_truncation.py`` for the pool, ``test_corpus_truncation_static.py`` for the clock);
this file pins the decision itself, including that the names it tells an operator to raise are live
surfaces and not strings that only look like them.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.adapters.static_runtime_support import (  # noqa: E402
    build_static_source_runtime_config,
)
from tests.tools.test_corpus_truncation import (  # noqa: E402
    PRIMARY_HOST,
    PRIMARY_LINK,
    _rows,
)
from tests.tools.test_corpus_truncation_static import (  # noqa: E402
    BOARD_NAME,
    BOARD_PAGE,
    DETAIL_URL,
    _clock_error,
    _static_row,
)
from tools.measurements.corpus import run_fetch, truncation  # noqa: E402


def test_a_clean_record_is_certified_silently() -> None:
    """No finding, nothing to refuse: a healthy record must not pay for the check with noise."""

    links = [PRIMARY_LINK.format(index=index) for index in range(3)]
    report = truncation.audit(rows=_rows(links), requested_urls=links, pool=16)

    assert truncation.referee_verdict(report) == {
        "eligible": True,
        "accepted": False,
        "blocking": [],
        "knobs": [],
        "notes": [],
    }
    assert truncation.referee_lines(report) == []


def test_an_unrequested_candidate_refuses_the_certification_and_names_the_pool() -> None:
    """A bound replay does not share is not a fact about the board, so it cannot carry a delta."""

    links = [PRIMARY_LINK.format(index=index) for index in range(10)]
    report = truncation.audit(rows=_rows(links), requested_urls=links[:3], pool=16)

    verdict = truncation.referee_verdict(report)
    assert verdict["eligible"] is False
    assert verdict["accepted"] is False
    (blocker,) = verdict["blocking"]
    assert blocker["reason"] == "redirect_pool"
    assert blocker["hosts"] == [PRIMARY_HOST]
    assert blocker["unrequestedCandidates"] == 7
    assert verdict["knobs"] == [truncation.KNOB_REDIRECT_POOL]
    assert verdict["notes"] == []

    lines = truncation.referee_lines(report)
    assert truncation.KNOB_REDIRECT_POOL in lines[0]
    assert "7 candidate link(s) were never requested" in lines[0]
    assert "refusing the certification" in lines[-1]


def test_the_override_records_the_bound_instead_of_erasing_it() -> None:
    """``accepted`` is an override, not a clean read: the blockers and knobs stay in the payload."""

    links = [PRIMARY_LINK.format(index=index) for index in range(4)]
    report = truncation.audit(rows=_rows(links), requested_urls=links[:1], pool=16)

    accepted = truncation.referee_verdict(report, accepted=True)
    assert accepted["eligible"] is False
    assert accepted["accepted"] is True
    assert accepted["knobs"] == [truncation.KNOB_REDIRECT_POOL]
    assert (
        "accepted anyway by --accept-truncated-referee"
        in truncation.referee_lines(report, accepted=True)[-1]
    )
    # Nothing to override on a clean record, so the flag cannot make one look excepted.
    clean = truncation.audit(rows=[], requested_urls=[], pool=16)
    assert truncation.referee_verdict(clean, accepted=True)["accepted"] is False


def test_the_knobs_are_surfaces_that_exist(monkeypatch: pytest.MonkeyPatch) -> None:
    """The refusal names a flag and an environment variable; both are pinned to the real surfaces.

    Advice an operator cannot act on is worse than none, so the constant is read off the driver's
    own parser and the variable is applied to the product's own config builder rather than trusted
    as a string that merely looks like one.
    """

    args = run_fetch.build_parser().parse_args(
        ["--mode", "record", truncation.KNOB_REDIRECT_POOL, "99"]
    )
    assert args.google_sheets_redirect_concurrency == 99

    monkeypatch.setenv(truncation.KNOB_STATIC_SOURCE_CLOCK, "777")
    assert build_static_source_runtime_config(4).static_source_time_budget_s == 777


def test_a_clock_stop_refuses_the_certification_and_names_the_clock_knob() -> None:
    """The refusal has to name what to raise, or the operator is told no and nothing else."""

    row = _static_row(error=_clock_error((DETAIL_URL, "Eipix (GameDevMap)")))
    report = truncation.audit(rows=[], requested_urls=[BOARD_PAGE], sources=[row])

    verdict = truncation.referee_verdict(report)
    assert verdict["eligible"] is False
    (blocker,) = verdict["blocking"]
    assert blocker["reason"] == "source_clock"
    assert blocker["sources"] == [BOARD_NAME]
    assert blocker["unrequestedUrls"] == 1
    assert verdict["knobs"] == [truncation.KNOB_STATIC_SOURCE_CLOCK]

    lines = truncation.referee_lines(report)
    assert truncation.KNOB_STATIC_SOURCE_CLOCK in lines[0]
    assert "refusing the certification" in lines[-1]


def test_a_declared_cap_alone_does_not_refuse_the_certification() -> None:
    """A count-driven drop is one a replay reproduces, so it cannot be why a delta is refused.

    The adaptive stop fires on two recorded detail failures and the external fanout cap is a pure
    function of the links' hosts, so a replay of the same bytes stops in the same place. Refusing
    here would disqualify records that replay perfectly -- and teach whoever records them to pass
    the override every time, which is how a certification stops meaning anything.
    """

    row = _static_row(
        stats={
            "candidate_links_found": 136,
            "detail_pages_visited": 19,
            "detail_pages_skipped_by_adaptive_stop": 1197,
        }
    )
    report = truncation.audit(rows=[], requested_urls=[], sources=[row])

    verdict = truncation.referee_verdict(report)
    assert verdict["eligible"] is True
    assert verdict["blocking"] == []
    assert verdict["knobs"] == []
    assert truncation.referee_lines(report) == []
    (note,) = verdict["notes"]
    assert note["reason"] == "detail_cap"
    assert note["sources"] == [BOARD_NAME]
    assert note["declaredDroppedDetails"] == 1197
    # No knob exists for the adaptive stop, and the note says so rather than inventing one.
    assert note["knobs"] == []
    assert "no knob" in note["detail"]


def test_the_external_fanout_cap_names_the_domain_profile_knob() -> None:
    """One cap does have a knob -- a host's ``max_external_detail_links`` -- and the note carries it."""

    row = _static_row(stats={"external_detail_links_capped": 3})
    report = truncation.audit(rows=[], requested_urls=[], sources=[row])

    verdict = truncation.referee_verdict(report)
    assert verdict["eligible"] is True
    (note,) = verdict["notes"]
    assert note["knobs"] == [truncation.KNOB_EXTERNAL_FANOUT_CAP]
    assert "no knob" not in note["detail"]
