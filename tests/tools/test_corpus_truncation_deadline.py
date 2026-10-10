"""The whole-batch deadline: a wall-clock bound a record reaches and a replay cannot.

``src/shared/http_batch.py`` bounds a whole batch of page fetches by wall clock (the per-row timeout
times the batch divided by the concurrency in force, plus a grace). That is a different object from
the static lane's per-source clock, and a replay shares neither: measured on the 2026-10-10
full-source epoch, 16 static sources read ``error`` carrying ``total batch deadline exceeded (30s)``
-- every one of them keeping 0 rows -- while the replay, serving recorded bodies instantly, never
arrives at the bound.

So the audit has to read the two bounds apart from each other, and declare only this one as a status
flip. A clock stop is a break in the work that a replay walks past (its named URLs are checked
against the corpus); a batch deadline is a wall clock with no knob in the product, so the only honest
handling is to name it beside the verdict rather than leave it to be rediscovered as an unexplained
``error`` -> ``ok`` flip. These tests pin the declaration, the separation, and -- because a study has
to be able to be measured on the epoch -- that it refuses nothing.

The fixture is built the way the sibling static suite builds it: the row wrapper comes from the
product's own ``format_source_error`` and the inner text is verbatim what the batch writes, so the
fixture cannot drift from the shape it stands for.
"""

from __future__ import annotations

import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.reporting_summary import format_source_error  # noqa: E402
from tests.tools.test_corpus_truncation_static import (  # noqa: E402
    BOARD_NAME,
    BOARD_PAGE,
    BOARD_PAGE_2,
    DETAIL_URL,
    _clock_error,
    _static_row,
)
from tools.measurements.corpus import truncation  # noqa: E402


def _deadline_error(*segments: tuple[str, str]) -> str:
    """The report row's error text for a whole-batch deadline, in the product's wrapper shape."""

    return "; ".join(
        format_source_error(
            BOARD_NAME, f"static:{studio}:{url}: total batch deadline exceeded (30s)"
        )
        for url, studio in segments
    )


def test_a_batch_deadline_is_declared_and_refuses_nothing() -> None:
    """The batch asked for everything it was going to ask for, so this is a bound, not a gap."""

    row = _static_row(error=_deadline_error((BOARD_PAGE, "Roblox (GameDevMap)")))
    report = truncation.audit(rows=[], requested_urls=[BOARD_PAGE], sources=[row])

    assert report["status"] == "flagged"
    (finding,) = report["staticSources"]
    assert finding["reasons"] == ["batch_deadline"]
    assert finding["replayRisk"] == "diverges"
    # Nothing was dropped and no lost URL is claimed: no URL is implicated by a batch clock.
    assert finding["unrequestedUrls"] == []
    assert report["staticUnrequestedUrls"] == 0
    assert report["staticBatchDeadlineSources"] == 1
    assert "total batch deadline exceeded" in finding["batchDeadlineSamples"][0]
    assert BOARD_PAGE in finding["batchDeadlineSamples"][0]
    assert truncation.summary(report)["staticBatchDeadlineSources"] == 1

    # A declaration, not a blocker: the certification stands, and the note names the flip.
    verdict = truncation.referee_verdict(report)
    assert verdict["eligible"] is True
    assert verdict["blocking"] == []
    assert [item["reason"] for item in verdict["notes"]] == ["batch_deadline"]
    note = verdict["notes"][0]
    assert note["sources"] == [BOARD_NAME]
    assert "cannot expire in a replay" in note["detail"]

    lines = truncation.warning_lines(report)
    assert any("expired the whole-batch deadline" in line for line in lines)
    # The two wall-clock bounds are read separately: this one is not a clock stop.
    assert not any("source clock" in line for line in lines)


def test_the_batch_deadline_and_the_source_clock_are_separate_declarations() -> None:
    """A row can carry both bound texts, and each family has to report only its own share."""

    both = "; ".join(
        (
            _clock_error((DETAIL_URL, "Eipix (GameDevMap)")),
            _deadline_error((BOARD_PAGE_2, "Board Studio (Sheet)")),
        )
    )
    report = truncation.audit(
        rows=[], requested_urls=[BOARD_PAGE, BOARD_PAGE_2], sources=[_static_row(error=both)]
    )

    (finding,) = report["staticSources"]
    assert finding["reasons"] == ["source_clock", "batch_deadline"]
    # The clock half still verifies against the corpus; the deadline half claims no URL.
    assert finding["unrequestedUrls"] == [{"url": DETAIL_URL, "kind": "detail"}]
    assert report["staticUnrequestedUrls"] == 1
    assert report["staticBatchDeadlineSources"] == 1
    verdict = truncation.referee_verdict(report)
    assert [item["reason"] for item in verdict["blocking"]] == ["source_clock"]
    assert [item["reason"] for item in verdict["notes"]] == ["batch_deadline"]
