"""The static half of the truncation audit: a per-source clock or detail cap drops page and detail work.

The redirect family joins two halves the record owns -- candidate links a kept row declares, minus
the requests the corpus holds. A static board needs a different anchor, because the candidate a
traversal never reached is nowhere in the artifacts. What the record does own is its own
accounting: the static lane writes a refusal naming the URL it was about to fetch when the clock
refused it, and it books counters for the detail pages a cap skipped. These tests pin both
declarations, the corpus verification the clock family gets, and the end-to-end wiring.

The clock fixture strings are copied verbatim from the second full-source epoch of 2026-10-09
(``_out/perf-runs/record-full-20261009-110605``), whose replay died on the very URL the refusal
names -- ``https://www.eipix.com/career/content-creatorcopywriter``. The verdict tests pin where
the certification line falls: a clock stop refuses it, a declared cap does not, because only the
first is a bound a replay does not share. The report row wrapper is
built with the product's own ``format_source_error`` and the counter names are asserted against
``build_static_entry_report``, so a fixture cannot drift from the shape it stands for.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.adapters.static_runtime_support import build_static_entry_report  # noqa: E402
from src.jobs.reporting_summary import format_source_error  # noqa: E402
from tests.tools.test_corpus_capture import local_http_server  # noqa: E402
from tools.measurements.corpus import run_fetch, truncation  # noqa: E402

BOARD_PAGE = "https://board.example.com/jobs/"
BOARD_PAGE_2 = "https://board.example.com/jobs/page-2/"
BOARD_NAME = f"static_source::static:listing_url:{BOARD_PAGE}"
# The URL the 90s-clock epoch refused and its replay then died on.
DETAIL_URL = "https://www.eipix.com/career/content-creatorcopywriter"


def _static_row(
    *,
    name: str = BOARD_NAME,
    adapter: str = "static",
    studio: str = "Board Studio",
    kept: int = 0,
    stats: dict[str, Any] | None = None,
    error: str = "",
    detail_error: str = "",
    pages: tuple[str, ...] = (BOARD_PAGE,),
) -> dict[str, Any]:
    """A report row shaped like a real static source row, counters started from the product's own."""

    base_stats = build_static_entry_report(
        source={"name": name, "studio": studio},
        source_name=name,
        pages=list(pages),
        company=studio,
    )["stats"]
    for stat_name, _label in truncation.STATIC_DROP_STATS:
        assert stat_name in base_stats, stat_name
    base_stats.update(stats or {})
    return {
        "name": name,
        "adapter": adapter,
        "studio": studio,
        "status": "ok",
        "keptCount": kept,
        "error": error,
        "details": [
            {
                "adapter": adapter,
                "name": name,
                "studio": studio,
                "status": "error" if detail_error else "ok",
                "keptCount": kept,
                "error": detail_error,
                "pages": list(pages),
                "stats": base_stats,
            }
        ],
    }


def _clock_error(*refusals: tuple[str, str]) -> str:
    """The report row's error text for clock refusals, in the product's own wrapper shape.

    The inner string is the static lane's ``static:{studio}:{target_url}: {message}`` segment, as
    written by ``StaticSourceContext.stop_for_budget_exhaustion`` and the detail fetch's
    ``TimeoutError``; the wrapper is the pipeline's own ``format_source_error``.
    """

    return "; ".join(
        format_source_error(BOARD_NAME, f"static:{studio}:{url}: time budget exceeded (90s)")
        for url, studio in refusals
    )


def test_a_healthy_static_report_is_silent() -> None:
    """No declaration, no finding: this check must not fire on an ordinary record."""

    report = truncation.audit(rows=[], requested_urls=[BOARD_PAGE], sources=[_static_row()])

    assert report["status"] == "clean"
    assert report["staticSourcesAudited"] == 1
    assert report["staticSources"] == []
    assert report["staticDeclaredDroppedDetails"] == 0
    assert truncation.warning_lines(report) == []


def test_a_clock_refusal_names_the_url_the_corpus_lacks() -> None:
    """The finding is declared by the report and verified by the corpus, URL by URL."""

    row = _static_row(error=_clock_error((DETAIL_URL, "Eipix (GameDevMap)")))
    report = truncation.audit(rows=[], requested_urls=[BOARD_PAGE], sources=[row])

    assert report["status"] == "flagged"
    (finding,) = report["staticSources"]
    assert finding["reasons"] == ["source_clock"]
    assert finding["replayRisk"] == "diverges"
    assert finding["unrequestedUrls"] == [{"url": DETAIL_URL, "kind": "detail"}]
    assert report["staticUnrequestedUrls"] == 1
    assert truncation.candidate_sample_urls(report) == [DETAIL_URL]

    lines = truncation.warning_lines(report)
    clock_lines = [line for line in lines if "source clock" in line]
    assert len(clock_lines) == 1
    assert clock_lines[0].startswith("[corpus] WARNING truncation:")
    assert "1 URL(s) the report names have no corpus request" in clock_lines[0]
    # The clock's own closing line, and not the cap's.
    assert any("will request beyond this record" in line for line in lines)
    assert not any("a static cap dropped detail work" in line for line in lines)


def test_clock_refusals_read_both_message_shapes() -> None:
    """The label before the target is a studio in one shape and the source name in another.

    In the second shape the label ends with the board's own URL, so label and target are
    concatenated with no more than a colon between them: a parser that splits the segment into
    fields reads a URL that does not exist. That shape is what the loopback driver test below
    produces, through ``StaticSourceContext.stop_for_budget_exhaustion``.
    """

    studio_shape = format_source_error(
        BOARD_NAME, f"static:Eipix (GameDevMap):{DETAIL_URL}: time budget exceeded (90s)"
    )
    assert truncation.clock_refusals(studio_shape) == [DETAIL_URL]

    page = "http://127.0.0.1:1234/listing"
    name = f"static_source::static:listing_url:{page}"
    source_name_shape = format_source_error(name, f"static:{name}:{page}: time_budget_exceeded")
    assert truncation.clock_refusals(source_name_shape) == [page]

    ordinary = format_source_error(BOARD_NAME, f"static:Board Studio:{page}: HTTP 500")
    assert truncation.clock_refusals(ordinary) == []


def test_a_refused_url_the_corpus_holds_is_not_reported_lost() -> None:
    """A refusal is only a gap when the corpus confirms no request was made for it."""

    row = _static_row(error=_clock_error((BOARD_PAGE, "Board Studio")))
    report = truncation.audit(rows=[], requested_urls=[BOARD_PAGE], sources=[row])

    (finding,) = report["staticSources"]
    assert finding["reasons"] == ["source_clock"]
    assert finding["replayRisk"] == "diverges"
    assert finding["unrequestedUrls"] == []
    assert report["staticUnrequestedUrls"] == 0
    assert report["staticSourcesWithUnrequestedUrls"] == 0


@pytest.mark.parametrize(
    "requested",
    [
        "https://board.example.com/jobs",
        "https://board.example.com/jobs/?utm_source=newsletter",
    ],
)
def test_a_differently_spelled_request_counts_as_recorded(requested: str) -> None:
    """The join runs through the static lane's own normalisation, not raw string equality.

    The lane strips a trailing slash and tracking parameters before it fetches, so a refusal
    compared as written would report a URL the corpus does hold as missing.
    """

    row = _static_row(error=_clock_error((BOARD_PAGE, "Board Studio")))
    report = truncation.audit(rows=[], requested_urls=[requested], sources=[row])

    assert report["staticUnrequestedUrls"] == 0, requested


def test_a_page_the_source_lists_is_named_as_page_work() -> None:
    """A refusal for a registered listing page is page work, not detail work."""

    row = _static_row(
        pages=(BOARD_PAGE, BOARD_PAGE_2),
        error=_clock_error((BOARD_PAGE_2, "Board Studio")),
    )
    report = truncation.audit(rows=[], requested_urls=[BOARD_PAGE], sources=[row])

    (finding,) = report["staticSources"]
    assert finding["unrequestedUrls"] == [{"url": BOARD_PAGE_2, "kind": "page"}]


def test_a_detail_cap_is_declared_with_counts_and_no_urls() -> None:
    """Unity's real numbers from the 1800s-clock epoch: a cap drop is a count, not a URL."""

    row = _static_row(
        stats={
            "candidate_links_found": 136,
            "detail_pages_visited": 19,
            "detail_pages_skipped_by_adaptive_stop": 1197,
        }
    )
    report = truncation.audit(rows=[], requested_urls=[], sources=[row])

    (finding,) = report["staticSources"]
    assert finding["reasons"] == ["detail_cap"]
    assert finding["replayRisk"] == "dropped-work"
    assert finding["unrequestedUrls"] == []
    assert finding["declared"] == {
        "candidatesFound": 136,
        "detailsVisited": 19,
        "detailsSkippedByAdaptiveStop": 1197,
        "externalDetailLinksCapped": 0,
    }
    assert report["staticDeclaredDroppedDetails"] == 1197

    lines = truncation.warning_lines(report)
    assert any("1197 skipped by an adaptive stop" in line for line in lines)
    assert any("a static cap dropped detail work" in line for line in lines)
    assert not any("will request beyond this record" in line for line in lines)


def test_a_clock_and_a_cap_on_one_source_report_both_reasons() -> None:
    """Sledgehammer Games' real shape: it was clock-stopped *and* left detail pages skipped."""

    row = _static_row(
        error=_clock_error((DETAIL_URL, "Sledgehammer Games")),
        stats={
            "candidate_links_found": 5,
            "detail_pages_visited": 4,
            "detail_pages_skipped_by_adaptive_stop": 2,
        },
    )
    report = truncation.audit(rows=[], requested_urls=[], sources=[row])

    (finding,) = report["staticSources"]
    assert finding["reasons"] == ["source_clock", "detail_cap"]
    assert finding["replayRisk"] == "diverges"
    assert finding["unrequestedUrls"] == [{"url": DETAIL_URL, "kind": "detail"}]
    assert report["staticDeclaredDroppedDetails"] == 2
    # The source is claimed as a blocker for its clock and noted for its cap, which is the whole
    # distinction: the clock is a bound replay removes, the cap is a drop it reproduces.
    verdict = truncation.referee_verdict(report)
    assert [item["reason"] for item in verdict["blocking"]] == ["source_clock"]
    assert [item["reason"] for item in verdict["notes"]] == ["detail_cap"]
    assert verdict["notes"][0]["declaredDroppedDetails"] == 2


def test_an_ordinary_failure_is_not_a_truncation() -> None:
    """Only the clock's own message declares a clock stop; every other error stays quiet."""

    row = _static_row(
        error=format_source_error(
            BOARD_NAME,
            f"static:Board Studio:{BOARD_PAGE}: HTTP 500",  # noqa: E501
        )
    )
    report = truncation.audit(rows=[], requested_urls=[], sources=[row])

    assert report["status"] == "clean"
    assert report["staticSources"] == []


def test_a_non_static_source_row_is_not_audited() -> None:
    """The family is scoped to the static adapter, so another lane's counters cannot leak in."""

    row = _static_row(adapter="scrapy", stats={"detail_pages_skipped_by_adaptive_stop": 5})
    report = truncation.audit(rows=[], requested_urls=[], sources=[row])

    assert report["staticSourcesAudited"] == 0
    assert report["staticSources"] == []


def test_the_manifest_summary_carries_both_families() -> None:
    """The summary is what ``EPOCH.json`` keeps, so both families' counts have to be in it."""

    row = _static_row(
        error=_clock_error((DETAIL_URL, "Sledgehammer Games")),
        stats={"external_detail_links_capped": 3},
    )
    report = truncation.audit(rows=[], requested_urls=[], sources=[row], pool=16, scope="unit")
    summary = truncation.summary(report)

    assert summary["staticSourcesAudited"] == 1
    assert summary["staticSourcesFlagged"] == 1
    assert summary["staticUnrequestedUrls"] == 1
    assert summary["staticDeclaredDroppedDetails"] == 3
    assert summary["report"] == truncation.REPORT_NAME
    # The redirect family's own fields are untouched by the new one.
    assert summary["candidates"] == 0
    assert summary["unrequestedCandidates"] == 0
    assert summary["hostsWithUnrequestedCandidates"] == 0
    assert report["schemaVersion"] == 2


def _static_board_loader(page: str, name: str) -> Any:
    """A loader for one static board whose only listing page is ``page``."""

    from src.jobs.adapters.static_sources import run_static_source_entry_source

    row = {
        "id": f"static:listing_url:{page}",
        "name": name,
        "studio": "Clock Stop Studio",
        "adapter": "static",
        "company": "Clock Stop Studio",
        "pages": [page],
        "enabledByDefault": True,
    }

    def loader(**kwargs: Any) -> Any:
        return run_static_source_entry_source(source_row=row, diagnostics_name=name, **kwargs)

    return loader


def test_a_real_record_flags_the_static_source_its_clock_stopped(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """End to end through ``main``: a static board with a one-second per-host clock.

    The clock is the product's own per-domain override
    (``DOMAIN_PROFILES[...]["static_source_time_budget_s"]``), which is reachable per host and so
    stops the source deterministically and without a sleep: the page is refused before it is ever
    fetched, which makes it exactly the page work this family exists to report.
    """

    from src.scrapers import domain_profiles

    corpus_dir = tmp_path / "corpus"
    run_dir = tmp_path / "record"
    with local_http_server() as base:
        page = f"{base}/listing"
        name = f"static_source::static:listing_url:{page}"
        monkeypatch.setitem(
            domain_profiles.DOMAIN_PROFILES,
            base.split("//", 1)[1],
            {"static_source_time_budget_s": 1},
        )
        monkeypatch.setattr(
            run_fetch, "_loaders", lambda: [(name, _static_board_loader(page, name))]
        )
        exit_code = run_fetch.main(
            [
                "--mode",
                "record",
                "--scope",
                "static-clock-wiring",
                "--only-sources",
                name,
                "--max-workers",
                "1",
                "--corpus-dir",
                str(corpus_dir),
                "--run-dir",
                str(run_dir),
            ]
        )

    # The record is written and then refused as a delta referee: replay has no clock, so it would
    # walk past this stop and request the page the record never asked for.
    assert exit_code == run_fetch.EXIT_REFEREE_REFUSED
    report = truncation.load_report(corpus_dir)
    assert report["staticSourcesAudited"] == 1
    (finding,) = report["staticSources"]
    assert finding["source"] == name
    assert finding["reasons"] == ["source_clock"]
    assert finding["replayRisk"] == "diverges"
    assert finding["unrequestedUrls"] == [{"url": page, "kind": "page"}]

    manifest = json.loads((corpus_dir / "EPOCH.json").read_text(encoding="utf-8"))
    assert manifest["truncation"] == truncation.summary(report)
    assert manifest["truncation"]["staticSourcesFlagged"] == 1
    assert manifest["deltaReferee"]["eligible"] is False
    assert manifest["deltaReferee"]["accepted"] is False
    assert manifest["deltaReferee"]["knobs"] == [truncation.KNOB_STATIC_SOURCE_CLOCK]
    assert manifest["deltaReferee"]["blocking"][0]["sources"] == [name]

    stderr = capsys.readouterr().err
    assert "[corpus] WARNING truncation" in stderr
    assert "source clock stopped the run" in stderr
    assert truncation.KNOB_STATIC_SOURCE_CLOCK in stderr
