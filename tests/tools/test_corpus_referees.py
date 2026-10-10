"""Picking a delta referee is a query, so the listing has to answer it -- and say where the answer came from.

Three sources can supply a verdict, and they are not interchangeable: the epoch's own recorded block,
its stored audit, or the audit re-run now over its artifacts. These tests pin that each source is
read in the right order and labelled, that a corpus nobody can judge is *listed* as such rather than
dropped (which would make it look like a corpus that was never there), and that the knob a refused
epoch names is the live surface (`truncation.KNOB_*`) rather than a copy that can drift from it.

The fixtures build epochs through the real writers (`epoch.create_epoch`, `truncation.write_report`)
and derive verdicts through the real `truncation.referee_verdict`, so a fixture cannot pass while
standing for something the recorder would never produce.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest

_ROOT = Path(__file__).resolve().parents[2]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.jobs.common.config import SUPPORTED_REDIRECT_HOSTS  # noqa: E402
from tools.measurements.corpus import epoch as epoch_mod  # noqa: E402
from tools.measurements.corpus import referees, truncation  # noqa: E402
from tools.measurements.corpus.store import CorpusStore  # noqa: E402

PRIMARY_HOST = sorted(SUPPORTED_REDIRECT_HOSTS)[0]
_REQUESTED = f"https://{PRIMARY_HOST}/rd/0"
_UNREQUESTED = f"https://{PRIMARY_HOST}/rd/1"


def _link(index: int) -> str:
    return f"https://{PRIMARY_HOST}/rd/{index}"


def _redirect_report(*, candidates: list[str], requested: list[str]) -> dict[str, Any]:
    """The audit as the recorder runs it, so the verdict derived from it is the recorder's too."""

    return truncation.audit(
        rows=[{"jobLink": link, "title": "Job"} for link in candidates],
        requested_urls=requested,
        pool=16,
    )


def _refused_verdict() -> dict[str, Any]:
    return truncation.referee_verdict(
        _redirect_report(candidates=[_REQUESTED, _UNREQUESTED], requested=[_REQUESTED])
    )


def _clean_verdict() -> dict[str, Any]:
    return truncation.referee_verdict(
        _redirect_report(candidates=[_REQUESTED], requested=[_REQUESTED])
    )


def _epoch(
    root: Path,
    name: str,
    *,
    scope: str = "subset",
    when: datetime | None = None,
    run_dir: str = "",
    delta_referee: dict[str, Any] | None = None,
    superseded_by: str = "",
) -> Path:
    """One epoch directory, written by the epoch module's own writer."""

    directory = root / name
    manifest = epoch_mod.create_epoch(
        directory,
        scope=scope,
        counts={"entries": 2, "uniqueBodies": 1},
        now=when or datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC),
        extra={"recordedRunDir": str(run_dir)} if run_dir else None,
    )
    if delta_referee is not None:
        manifest["deltaReferee"] = delta_referee
    if superseded_by:
        manifest["supersededBy"] = superseded_by
    epoch_mod.write_epoch(directory, manifest)
    return directory


def _run_dir(path: Path, *, rows: list[str], sources: list[dict[str, Any]] | None = None) -> Path:
    """A recorded run's artifacts: the rows it kept and the report its per-source accounting lives in."""

    data = path / "data"
    data.mkdir(parents=True, exist_ok=True)
    (data / "jobs-unified.json").write_text(
        json.dumps([{"jobLink": link} for link in rows]) + "\n", encoding="utf-8"
    )
    (data / "jobs-fetch-report.json").write_text(
        json.dumps(
            {
                "runtime": {"googleSheetsRedirectConcurrency": 16},
                "sources": sources or [],
            }
        )
        + "\n",
        encoding="utf-8",
    )
    return path


def _row(directory: Path, **kwargs: Any) -> dict[str, Any]:
    return referees.epoch_row(directory, corpus_parent=directory.parent, **kwargs)


def _pick(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """The pinned epoch. `pick_referee` returns ``None`` when nothing is eligible, so a test that
    indexes its result is claiming a pick exists -- assert the claim where it is relied on."""

    picked = referees.pick_referee(rows)
    assert picked is not None
    return picked


def test_a_recorded_verdict_is_used_as_the_epoch_s_own_statement(tmp_path: Path) -> None:
    """``EPOCH.json`` first: it is what the record decided, override included, under its own code."""

    directory = _epoch(
        tmp_path, "corpus-recorded-20261009-120000", delta_referee=_refused_verdict()
    )

    row = _row(directory)

    assert row["verdictSource"] == referees.VERDICT_RECORDED
    assert row["verdict"]["eligible"] is False
    assert row["knobs"] == [truncation.KNOB_REDIRECT_POOL]
    assert row["blockedBy"] == [f"redirect_pool (unrequestedCandidates=1) on {PRIMARY_HOST}"]
    assert row["reason"] == ""
    assert referees.summarize([row]) == {
        "epochs": 1,
        "eligible": 0,
        "refused": 1,
        "acceptedByOverride": 0,
        "notJudgeable": 0,
    }


def test_an_override_epoch_stays_visible_as_accepted_and_is_never_the_pick(tmp_path: Path) -> None:
    """A deliberate exception must not be listed as a clean record, nor chosen by accident."""

    accepted = truncation.referee_verdict(
        _redirect_report(candidates=[_REQUESTED, _UNREQUESTED], requested=[_REQUESTED]),
        accepted=True,
    )
    refused_dir = _epoch(tmp_path, "corpus-accepted-20261009-120000", delta_referee=accepted)
    clean_dir = _epoch(tmp_path, "corpus-clean-20261009-130000", delta_referee=_clean_verdict())

    rows = [
        _row(clean_dir),
        _row(refused_dir),
    ]

    assert referees.is_accepted(rows[1]) is True
    assert referees.summarize(rows)["acceptedByOverride"] == 1
    # The accepted epoch still names the knob that would have certified it without the override.
    assert rows[1]["knobs"] == [truncation.KNOB_REDIRECT_POOL]
    assert _pick(rows)["epochId"] == clean_dir.name


def test_a_stored_audit_is_re_derived_without_a_re_run(tmp_path: Path) -> None:
    """An epoch recorded before the manifest block existed still has its own written audit on disk."""

    directory = _epoch(tmp_path, "corpus-stored-20261009-120000")
    truncation.write_report(
        _redirect_report(candidates=[_REQUESTED, _UNREQUESTED], requested=[_REQUESTED]),
        directory / truncation.REPORT_NAME,
    )

    row = _row(directory)

    assert row["verdictSource"] == referees.VERDICT_REPORT
    assert row["verdict"]["eligible"] is False
    assert row["knobs"] == [truncation.KNOB_REDIRECT_POOL]


def test_an_unrecorded_epoch_is_recomputed_over_its_own_artifacts(tmp_path: Path) -> None:
    """No block, no stored audit: the join is re-run -- and the row says it was a recomputation."""

    store = CorpusStore(tmp_path / "corpus-recomputed-20261009-120000")
    store.record(url=_REQUESTED, method="HEAD", request_headers={}, status_code=200, text="")
    run = _run_dir(tmp_path / "record-20261009-120000", rows=[_REQUESTED, _UNREQUESTED])
    _epoch(
        tmp_path,
        "corpus-recomputed-20261009-120000",
        run_dir=str(run),
    )

    row = _row(tmp_path / "corpus-recomputed-20261009-120000")

    assert row["verdictSource"] == referees.VERDICT_RECOMPUTED
    assert row["verdict"]["eligible"] is False
    assert row["blockedBy"] == [f"redirect_pool (unrequestedCandidates=1) on {PRIMARY_HOST}"]
    assert row["knobs"] == [truncation.KNOB_REDIRECT_POOL]
    # The pool the phase resolved is read out of the run's own report, so the recomputation states
    # the bound the record ran under rather than the flag's 0.
    assert row["verdict"]["blocking"][0]["reason"] == "redirect_pool"

    # ``--no-recompute`` is the fast path, and on this path it must say the epoch is unjudged rather
    # than fall back to a verdict nobody recorded.
    skipped = _row(tmp_path / "corpus-recomputed-20261009-120000", recompute=False)
    assert skipped["verdictSource"] == referees.VERDICT_UNAVAILABLE
    assert skipped["verdict"] is None
    assert "--no-recompute" in skipped["reason"]


def test_a_recomputed_clean_epoch_is_eligible_with_no_knob(tmp_path: Path) -> None:
    """The negative control: nothing dropped, so nothing to raise -- and the listing says so."""

    store = CorpusStore(tmp_path / "corpus-cleanly-20261009-120000")
    for link in (_REQUESTED, _UNREQUESTED):
        store.record(url=link, method="HEAD", request_headers={}, status_code=200, text="")
    run = _run_dir(tmp_path / "record-clean", rows=[_REQUESTED, _UNREQUESTED])
    _epoch(tmp_path, "corpus-cleanly-20261009-120000", run_dir=str(run))

    row = _row(tmp_path / "corpus-cleanly-20261009-120000")

    assert row["verdictSource"] == referees.VERDICT_RECOMPUTED
    assert row["verdict"]["eligible"] is True
    assert row["knobs"] == []
    assert row["blockedBy"] == []


def test_a_clock_stop_names_the_source_and_its_knob(tmp_path: Path) -> None:
    """The static family's refusal has to reach the listing with the knob that unbinds it.

    The row is built by the product's own accounting helpers (the static suite's), so the refusal
    this listing reads is the shape a real record writes.
    """

    from tests.tools.test_corpus_truncation_static import (  # noqa: PLC0415
        BOARD_NAME,
        BOARD_PAGE,
        DETAIL_URL,
        _clock_error,
        _static_row,
    )

    report = truncation.audit(
        rows=[],
        requested_urls=[BOARD_PAGE],
        sources=[_static_row(error=_clock_error((DETAIL_URL, "Clock Stop Studio")))],
    )
    directory = _epoch(
        tmp_path,
        "corpus-clock-20261009-120000",
        delta_referee=truncation.referee_verdict(report),
    )

    row = _row(directory)

    assert row["knobs"] == [truncation.KNOB_STATIC_SOURCE_CLOCK]
    # Declared and verified: the URL the refusal names has no corpus request, and the row says how
    # many such URLs there are -- that count is what a replay walks into with no clock to stop it.
    assert row["blockedBy"] == [f"source_clock (unrequestedUrls=1) on {BOARD_NAME}"]


def test_a_corpus_that_cannot_be_judged_is_listed_not_dropped(tmp_path: Path) -> None:
    """Absence of a verdict is a row, not a silence: both the unreadable and the un-runnable kind."""

    crashed = tmp_path / "corpus-crashed-20261009-120000"
    crashed.mkdir()
    _epoch(tmp_path, "corpus-no-run-20261009-120000")
    corrupt = _epoch(tmp_path, "corpus-corrupt-20261009-120000")
    (corrupt / truncation.REPORT_NAME).write_text("{not json", encoding="utf-8")

    rows = referees.epochs_in_scope(corpus_parent=tmp_path)
    by_id = {row["epochId"]: row for row in rows}

    assert set(by_id) == {crashed.name, "corpus-no-run-20261009-120000", corrupt.name}
    assert by_id[crashed.name]["reason"] == f"no readable {epoch_mod.EPOCH_FILE}"
    assert "recordedRunDir" in by_id["corpus-no-run-20261009-120000"]["reason"]
    # A corrupt audit is reported as corrupt rather than silently recomputed: the epoch has an
    # audit, and it is unreadable, which is a finding about the epoch.
    assert truncation.REPORT_NAME in by_id[corrupt.name]["reason"]
    assert referees.summarize(rows)["notJudgeable"] == 3
    assert "Not judgeable" in referees.render_markdown(rows, corpus_parent=tmp_path)


def test_the_pick_prefers_a_current_epoch_and_says_when_none_is(tmp_path: Path) -> None:
    """Rows are newest first, supersession is a tiebreak rather than a filter, a refusal is never
    promoted by recency, and a pick that is retired has to be flagged as retired."""

    retired = _epoch(
        tmp_path,
        "corpus-retired-20261009-100000",
        when=datetime(2026, 10, 9, 10, 0, 0, tzinfo=UTC),
        delta_referee=_clean_verdict(),
        superseded_by="corpus-new-20261009-110000",
    )
    current = _epoch(
        tmp_path,
        "corpus-new-20261009-110000",
        when=datetime(2026, 10, 9, 11, 0, 0, tzinfo=UTC),
        delta_referee=_clean_verdict(),
    )
    refused = _epoch(
        tmp_path,
        "corpus-refused-20261009-120000",
        when=datetime(2026, 10, 9, 12, 0, 0, tzinfo=UTC),
        delta_referee=_refused_verdict(),
        superseded_by="corpus-new-20261009-110000",
    )

    rows = referees.epochs_in_scope(corpus_parent=tmp_path)

    assert [row["epochId"] for row in rows] == [refused.name, current.name, retired.name]
    assert [row["dir"] for row in rows] == [str(refused), str(current), str(retired)]
    assert _pick(rows)["epochId"] == current.name
    # A current pick carries no retirement caveat, and the newest row being a refusal does not
    # promote it: eligibility decides the pick, recency only settles a tie.
    assert "superseded" not in referees.pick_line(rows, referees.pick_referee(rows))

    only_retired = [row for row in rows if row["epochId"] != current.name]
    retired_pick = _pick(only_retired)
    assert retired_pick["epochId"] == retired.name
    assert "every eligible epoch is superseded" in referees.pick_line(only_retired, retired_pick)
    assert not referees.is_eligible(only_retired[0])


def test_main_lists_the_knob_and_gates_a_study_that_has_no_referee(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """End to end through ``main``: the knob is named, and the gate is the record's own exit code."""

    _epoch(tmp_path, "corpus-refused-20261009-120000", delta_referee=_refused_verdict())
    json_path = tmp_path / "out" / "referees.json"

    gated = referees.main(
        ["--corpus-parent", str(tmp_path), "--require-eligible", "--json", str(json_path)]
    )
    printed = capsys.readouterr().out

    assert gated == referees.EXIT_NO_REFEREE == truncation.EXIT_REFEREE_REFUSED
    assert truncation.KNOB_REDIRECT_POOL in printed
    assert "--accept-truncated-referee" in printed
    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert payload["pick"] is None
    assert payload["counts"] == {
        "acceptedByOverride": 0,
        "eligible": 0,
        "epochs": 1,
        "notJudgeable": 0,
        "refused": 1,
    }
    assert payload["epochs"][0]["verdictSource"] == referees.VERDICT_RECORDED

    _epoch(tmp_path, "corpus-clean-20261009-130000", delta_referee=_clean_verdict())
    assert (
        referees.main(["--corpus-parent", str(tmp_path), "--require-eligible"]) == referees.EXIT_OK
    )
    capsys.readouterr()


def test_a_missing_parent_is_an_infrastructure_error_not_an_empty_listing(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A mistyped parent must not read as 'no epoch can referee': that answer is about real epochs."""

    assert referees.main(["--corpus-parent", str(tmp_path / "nope")]) == (
        referees.EXIT_INFRASTRUCTURE
    )
    assert "no epoch parent" in capsys.readouterr().err


def test_the_listed_knobs_are_the_live_surfaces(tmp_path: Path) -> None:
    """Pinned so a rename inside ``truncation`` cannot leave this listing naming a dead knob.

    The knob is the deliverable: advice an operator cannot paste is not advice, so the listing must
    carry the exact spelling the driver or the lane reads, taken from the constant rather than
    restated here.
    """

    directory = _epoch(tmp_path, "corpus-knobs-20261009-120000", delta_referee=_refused_verdict())
    rendered = referees.render_markdown([_row(directory)], corpus_parent=tmp_path)

    assert referees.EXIT_NO_REFEREE == truncation.EXIT_REFEREE_REFUSED
    assert f"`{truncation.KNOB_REDIRECT_POOL}`" in rendered
    assert truncation.KNOB_STATIC_SOURCE_CLOCK.startswith("BALUFFO_")
