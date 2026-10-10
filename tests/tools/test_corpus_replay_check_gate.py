"""Aggregation across samples, the skip for epochs the listing refuses, and the exit-code gate.

Three things this pins that the receipts tests cannot. **Every sample must hold**: a clean first
replay cannot outweigh a second one that missed, so the epoch verdict is the worst of its samples.
**An epoch the listing refuses is never replayed**, and its refusal travels through with the knob
that would certify it -- a check that quietly dropped those rows would report a smaller, cleaner
run than it performed. And **the gate tells the three situations apart**: a falsified certification
(1), an epoch that could not be judged (2), and nothing eligible to verify at all (3, the refusal
code a record uses about itself), so a study script cannot read "I verified nothing" as a pass.

Nothing here starts a subprocess: ``replay_check.run_sample`` is monkeypatched.
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

from tests.helpers.corpus_replay_fixtures import (  # noqa: E402
    UNREQUESTED,
    epoch_dir,
    listing_row,
    miss,
    sample,
)
from tools.measurements.corpus import replay_check as check  # noqa: E402
from tools.measurements.corpus import truncation


def test_check_epoch_reports_the_recorded_config_and_the_worst_sample(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def two_samples(**kwargs: Any) -> dict[str, Any]:
        index = int(kwargs["index"])
        if index == 0:
            return sample(index=0, runDir=str(kwargs["run_dir"]))
        return sample(
            index=1,
            runDir=str(kwargs["run_dir"]),
            verdict=check.VERDICT_MISSED,
            exitCode=2,
            problems=[{"kind": check.VERDICT_MISSED, "detail": "1 miss", "urls": [UNREQUESTED]}],
            misses=[miss(UNREQUESTED)],
        )

    monkeypatch.setattr(check, "run_sample", two_samples)
    directory = epoch_dir(
        tmp_path,
        "corpus-recorded-20261009-120000",
        recorded_config={"families": ["google_sheets"], "staticSources": 8},
    )

    result = check.check_epoch(
        listing_row(directory), out_dir=tmp_path / "out", samples=2, timeout_s=60
    )

    assert result["configSource"] == check.CONFIG_RECORDED
    assert result["verdict"] == check.VERDICT_MISSED
    assert result["problems"][0]["urls"] == [UNREQUESTED]
    assert result["misses"] == [miss(UNREQUESTED)]
    assert len(result["samples"]) == 2


def test_an_ineligible_epoch_is_carried_through_and_never_replayed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The listing's refusal is echoed with its knob, and no replay is spent on it."""

    calls: list[str] = []

    def spy(**kwargs: Any) -> dict[str, Any]:
        calls.append(str(kwargs["epoch_dir"]))
        return sample(runDir=str(kwargs["run_dir"]))

    monkeypatch.setattr(check, "run_sample", spy)
    eligible = epoch_dir(tmp_path, "corpus-good-20261009-130000")
    refused = epoch_dir(tmp_path, "corpus-refused-20261009-120000", eligible=False)

    report = check.run_check(
        rows=[listing_row(refused), listing_row(eligible)],
        out_dir=tmp_path / "out",
        samples=1,
        timeout_s=60,
    )

    by_id = {row["epochId"]: row for row in report["epochs"]}
    assert calls == [str(eligible)]
    assert by_id[refused.name]["verdict"] == check.VERDICT_NOT_ELIGIBLE
    assert by_id[refused.name]["knobs"] == [truncation.KNOB_REDIRECT_POOL]
    assert report["counts"]["notEligible"] == 1
    assert report["counts"]["eligible"] == 1
    assert report["exitCode"] == check.EXIT_OK


def test_the_gate_distinguishes_falsified_from_unjudged_and_from_nothing_to_check() -> None:
    """Three different situations, three codes: a study script must be able to tell them apart."""

    def counts(*verdicts: str) -> dict[str, Any]:
        return check.summarize([{"verdict": verdict} for verdict in verdicts])

    assert check.verdict_exit_code(counts(check.VERDICT_VERIFIED)) == check.EXIT_OK
    assert (
        check.verdict_exit_code(counts(check.VERDICT_VERIFIED, check.VERDICT_DIVERGED))
        == check.EXIT_FALSIFIED
    )
    assert check.verdict_exit_code(counts(check.VERDICT_HARNESS)) == check.EXIT_INFRASTRUCTURE
    assert (
        check.verdict_exit_code(counts(check.VERDICT_NOT_ELIGIBLE)) == check.EXIT_NOTHING_TO_CHECK
    )
    assert check.verdict_exit_code(counts()) == check.EXIT_NOTHING_TO_CHECK
    # A falsification outranks an unjudged epoch: a finding must not be softened by a sibling
    # that could not run.
    assert (
        check.verdict_exit_code(counts(check.VERDICT_EGRESS, check.VERDICT_MISSED))
        == check.EXIT_FALSIFIED
    )
    assert check.EXIT_NOTHING_TO_CHECK == truncation.EXIT_REFEREE_REFUSED


def test_main_verifies_the_eligible_set_and_reports_a_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    def spy(**kwargs: Any) -> dict[str, Any]:
        return sample(
            runDir=str(kwargs["run_dir"]),
            verdict=check.VERDICT_MISSED,
            exitCode=2,
            problems=[
                {"kind": check.VERDICT_MISSED, "detail": "1 request(s)", "urls": [UNREQUESTED]}
            ],
            misses=[miss(UNREQUESTED)],
        )

    monkeypatch.setattr(check, "run_sample", spy)
    epoch_dir(tmp_path, "corpus-eligible-20261009-120000")
    out_dir = tmp_path / "out"

    code = check.main(["--corpus-parent", str(tmp_path), "--out", str(out_dir)])
    printed = capsys.readouterr().out

    assert code == check.EXIT_FALSIFIED
    assert UNREQUESTED in printed
    assert "corpus-miss" in printed
    payload = json.loads((out_dir / "replay-check.json").read_text(encoding="utf-8"))
    assert payload["counts"]["falsified"] == 1
    assert payload["counts"]["eligible"] == 1
    assert payload["config"]["corpusParent"] == str(tmp_path)
    assert (out_dir / "replay-check.md").is_file()


def test_main_verifies_cleanly_and_writes_under_its_own_out_dir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    seen: list[Path] = []

    def spy(**kwargs: Any) -> dict[str, Any]:
        seen.append(Path(str(kwargs["run_dir"])))
        return sample(runDir=str(kwargs["run_dir"]))

    monkeypatch.setattr(check, "run_sample", spy)
    epoch_dir(tmp_path, "corpus-eligible-20261009-120000")
    out_dir = tmp_path / "out"

    code = check.main(["--corpus-parent", str(tmp_path), "--out", str(out_dir)])
    printed = capsys.readouterr().out

    assert code == check.EXIT_OK
    assert "replayed end to end and reproduced its record" in printed
    assert seen and all(out_dir in run.parents for run in seen)
    payload = json.loads((out_dir / "replay-check.json").read_text(encoding="utf-8"))
    assert payload["counts"] == {
        "byVerdict": {check.VERDICT_VERIFIED: 1},
        "eligible": 1,
        "epochs": 1,
        "bound": 0,
        "falsified": 0,
        "notEligible": 0,
        "unjudged": 0,
        "verified": 1,
    }


def test_main_refuses_when_the_listing_calls_nothing_eligible(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A check that verified nothing must not exit 0: it shares the record's refusal code."""

    def spy(**kwargs: Any) -> dict[str, Any]:  # pragma: no cover - must not be called
        raise AssertionError("an ineligible epoch must never be replayed")

    monkeypatch.setattr(check, "run_sample", spy)
    epoch_dir(tmp_path, "corpus-refused-20261009-120000", eligible=False)
    out_dir = tmp_path / "out"

    code = check.main(["--corpus-parent", str(tmp_path), "--out", str(out_dir)])
    printed = capsys.readouterr().out

    assert code == check.EXIT_NOTHING_TO_CHECK == truncation.EXIT_REFEREE_REFUSED
    assert "Nothing to verify" in printed
    assert truncation.KNOB_REDIRECT_POOL in printed


def test_main_reports_a_missing_parent_as_infrastructure_not_as_a_pass(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    assert check.main(["--corpus-parent", str(tmp_path / "nope")]) == check.EXIT_INFRASTRUCTURE
    assert "no epoch parent" in capsys.readouterr().err
