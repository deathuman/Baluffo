"""Verifying a referee is a precondition of the study, not a query beside it.

``referees.py`` reports which epoch the record *says* may referee; ``replay_check.py`` tests that
claim with a fresh replay. This module pins the seam between the two and the A/B: ``ab.run_ab``
verifies its pinned epoch before the first pair, refuses with the check's own exit semantics when
the listing will not certify the epoch or the replay will not substantiate it, and leaves the
check's evidence behind. The property under test is not merely "a refusal happens" but *ordering*:
no arm may spend machine time on an unverified referee, because discovering the hole after paying
for pairs is the failure the gate exists to prevent.
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

from tests.helpers.corpus_replay_fixtures import epoch_dir, sample  # noqa: E402
from tools.measurements.corpus import ab, referees, truncation  # noqa: E402  # noqa: E402
from tools.measurements.corpus import replay_check as check


def _checked(
    directory: Path, *, verdict: str = check.VERDICT_VERIFIED, **overrides: Any
) -> dict[str, Any]:
    """An epoch-level row as ``replay_check.check_epoch`` returns one."""

    payload: dict[str, Any] = {
        "epochId": directory.name,
        "dir": str(directory),
        "scope": "subset",
        "recordedAt": "",
        "supersededBy": "",
        "verdictSource": "EPOCH.json",
        "knobs": [],
        "configSource": check.CONFIG_RECORDED,
        "staticSourceTimeBudgetS": "",
        "recordedRowCount": 2,
        "verdict": verdict,
        "problems": [],
        "misses": [],
        "samples": [sample(runDir=str(directory / "sample-0"))],
    }
    payload.update(overrides)
    return payload


def test_a_verified_epoch_yields_a_receipt_and_the_checks_own_row_is_the_verdict(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = epoch_dir(tmp_path, "corpus-good-20261009-120000")
    seen: list[dict[str, Any]] = []

    def fake_check(row: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        seen.append({"row": dict(row), **kwargs})
        return _checked(directory)

    monkeypatch.setattr(check, "check_epoch", fake_check)
    receipt = ab.verify_referee(directory, out_dir=tmp_path / "verify", timeout_s=60)

    assert receipt["verdict"] == check.VERDICT_VERIFIED
    assert receipt["exitCode"] == check.EXIT_OK
    assert receipt["configSource"] == check.CONFIG_RECORDED
    assert set(receipt["code"]) == {"gitSha", "dirtyDiffHash"}
    # The row handed to the check is the listing's own, so the gate cannot drift from the listing.
    assert seen[0]["row"]["epochId"] == directory.name
    assert referees.is_eligible(seen[0]["row"]) is True
    assert seen[0]["out_dir"] == tmp_path / "verify"
    assert seen[0]["samples"] == 1
    written = json.loads(Path(receipt["receipt"]).read_text(encoding="utf-8"))
    assert written["verdict"] == check.VERDICT_VERIFIED
    assert written["sampleRunDirs"] == [str(directory / "sample-0")]


def test_the_listing_refusal_is_echoed_with_its_knob_and_no_replay_is_spent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = epoch_dir(tmp_path, "corpus-refused-20261009-120000", eligible=False)

    def must_not_run(*_: Any, **__: Any) -> dict[str, Any]:
        raise AssertionError("an epoch the listing refuses must never be replayed")

    monkeypatch.setattr(check, "check_epoch", must_not_run)
    with pytest.raises(ab.RefereeRefused) as raised:
        ab.verify_referee(directory, out_dir=tmp_path / "verify", timeout_s=60)

    assert raised.value.exit_code == ab.EXIT_NO_REFEREE == truncation.EXIT_REFEREE_REFUSED
    assert truncation.KNOB_REDIRECT_POOL in str(raised.value)
    assert raised.value.listing["epochId"] == directory.name


def test_a_falsified_replay_refuses_and_names_the_miss(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = epoch_dir(tmp_path, "corpus-holed-20261009-120000")
    url = "https://boards.example/job/9"
    monkeypatch.setattr(
        check,
        "check_epoch",
        lambda *_, **__: _checked(
            directory,
            verdict=check.VERDICT_MISSED,
            problems=[{"kind": check.VERDICT_MISSED, "detail": "1 request(s)", "urls": [url]}],
            misses=[{"url": url, "surface": "fetch_text"}],
        ),
    )

    with pytest.raises(ab.RefereeRefused) as raised:
        ab.verify_referee(directory, out_dir=tmp_path / "verify", timeout_s=60)

    assert raised.value.exit_code == check.EXIT_FALSIFIED == ab.EXIT_PARITY_FAILED
    assert url in str(raised.value)
    # The evidence is written before the refusal is raised: a refusal with no receipt is an
    # assertion, and this one is a measurement.
    receipt = json.loads((tmp_path / "verify" / ab.VERIFICATION_FILE).read_text(encoding="utf-8"))
    assert receipt["verdict"] == check.VERDICT_MISSED
    assert receipt["misses"][0]["url"] == url


def test_an_unjudged_replay_refuses_as_infrastructure_not_as_a_finding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    directory = epoch_dir(tmp_path, "corpus-dark-20261009-120000")
    monkeypatch.setattr(
        check,
        "check_epoch",
        lambda *_, **__: _checked(
            directory,
            verdict=check.VERDICT_HARNESS,
            problems=[{"kind": check.VERDICT_HARNESS, "detail": "driver could not start"}],
        ),
    )

    with pytest.raises(ab.RefereeRefused) as raised:
        ab.verify_referee(directory, out_dir=tmp_path / "verify", timeout_s=60)

    assert raised.value.exit_code == check.EXIT_INFRASTRUCTURE == ab.EXIT_INFRASTRUCTURE
    assert "could not be judged" in str(raised.value)


def test_run_ab_verifies_the_referee_before_any_arm_and_reports_the_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    epoch = epoch_dir(tmp_path, "corpus-subset-gated")
    out = tmp_path / "study"
    order: list[str] = []

    def fake_verify(*_: Any, **__: Any) -> dict[str, Any]:
        order.append("verify")
        return {
            "epochId": epoch.name,
            "verdict": check.VERDICT_VERIFIED,
            "receipt": str(out / ab.VERIFICATION_FILE),
        }

    def fake_arm(**kwargs: Any) -> dict[str, Any]:
        order.append(f"arm-{kwargs['arm']}")
        return {
            "label": kwargs["labels"][kwargs["arm"]],
            "runDir": str(ab.pair_dir(out, int(kwargs["index"]), str(kwargs["arm"]))),
            "command": "fake",
            "exitCode": 0,
            "wallMs": 1000,
            "reportWallMs": 1000,
            "rows": 2,
            "units": {unit: None for unit in ab.PARITY_UNITS},
            "repeatedFetchSharePct": 0.0,
            "guard": {"blockedRemote": 0},
            "stderrTail": "",
        }

    monkeypatch.setattr(ab, "verify_referee", fake_verify)
    monkeypatch.setattr(ab, "_run_arm", fake_arm)
    report = ab.run_ab(
        epoch_dir=epoch,
        out_dir=out,
        pairs=1,
        sources=4,
        families="google_sheets",
        max_workers=1,
        arm_a_concurrency=32,
        arm_b_concurrency=48,
    )

    assert order == ["verify", "arm-a", "arm-b"]
    assert report["referee"]["verdict"] == check.VERDICT_VERIFIED
    assert report["referee"]["receipt"] == str(out / ab.VERIFICATION_FILE)
    assert "**Referee verification:**" in ab.render_markdown(report)
    assert any("verified by one fresh replay" in note for note in report["notes"])


def test_a_refused_referee_stops_the_study_before_it_spends_an_arm(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    epoch = epoch_dir(tmp_path, "corpus-subset-refused")

    def refuse(*_: Any, **__: Any) -> dict[str, Any]:
        raise ab.RefereeRefused("refusing to pin", exit_code=ab.EXIT_PARITY_FAILED)

    monkeypatch.setattr(ab, "verify_referee", refuse)
    monkeypatch.setattr(ab, "_run_arm", lambda **_: pytest.fail("no arm may run"))
    with pytest.raises(ab.RefereeRefused):
        ab.run_ab(epoch_dir=epoch, out_dir=tmp_path / "study", pairs=2)


def test_main_returns_the_checks_refusal_code_and_prints_the_finding(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    epoch = epoch_dir(tmp_path, "corpus-subset-main")

    def refuse(*_: Any, **__: Any) -> dict[str, Any]:
        raise ab.RefereeRefused(
            "refusing to pin `corpus-subset-main`: corpus-miss", exit_code=check.EXIT_FALSIFIED
        )

    monkeypatch.setattr(ab, "verify_referee", refuse)
    code = ab.main(["run", "--epoch", str(epoch), "--out", str(tmp_path / "study"), "--pairs", "1"])

    assert code == ab.EXIT_PARITY_FAILED
    assert "corpus-miss" in capsys.readouterr().err
