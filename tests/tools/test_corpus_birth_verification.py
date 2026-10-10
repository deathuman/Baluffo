"""A certified record tests its own certification before the manifest is final.

The truncation audit answers whether the record *may* be a delta referee from artifacts alone;
``replay_check`` answers whether the claim actually holds by replaying. This module pins the third
seam: record mode runs that check on the epoch it just wrote and attaches the result to
``EPOCH.json`` as ``replayVerification``, with the full receipt under the epoch directory. Four
decisions are load-bearing and tested here rather than described: a birth replay that does not
verify **fails the record** with the check's own code (1 falsified, 2 unjudgeable) after writing
the epoch and **moves it to a sibling ``rejected/``**, where no listing scans and where it
supersedes nothing; ``--accept-unverified-referee`` keeps such an epoch in place instead, marked
accepted in the block, while the study gate still re-runs the check rather than trusting the
label; and an epoch whose certification the audit already refused is **not checked** at birth --
there is no certification to test.
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

from tests.helpers.corpus_replay_fixtures import sample  # noqa: E402
from tests.tools.test_corpus_capture import _surface_loader, local_http_server  # noqa: E402
from tools.measurements.corpus import (  # noqa: E402
    ab,
    harness,
    referees,
    run_fetch,
    truncation,
)
from tools.measurements.corpus import replay_check as check  # noqa: E402


def _record_args(corpus_dir: Path, run_dir: Path, *extra: str) -> list[str]:
    return [
        "--mode",
        "record",
        "--scope",
        "birth-verification",
        "--only-sources",
        "loopback",
        "--max-workers",
        "1",
        "--corpus-dir",
        str(corpus_dir),
        "--run-dir",
        str(run_dir),
        *extra,
    ]


def _checked_row(
    row: dict[str, Any],
    out_dir: Path,
    *,
    verdict: str = check.VERDICT_VERIFIED,
    problems: list[dict[str, Any]] | None = None,
    misses: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """An epoch-level check result as ``replay_check.check_epoch`` returns one."""

    return {
        "epochId": row["epochId"],
        "dir": row["dir"],
        "scope": row["scope"],
        "recordedAt": row["recordedAt"],
        "supersededBy": "",
        "verdictSource": row["verdictSource"],
        "configSource": check.CONFIG_RECORDED,
        "verdict": verdict,
        "problems": problems or [],
        "misses": misses or [],
        "samples": [sample(runDir=str(out_dir / "sample-0"))],
    }


def _manifest(corpus_dir: Path) -> dict[str, Any]:
    """The epoch's manifest, with the document-shape claim asserted rather than assumed."""

    payload = json.loads((corpus_dir / "EPOCH.json").read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def test_a_certified_record_attaches_its_verified_birth_replay(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    corpus_dir = tmp_path / "corpus"
    seen: dict[str, Any] = {}

    def fake_check(row: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        seen["row"] = dict(row)
        seen["out_dir"] = kwargs["out_dir"]
        return _checked_row(row, kwargs["out_dir"])

    monkeypatch.setattr(check, "check_epoch", fake_check)
    with local_http_server() as base:
        loader = _surface_loader(base)
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader)])
        exit_code = run_fetch.main(_record_args(corpus_dir, tmp_path / "record"))

    printed = capsys.readouterr().out
    manifest = _manifest(corpus_dir)
    assert exit_code == run_fetch.EXIT_OK
    assert manifest["deltaReferee"]["eligible"] is True
    birth = manifest["replayVerification"]
    assert birth["verdict"] == check.VERDICT_VERIFIED
    assert birth["exitCode"] == 0
    assert birth["accepted"] is False
    assert birth["configSource"] == check.CONFIG_RECORDED
    assert set(birth["code"]) == {"gitSha", "dirtyDiffHash"}
    assert birth["problems"] == 0 and birth["misses"] == 0

    # The check ran on the epoch the record just wrote, under the epoch directory, and its receipt
    # is the evidence -- not a copy of a summary.
    assert seen["row"]["dir"] == str(corpus_dir)
    assert seen["row"]["verdictSource"] == referees.VERDICT_RECORDED
    assert seen["out_dir"] == corpus_dir / check.VERIFICATION_DIRNAME
    receipt_path = corpus_dir / check.VERIFICATION_DIRNAME / check.RECEIPT_NAME
    assert Path(birth["receipt"]) == receipt_path
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["verdict"] == check.VERDICT_VERIFIED
    assert receipt["epochDir"] == str(corpus_dir)
    assert receipt["sampleRunDirs"] == [str(seen["out_dir"] / "sample-0")]
    assert "replay verification: verified" in printed
    # The block reaches the listing a study picks its referee from, as the record's own statement.
    listed = referees.epoch_row(corpus_dir, corpus_parent=corpus_dir.parent)
    assert listed["replayVerification"]["verdict"] == check.VERDICT_VERIFIED
    assert referees._birth_cell(listed) == check.VERDICT_VERIFIED


def test_a_falsified_birth_replay_is_quarantined_after_writing_the_epoch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A hole found at birth refuses the record *and* takes the epoch out of the parent's
    candidates: the certified sibling is untouched, and the moved manifest says why."""

    good_dir = tmp_path / "corpus-good"
    corpus_dir = tmp_path / "corpus-bad"
    url = "https://boards.example.com/rd/9"
    checks = {"count": 0}

    def fake_check(row: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        checks["count"] += 1
        if checks["count"] == 1:
            return _checked_row(row, kwargs["out_dir"])
        return _checked_row(
            row,
            kwargs["out_dir"],
            verdict=check.VERDICT_MISSED,
            problems=[{"kind": check.VERDICT_MISSED, "detail": "1 request(s)", "urls": [url]}],
            misses=[{"url": url, "surface": "fetch_text"}],
        )

    monkeypatch.setattr(check, "check_epoch", fake_check)
    with local_http_server() as base:
        loader = _surface_loader(base)
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader)])
        good = run_fetch.main(_record_args(good_dir, tmp_path / "record-good"))
        exit_code = run_fetch.main(_record_args(corpus_dir, tmp_path / "record-bad"))

    stderr = capsys.readouterr().err
    rejected = tmp_path / run_fetch.REJECTED_DIRNAME / corpus_dir.name
    assert good == run_fetch.EXIT_OK
    assert exit_code == check.EXIT_FALSIFIED
    assert not corpus_dir.exists()
    manifest = _manifest(rejected)
    birth = manifest["replayVerification"]
    assert birth["verdict"] == check.VERDICT_MISSED
    assert birth["exitCode"] == 1
    assert birth["accepted"] is False
    assert birth["problemKinds"] == [check.VERDICT_MISSED]
    assert birth["problems"] == 1 and birth["misses"] == 1
    # The compact manifest copy carries counts and paths; the URL stays in the full receipt.
    assert url not in json.dumps(birth)
    quarantine = manifest["quarantine"]
    assert quarantine["reason"] == "birth-replay-not-verified"
    assert quarantine["originalDir"] == str(corpus_dir)
    assert quarantine["movedTo"] == str(rejected)
    receipt_path = Path(birth["receipt"])
    assert receipt_path == rejected / check.VERIFICATION_DIRNAME / check.RECEIPT_NAME
    receipt = json.loads(receipt_path.read_text(encoding="utf-8"))
    assert receipt["problems"][0]["urls"] == [url]
    assert receipt["misses"][0]["url"] == url
    assert receipt["epochDir"] == str(rejected)
    # Every path that named the epoch before the move names it after the move too: the receipt's
    # sample run is the evidence a reader follows, and a path left behind points at a directory
    # the move removed.
    moved_sample = str(rejected / check.VERIFICATION_DIRNAME / "sample-0")
    assert receipt["sampleRunDirs"] == [moved_sample]
    assert birth["sampleRunDirs"] == [moved_sample]
    # Nothing the epoch moved with still names the old directory: the receipt a reader follows to
    # the sample runs, and the manifest, both point at where the evidence now lives. The one place
    # the old path belongs is the block that records the move.
    for document in sorted(rejected.rglob("*.json")):
        if document.name == "EPOCH.json":
            continue
        assert str(corpus_dir) not in document.read_text(encoding="utf-8"), document
    moved_without_move_block = dict(manifest)
    del moved_without_move_block["quarantine"]
    assert str(corpus_dir) not in json.dumps(moved_without_move_block)
    assert "refusing the record" in stderr
    assert "--accept-unverified-referee" in stderr
    assert check.VERDICT_MISSED in stderr

    # The rejected record retired nothing and is not a candidate: the certified sibling is still
    # the parent's current epoch, and the listing scans only the parent's direct children.
    assert _manifest(good_dir)["supersededBy"] == ""
    assert [row["epochId"] for row in referees.epochs_in_scope(corpus_parent=tmp_path)] == [
        good_dir.name
    ]


def test_the_override_keeps_a_holed_corpus_labelled_and_still_unpinnable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Acceptance is a record-keeping choice, not a certification: the block says accepted, and
    the study's own fresh check still refuses the epoch."""

    corpus_dir = tmp_path / "corpus"
    url = "https://boards.example.com/rd/9"

    def fake_check(row: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        return _checked_row(
            row,
            kwargs["out_dir"],
            verdict=check.VERDICT_MISSED,
            misses=[{"url": url, "surface": "fetch_text"}],
        )

    monkeypatch.setattr(check, "check_epoch", fake_check)
    with local_http_server() as base:
        loader = _surface_loader(base)
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader)])
        exit_code = run_fetch.main(
            _record_args(corpus_dir, tmp_path / "record", "--accept-unverified-referee")
        )

    stderr = capsys.readouterr().err
    birth = _manifest(corpus_dir)["replayVerification"]
    assert exit_code == run_fetch.EXIT_OK
    assert birth["verdict"] == check.VERDICT_MISSED
    assert birth["accepted"] is True
    assert "accepted anyway by --accept-unverified-referee" in stderr
    assert (corpus_dir / "EPOCH.json").is_file()
    assert not (corpus_dir.parent / run_fetch.REJECTED_DIRNAME).exists()
    listed = referees.epoch_row(corpus_dir, corpus_parent=corpus_dir.parent)
    assert referees._birth_cell(listed) == f"{check.VERDICT_MISSED} (1, accepted)"

    # The label does not open the gate: the study runs its own check and refuses.
    with pytest.raises(ab.RefereeRefused) as refusal:
        ab.verify_referee(corpus_dir, out_dir=tmp_path / "study-verification")
    assert refusal.value.exit_code == check.EXIT_FALSIFIED
    assert refusal.value.verification["verdict"] == check.VERDICT_MISSED


def test_an_unjudgeable_birth_replay_fails_with_the_checks_own_code(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A check that could not judge the epoch is not a green record either: exit 2, the check's
    own meaning for 'not evidence either way', carried in the block."""

    corpus_dir = tmp_path / "corpus"

    def fake_check(row: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        return _checked_row(row, kwargs["out_dir"], verdict=check.VERDICT_EGRESS)

    monkeypatch.setattr(check, "check_epoch", fake_check)
    with local_http_server() as base:
        loader = _surface_loader(base)
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader)])
        exit_code = run_fetch.main(_record_args(corpus_dir, tmp_path / "record"))

    stderr = capsys.readouterr().err
    rejected = corpus_dir.parent / run_fetch.REJECTED_DIRNAME / corpus_dir.name
    birth = _manifest(rejected)["replayVerification"]
    assert not corpus_dir.exists()
    assert exit_code == check.EXIT_INFRASTRUCTURE == 2
    assert birth["verdict"] == check.VERDICT_EGRESS
    assert birth["exitCode"] == 2
    assert birth["accepted"] is False
    assert "refusing the record" in stderr
    assert check.VERDICT_EGRESS in stderr


def test_a_birth_check_that_cannot_build_a_row_fails_as_a_harness_error(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """No listing row means the check could not judge the epoch: exit 2 with the finding
    attached, never a silent birth."""

    corpus_dir = tmp_path / "corpus"

    def no_row(_: Any) -> dict[str, Any]:
        raise harness.HarnessError("no listing row could be built")

    monkeypatch.setattr(check, "epoch_row_for", no_row)
    with local_http_server() as base:
        loader = _surface_loader(base)
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader)])
        exit_code = run_fetch.main(_record_args(corpus_dir, tmp_path / "record"))

    stderr = capsys.readouterr().err
    rejected = corpus_dir.parent / run_fetch.REJECTED_DIRNAME / corpus_dir.name
    birth = _manifest(rejected)["replayVerification"]
    assert not corpus_dir.exists()
    assert exit_code == check.EXIT_INFRASTRUCTURE == 2
    assert birth["verdict"] == check.VERDICT_HARNESS
    assert birth["exitCode"] == 2
    assert birth["problems"] == 1
    assert "refusing the record" in stderr


def test_a_refused_certification_is_not_checked_at_birth(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """There is no certification to test when the audit already refuses one."""

    corpus_dir = tmp_path / "corpus"
    refused = {
        "eligible": False,
        "accepted": False,
        "blocking": [
            {
                "reason": "redirect_pool",
                "detail": "redirect candidates the record never requested",
                "knob": truncation.KNOB_REDIRECT_POOL,
            }
        ],
        "knobs": [truncation.KNOB_REDIRECT_POOL],
        "notes": [],
    }
    monkeypatch.setattr(truncation, "referee_verdict", lambda *_, **__: refused)

    def must_not_run(*_: Any, **__: Any) -> dict[str, Any]:
        raise AssertionError("a refused certification has nothing to test at birth")

    monkeypatch.setattr(check, "check_epoch", must_not_run)
    with local_http_server() as base:
        loader = _surface_loader(base)
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader)])
        exit_code = run_fetch.main(_record_args(corpus_dir, tmp_path / "record"))

    assert exit_code == run_fetch.EXIT_REFEREE_REFUSED
    assert "replayVerification" not in _manifest(corpus_dir)


def test_the_skip_flag_leaves_a_certified_record_without_a_birth_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    corpus_dir = tmp_path / "corpus"

    def must_not_run(*_: Any, **__: Any) -> dict[str, Any]:
        raise AssertionError("--no-referee-verification must skip the check entirely")

    monkeypatch.setattr(check, "check_epoch", must_not_run)
    with local_http_server() as base:
        loader = _surface_loader(base)
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader)])
        exit_code = run_fetch.main(
            _record_args(corpus_dir, tmp_path / "record", "--no-referee-verification")
        )

    manifest = _manifest(corpus_dir)
    assert exit_code == run_fetch.EXIT_OK
    assert manifest["deltaReferee"]["eligible"] is True
    assert "replayVerification" not in manifest
    assert not (corpus_dir / check.VERIFICATION_DIRNAME).exists()


def test_the_manifest_copy_counts_the_evidence_without_copying_every_url() -> None:
    """A manifest is read on every listing query; the evidence belongs in the receipt beside it."""

    attachment = check.manifest_attachment(
        {
            "verdict": "corpus-miss",
            "checkedAt": "2026-10-09T00:00:00Z",
            "exitCode": 1,
            "configSource": "defaults",
            "listingVerdictSource": "recomputed",
            "code": {"gitSha": "a", "dirtyDiffHash": "b"},
            "problems": [{"kind": "corpus-miss", "detail": "d", "urls": ["u1", "u2"]}],
            "misses": [{"url": "u1"}, {"url": "u2"}],
            "sampleRunDirs": ["sample-0"],
            "receipt": "receipt.json",
        }
    )

    assert attachment["verdict"] == "corpus-miss"
    assert attachment["problemKinds"] == ["corpus-miss"]
    assert attachment["problems"] == 1 and attachment["misses"] == 2
    assert attachment["receipt"] == "receipt.json"
    assert "u1" not in json.dumps(attachment)
