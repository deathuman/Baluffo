"""A bound drift is a labelled pass at every gate that judges one: verdict, birth, study, pairs.

``parity-bound`` is not ``verified``, and the difference is the whole design: the replay did *not*
reproduce the record, and the record's own conflict declaration explains every row it differs by.
This module pins what each caller does with that:

* ``replay_check.judge_sample`` returns the verdict with no problems and the binding beside it, and
  the aggregation counts it as a pass (exit 0) while still refusing the divergence that a source
  outside the declared set produces -- with the binding's own reason in the finding.
* the receipt keeps the full per-host block, the manifest keeps the compact form, and the report says
  what was bound rather than folding it into "verified".
* a **record** whose birth replay is bound keeps the epoch in the parent and supersedes, where a
  falsified one is still quarantined: the epoch that could not reproduce itself for a class the
  boards served is not a hole.
* the **study** gate pins the bound epoch, and the same bind is applied to its arms: a pair whose
  arms both drift inside the declared class *and agree with each other* still yields a verdict, a
  pair whose arms disagree is refused whatever either arm's residue looks like, and an arm bound
  while its sibling is not keeps the refusal.

The residual limit is stated rather than hidden: the birth case is exercised with the check stubbed
at ``check_epoch``, which is the seam the record calls, so the decision path (held, kept, printed,
attached) is real while the replay behind it is not run here.
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

from tests.helpers.corpus_binding_fixtures import (  # noqa: E402
    LINKEDIN,
    diverging_pair,
    epoch_dir,
)
from tests.tools.test_corpus_birth_verification import _record_args  # noqa: E402
from tests.tools.test_corpus_capture import _surface_loader, local_http_server  # noqa: E402
from tools.measurements.corpus import ab, harness, referees, run_fetch  # noqa: E402
from tools.measurements.corpus import epoch as epoch_mod  # noqa: E402
from tools.measurements.corpus import replay_check as check  # noqa: E402


def _bound_block() -> dict[str, Any]:
    """The full binding block a bound verdict carries, in the shape ``binding.bind`` returns."""

    return {
        "schemaVersion": 1,
        "bound": True,
        "reason": (
            f"every one of the 3 differing row(s) belongs to a host the record declared as a key "
            f"conflict: {LINKEDIN}"
        ),
        "declared": {
            "hosts": [LINKEDIN],
            "hostCount": 1,
            "source": "report",
            "document": "conflict-report.json",
            "complete": True,
        },
        "residue": {
            "recordRows": 3,
            "replayRows": 3,
            "recordOnly": 2,
            "replayOnly": 1,
            "drifted": 0,
            "rows": 3,
            "attributedRows": 3,
            "duplicateIdentities": 0,
            "unattributed": 0,
            "fields": {},
            "hosts": [{"host": LINKEDIN, "recordOnly": 2, "replayOnly": 1, "drifted": 0}],
            "outside": [],
        },
        "epochDir": "/tmp/epoch",
        "runDir": "/tmp/run",
    }


def test_a_bound_divergence_is_a_labelled_pass(tmp_path: Path) -> None:
    """No problems, the binding in the sample, exit 0 from the aggregation -- and never
    ``verified``, because the record was not reproduced."""

    epoch, replay_run = diverging_pair(tmp_path, declared=(LINKEDIN,))
    judged = check.judge_sample(
        run_dir=replay_run,
        exit_code=check.EXIT_FALSIFIED,
        recorded=harness.run_summary(tmp_path / "record"),
        epoch_dir=epoch,
    )

    assert judged["verdict"] == check.VERDICT_BOUND
    assert judged["problems"] == []
    assert judged["divergences"]
    assert judged["binding"]["bound"] is True
    assert check.accepted(judged["verdict"]) is True
    assert judged["verdict"] not in check.FALSIFYING | check.UNJUDGEABLE

    counts = check.summarize([{"verdict": judged["verdict"]}])
    assert counts["bound"] == 1 and counts["falsified"] == 0
    assert check.verdict_exit_code(counts) == check.EXIT_OK
    assert check.certification_held({"verdict": judged["verdict"]}) is True

    # The receipt keeps the block the verdict was bound on; the manifest keeps the compact form.
    receipt = check.verification_receipt(
        row={"dir": str(epoch), "epochId": epoch.name},
        result={**judged, "epochId": epoch.name},
        out_dir=tmp_path / "verify",
    )
    attachment = check.manifest_attachment(receipt)
    assert attachment["binding"]["bound"] is True
    assert attachment["binding"]["residueHosts"] == [LINKEDIN]
    assert "residue" not in attachment["binding"]
    written = json.loads(Path(receipt["receipt"]).read_text(encoding="utf-8"))
    assert written["binding"]["residue"]["hosts"][0]["host"] == LINKEDIN

    markdown = check.render_markdown(
        [
            {
                "epochId": epoch.name,
                "verdict": check.VERDICT_BOUND,
                "configSource": check.CONFIG_RECORDED,
                "samples": [judged],
                "binding": judged["binding"],
                "problems": [],
                "misses": [],
            }
        ],
        corpus_parent=tmp_path,
        out_dir=tmp_path / "out",
    )
    assert "Bound to a conflict class the record declared" in markdown
    assert "1 bound to a declared conflict class" in markdown
    assert LINKEDIN in markdown


def test_a_divergence_outside_the_declared_set_still_falsifies_and_names_the_host(
    tmp_path: Path,
) -> None:
    """The refusal the request turns on: one differing source outside the declared hosts, and the
    drift is the falsification it always was -- with the binding's own reason in the finding."""

    epoch, replay_run = diverging_pair(tmp_path, declared=("koeitecmo.vn",))
    judged = check.judge_sample(
        run_dir=replay_run,
        exit_code=check.EXIT_FALSIFIED,
        recorded=harness.run_summary(tmp_path / "record"),
        epoch_dir=epoch,
    )

    assert judged["verdict"] == check.VERDICT_DIVERGED
    (problem,) = judged["problems"]
    assert "did not declare" in problem["detail"] and LINKEDIN in problem["detail"]
    assert problem["binding"]["outsideHosts"] == [LINKEDIN]
    assert check.verdict_exit_code(check.summarize([{"verdict": judged["verdict"]}])) == (
        check.EXIT_FALSIFIED
    )
    assert check.certification_held({"verdict": judged["verdict"]}) is False


def test_a_bound_verdict_without_an_epoch_is_never_reached(tmp_path: Path) -> None:
    """A caller that names no epoch gets the plain divergence: there is no declaration to bind to,
    and the finding says so rather than passing on an unread residue."""

    _, replay_run = diverging_pair(tmp_path, declared=(LINKEDIN,))
    judged = check.judge_sample(
        run_dir=replay_run,
        exit_code=check.EXIT_FALSIFIED,
        recorded=harness.run_summary(tmp_path / "record"),
    )

    assert judged["verdict"] == check.VERDICT_DIVERGED
    assert "no epoch was named" in judged["problems"][0]["detail"]
    assert judged["binding"]["bound"] is False


def test_a_bound_birth_keeps_the_epoch_in_place_and_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """A birth replay that is bound is not a hole: the epoch stays in the parent, supersedes like
    any certified record, and the manifest carries the binding beside the verdict."""

    corpus_dir = tmp_path / "corpus"
    block = _bound_block()

    def fake_check(row: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        return {
            "epochId": row["epochId"],
            "dir": row["dir"],
            "scope": row["scope"],
            "recordedAt": row["recordedAt"],
            "supersededBy": "",
            "verdictSource": row["verdictSource"],
            "configSource": check.CONFIG_RECORDED,
            "verdict": check.VERDICT_BOUND,
            "problems": [],
            "binding": block,
            "misses": [],
            "samples": [
                {
                    "index": 0,
                    "runDir": str(Path(kwargs["out_dir"]) / "sample-0"),
                    "exitCode": check.EXIT_FALSIFIED,
                    "wallMs": 5,
                    "status": "parity-failed",
                    "verdict": check.VERDICT_BOUND,
                    "problems": [],
                    "matches": {},
                    "divergences": [],
                    "binding": block,
                    "misses": [],
                    "ledger": {},
                    "guard": {},
                    "guardBlockedSources": [],
                    "pipelineError": "",
                    "recordedRowCount": 0,
                    "replayedRowCount": 0,
                    "stderrTail": "",
                }
            ],
        }

    monkeypatch.setattr(check, "check_epoch", fake_check)
    with local_http_server() as base:
        loader = _surface_loader(base)
        monkeypatch.setattr(run_fetch, "_loaders", lambda: [("loopback", loader)])
        exit_code = run_fetch.main(_record_args(corpus_dir, tmp_path / "record"))

    printed = capsys.readouterr().out
    manifest = json.loads((corpus_dir / "EPOCH.json").read_text(encoding="utf-8"))
    assert exit_code == run_fetch.EXIT_OK
    assert "parity-bound" in printed and "did not reproduce its record" in printed
    assert corpus_dir.is_dir()
    assert not (corpus_dir.parent / run_fetch.REJECTED_DIRNAME).exists()
    birth = manifest["replayVerification"]
    assert birth["verdict"] == check.VERDICT_BOUND
    assert birth["exitCode"] == check.EXIT_OK
    assert birth["accepted"] is False
    assert birth["binding"]["bound"] is True
    assert birth["binding"]["residueHosts"] == [LINKEDIN]
    assert birth["binding"]["declaredHostCount"] == 1
    listed = referees.epoch_row(corpus_dir, corpus_parent=corpus_dir.parent)
    assert referees._birth_cell(listed) == check.VERDICT_BOUND


def test_the_study_gate_pins_a_bound_referee(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """``ab.verify_referee`` accepts ``parity-bound`` for the same reason it accepts ``verified``:
    it is a pass, and the study's own pair comparison applies the bind again to the arms."""

    directory = epoch_dir(tmp_path, "corpus-subset-20261010-090000", record_run=None)

    def fake_check(row: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        return {
            "epochId": row["epochId"],
            "dir": row["dir"],
            "scope": row["scope"],
            "recordedAt": "",
            "supersededBy": "",
            "verdictSource": row["verdictSource"],
            "configSource": check.CONFIG_RECORDED,
            "verdict": check.VERDICT_BOUND,
            "problems": [],
            "binding": _bound_block(),
            "misses": [],
            "samples": [],
        }

    monkeypatch.setattr(check, "check_epoch", fake_check)
    receipt = ab.verify_referee(directory, out_dir=tmp_path / "verify", timeout_s=60)

    assert receipt["verdict"] == check.VERDICT_BOUND
    assert receipt["exitCode"] == check.EXIT_OK
    assert receipt["binding"]["bound"] is True
    assert epoch_mod.read_epoch(directory)["deltaReferee"]["eligible"] is True


def test_pair_divergences_are_bound_only_while_the_arms_agree() -> None:
    """A pair is comparable while both arms drift inside the declared class *and* agree with each
    other: the study measures which arm is faster at the same work, so arms that produced different
    jobs have no delta -- and a declaration cannot explain arms that disagree with each other."""

    recorded = {
        "rowCount": 100,
        "identityDigest": "aaaa",
        "rowProjectionDigest": "bbbb",
        "sourceStatusCounts": {"ok": 4},
    }
    bound = {
        "bound": True,
        "reason": "every differing row belongs to a declared host",
        "declared": {"hosts": [LINKEDIN]},
        "residue": {"hosts": [{"host": LINKEDIN}]},
    }
    refused = {
        "bound": False,
        "reason": "differing rows belong to 1 host(s) the record did not declare: evil.example",
    }
    drifted = {
        "rowCount": 97,
        "identityDigest": "cccc",
        "rowProjectionDigest": "dddd",
        "sourceStatusCounts": {"ok": 4},
    }

    def arm(units: dict[str, Any], block: dict[str, Any]) -> dict[str, Any]:
        return {"runDir": f"/tmp/{units['identityDigest']}", "units": units, "binding": block}

    clean = {"index": 0, "a": arm(recorded, {}), "b": arm(recorded, {})}
    agreed = {"index": 1, "a": arm(drifted, bound), "b": arm(drifted, bound)}
    disagreeing = {
        "index": 2,
        "a": arm(drifted, bound),
        "b": arm({**drifted, "rowCount": 90}, bound),
    }
    one_arm_refused = {"index": 3, "a": arm(drifted, bound), "b": arm(drifted, refused)}

    clean_only = ab.classify_pair_divergences([clean], recorded)
    assert clean_only["declared"] == [] and clean_only["unexplained"] == []
    assert clean_only["boundPairs"] == 0 and clean_only["divergingArms"] == 0

    both_bound = ab.classify_pair_divergences([agreed], recorded)
    assert both_bound["unexplained"] == []
    assert both_bound["boundPairs"] == 1 and both_bound["boundArms"] == 2
    assert both_bound["divergingArms"] == 2
    assert both_bound["residueHosts"] == [LINKEDIN]
    # Every unit both arms differ by is declared; ``sourceStatusCounts`` matches the record, so it is
    # not a divergence at all and appears nowhere.
    assert {entry["unit"] for entry in both_bound["declared"]} == {
        "rowCount",
        "identityDigest",
        "rowProjectionDigest",
    }
    assert all(entry["binding"]["bound"] is True for entry in both_bound["declared"])
    # And the declaration turns a bound pair into a verdict rather than a refusal.
    assert ab.decide(
        both_bound["unexplained"], [], median=-7.0, ci=[-9.0, -5.0], min_effect_pct=2.0
    ) == ("ok", "arm-b-faster")

    crossed = ab.classify_pair_divergences([disagreeing], recorded)
    assert crossed["boundPairs"] == 0 and crossed["boundArms"] == 0
    assert crossed["declared"] == []
    assert any(
        "arms that disagree with each other" in entry["reason"] for entry in crossed["unexplained"]
    )
    assert ab.decide(
        crossed["unexplained"], [], median=-3.0, ci=[-5.0, -1.0], min_effect_pct=2.0
    ) == ("parity-failed", "inconclusive")

    partial = ab.classify_pair_divergences([one_arm_refused], recorded)
    assert partial["boundPairs"] == 0 and partial["boundArms"] == 1
    assert partial["declared"] and partial["unexplained"]
    assert any("did not declare" in str(entry["reason"]) for entry in partial["unexplained"])
    assert ab.decide(
        partial["unexplained"], [], median=-3.0, ci=[-5.0, -1.0], min_effect_pct=2.0
    ) == ("parity-failed", "inconclusive")
