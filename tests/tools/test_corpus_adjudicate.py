"""Tests for the S0 adjudication runner (``tools/measurements/corpus/adjudicate.py``).

Two kinds of test here, deliberately:

* the **verdict rules**, as pure functions over fabricated A/B reports -- including the rule that
  matters most, that an unreachable variable yields ``inconclusive-by-harness`` rather than a
  confident "no effect";
* the **evidence readers**, against the real repository source, because the whole adjudication
  rests on ``resolve_fetch_text_impl`` still short-circuiting a custom ``fetch_text``. If that
  early return ever disappears, this file fails instead of the verdict silently becoming wrong.
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

from tools.measurements.corpus import adjudicate as adj  # noqa: E402
from tools.measurements.corpus import harness  # noqa: E402


def _ab_report(
    *,
    status: str = "ok",
    median: float = -11.0,
    ci: list[float] | None = None,
    pair_run_dir: str = "",
) -> dict[str, Any]:
    return {
        "status": status,
        "medianDeltaPct": median,
        "ci95": ci if ci is not None else [median - 2.0, median + 2.0],
        "pairs": [{"index": 0, "order": ["a", "b"], "a": {"runDir": pair_run_dir}, "b": {}}],
        "arms": {
            "a": {"label": "pool-64", "medianWallMs": 1000, "rows": 5},
            "b": {"label": "pool-96"},
        },
        "epoch": {"epochId": "corpus-subset-test"},
        "parity": {"allMatched": status == "ok"},
        "repeatedFetches": {"a": 0.0, "b": 0.0},
    }


def test_unreachable_variable_is_inconclusive_by_harness() -> None:
    """The case this tool exists for: the numbers agree and that must not read as "no effect"."""

    verdict = adj.decide_verdict(
        ab_report=_ab_report(median=-11.3, ci=[-11.5, -11.1]),
        reachable=False,
        claim_pct=-11.3,
        min_effect_pct=2.0,
    )

    assert verdict["verdict"] == adj.VERDICT_INCONCLUSIVE
    assert verdict["verdictKind"] == adj.KIND_UNREACHABLE
    assert verdict["claimInsideCi"] is True
    assert "bypassed" in verdict["reason"]
    assert adj.LIVE_PROTOCOL == verdict["whatWouldSettleIt"]
    assert adj.release_note_text(verdict) == ""


def test_engine_time_going_the_wrong_way_refutes() -> None:
    verdict = adj.decide_verdict(
        ab_report=_ab_report(median=6.0, ci=[3.0, 9.0]),
        reachable=True,
        claim_pct=-11.3,
        min_effect_pct=2.0,
    )

    assert verdict["verdict"] == adj.VERDICT_REFUTED
    assert verdict["verdictKind"] == adj.KIND_REFUTED
    assert verdict["fasterArm"] == "baseline"
    draft = adj.release_note_text(verdict)
    assert adj.CLAIM["id"] in draft and "not reproducible" in draft


def test_confirmation_still_needs_a_live_run() -> None:
    """A replay can never confirm a wall-clock win: it removed the wire the claim is about."""

    verdict = adj.decide_verdict(
        ab_report=_ab_report(median=-11.3, ci=[-13.0, -9.0]),
        reachable=True,
        claim_pct=-11.3,
        min_effect_pct=2.0,
    )

    assert verdict["verdict"] == adj.VERDICT_CONFIRMED
    assert verdict["verdictKind"] == adj.KIND_NEEDS_LIVE
    assert verdict["claimInsideCi"] is True
    assert "not yet a production wall-clock win" in verdict["reason"]
    assert adj.release_note_text(verdict) == ""


def test_interval_crossing_zero_and_small_effects_are_inconclusive() -> None:
    crossing = adj.decide_verdict(
        ab_report=_ab_report(median=-4.0, ci=[-9.0, 1.0]),
        reachable=True,
        claim_pct=-11.3,
        min_effect_pct=2.0,
    )
    tiny = adj.decide_verdict(
        ab_report=_ab_report(median=-0.5, ci=[-1.0, -0.1]),
        reachable=True,
        claim_pct=-11.3,
        min_effect_pct=2.0,
    )

    assert crossing["verdictKind"] == adj.KIND_NO_EFFECT
    assert tiny["verdictKind"] == adj.KIND_NO_EFFECT
    for verdict in (crossing, tiny):
        assert verdict["verdict"] == adj.VERDICT_INCONCLUSIVE
        assert verdict["claimInsideCi"] is False


def test_a_failed_ab_produces_no_verdict_at_all() -> None:
    verdict = adj.decide_verdict(
        ab_report=_ab_report(status="parity-failed", median=-11.3),
        reachable=True,
        claim_pct=-11.3,
        min_effect_pct=2.0,
    )

    assert verdict["verdictKind"] == adj.KIND_HARNESS
    assert "did not produce comparable arms" in verdict["reason"]
    assert adj.release_note_text(verdict) == ""


def test_claim_inside_ci_checks_both_ends() -> None:
    assert adj.claim_inside_ci(-11.3, [-12.0, -10.0]) is True
    assert adj.claim_inside_ci(-11.3, [-12.0, -11.3]) is True
    assert adj.claim_inside_ci(-11.3, [-10.0, -1.0]) is False
    assert adj.claim_inside_ci(-11.3, []) is False


def test_short_circuit_evidence_reads_the_real_transport() -> None:
    """The adjudication's load-bearing premise, checked against the product's own source."""

    evidence = adj.short_circuit_evidence()

    assert evidence["readable"] is True
    assert evidence["shortCircuitsCustomFetch"] is True
    missing = adj.short_circuit_evidence(root=Path("does-not-exist"))
    assert missing["readable"] is False
    assert "error" in missing


def test_variable_consumers_lists_where_the_token_is_read(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "sample.py").write_text(
        "adapter_http_concurrency = 48\nother = 1\n", encoding="utf-8"
    )

    found = adj.variable_consumers(root=tmp_path)

    assert found["count"] == 1
    assert found["occurrences"][0].startswith("src/sample.py")
    assert adj.variable_consumers()["count"] > 0


def test_reachability_reports_the_bypass_with_its_evidence(tmp_path: Path) -> None:
    """``fetchClient: custom`` plus the short-circuit is the bypass; an unreadable report is not."""

    epoch = tmp_path / "epoch"
    epoch.mkdir(parents=True, exist_ok=True)
    (epoch / "EPOCH.json").write_text(
        json.dumps({"epochId": "corpus-subset-test", "scope": "subset"}), encoding="utf-8"
    )

    def _runtime(rundir: str) -> Path:
        data = tmp_path / rundir / "data"
        data.mkdir(parents=True)
        report = {
            "runtime": {"fetchClient": "custom", "adapterHttpConcurrency": 48, "maxWorkers": 1}
        }
        (data / harness.REPORT_FILE).write_text(json.dumps(report), encoding="utf-8")
        return tmp_path / rundir

    with_report = adj.reachability(epoch_dir=epoch, run_dir=_runtime("run-with-report"), root=_ROOT)
    without_report = adj.reachability(epoch_dir=epoch, run_dir=tmp_path / "empty", root=_ROOT)

    assert with_report["reachable"] is False
    assert with_report["reachabilityProven"] is True
    assert with_report["fetchClient"] == "custom"
    assert "AsyncHttpTextFetcher" in with_report["reason"]
    assert with_report["evidence"]["runConfig"]["adapterHttpConcurrency"] == 48
    # No report means no proof either way, and an unproven reach must not read as "reachable".
    assert without_report["reachable"] is False
    assert without_report["reachabilityProven"] is False


def test_run_adjudication_uses_the_claim_arms(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The A/B must vary exactly the claim's variable, in the claim's direction."""

    captured: dict[str, Any] = {}

    def fake_run_ab(**kwargs: Any) -> dict[str, Any]:
        captured.update(kwargs)
        return _ab_report(median=-11.0, ci=[-12.0, -10.0])

    monkeypatch.setattr(adj.ab, "run_ab", fake_run_ab)
    payload = adj.run_adjudication(epoch_dir=tmp_path / "epoch", out_dir=tmp_path / "out", pairs=3)

    assert captured["arm_a_concurrency"] == adj.CLAIM["baseline"]["concurrency"] == 32
    assert captured["arm_b_concurrency"] == adj.CLAIM["candidate"]["concurrency"] == 48
    assert captured["arm_a_label"] == "pool-64" and captured["arm_b_label"] == "pool-96"
    assert captured["pairs"] == 3
    assert payload["verdict"]["verdictKind"] == adj.KIND_UNREACHABLE
    assert payload["claim"]["id"] == "61c43fab"


def test_render_markdown_states_the_verdict_kind_and_the_caveat() -> None:
    verdict = adj.decide_verdict(
        ab_report=_ab_report(median=-11.0, ci=[-12.0, -10.0]),
        reachable=False,
        claim_pct=-11.3,
        min_effect_pct=2.0,
    )
    markdown = adj.render_markdown(
        {
            "claim": adj.CLAIM,
            "epoch": {"epochId": "corpus-subset-test"},
            "config": {"pairs": 6, "scope": "subset"},
            "reachability": {
                "reachable": False,
                "fetchClient": "custom",
                "reason": "bypassed",
                "evidence": {},
            },
            "ab": _ab_report(median=-11.0, ci=[-12.0, -10.0]),
            "verdict": verdict,
            "releaseNoteDraft": "",
        }
    )

    assert "# S0 adjudication" in markdown
    assert adj.KIND_UNREACHABLE in markdown
    assert "-11.3" in markdown
    assert "measures the engine, not the wire" in markdown
    assert "What would settle it" in markdown
    assert "Release note draft" not in markdown


def test_main_returns_a_harness_error_when_the_epoch_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def boom(_epoch: str, _parent: str) -> Path:
        raise harness.HarnessError("no epoch")

    monkeypatch.setattr(adj.harness, "resolve_epoch", boom)
    exit_code = adj.main(["--epoch", "nope", "--out", str(tmp_path / "out")])

    assert exit_code == harness.EXIT_INFRASTRUCTURE


def test_append_release_note_appends_the_draft(tmp_path: Path) -> None:
    target = tmp_path / "release-notes.md"
    target.write_text("existing\n", encoding="utf-8")

    written = adj.append_release_note("## correction\n", path=target)

    assert written == target
    assert target.read_text(encoding="utf-8") == "existing\n\n## correction\n"


def test_main_returns_the_referees_refusal_code(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """The study's precondition failure is the S0 command's failure: the code travels through."""

    def boom(**_kwargs: Any) -> dict[str, Any]:
        raise adj.ab.RefereeRefused("refusing to pin `x`", exit_code=adj.ab.EXIT_NO_REFEREE)

    monkeypatch.setattr(adj.ab, "run_ab", boom)
    monkeypatch.setattr(adj.harness, "resolve_epoch", lambda *_args, **_kwargs: tmp_path / "epoch")

    assert adj.main(["--out", str(tmp_path / "out")]) == adj.ab.EXIT_NO_REFEREE == 3
