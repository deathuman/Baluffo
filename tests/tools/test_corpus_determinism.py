"""S6's decision logic is the gate on every later number, so it is tested without a replay.

Each test can fail for a reason a reader cares about: a unit quietly skipped, a spread reported
but not enforced, a ceiling judged without its raw medians, or an artifact written even though the
samples never ran. Nothing here starts a subprocess -- ``run_sample`` is monkeypatched -- so the
tests stay fast and offline while still exercising ``collect``, ``main`` and the exit codes.
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

from tools.measurements.corpus import determinism as det  # noqa: E402

RECORDED: dict[str, Any] = {
    "rowCount": 3,
    "identityDigest": "abc",
    "rowProjectionDigest": "p1",
    "sourceStatusCounts": {"ok": 1},
}


def _sample(
    *,
    index: int = 0,
    wall: int = 1000,
    units: dict[str, Any] | None = None,
    exit_code: int = 0,
) -> dict[str, Any]:
    return {
        "index": index,
        "runDir": f"/tmp/sample-{index}",
        "exitCode": exit_code,
        "wallMs": wall,
        "wallSource": "report",
        "subprocessWallMs": wall + 50,
        "units": dict(RECORDED if units is None else units),
        "guard": {"blockedRemote": 0},
        "parityStatus": "parity-ok",
        "stderrTail": "",
    }


def _args(**overrides: Any) -> Any:
    argv: list[str] = ["--epoch", "/tmp/epoch"]
    for name, value in overrides.items():
        argv.extend([f"--{name.replace('_', '-')}", str(value)])
    return det.build_parser().parse_args(argv)


# ── spread and units ────────────────────────────────────────────────────────


def test_spread_metrics_reports_min_median_max_and_percentage() -> None:
    spread = det.spread_metrics([100, 110, 120])

    assert spread == {
        "samples": 3,
        "minMs": 100,
        "medianMs": 110,
        "maxMs": 120,
        "spreadPct": 18.18,
        "usable": True,
    }


def test_spread_metrics_ignores_unreadable_walls_and_can_be_unusable() -> None:
    assert det.spread_metrics([0, 0])["usable"] is False
    assert det.spread_metrics([100, 0, 200])["spreadPct"] == 66.67


def test_compare_units_requires_every_unit_and_treats_absence_as_mismatch() -> None:
    assert det.compare_units(RECORDED, dict(RECORDED)) == {
        "rowCount": True,
        "identityDigest": True,
        "rowProjectionDigest": True,
        "sourceStatusCounts": True,
    }
    # A unit nobody measured does not count as matching, on either side.
    assert det.compare_units(RECORDED, {"rowCount": 3})["identityDigest"] is False
    assert det.compare_units({}, {})["sourceStatusCounts"] is False
    assert det.compare_units(RECORDED, {"sourceStatusCounts": {}})["sourceStatusCounts"] is False
    assert det.diverging_units(RECORDED, {"rowCount": 3, "identityDigest": "other"}) == [
        "identityDigest",
        "rowProjectionDigest",
        "sourceStatusCounts",
    ]


# ── the verdict ─────────────────────────────────────────────────────────────


def test_determinism_verdict_passes_only_when_rows_and_spread_both_hold() -> None:
    verdict = det.determinism_verdict(
        [_sample(index=0), _sample(index=1, wall=1050), _sample(index=2, wall=1000)],
        RECORDED,
        spread_limit_pct=15.0,
    )

    assert verdict["verdict"] == det.VERDICT_DETERMINISTIC
    assert verdict["rowsMatch"] is True and verdict["spreadTight"] is True
    assert verdict["divergences"] == []


def test_determinism_verdict_fails_on_a_single_row_divergence() -> None:
    drifted = dict(RECORDED, identityDigest="different")
    verdict = det.determinism_verdict(
        [_sample(index=0), _sample(index=1, units=drifted), _sample(index=2)],
        RECORDED,
        spread_limit_pct=15.0,
    )

    assert verdict["verdict"] == det.VERDICT_NOT_DETERMINISTIC
    assert verdict["rowsMatch"] is False
    assert [(item["sample"], item["unit"]) for item in verdict["divergences"]] == [
        (1, "identityDigest")
    ]
    assert verdict["divergences"][0]["expected"] == "abc"


def test_determinism_verdict_fails_when_rows_match_but_the_spread_is_wide() -> None:
    """Identical rows are half the gate: a wide spread still makes an A/B unreadable."""

    verdict = det.determinism_verdict(
        [_sample(index=0, wall=1000), _sample(index=1, wall=2000)],
        RECORDED,
        spread_limit_pct=15.0,
    )

    assert verdict["verdict"] == det.VERDICT_NOT_DETERMINISTIC
    assert verdict["rowsMatch"] is True
    assert verdict["spreadTight"] is False
    assert verdict["divergences"] == []
    assert verdict["spread"]["spreadPct"] == 66.67


def test_determinism_verdict_refuses_to_call_unreadable_walls_tight() -> None:
    verdict = det.determinism_verdict(
        [_sample(index=0, wall=0), _sample(index=1, wall=0)],
        RECORDED,
        spread_limit_pct=15.0,
    )

    assert verdict["spread"]["usable"] is False
    assert verdict["spreadTight"] is False
    assert verdict["verdict"] == det.VERDICT_NOT_DETERMINISTIC


# ── ceilings ────────────────────────────────────────────────────────────────


def test_delta_pct_guards_a_zero_baseline() -> None:
    assert det.delta_pct(0, 500) == 0.0
    assert det.delta_pct(1000, 1100) == 10.0
    assert det.delta_pct(1000, 900) == -10.0


def test_ceiling_verdict_prefers_noise_then_the_ceiling() -> None:
    # Below the measured spread: the honest answer is that nothing was measured.
    assert det.ceiling_verdict(0.4, 1.0, 0.5) == det.CEILING_WITHIN_NOISE
    # Above noise, under the ceiling.
    assert det.ceiling_verdict(0.4, 0.1, 0.5) == det.CEILING_MET
    # Above noise, over the ceiling: the slice ships with counters disabled.
    assert det.ceiling_verdict(0.4, 0.1, 0.2) == det.CEILING_EXCEEDED
    assert det.ceiling_verdict(3.0, 1.0, 2.0) == det.CEILING_EXCEEDED


def test_measure_ceilings_classifies_each_arm_against_the_noise_floor(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    """Each arm keeps its raw median; the verdict is derived, never asserted."""

    def fake_run_sample(**kwargs: Any) -> dict[str, Any]:
        env = kwargs.get("env_extra") or {}
        if det.harness.COUNTERS_ENV in env:
            wall = 1004
        elif det.harness.PROFILE_ENV in env:
            wall = 1300
        else:
            wall = 1000
        return _sample(wall=wall)

    monkeypatch.setattr(det, "run_sample", fake_run_sample)
    ceilings = det.measure_ceilings(
        epoch_dir=tmp_path,
        out_dir=tmp_path,
        args=_args(ceiling_runs=2),
        baseline_median_ms=1000,
        noise_floor_pct=0.1,
    )

    by_name = {arm["name"]: arm for arm in ceilings["arms"]}
    counters = by_name["always-on-counters"]
    profiler = by_name["opt-in-profiler"]
    # Above the noise floor and under 0.5%: the one arm that may claim the ceiling.
    assert counters["medianMs"] == 1004 and counters["deltaPct"] == 0.4
    assert counters["verdict"] == det.CEILING_MET
    assert counters["ceilingPct"] == det.COUNTERS_CEILING_PCT
    assert len(counters["runs"]) == 2
    assert profiler["deltaPct"] == 30.0
    assert profiler["verdict"] == det.CEILING_EXCEEDED
    assert ceilings["baselineMedianMs"] == 1000
    assert "within-noise" in ceilings["rule"]


# ── report rendering ────────────────────────────────────────────────────────


def test_render_markdown_states_verdict_ceilings_and_caveats() -> None:
    report: dict[str, Any] = {
        "epoch": {
            "epochId": "corpus-subset-20261008-183226",
            "scope": "subset",
            "recordedAt": "2026-10-08T18:19:47Z",
            "recordedSummary": RECORDED,
        },
        "samples": [_sample(index=0), _sample(index=1, wall=1050)],
        "determinism": det.determinism_verdict(
            [_sample(index=0), _sample(index=1, wall=1050)], RECORDED, spread_limit_pct=15.0
        ),
        "ceilings": {
            "baselineMedianMs": 1000,
            "noiseFloorPct": 4.8,
            "arms": [
                {
                    "name": "always-on-counters",
                    "env": {det.harness.COUNTERS_ENV: "1"},
                    "ceilingPct": 0.5,
                    "runs": [{"wallMs": 1010}],
                    "medianMs": 1010,
                    "deltaPct": 1.0,
                    "verdict": det.CEILING_MET,
                }
            ],
        },
    }

    markdown = det.render_markdown(report)

    assert "Verdict: **deterministic**" in markdown
    assert "always-on-counters" in markdown
    assert "BALUFFO_FETCH_COUNTERS=1" in markdown
    assert "meets-ceiling" in markdown
    assert "noise floor" in markdown
    assert "measures the engine, not the wire" in markdown
    assert "interpreter start-up" in markdown


def test_render_markdown_says_when_ceilings_were_not_measured() -> None:
    report: dict[str, Any] = {
        "epoch": {"epochId": "e", "recordedSummary": RECORDED},
        "samples": [_sample()],
        "determinism": det.determinism_verdict([_sample()], RECORDED, spread_limit_pct=15.0),
    }

    markdown = det.render_markdown(report)

    assert "not measured in this pass" in markdown


def test_render_markdown_lists_divergences_with_expected_and_observed() -> None:
    verdict = det.determinism_verdict(
        [_sample(index=0, units=dict(RECORDED, rowCount=4))], RECORDED, spread_limit_pct=15.0
    )
    markdown = det.render_markdown(
        {"epoch": {"epochId": "e", "recordedSummary": RECORDED}, "determinism": verdict}
    )

    assert "Divergences: sample 0 `rowCount`" in markdown
    assert "expected `3` observed `4`" in markdown


# ── collect, artifacts and exit codes (no subprocess) ───────────────────────


def test_collect_returns_the_full_report_over_faked_samples(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(det.harness, "epoch_summary", lambda _dir: _epoch())
    monkeypatch.setattr(det, "run_sample", lambda **kwargs: _sample(wall=1000))

    report = det.collect(epoch_dir=tmp_path, out_dir=tmp_path / "out", args=_args(samples=3))

    assert report["tool"] == "corpus-determinism"
    assert len(report["samples"]) == 3
    assert all(sample["matches"]["rowCount"] for sample in report["samples"])
    assert report["determinism"]["verdict"] == det.VERDICT_DETERMINISTIC
    assert report["ceilings"]["arms"]
    assert report["verdict"] == det.VERDICT_DETERMINISTIC


def test_main_writes_json_and_markdown_and_returns_zero_when_deterministic(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(det.harness, "epoch_summary", lambda _dir: _epoch())
    monkeypatch.setattr(det.harness, "resolve_epoch", lambda _epoch, _parent: tmp_path)
    monkeypatch.setattr(det, "run_sample", lambda **kwargs: _sample(wall=1000))

    exit_code = det.main(
        ["--epoch", str(tmp_path), "--out", str(tmp_path / "out"), "--no-ceilings"]
    )

    assert exit_code == det.harness.EXIT_OK
    payload = json.loads((tmp_path / "out" / "determinism.json").read_text(encoding="utf-8"))
    assert payload["verdict"] == det.VERDICT_DETERMINISTIC
    assert (tmp_path / "out" / "determinism.md").exists()


def test_main_returns_parity_failed_when_a_sample_diverges(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    drifted = dict(RECORDED, identityDigest="different")
    monkeypatch.setattr(det.harness, "epoch_summary", lambda _dir: _epoch())
    monkeypatch.setattr(det.harness, "resolve_epoch", lambda _epoch, _parent: tmp_path)
    monkeypatch.setattr(det, "run_sample", lambda **kwargs: _sample(units=drifted))

    exit_code = det.main(
        ["--epoch", str(tmp_path), "--out", str(tmp_path / "out"), "--no-ceilings"]
    )

    assert exit_code == det.harness.EXIT_PARITY_FAILED
    payload = json.loads((tmp_path / "out" / "determinism.json").read_text(encoding="utf-8"))
    assert payload["verdict"] == det.VERDICT_NOT_DETERMINISTIC
    assert payload["determinism"]["divergences"][0]["unit"] == "identityDigest"


def test_main_reports_a_harness_error_instead_of_a_verdict(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    monkeypatch.setattr(det.harness, "epoch_summary", lambda _dir: _epoch())
    monkeypatch.setattr(det.harness, "resolve_epoch", lambda _epoch, _parent: tmp_path)
    monkeypatch.setattr(det, "run_sample", lambda **kwargs: _sample(exit_code=2))

    exit_code = det.main(
        ["--epoch", str(tmp_path), "--out", str(tmp_path / "out"), "--no-ceilings"]
    )

    assert exit_code == det.harness.EXIT_INFRASTRUCTURE
    payload = json.loads((tmp_path / "out" / "determinism.json").read_text(encoding="utf-8"))
    assert payload["verdict"] == det.VERDICT_HARNESS_ERROR
    assert payload["harnessErrors"][0]["exitCode"] == 2


def test_main_returns_infrastructure_when_the_epoch_is_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    def _raise(_epoch: str, _parent: str) -> Path:
        raise det.harness.HarnessError("no epoch found")

    monkeypatch.setattr(det.harness, "resolve_epoch", _raise)

    exit_code = det.main(["--epoch", str(tmp_path), "--out", str(tmp_path / "out")])

    assert exit_code == det.harness.EXIT_INFRASTRUCTURE
    assert not (tmp_path / "out" / "determinism.json").exists()


# ── cli ─────────────────────────────────────────────────────────────────────


def test_parser_defaults_and_rejects_bad_numbers() -> None:
    args = det.build_parser().parse_args([])

    assert args.samples == det.DEFAULT_SAMPLES
    assert args.spread_limit == det.DEFAULT_SPREAD_LIMIT_PCT
    assert args.ceiling_runs == det.DEFAULT_CEILING_RUNS
    assert args.no_ceilings is False
    assert args.timeout_s == 3600
    with pytest.raises(SystemExit):
        det.build_parser().parse_args(["--samples", "many"])


def test_ceilings_can_be_skipped_entirely(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setattr(det.harness, "epoch_summary", lambda _dir: _epoch())
    monkeypatch.setattr(det, "run_sample", lambda **kwargs: _sample(wall=1000))
    args = det.build_parser().parse_args(["--epoch", "/tmp/epoch", "--no-ceilings"])

    report = det.collect(epoch_dir=tmp_path, out_dir=tmp_path / "out", args=args)

    assert "ceilings" not in report
    assert report["verdict"] == det.VERDICT_DETERMINISTIC


def _epoch() -> dict[str, Any]:
    return {
        "epochId": "corpus-subset-test",
        "scope": "subset",
        "recordedSummary": dict(RECORDED),
        "recordedAt": "2026-10-08T18:19:47Z",
    }
