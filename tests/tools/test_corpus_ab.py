"""The A/B's verdict is the thing a human will act on, so its statistics are tested directly.

Two properties carry the weight and neither needs a replayed run to prove:

* ``inconclusive`` must be reachable. A harness whose interval always excludes zero would turn
  noise into a winner, which is exactly how the claim this tool exists to adjudicate got shipped.
* the alternating order and the parity gate must actually work, because a comparison whose arms
  ran in the same order, or produced different jobs, is not a comparison at all.

The rest pins the report's honesty: the banner, both arms' numbers and the retry share have to be
in the markdown a human reads, not only in the JSON nobody opens.
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

from tools.measurements.corpus import ab, run_fetch  # noqa: E402


def _pair(index: int, a_wall: int, b_wall: int) -> dict[str, Any]:
    return {
        "index": index,
        "order": list(ab.alternating_order(index)),
        "a": {"wallMs": a_wall, "reportWallMs": a_wall, "exitCode": 0, "rows": 10},
        "b": {"wallMs": b_wall, "reportWallMs": b_wall, "exitCode": 0, "rows": 10},
    }


def test_the_arm_command_carries_all_sources_only_when_asked() -> None:
    """The flag decides the source set, so an arm that drops it must not run as a
    full-registry arm: a study on a full-source epoch whose command silently fell back to a
    stride sample would measure different work in the same path as the reference.
    """

    sampled = ab.harness.build_command(mode="replay", epoch_dir="/e", run_dir="/r")
    assert "--all-sources" not in sampled

    commanded = ab.harness.build_command(
        mode="replay", epoch_dir="/e", run_dir="/r", scope="full", all_sources=True
    )
    assert commanded.count("--all-sources") == 1
    # A flag, never an ``--all-sources true`` pair the driver's argparse would reject.
    assert commanded[commanded.index("--all-sources") + 1 :] == []


def test_the_sheet_redirect_pool_is_a_recorded_workload_input() -> None:
    """The pool is recorded, because it bounds what a record captures.

    A host that fails every response is abandoned by the resolver after roughly twice this pool in
    response-less failures, and the candidates after the breaker are short-circuited -- never
    requested, so never recorded, so missing from every replay that walks further (measured in
    docs/snapshots/fetch-pipeline-full-source-epoch-2026-10-09.md). That makes it a workload input
    a replay has to share, like the static per-source clock.
    """

    args = run_fetch.build_parser().parse_args(
        ["--mode", "record", "--google-sheets-redirect-concurrency", "256"]
    )
    assert args.google_sheets_redirect_concurrency == 256
    # Unset means "leave the product default alone": recorded as 0, not as a made-up number.
    default_args = run_fetch.build_parser().parse_args(["--mode", "record"])
    recorded = run_fetch.run_config(default_args, ("fetch_text", "urlopen", "httpx"))
    assert recorded["googleSheetsRedirectConcurrency"] == 0
    raised = run_fetch.run_config(args, ("fetch_text", "urlopen", "httpx"))
    assert raised["googleSheetsRedirectConcurrency"] == 256


def test_the_arm_command_carries_the_sheet_redirect_pool_only_when_asked() -> None:
    """The sheet redirect pool has to be spelled the same in record and every replay arm.

    The resolver stops requesting a host after roughly twice this pool in response-less
    failures, so a record taken at the product's default 16 captures only a prefix of a failing
    host's candidates -- and the replay, running the same pool over the same candidates, asks
    for links the record never requested and dies on a corpus miss (measured: 47 of 465
    candidates recorded, 200 links missed). Omitted means "leave the product default alone", so
    it must not be emitted as ``--google-sheets-redirect-concurrency 0``.
    """

    default = ab.harness.build_command(mode="replay", epoch_dir="/e", run_dir="/r")
    assert "--google-sheets-redirect-concurrency" not in default

    raised = ab.harness.build_command(
        mode="replay",
        epoch_dir="/e",
        run_dir="/r",
        google_sheets_redirect_concurrency=512,
    )
    assert raised.count("--google-sheets-redirect-concurrency") == 1
    assert raised[raised.index("--google-sheets-redirect-concurrency") + 1] == "512"


def test_alternating_order_starts_with_a_on_even_pairs() -> None:
    assert ab.alternating_order(0) == ("a", "b")
    assert ab.alternating_order(1) == ("b", "a")
    assert ab.alternating_order(2) == ("a", "b")
    assert [ab.alternating_order(index) for index in range(4)] == [
        ("a", "b"),
        ("b", "a"),
        ("a", "b"),
        ("b", "a"),
    ]


def test_paired_deltas_are_signed_against_arm_a_and_skip_unmeasurable_pairs() -> None:
    pairs = [_pair(0, 1000, 900), _pair(1, 1000, 1100), _pair(2, 0, 500)]

    assert ab.paired_deltas(pairs) == [-10.0, 10.0]


def test_paired_deltas_prefer_the_run_report_window_over_process_wall() -> None:
    pair = _pair(0, 1000, 1000)
    pair["a"]["reportWallMs"] = 500
    pair["b"]["reportWallMs"] = 400

    assert ab.paired_deltas([pair]) == [-20.0]


def test_bootstrap_ci_is_deterministic_for_a_fixed_seed() -> None:
    values = [-10.0, -12.0, -9.0, -11.0, -10.5, -9.5]

    first = ab.bootstrap_ci(values, iterations=500)
    second = ab.bootstrap_ci(values, iterations=500)

    assert first == second
    assert first[0] < first[1]


def test_bootstrap_ci_brackets_a_clear_shift_and_contains_zero_for_noise() -> None:
    shifted = ab.bootstrap_ci([-12.0, -11.0, -10.0, -11.5, -10.5, -12.5], iterations=2000)
    noisy = ab.bootstrap_ci([-4.0, 3.0, -2.0, 5.0, 0.5, -1.0], iterations=2000)

    assert shifted[1] < 0.0  # the whole interval is below zero: B is faster
    assert noisy[0] <= 0.0 <= noisy[1]  # the interval straddles zero: nothing to claim


def test_bootstrap_ci_of_one_sample_is_that_sample() -> None:
    assert ab.bootstrap_ci([7.5]) == [7.5, 7.5]
    assert ab.bootstrap_ci([]) == [0.0, 0.0]


def test_verdict_is_inconclusive_when_the_interval_crosses_zero() -> None:
    assert ab.verdict_for(-5.0, [-9.0, 1.0], 2.0) == "inconclusive"
    assert ab.verdict_for(5.0, [-1.0, 9.0], 2.0) == "inconclusive"


def test_verdict_is_inconclusive_below_the_materiality_bar() -> None:
    """A statistically clean but tiny effect is still not worth naming."""

    assert ab.verdict_for(-0.4, [-0.8, -0.1], 2.0) == "inconclusive"
    assert ab.verdict_for(-0.4, [-0.8, -0.1], 0.0) == "arm-b-faster"


def test_verdict_names_the_faster_arm_when_the_interval_excludes_zero() -> None:
    assert ab.verdict_for(-11.3, [-14.0, -8.0], 2.0) == "arm-b-faster"
    assert ab.verdict_for(11.3, [8.0, 14.0], 2.0) == "arm-a-faster"


def test_parity_divergences_name_the_run_and_the_unit() -> None:
    recorded = {
        "rowCount": 30047,
        "identityDigest": "abc",
        "rowProjectionDigest": "p1",
        "sourceStatusCounts": {"error": 1, "ok": 4},
    }
    runs = [
        {"run": "pair-01/arm-a", "units": dict(recorded)},
        {
            "run": "pair-01/arm-b",
            "units": {**recorded, "rowCount": 30046, "identityDigest": "def"},
        },
    ]

    divergences = ab.parity_divergences(runs, recorded)

    assert [entry["run"] for entry in divergences] == ["pair-01/arm-b", "pair-01/arm-b"]
    assert sorted(entry["unit"] for entry in divergences) == ["identityDigest", "rowCount"]


def test_a_parity_divergence_suppresses_the_verdict_and_a_failure_outranks_it() -> None:
    divergence = [{"run": "pair-01/arm-b", "unit": "rowCount", "expected": 1, "actual": 2}]

    assert ab.decide(divergence, [], median=-11.3, ci=[-14.0, -8.0], min_effect_pct=2.0) == (
        "parity-failed",
        "inconclusive",
    )
    assert ab.decide(
        divergence, [{"run": "x"}], median=-11.3, ci=[-14.0, -8.0], min_effect_pct=2.0
    ) == ("harness-error", "inconclusive")
    assert ab.decide([], [], median=-11.3, ci=[-14.0, -8.0], min_effect_pct=2.0) == (
        "ok",
        "arm-b-faster",
    )


def _report() -> dict[str, Any]:
    return {
        "status": "ok",
        "verdict": "inconclusive",
        "medianDeltaPct": -1.4,
        "ci95": [-6.2, 3.9],
        "minEffectPct": 2.0,
        "pairs": [_pair(0, 1000, 900)],
        "deltasPct": [-1.4],
        "arms": {
            "a": {
                "label": "baseline",
                "medianReportWallMs": 73000,
                "medianWallMs": 75500,
                "rows": [30047],
                "repeatedFetchSharePct": 3.5,
            },
            "b": {
                "label": "candidate",
                "medianReportWallMs": 72000,
                "medianWallMs": 74400,
                "rows": [30047],
                "repeatedFetchSharePct": 3.6,
            },
        },
        "parity": {"epochId": "corpus-subset-x", "divergences": []},
        "referee": {
            "epochId": "corpus-subset-x",
            "verdict": "verified",
            "receipt": "study/referee-verification/referee-verification.json",
        },
        "notes": [],
        "advisory": "not CI-gating (D7)",
    }


def test_markdown_carries_the_verdict_the_interval_and_the_not_production_banner() -> None:
    markdown = ab.render_markdown(_report())

    assert "inconclusive" in markdown
    assert "95% CI" in markdown and "3.90" in markdown
    assert ab.NOT_PRODUCTION_BANNER in markdown
    assert "measures the engine, not the wire" in markdown
    # Both arms and their row counts are readable without opening the JSON.
    assert "baseline" in markdown and "candidate" in markdown
    assert "30047" in markdown
    assert "Repeated-fetch exposure" in markdown
    # The study's precondition is stated where a human reads it, with the receipt to check.
    assert "Referee verification" in markdown and "verified" in markdown
    assert "referee-verification.json" in markdown
    assert "not CI-gating (D7)" in markdown


def test_markdown_flags_a_parity_divergence_as_invalidating() -> None:
    report = _report()
    report["status"] = "parity-failed"
    report["verdict"] = "inconclusive"
    report["parity"] = {"epochId": "corpus-subset-x", "divergences": [{"unit": "rowCount"}]}

    markdown = ab.render_markdown(report)

    assert "1 divergence(s)" in markdown
    assert "invalidates the comparison" in markdown


def test_parse_env_pairs_rejects_a_malformed_override() -> None:
    assert ab.parse_env_pairs(["BALUFFO_FETCH_COUNTERS=1", "BALUFFO_PROFILE=off"]) == {
        "BALUFFO_FETCH_COUNTERS": "1",
        "BALUFFO_PROFILE": "off",
    }
    with pytest.raises(ValueError):
        ab.parse_env_pairs(["NO_EQUALS_SIGN"])


def test_cli_requires_an_epoch_and_the_run_subcommand() -> None:
    with pytest.raises(SystemExit):
        ab.build_parser().parse_args(["run"])
    with pytest.raises(SystemExit):
        ab.build_parser().parse_args(["--epoch", "x"])

    args = ab.build_parser().parse_args(["run", "--epoch", "e", "--pairs", "3"])
    assert (args.command, args.epoch, args.pairs) == ("run", "e", 3)
    assert args.min_effect_pct == ab.DEFAULT_MIN_EFFECT_PCT


def test_arm_env_defaults_counters_on_without_overriding_a_caller_choice() -> None:
    default = ab.arm_env(None)
    explicit = ab.arm_env({ab.harness.COUNTERS_ENV: "0"})

    assert default[ab.harness.COUNTERS_ENV] == "1"
    assert explicit[ab.harness.COUNTERS_ENV] == "0"


def test_baseline_records_are_written_into_the_study_directory(tmp_path: Path) -> None:
    report = _report()
    report["epoch"] = {"epochId": "corpus-subset-x"}

    ab.write_arm_baselines(report, tmp_path)

    baselines = sorted(path.name for path in (tmp_path / "baselines").glob("*.json"))
    # One file per arm: a shared mode would overwrite one arm's record with the other's.
    assert baselines == [
        "replay-ab-corpus-subset-x-arm-a-baseline.json",
        "replay-ab-corpus-subset-x-arm-b-baseline.json",
    ]
    records = [
        json.loads((tmp_path / "baselines" / name).read_text(encoding="utf-8"))
        for name in baselines
    ]
    assert {record["arm"] for record in records} == {"baseline", "candidate"}
    assert all(record["mode"].startswith("replay-ab-corpus-subset-x-arm-") for record in records)
    assert all(record["totalDurationMs"] > 0 for record in records)
    assert (
        len((tmp_path / "perf-trend.ndjson").read_text(encoding="utf-8").strip().splitlines()) == 2
    )
