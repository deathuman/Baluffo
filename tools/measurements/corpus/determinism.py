"""S6: prove replay is deterministic before any A/B on this harness is believed.

The plan makes this the gate on every later number -- *"identical rows and a tight wall-clock
spread. Only after this does any A/B on this harness mean anything"* -- so the tool replays one
epoch several times, unchanged code, and reports two things that can each fail:

1. **Rows.** Every sample must match the epoch's recorded row count, identity digest, row
   projection digest and per-source status counts. A replay returning different jobs is not a
   fast or slow run; it is a different run, and its timing means nothing.
2. **Spread.** The wall clocks must be close enough that a later A/B can attribute a difference to
   its variable rather than to noise. That spread is also the **noise floor** for the ceilings: an
   overhead below it is not measurable, and saying so is what separates a measurement from a claim.

The two D1 ceilings are measured rather than asserted -- **always-on <=0.5%** for
``BALUFFO_FETCH_COUNTERS=1`` versus off and **opt-in <=2%** for ``BALUFFO_PROFILE=1`` versus off
-- and ``exceeds-ceiling`` means the plan's rule applies: counters ship disabled, not over budget.
Scope: replay removes network time, so this measures the **engine, not the wire**, on the
full-fetch workload (D5), over the intercepted lanes only.
"""

from __future__ import annotations

import argparse
import statistics
import sys
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.corpus import harness  # noqa: E402
from tools.measurements.corpus.run_fetch import new_run_token, write_json  # noqa: E402

DEFAULT_SAMPLES = 3
DEFAULT_SPREAD_LIMIT_PCT = 15.0
DEFAULT_CEILING_RUNS = 2
DEFAULT_CORPUS_PARENT = "_out/perf-runs"
COUNTERS_CEILING_PCT = 0.5
PROFILE_CEILING_PCT = 2.0
#: The units a replay must reproduce. Rows first: a matching wall clock on different rows is the
#: failure that would otherwise read as a result.
PARITY_UNITS = ("rowCount", "identityDigest", "rowProjectionDigest", "sourceStatusCounts")
VERDICT_DETERMINISTIC = "deterministic"
VERDICT_NOT_DETERMINISTIC = "not-deterministic"
VERDICT_HARNESS_ERROR = "harness-error"
CEILING_WITHIN_NOISE = "within-noise"
CEILING_MET = "meets-ceiling"
CEILING_EXCEEDED = "exceeds-ceiling"
WALL_CLOCK_NOTE = (
    "the spread uses each run's own report window (startedAt..finishedAt); the subprocess wall "
    "clock is recorded separately because it also contains interpreter start-up"
)


def _empty_spread() -> dict[str, Any]:
    return {"samples": 0, "minMs": 0, "medianMs": 0, "maxMs": 0, "spreadPct": 0.0, "usable": False}


def compare_units(recorded: Mapping[str, Any], observed: Mapping[str, Any]) -> dict[str, bool]:
    """Which parity units a sample reproduced. A unit nobody measured does not match."""

    matches: dict[str, bool] = {}
    for unit in PARITY_UNITS:
        expected, actual = recorded.get(unit), observed.get(unit)
        if unit == "sourceStatusCounts":
            matches[unit] = bool(actual) and dict(actual or {}) == dict(expected or {})
        else:
            matches[unit] = actual is not None and expected is not None and actual == expected
    return matches


def diverging_units(recorded: Mapping[str, Any], observed: Mapping[str, Any]) -> list[str]:
    """The units a sample failed to reproduce, named in the plan's order."""

    return [unit for unit, ok in compare_units(recorded, observed).items() if not ok]


def spread_metrics(wall_ms: Sequence[int]) -> dict[str, Any]:
    """min/median/max and the spread as a percentage of the median: the noise floor."""

    usable = sorted(int(value) for value in wall_ms if int(value) > 0)
    if not usable:
        return _empty_spread()
    median = int(statistics.median(usable))
    spread = ((usable[-1] - usable[0]) / median * 100.0) if median else 0.0
    return {
        "samples": len(usable),
        "minMs": usable[0],
        "medianMs": median,
        "maxMs": usable[-1],
        "spreadPct": round(spread, 2),
        "usable": True,
    }


def delta_pct(baseline_ms: int, candidate_ms: int) -> float:
    """Overhead of the candidate arm as a percentage of the baseline median."""

    if int(baseline_ms) <= 0:
        return 0.0
    return round(((int(candidate_ms) - int(baseline_ms)) / int(baseline_ms)) * 100.0, 3)


def ceiling_verdict(delta: float, noise_floor_pct: float, ceiling_pct: float) -> str:
    """Judge one overhead arm: noise first, then the ceiling it must actually meet."""

    if abs(float(delta)) <= max(0.0, float(noise_floor_pct)):
        return CEILING_WITHIN_NOISE
    return CEILING_MET if float(delta) <= float(ceiling_pct) else CEILING_EXCEEDED


def determinism_verdict(
    samples: Sequence[Mapping[str, Any]],
    recorded: Mapping[str, Any],
    *,
    spread_limit_pct: float,
) -> dict[str, Any]:
    """Rows first, then spread. Either failing makes the harness untrustworthy for A/B."""

    divergences: list[dict[str, Any]] = []
    for index, sample in enumerate(samples):
        units = dict(sample.get("units") or {})
        divergences += [
            {
                "sample": index,
                "runDir": str(sample.get("runDir") or ""),
                "unit": unit,
                "expected": recorded.get(unit),
                "observed": units.get(unit),
            }
            for unit in diverging_units(recorded, units)
        ]
    spread = spread_metrics([int(sample.get("wallMs") or 0) for sample in samples])
    tight = bool(spread["usable"]) and float(spread["spreadPct"]) <= float(spread_limit_pct)
    verdict = VERDICT_DETERMINISTIC if (not divergences and tight) else VERDICT_NOT_DETERMINISTIC
    return {
        "verdict": verdict,
        "rowsMatch": not divergences,
        "spreadTight": tight,
        "spreadLimitPct": round(float(spread_limit_pct), 3),
        "divergences": divergences,
        "spread": spread,
    }


def run_record(sample: Mapping[str, Any]) -> dict[str, Any]:
    """The four figures a ceiling arm needs, without the whole per-run summary."""

    return {
        "runDir": str(sample.get("runDir") or ""),
        "exitCode": int(sample.get("exitCode") or 0),
        "wallMs": int(sample.get("wallMs") or 0),
        "subprocessWallMs": int(sample.get("subprocessWallMs") or 0),
    }


def run_sample(
    *,
    epoch_dir: Path,
    run_dir: Path,
    args: argparse.Namespace,
    label: str,
    env_extra: Mapping[str, str] | None = None,
) -> dict[str, Any]:
    """One fresh-process replay, observed from the receipts it wrote."""

    command = harness.build_command(
        mode="replay",
        epoch_dir=epoch_dir,
        run_dir=run_dir,
        scope=args.scope,
        label=label,
        sources=args.sources,
        families=args.families,
        all_sources=bool(args.all_sources),
        max_workers=args.max_workers,
        adapter_http_concurrency=args.adapter_http_concurrency,
    )
    outcome = harness.run_driver(command, env_extra=env_extra, timeout_s=args.timeout_s)
    observed = harness.observe(run_dir)
    report_wall = int(observed["wallMs"] or 0)
    return {
        "runDir": str(run_dir),
        "exitCode": int(outcome["exitCode"]),
        "wallMs": report_wall or int(outcome["wallMs"]),
        "wallSource": "report" if report_wall else "subprocess",
        "subprocessWallMs": int(outcome["wallMs"]),
        "units": dict(observed["summary"]),
        "guard": dict((observed["parity"] or {}).get("guard") or {}),
    }


def measure_ceilings(
    *,
    epoch_dir: Path,
    out_dir: Path,
    args: argparse.Namespace,
    baseline_median_ms: int,
    noise_floor_pct: float,
) -> dict[str, Any]:
    """The two D1 ceilings, each a median over fresh processes against the same baseline."""

    plans = (
        ("always-on-counters", {harness.COUNTERS_ENV: "1"}, COUNTERS_CEILING_PCT),
        ("opt-in-profiler", {harness.PROFILE_ENV: "1"}, PROFILE_CEILING_PCT),
    )
    arms: list[dict[str, Any]] = []
    for name, env_extra, ceiling_pct in plans:
        runs = [
            run_record(
                run_sample(
                    epoch_dir=epoch_dir,
                    run_dir=out_dir / f"ceiling-{name}-{index}",
                    args=args,
                    label=f"{args.label or 's6'} {name} {index}",
                    env_extra=env_extra,
                )
            )
            for index in range(max(1, int(args.ceiling_runs)))
        ]
        walls = [int(run["wallMs"]) for run in runs]
        median_ms = int(statistics.median(walls)) if walls else 0
        delta = delta_pct(baseline_median_ms, median_ms)
        arms.append(
            {
                "name": name,
                "env": dict(env_extra),
                "ceilingPct": ceiling_pct,
                "runs": runs,
                "medianMs": median_ms,
                "deltaPct": delta,
                "verdict": ceiling_verdict(delta, noise_floor_pct, ceiling_pct),
            }
        )
    return {
        "baselineMedianMs": int(baseline_median_ms),
        "noiseFloorPct": round(float(noise_floor_pct), 3),
        "arms": arms,
        "rule": (
            "within-noise when |delta| <= the measured spread; meets-ceiling when it is under the "
            "D1 ceiling; exceeds-ceiling otherwise, and then the slice ships counters off"
        ),
    }


def collect(*, epoch_dir: Path, out_dir: Path, args: argparse.Namespace) -> dict[str, Any]:
    """Every sample, then the ceilings, then the verdict -- all in one artifact."""

    epoch = harness.epoch_summary(epoch_dir)
    recorded = dict(epoch["recordedSummary"])
    samples: list[dict[str, Any]] = []
    for index in range(max(1, int(args.samples))):
        sample = run_sample(
            epoch_dir=epoch_dir,
            run_dir=out_dir / f"sample-{index}",
            args=args,
            label=f"{args.label or 's6'} sample {index}",
        )
        sample["index"] = index
        sample["matches"] = compare_units(recorded, sample["units"])
        samples.append(sample)
    determinism = determinism_verdict(samples, recorded, spread_limit_pct=float(args.spread_limit))
    report: dict[str, Any] = {
        "tool": "corpus-determinism",
        "generatedAt": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
        "outDir": str(out_dir),
        "epoch": epoch,
        "config": {
            "samples": int(args.samples),
            "spreadLimitPct": float(args.spread_limit),
            "ceilingRuns": int(args.ceiling_runs),
            "scope": args.scope,
            "sources": int(args.sources),
            "families": args.families,
            "allSources": bool(args.all_sources),
            "maxWorkers": int(args.max_workers),
            "adapterHttpConcurrency": int(args.adapter_http_concurrency),
        },
        "wallClockNote": WALL_CLOCK_NOTE,
        "samples": samples,
        "determinism": determinism,
    }
    failed = [s for s in samples if int(s["exitCode"]) not in (0, harness.EXIT_PARITY_FAILED)]
    if failed:
        # A sample that could not run at all is not a determinism result in either direction.
        determinism["verdict"] = VERDICT_HARNESS_ERROR
        report["harnessErrors"] = [
            {"sample": s["index"], "exitCode": s["exitCode"], "runDir": s["runDir"]} for s in failed
        ]
    if not args.no_ceilings:
        report["ceilings"] = measure_ceilings(
            epoch_dir=epoch_dir,
            out_dir=out_dir,
            args=args,
            baseline_median_ms=int(determinism["spread"]["medianMs"] or 0),
            noise_floor_pct=float(determinism["spread"]["spreadPct"] or 0.0),
        )
    report["verdict"] = determinism["verdict"]
    return report


def _tick(value: Any) -> str:
    """``yes`` / ``no`` / ``?``: a unit nobody measured must not render as a tick."""

    return "yes" if value is True else ("**no**" if value is False else "?")


def render_markdown(report: Mapping[str, Any]) -> str:
    """The markdown a reviewer reads: verdict, per-sample units, ceilings, caveats.

    Lives here rather than in a second module because this is the artifact a reviewer actually
    reads: the verdict, the evidence behind it and the caveats that qualify it belong in one
    place, and ``tools/`` carries no per-file line budget.
    """

    epoch = report.get("epoch") or {}
    determinism = report.get("determinism") or {}
    spread = determinism.get("spread") or {}
    recorded = epoch.get("recordedSummary") or {}
    ceilings = report.get("ceilings") or {}
    lines = [
        f"# S6 determinism — {epoch.get('epochId', '?')}",
        "",
        f"Verdict: **{determinism.get('verdict', '?')}** ({spread.get('samples', 0)} samples, "
        f"report-wall spread {spread.get('spreadPct', 0)}% vs limit "
        f"{determinism.get('spreadLimitPct', 0)}%)",
        "",
        f"- scope `{epoch.get('scope', '')}`, recorded {epoch.get('recordedAt', '')}, rows "
        f"{recorded.get('rowCount', '?')}, identity `{recorded.get('identityDigest', '?')}`, "
        f"noise floor {spread.get('spreadPct', 0)}%",
        "",
        "## Samples",
        "",
        "| # | wall | proc wall | rows | id | proj | statuses | remote |",
        "|---|---|---|---|---|---|---|---|",
    ]
    lines += [
        f"| {s.get('index', '?')} | {s.get('wallMs', 0)} ms | {s.get('subprocessWallMs', 0)} ms | "
        f"{_tick((s.get('matches') or {}).get('rowCount'))} | "
        f"{_tick((s.get('matches') or {}).get('identityDigest'))} | "
        f"{_tick((s.get('matches') or {}).get('rowProjectionDigest'))} | "
        f"{_tick((s.get('matches') or {}).get('sourceStatusCounts'))} | "
        f"{(s.get('guard') or {}).get('blockedRemote', '?')} |"
        for s in report.get("samples") or []
    ]
    divergences = determinism.get("divergences") or []
    if divergences:
        lines.append(
            "Divergences: "
            + "; ".join(
                f"sample {item['sample']} `{item['unit']}` expected `{item['expected']}` "
                f"observed `{item['observed']}`"
                for item in divergences
            )
        )
    lines += ["", "## Overhead ceilings (D1)", ""]
    if not ceilings.get("arms"):
        lines.append("- not measured in this pass (`--no-ceilings`)")
    else:
        lines += [
            f"Baseline (counters off, profiler off): {ceilings.get('baselineMedianMs', 0)} ms",
            "",
            "| arm | env | runs | median | delta | ceiling | verdict |",
            "|---|---|---|---|---|---|---|",
        ]
        lines += [
            f"| {a.get('name', '?')} | "
            f"`{','.join(f'{k}={v}' for k, v in (a.get('env') or {}).items())}` | "
            f"{len(a.get('runs') or [])} | {a.get('medianMs', 0)} ms | {a.get('deltaPct', 0)}% | "
            f"{a.get('ceilingPct', 0)}% | {a.get('verdict', '?')} |"
            for a in ceilings["arms"]
        ]
    lines += [
        "",
        "## Caveats",
        "",
        f"- {report.get('wallClockNote') or WALL_CLOCK_NOTE}.",
        "- replay measures the engine, not the wire: it removes network time, and coverage is the "
        "intercepted lanes only (D5 full-fetch workload).",
        "- a busy host widens the noise floor, which hides a small overhead rather than inventing "
        "one.",
        "",
    ]
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--epoch", default="", help="epoch directory to replay")
    parser.add_argument("--corpus-parent", default=DEFAULT_CORPUS_PARENT)
    parser.add_argument("--samples", type=int, default=DEFAULT_SAMPLES)
    parser.add_argument("--spread-limit", type=float, default=DEFAULT_SPREAD_LIMIT_PCT)
    parser.add_argument("--ceiling-runs", type=int, default=DEFAULT_CEILING_RUNS)
    parser.add_argument("--no-ceilings", action="store_true")
    parser.add_argument("--scope", default="subset")
    parser.add_argument("--sources", type=int, default=4)
    parser.add_argument("--families", default="google_sheets")
    parser.add_argument(
        "--all-sources",
        action="store_true",
        help="replay every registered loader (a full-source epoch) instead of a stride sample",
    )
    parser.add_argument("--max-workers", type=int, default=1)
    parser.add_argument("--adapter-http-concurrency", type=int, default=0)
    parser.add_argument("--out", default="")
    parser.add_argument("--label", default="")
    parser.add_argument("--timeout-s", type=int, default=3600)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if args.out:
        out_dir = Path(args.out)
    else:
        out_dir = Path(DEFAULT_CORPUS_PARENT) / new_run_token("determinism")
    try:
        epoch_dir = harness.resolve_epoch(args.epoch, args.corpus_parent)
        report = collect(epoch_dir=epoch_dir, out_dir=out_dir, args=args)
    except harness.HarnessError as exc:
        print(f"[s6] {exc}", file=sys.stderr)
        return harness.EXIT_INFRASTRUCTURE
    write_json(out_dir / "determinism.json", report)
    markdown = render_markdown(report)
    (out_dir / "determinism.md").write_text(markdown, encoding="utf-8")
    print(markdown)
    if report["verdict"] == VERDICT_HARNESS_ERROR:
        print("[s6] a sample could not run: not a determinism verdict", file=sys.stderr)
        return harness.EXIT_INFRASTRUCTURE
    if report["verdict"] == VERDICT_DETERMINISTIC:
        return harness.EXIT_OK
    return harness.EXIT_PARITY_FAILED


if __name__ == "__main__":
    raise SystemExit(main())
