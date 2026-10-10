"""Rank one past run's bottlenecks: "where did the time go?" without opening raw JSON.

The plan's first question is *"where did the time go?" -- minutes, not hours, from any recent
run*. The answer is already in the report -- a stage ledger, per-source durations, a zero-kept
tail -- but that report is 7 MB of nested JSON, so reading it costs more than the run did.

Four decisions make the ranking trustworthy rather than merely readable:

**Absence is stated, never zeroed.** A missing field prints as ``not present in this run``. Every
expensive mistake this repo has made was a confident zero, and a page rendering ``retries: 0``
for a run whose counters were off is that same mistake in miniature.

**Unlabelled time stays visible.** Time no stage claimed is printed as its own ``unattributedMs``
row; folding it into a named stage would flatter whichever stage absorbed it.

**The configuration travels with the numbers.** Worker count, fetch client and pool sizing decide
what a duration even means, so they print on the same page; otherwise two runs of one corpus get
compared as if they were the same run.

**A replay says so.** Corpus-served bytes make the wall clock an engine figure, and the page
carries that banner so nobody quotes it as a production number.
"""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

_ROOT = Path(__file__).resolve().parents[3]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from tools.measurements.corpus import harness  # noqa: E402

# One shared reader for every corpus number: the local body was a third copy of this.
from tools.measurements.corpus.harness import to_int as _int

DEFAULT_CORPUS_PARENT = _ROOT / "_out" / "perf-runs"
DEFAULT_TOP = 8
EXIT_OK = 0
EXIT_INFRASTRUCTURE = 2
NOT_PRESENT = "not present in this run"
PARITY_FILE = "replay-parity.json"
REPLAY_BANNER = (
    "Replay: bytes came from a corpus, so these are engine figures. Replay measures the engine, "
    "not the wire -- never quote this wall clock as a production number."
)


class ExplainError(RuntimeError):
    """The run directory has no readable fetch report."""


def share_pct(part_ms: Any, total_ms: Any) -> float:
    """A share of a total, or ``0.0`` when the total is unknown -- never a division error."""

    total = _int(total_ms)
    return 0.0 if total <= 0 else round((_int(part_ms) / total) * 100.0, 2)


def rank_stages(stage_shares: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Every stage by descending time, each with its share of the summed source time."""

    total = _int(stage_shares.get("totalSourceMs"))
    by_stage = stage_shares.get("byStageMs")
    rows: list[dict[str, Any]] = []
    if isinstance(by_stage, Mapping):
        for name, value in by_stage.items():
            amount = _int(value)
            rows.append({"stage": str(name), "ms": amount, "sharePct": share_pct(amount, total)})
    return sorted(rows, key=lambda item: (-int(item["ms"]), str(item["stage"])))


def top_sources(rows: Sequence[Any], top: int = DEFAULT_TOP) -> list[dict[str, Any]]:
    """The slowest sources, with what they kept.

    Non-mapping rows are skipped rather than coerced: a row this tool cannot read is not a row
    that kept nothing.
    """

    cleaned = [row for row in rows if isinstance(row, Mapping)]
    ordered = sorted(
        cleaned, key=lambda row: (-_int(row.get("durationMs")), str(row.get("name") or ""))
    )
    total = sum(_int(row.get("durationMs")) for row in cleaned)
    ranked: list[dict[str, Any]] = []
    for row in ordered[: max(0, int(top))]:
        duration = _int(row.get("durationMs"))
        ranked.append(
            {
                "name": str(row.get("name") or ""),
                "ms": duration,
                "sharePct": share_pct(duration, total),
                "keptCount": row.get("keptCount"),
                "status": str(row.get("status") or ""),
                "failureBucket": str(row.get("failureBucket") or ""),
                "zeroKept": row.get("keptCount") is not None and _int(row.get("keptCount")) == 0,
            }
        )
    return ranked


def zero_kept_cost(rows: Sequence[Any]) -> dict[str, Any]:
    """How much of the run went to sources that kept nothing (the plan's known cost centre)."""

    total = zero_ms = zero_count = unknown_count = 0
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        duration = _int(row.get("durationMs"))
        total += duration
        if row.get("keptCount") is None:
            unknown_count += 1
        elif _int(row.get("keptCount")) == 0:
            zero_count += 1
            zero_ms += duration
    return {
        "zeroKeptSources": zero_count,
        "zeroKeptMs": zero_ms,
        "sharePct": share_pct(zero_ms, total),
        "totalSourceMs": total,
        "keptCountUnknownSources": unknown_count,
    }


def retry_exposure(backoff: Mapping[str, Any]) -> dict[str, Any]:
    """The repeated-fetch share when the counters ran, and an explicit unknown when they did not.

    The counters are default-off, so most past runs cannot answer this. Saying "unknown" is the
    honest answer; reporting ``0%`` would read as "no source retried".
    """

    if _int(backoff.get("sourcesWithCounters")) <= 0:
        return {
            "available": False,
            "reason": (
                "counters were off in this run (BALUFFO_FETCH_COUNTERS unset), so retry and "
                "backoff time is unknown"
            ),
        }
    return {
        "available": True,
        "sourcesWithCounters": _int(backoff.get("sourcesWithCounters")),
        "repeatedFetchSources": _int(backoff.get("repeatedFetchSources")),
        "repeatedFetchShareMs": _int(backoff.get("repeatedFetchShareMs")),
        "sharePct": float(backoff.get("sharePct") or 0.0),
        "proxy": str(backoff.get("proxy") or ""),
    }


def is_replay(run_name: str, parity: Mapping[str, Any], run_config: Mapping[str, Any]) -> bool:
    """A replay by its own artifacts first, by its directory name only as a fallback."""

    if str(run_config.get("mode") or "") == "replay":
        return True
    if str(parity.get("mode") or "") == "replay":
        return True
    return str(run_name).startswith("replay-")


def resolve_run_dir(run: str, corpus_parent: Path | str = "") -> Path:
    """An explicit directory, or a bare token resolved under the perf-runs parent."""

    candidate = Path(str(run))
    if candidate.is_dir():
        return candidate
    parent = Path(corpus_parent) if str(corpus_parent) else DEFAULT_CORPUS_PARENT
    token_dir = parent / str(run)
    if token_dir.is_dir():
        return token_dir
    raise ExplainError(f"no run directory for {run!r} (looked at {candidate} and {token_dir})")


def _report_path(run_dir: Path) -> Path:
    for base in (run_dir, run_dir / "data"):
        candidate = base / "jobs-fetch-report.json"
        if candidate.is_file():
            return candidate
    raise ExplainError(f"no jobs-fetch-report.json under {run_dir}")


def analyze(rundir: Path | str, top: int = DEFAULT_TOP) -> dict[str, Any]:
    """The whole ranking for one run, as plain data the renderer and the artifacts share."""

    run_dir = Path(rundir)
    report_path = _report_path(run_dir)
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ExplainError(f"unreadable fetch report {report_path}: {exc}") from exc
    if not isinstance(report, Mapping):
        raise ExplainError(f"fetch report {report_path} is not a JSON object")

    parity = harness.read_json_artifact(run_dir, PARITY_FILE)
    parity_map = dict(parity) if isinstance(parity, Mapping) else {}
    runtime = dict(report.get("runtime") or {})
    run_config = dict(parity_map.get("runConfig") or {}) or runtime
    rows = harness.source_rows(run_dir)
    stages = harness.stage_shares(run_dir)
    parallelism = harness.parallelism(run_dir)
    task_counts = dict((report.get("taskProgress") or {}).get("counts") or {})
    summary = dict(report.get("summary") or {})

    return {
        "run": {
            "dir": str(run_dir),
            "name": run_dir.name,
            "reportPath": str(report_path),
            "isReplay": is_replay(run_dir.name, parity_map, run_config),
            "epochId": str(parity_map.get("epochId") or ""),
            "label": str(parity_map.get("label") or report.get("runId") or ""),
            "runId": str(report.get("runId") or ""),
            "parityStatus": str(parity_map.get("status") or ""),
        },
        "wall": {
            "reportWindowMs": harness.run_wall_ms(run_dir) or None,
            "summedSourceMs": _int(stages.get("totalSourceMs")),
            "parallelismRatio": float(parallelism.get("ratio") or 0.0),
        },
        "counts": {
            "inputCount": summary.get("inputCount"),
            "mergedCount": summary.get("mergedCount"),
            "outputCount": summary.get("outputCount"),
            "sourceCount": task_counts.get("sourceCount"),
            "resolvedSources": task_counts.get("resolvedSources"),
            "failedSources": task_counts.get("failedSources"),
            "excludedSources": task_counts.get("excludedSources"),
        },
        "stages": rank_stages(stages),
        "topSources": top_sources(rows, top=top),
        "zeroKept": zero_kept_cost(rows),
        "retry": retry_exposure(harness.backoff_exposure(run_dir)),
        "config": {
            "maxWorkers": run_config.get("maxWorkers") or runtime.get("maxWorkers"),
            "fetchClient": runtime.get("fetchClient"),
            "adapterHttpConcurrency": runtime.get("adapterHttpConcurrency"),
            "forceRefreshAll": runtime.get("forceRefreshAll"),
            "browserFallbackEnabled": runtime.get("browserFallbackEnabled"),
            "selectedSourceCount": runtime.get("selectedSourceCount"),
        },
    }


def _value(value: Any) -> str:
    if value is None or value == "":
        return NOT_PRESENT
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _seconds(ms: Any) -> str:
    amount = _int(ms)
    return NOT_PRESENT if amount <= 0 else f"{amount / 1000:.1f} s"


def render_markdown(analysis: Mapping[str, Any]) -> str:
    """The page a human reads instead of the JSON."""

    run = dict(analysis.get("run") or {})
    wall = dict(analysis.get("wall") or {})
    counts = dict(analysis.get("counts") or {})
    config = dict(analysis.get("config") or {})
    zero = dict(analysis.get("zeroKept") or {})
    retry = dict(analysis.get("retry") or {})
    top_rows = list(analysis.get("topSources") or [])
    lines: list[str] = [f"# Where did the time go? -- {_value(run.get('name'))}", ""]
    if run.get("isReplay"):
        lines.extend([f"> {REPLAY_BANNER}", ""])
    if run.get("epochId"):
        lines.append(
            f"- epoch: `{run.get('epochId')}` (parity: `{_value(run.get('parityStatus'))}`)"
        )
    lines.extend(
        [
            f"- wall clock (report window): **{_seconds(wall.get('reportWindowMs'))}**",
            f"- summed source time: {_seconds(wall.get('summedSourceMs'))}",
            f"- parallelism (summed source / wall): {_value(wall.get('parallelismRatio'))} --"
            " near 1 means sources ran one at a time, so wall-clock changes come from per-source"
            " work",
            f"- rows: in {_value(counts.get('inputCount'))} / out"
            f" {_value(counts.get('outputCount'))} (merged {_value(counts.get('mergedCount'))})",
            f"- sources: {_value(counts.get('resolvedSources'))} resolved,"
            f" {_value(counts.get('failedSources'))} failed,"
            f" {_value(counts.get('excludedSources'))} excluded",
            "",
            "## Where the source time went",
            "",
            "| stage | ms | share of summed source time |",
            "|---|---:|---:|",
        ]
    )
    stages = list(analysis.get("stages") or [])
    for stage in stages or [{"stage": NOT_PRESENT, "ms": 0, "sharePct": 0.0}]:
        lines.append(
            f"| {_value(stage.get('stage'))} | {_int(stage.get('ms'))} |"
            f" {_value(stage.get('sharePct'))}% |"
        )
    lines.extend(
        [
            "",
            f"## Slowest sources (top {len(top_rows)})",
            "",
            "| source | ms | share | kept | status | bucket |",
            "|---|---:|---:|---:|---|---|",
        ]
    )
    for row in top_rows or [{"name": NOT_PRESENT, "ms": 0, "sharePct": 0.0}]:
        lines.append(
            f"| {_value(row.get('name'))} | {_int(row.get('ms'))} | {_value(row.get('sharePct'))}%"
            f" | {_value(row.get('keptCount'))} | {_value(row.get('status'))}"
            f" | {_value(row.get('failureBucket'))} |"
        )
    lines.extend(
        [
            "",
            "## Zero-kept cost",
            "",
            f"- {_value(zero.get('zeroKeptSources'))} sources kept nothing, spending"
            f" {_seconds(zero.get('zeroKeptMs'))} ({_value(zero.get('sharePct'))}% of summed source"
            " time)",
            f"- sources whose kept count is unknown: {_value(zero.get('keptCountUnknownSources'))}",
            "",
            "## Retry / backoff exposure",
            "",
        ]
    )
    if retry.get("available"):
        lines.append(
            f"- {_value(retry.get('repeatedFetchSources'))} of"
            f" {_value(retry.get('sourcesWithCounters'))} countered sources repeated a fetch,"
            f" spending {_seconds(retry.get('repeatedFetchShareMs'))}"
            f" ({_value(retry.get('sharePct'))}% of summed"
            " source time)"
        )
        lines.append(f"- proxy: {_value(retry.get('proxy'))}")
    else:
        lines.append(f"- {_value(retry.get('reason'))}")
    lines.extend(
        [
            "",
            "## Run configuration",
            "",
            f"- workers: {_value(config.get('maxWorkers'))}; fetch client:"
            f" {_value(config.get('fetchClient'))}; adapter HTTP concurrency:"
            f" {_value(config.get('adapterHttpConcurrency'))}",
            f"- forceRefreshAll: {_value(config.get('forceRefreshAll'))}; browser fallback:"
            f" {_value(config.get('browserFallbackEnabled'))}; selected sources:"
            f" {_value(config.get('selectedSourceCount'))}",
            f"- run directory: `{_value(run.get('dir'))}`",
            "",
        ]
    )
    return "\n".join(lines)


def write_artifacts(analysis: Mapping[str, Any], out_dir: Path | str) -> dict[str, str]:
    """Write the JSON and the page. The JSON is the machine copy, the page is the deliverable."""

    target = Path(out_dir)
    target.mkdir(parents=True, exist_ok=True)
    json_path = target / "explain.json"
    md_path = target / "explain.md"
    json_path.write_text(
        json.dumps(dict(analysis), indent=2, sort_keys=True, default=str) + "\n", encoding="utf-8"
    )
    md_path.write_text(render_markdown(analysis), encoding="utf-8")
    return {"json": str(json_path), "md": str(md_path)}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument(
        "--run",
        required=True,
        help="run token under the perf-runs parent, a run directory, or a data directory",
    )
    parser.add_argument("--top", type=int, default=DEFAULT_TOP, help="how many sources to rank")
    parser.add_argument("--out", default="", help="where explain.json/md go; default: the run dir")
    parser.add_argument("--corpus-parent", default=str(DEFAULT_CORPUS_PARENT))
    parser.add_argument("--label", default="", help="free-form note stored with the artifacts")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run_dir = resolve_run_dir(args.run, args.corpus_parent)
        analysis = analyze(run_dir, top=max(0, int(args.top)))
    except (ExplainError, harness.HarnessError) as exc:
        print(f"[explain] {exc}", file=sys.stderr)
        return EXIT_INFRASTRUCTURE
    if str(args.label or "").strip():
        analysis["label"] = str(args.label)
    out_dir = Path(args.out) if str(args.out or "").strip() else run_dir
    written = write_artifacts(analysis, out_dir)
    print(render_markdown(analysis))
    print(f"[explain] wrote {written['json']} and {written['md']}")
    return EXIT_OK


if __name__ == "__main__":
    raise SystemExit(main())
