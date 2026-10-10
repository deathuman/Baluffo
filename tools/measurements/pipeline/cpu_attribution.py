#!/usr/bin/env python3
"""Turn S1's raw inputs into one CPU-attribution report.

Inputs, all optional but at least one required. The report names which were used
and which were missing, because a silent omission is how a partial measurement
gets read as a complete one:

* a full-run fetch report (``*.json.gz`` carrying per-source ``durationMs`` and
  ``stageTimingsMs``) for wall-clock stage share;
* ``cpu-samples-*.json`` from the in-process all-threads sampler
  (``BALUFFO_PROFILE_SAMPLE``) for per-thread *and* per-function weight;
* ``thread-cpu-summary.json`` from ``thread_cpu.py``, which samples the fetch
  child from outside as an independent check on the in-process numbers.

Writes ``CPU-ATTRIBUTION.md`` and ``cpu-attribution.json`` into the run
directory. The verdict is computed from the numbers with printed thresholds and
an explicit ``inconclusive`` outcome, so no reader has to take a shape on trust.

Usage:
    python tools/measurements/pipeline/cpu_attribution.py \
        --run-dir _out/perf-runs/s1-<token> [--wall-clock-seconds N] [--cpu-quota 2.0]
"""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

DEFAULT_CPU_QUOTA = 2.0
TOP_SOURCES = 15
TOP_FRAMES = 40
TOP_THREADS = 20
UNMAPPED_ALERT_SHARE = 0.15
LOOP_DOMINANT_SHARE = 0.5
LOW_UTILIZATION = 0.5

# Ordered: the first matching rule wins, so specific stacks precede the
# orchestrator catch-all. Every frame lands in exactly one bucket or in
# ``unmapped`` -- which is reported, never hidden.
BUCKET_RULES: tuple[tuple[str, tuple[str, ...]], ...] = (
    (
        "body-decode",
        ("/zlib", "zlib.py", "/bz2", "/lzma", "/brotli", "/zstandard", "httpx/_decoders"),
    ),
    (
        "protocol-tls",
        (
            "/httpx/",
            "/httpcore/",
            "/h11/",
            "/h2/",
            "/ssl.py",
            "_ssl",
            "/socket.py",
            "/selectors.py",
            "sslproto",
            "/asyncio/",
            "/urllib/request.py",
            "/http/client.py",
            "/email/",
        ),
    ),
    (
        "html-parse",
        (
            "/src/jobs/adapters/",
            "html_parsers",
            "static_listing",
            "detail_heuristics",
            "scrapy",
            "/re/__init__.py",
            "re/_compiler",
            "sre_",
            "html/parser",
        ),
    ),
    (
        "canonicalise-dedup",
        (
            "/src/jobs/dedupe",
            "canonical",
            "availability_identity",
            "identity",
            "/src/jobs/text_utils",
            "normalize",
        ),
    ),
    (
        "state-io",
        (
            "/src/jobs/state_",
            "/src/storage",
            "/pathlib.py",
            "json/decoder",
            "json/encoder",
            "json/__init__",
            "gzip.py",
            "/src/jobs/common/http.py",
        ),
    ),
    ("orchestration", ("/src/jobs/", "/src/bridge/", "/src/shared/live_task")),
    ("sampler", ("/src/shared/profile_utils.py",)),
)


def classify_frame(frame_key: str) -> str:
    """Map a ``filename:firstlineno:name`` key to an attribution domain."""
    haystack = str(frame_key).replace("\\", "/")
    for domain, needles in BUCKET_RULES:
        for needle in needles:
            if needle in haystack:
                return domain
    return "unmapped"


def bucket_weights(entries: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Sum weight per domain, largest first, with shares over all entries."""
    totals: dict[str, float] = {}
    counts: dict[str, int] = {}
    samples: dict[str, int] = {}
    for entry in entries:
        domain = classify_frame(str(entry.get("frame") or ""))
        weight = float(entry.get("weightSeconds") or 0.0)
        totals[domain] = totals.get(domain, 0.0) + weight
        counts[domain] = counts.get(domain, 0) + 1
        samples[domain] = samples.get(domain, 0) + int(entry.get("samples") or 0)
    grand_total = sum(totals.values())
    rows = []
    for domain, weight in sorted(totals.items(), key=lambda item: (-item[1], item[0])):
        rows.append(
            {
                "domain": domain,
                "weightSeconds": round(weight, 6),
                "share": (weight / grand_total) if grand_total else 0.0,
                "sharePct": round(weight / grand_total * 100.0, 2) if grand_total else 0.0,
                "frames": counts[domain],
                "samples": samples[domain],
            }
        )
    return rows


def unmapped_frames(
    entries: Sequence[Mapping[str, Any]], *, limit: int = 15
) -> list[dict[str, Any]]:
    """Top unmapped frames -- printed when the unmapped bucket is material."""
    rows = [
        {
            "frame": str(entry.get("frame") or ""),
            "weightSeconds": round(float(entry.get("weightSeconds") or 0.0), 6),
            "samples": int(entry.get("samples") or 0),
        }
        for entry in entries
        if classify_frame(str(entry.get("frame") or "")) == "unmapped"
    ]
    rows.sort(key=lambda item: -item["weightSeconds"])
    return rows[:limit]


def _percentile(values: Sequence[int], fraction: float) -> int:
    if not values:
        return 0
    ordered = sorted(values)
    index = min(len(ordered) - 1, max(0, int(round(fraction * (len(ordered) - 1)))))
    return ordered[index]


def stage_share(report: Mapping[str, Any]) -> dict[str, Any]:
    """Per-source wall-clock share from a fetch report's own rows."""
    sources = [row for row in (report.get("sources") or []) if isinstance(row, Mapping)]
    durations = [(str(row.get("name") or ""), int(row.get("durationMs") or 0)) for row in sources]
    total_ms = sum(duration for _, duration in durations)
    stage_totals: dict[str, int] = {}
    for row in sources:
        timings = row.get("stageTimingsMs")
        if not isinstance(timings, Mapping):
            continue
        for stage, value in timings.items():
            if isinstance(value, (int, float)):
                stage_totals[str(stage)] = stage_totals.get(str(stage), 0) + int(value)
    zero_kept = [row for row in sources if int(row.get("keptCount") or 0) == 0]
    errored = [row for row in sources if str(row.get("status") or "ok").lower() != "ok"]
    zero_kept_ms = sum(int(row.get("durationMs") or 0) for row in zero_kept)
    errored_ms = sum(int(row.get("durationMs") or 0) for row in errored)
    values = [duration for _, duration in durations]
    top = sorted(durations, key=lambda item: (-item[1], item[0]))[:TOP_SOURCES]
    top10_ms = sum(sorted(values, reverse=True)[:10])
    return {
        "sourceCount": len(sources),
        "totalDurationMs": total_ms,
        "topSources": [{"name": name, "durationMs": duration} for name, duration in top],
        "stageTotalsMs": dict(sorted(stage_totals.items(), key=lambda item: (-item[1], item[0]))),
        "zeroKept": {
            "count": len(zero_kept),
            "sharePct": round(len(zero_kept) / len(sources) * 100.0, 2) if sources else 0.0,
            "durationMs": zero_kept_ms,
            "durationSharePct": round(zero_kept_ms / total_ms * 100.0, 2) if total_ms else 0.0,
        },
        "errored": {
            "count": len(errored),
            "sharePct": round(len(errored) / len(sources) * 100.0, 2) if sources else 0.0,
            "durationMs": errored_ms,
        },
        "durationStatsMs": {
            "median": _percentile(values, 0.5),
            "p90": _percentile(values, 0.9),
            "max": max(values) if values else 0,
            "top10SharePct": round(top10_ms / total_ms * 100.0, 2) if total_ms else 0.0,
        },
    }


def classify_thread(name: str, top_domain: str) -> str:
    """Thread role from its name and from the domain its own frames sit in.

    Names are evidence, not authority: the transport loop is created without one
    in ``transport.py``, so a thread is only called the loop when its frames say
    so.
    """
    if name == "baluffo-cpu-sampler":
        return "sampler"
    if name.startswith("ThreadPoolExecutor"):
        return "source-worker"
    if name == "MainThread":
        return "driver"
    if top_domain == "protocol-tls":
        return "async-transport-loop"
    return "unclassified"


def thread_table(samples: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Per-thread weight with a role, largest first, sampler row kept."""
    threads = [row for row in (samples.get("threads") or []) if isinstance(row, Mapping)]
    total = sum(
        float(row.get("weightSeconds") or 0.0) for row in threads if not row.get("isSampler")
    )
    rows: list[dict[str, Any]] = []
    for row in threads:
        top_frames = [
            entry for entry in (row.get("topSelfFrames") or []) if isinstance(entry, Mapping)
        ]
        top_frame = str(top_frames[0].get("frame") or "") if top_frames else ""
        top_domain = classify_frame(top_frame) if top_frame else ""
        weight = float(row.get("weightSeconds") or 0.0)
        rows.append(
            {
                "tid": row.get("tid"),
                "nativeId": row.get("nativeId"),
                "name": str(row.get("name") or ""),
                "role": classify_thread(str(row.get("name") or ""), top_domain),
                "isSampler": bool(row.get("isSampler")),
                "weightSeconds": round(weight, 6),
                "share": (weight / total) if total and not row.get("isSampler") else 0.0,
                "sharePct": round(weight / total * 100.0, 2)
                if total and not row.get("isSampler")
                else 0.0,
                "samples": int(row.get("samples") or 0),
                "topDomain": top_domain,
                "topFrame": top_frame,
            }
        )
    rows.sort(key=lambda item: -float(item["weightSeconds"]))
    return rows


def build_verdict(
    *,
    weighting: str,
    tables: Sequence[Mapping[str, Any]],
    wall_clock_seconds: float | None,
    cpu_quota: float | None,
    total_cpu_seconds: float | None = None,
    coverage: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Compute which shape the run's CPU actually has, with printed thresholds.

    ``total_cpu_seconds`` is the best available CPU total for the child. The
    in-process sampler covers only its own window -- it is killed with the child,
    so the finalize tail can be missing -- while the external sampler covers the
    whole life of the process. Utilisation is therefore computed from the more
    complete number, and the shares (which are a within-window property) from the
    thread table.
    """
    worker_roles = {"source-worker", "async-transport-loop", "unclassified", "driver"}
    table_total = sum(
        float(row.get("weightSeconds") or 0.0)
        for row in tables
        if not row.get("isSampler") and row.get("role") in worker_roles
    )
    total = float(total_cpu_seconds) if total_cpu_seconds else table_total
    loop = sum(
        float(row.get("weightSeconds") or 0.0)
        for row in tables
        if row.get("role") == "async-transport-loop"
    )
    workers = sum(
        float(row.get("weightSeconds") or 0.0)
        for row in tables
        if row.get("role") == "source-worker"
    )
    # Shares are a within-window property, so they divide by the thread table's
    # own total; only utilisation uses the widest available CPU total.
    loop_share = (loop / table_total) if table_total else 0.0
    worker_share = (workers / table_total) if table_total else 0.0
    utilization = None
    if wall_clock_seconds and cpu_quota and wall_clock_seconds > 0 and cpu_quota > 0:
        utilization = total / (wall_clock_seconds * cpu_quota)

    evidence = [
        f"weighting={weighting}",
        f"measuredCpuSeconds={round(total, 3)}",
        f"threadTableCpuSeconds={round(table_total, 3)}",
        f"asyncTransportLoopShare={round(loop_share * 100.0, 1)}%",
        f"sourceWorkerShare={round(worker_share * 100.0, 1)}%",
    ]
    if coverage:
        for key in ("inProcessCoverageSeconds", "externalCoverageSeconds", "uncoveredSeconds"):
            if coverage.get(key) is not None:
                evidence.append(f"{key}={coverage[key]}")
    if utilization is not None:
        evidence.append(f"cpuUtilization={round(utilization * 100.0, 1)}% of {cpu_quota} cores")

    if weighting != "proc-cpu" or total <= 0:
        return {
            "shape": "inconclusive-no-cpu-weighting",
            "confidence": "Low",
            "evidence": evidence,
            "reason": (
                "Per-thread CPU could only be weighted by elapsed time, so every thread's "
                "figure includes the time it spent blocked. Re-run the child where "
                "/proc/self/task is readable (the container) to separate CPU from waiting."
            ),
            "levers": [
                "Re-run with BALUFFO_PROFILE_SAMPLE set inside the container, not on a host without /proc."
            ],
        }
    # Checked before loop-dominance on purpose. When the seat is mostly idle,
    # which thread holds the little CPU that was used is a secondary question:
    # the binding constraint is the waiting, and naming a thread would send the
    # next optimisation at a stage that is not the cost.
    if utilization is not None and utilization < LOW_UTILIZATION:
        return {
            "shape": "latency-bound",
            "confidence": "High" if utilization < 0.35 else "Medium",
            "evidence": evidence,
            "reason": (
                f"CPU utilisation is {utilization * 100.0:.1f}% of the {cpu_quota}-core quota "
                f"(threshold {LOW_UTILIZATION * 100:.0f}%). Most wall-clock time is spent waiting, "
                "not computing, so the cost is in politeness, timeouts or retry backoff."
            ),
            "levers": [
                "Attribute the waiting: politeness gate vs timeouts vs retry backoff.",
                "Revisit the long tail before adding CPU optimisations.",
            ],
        }
    if loop_share >= LOOP_DOMINANT_SHARE:
        return {
            "shape": "single-thread-transport-bottleneck",
            "confidence": "High" if loop_share >= 0.7 else "Medium",
            "evidence": evidence,
            "reason": (
                f"The async transport loop holds {loop_share * 100.0:.1f}% of measured CPU "
                f"(threshold {LOOP_DOMINANT_SHARE * 100:.0f}%). Extra worker threads cannot help "
                "while one loop thread processes every response, so the flat 12-to-20-worker CPU "
                "curve is explained by a single-threaded stage rather than by lock contention."
            ),
            "levers": [
                "Move per-response CPU off the loop (decode/parse at the fetch seam).",
                "Run multiple transport loops/lanes and measure again.",
                "Process sharding is not the first lever while one loop thread owns the CPU.",
            ],
        }
    # A distracted seat is not a saturated one. Already-settled rule: the shares
    # say where the CPU that exists went, not whether the seat was the constraint,
    # so a run that only just clears the latency-bound cut must not be written up as
    # if it had confirmed the GIL story -- spread worker shares are consistent with
    # a half-idle seat, and a bigger pool cannot free a seat that was never full.
    near_cut = utilization is not None and utilization < LOW_UTILIZATION + 0.05
    if utilization is None:
        quota_phrase = "with no CPU quota given, so utilisation is unknown"
        tail = "Read the shares rather than a utilisation figure, and re-run with --cpu-quota."
    elif near_cut:
        quota_phrase = (
            f"and utilisation is {utilization * 100.0:.1f}% of the {cpu_quota}-core quota, only "
            f"{(utilization - LOW_UTILIZATION) * 100.0:.1f} points above the "
            f"{LOW_UTILIZATION * 100:.0f}% latency-bound cut, so the seat is close to half idle"
        )
        tail = (
            "The cost is a mix of Python work and waiting, and the shares do not separate the two."
        )
    else:
        quota_phrase = (
            f"and utilisation is {utilization * 100.0:.1f}% of the {cpu_quota}-core quota"
        )
        tail = "Contention, not idleness, is the binding constraint here."
    levers = [
        "Cut the hot Python work named in the function table first -- that is where the CPU is.",
        "Do not read spread worker shares as a GIL verdict by themselves: the loop thread is idle, so a larger worker pool frees nothing.",
    ]
    levers.append(
        "Re-measure before any concurrency change: headroom at this utilisation may be waiting "
        "rather than serialised work."
        if near_cut or utilization is None
        else "Confirm which shared state serialises the workers (locks, state writers) before reaching for process sharding."
    )
    return {
        "shape": "thread-contended-python-work",
        "confidence": "Medium" if near_cut or utilization is None else "High",
        "evidence": evidence,
        "reason": (
            f"CPU is spread across worker threads (loop {loop_share * 100.0:.1f}%, workers "
            f"{worker_share * 100.0:.1f}%), {quota_phrase}. {tail}"
        ),
        "levers": levers,
    }


def _load_fetch_report(path: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    if path.suffix == ".gz":
        raw = gzip.decompress(raw)
    value = json.loads(raw.decode("utf-8"))
    return value if isinstance(value, dict) else {}


def _newest(run_dir: Path, pattern: str) -> Path | None:
    matches = sorted(run_dir.rglob(pattern), key=lambda item: item.stat().st_mtime)
    return matches[-1] if matches else None


def discover_inputs(run_dir: Path) -> dict[str, Path | None]:
    return {
        "fetchReport": _newest(run_dir, "*fetch-report*.json.gz") or _newest(run_dir, "*.json.gz"),
        "cpuSamples": _newest(run_dir, "cpu-samples-*.json"),
        "threadCpu": _newest(run_dir, "thread-cpu-summary.json"),
    }


def _fmt_pct(value: float) -> str:
    return f"{value:.1f}%"


def _short(frame: str) -> str:
    parts = str(frame).replace("\\", "/").split("/")
    tail = "/".join(parts[-2:]) if len(parts) >= 2 else str(frame)
    return tail


def render_markdown(payload: Mapping[str, Any]) -> str:
    inputs = payload["inputs"]
    samples = payload["cpuSamples"] or {}
    thread_rows = payload["threads"]
    buckets = payload["selfBuckets"]
    cumulative = payload["cumulativeBuckets"]
    verdict = payload["verdict"]
    external = payload["external"]
    weighting = str(samples.get("weighting") or "unavailable")

    lines: list[str] = []
    lines.append("# S1 CPU attribution")
    lines.append("")
    lines.append(f"Run: `{payload['runDir']}`")
    lines.append("")
    lines.append("## What this measured, and what it did not")
    lines.append("")
    lines.append("| Input | Source | Status |")
    lines.append("|---|---|---|")
    lines.append(
        f"| Fetch report (stage share) | {inputs.get('fetchReport') or '-'} | {inputs['status']['fetchReport']} |"
    )
    lines.append(
        f"| In-process samples | {inputs.get('cpuSamples') or '-'} | {inputs['status']['cpuSamples']} |"
    )
    lines.append(
        f"| External thread sampler | {inputs.get('threadCpu') or '-'} | {inputs['status']['threadCpu']} |"
    )
    lines.append("")
    lines.append(
        f"Weighting: **{weighting}**. "
        + (
            "Per-thread CPU, read from `/proc/self/task/<tid>/stat` inside the fetch child."
            if weighting == "proc-cpu"
            else "Elapsed time, because `/proc` was unreadable -- every figure below includes "
            "blocked time and must not be quoted as CPU."
        )
    )
    lines.append("")
    lines.append(
        "Scope limits: the pipeline **fetch lane only**; the browser-fallback Node driver and the "
        "discovery subsystem are separate processes and are not attributed here. One run, one day: "
        "sampling attributes sampled time, and a short-lived thread can start and finish between "
        "ticks."
    )
    lines.append("")

    stage = payload["stageShare"]
    if stage:
        lines.append("## Where the wall-clock went (per source, from the fetch report)")
        lines.append("")
        lines.append(
            f"- {stage['sourceCount']} sources, summed duration {stage['totalDurationMs'] / 1000.0:.1f} s"
        )
        stats = stage["durationStatsMs"]
        lines.append(
            f"- median {stats['median'] / 1000.0:.2f} s, p90 {stats['p90'] / 1000.0:.2f} s, "
            f"max {stats['max'] / 1000.0:.2f} s; top 10 sources = {stats['top10SharePct']}% of the sum"
        )
        zero = stage["zeroKept"]
        lines.append(
            f"- zero-kept sources: {zero['count']} ({zero['sharePct']}% of sources) carrying "
            f"{zero['durationSharePct']}% of the summed duration"
        )
        lines.append(
            f"- non-ok sources: {stage['errored']['count']} ({stage['errored']['sharePct']}%), "
            f"{stage['errored']['durationMs'] / 1000.0:.1f} s"
        )
        if payload.get("wallClockSeconds"):
            concurrency = stage["totalDurationMs"] / 1000.0 / float(payload["wallClockSeconds"])
            lines.append(
                f"- run wall clock {payload['wallClockSeconds']} s gives a source-concurrency factor of "
                f"{concurrency:.2f}x (summed source durations / wall clock)"
            )
        lines.append("")
        lines.append("Slowest sources:")
        lines.append("")
        lines.append("| Source | Duration (s) |")
        lines.append("|---|---|")
        for row in stage["topSources"][:TOP_SOURCES]:
            lines.append(f"| {row['name']} | {row['durationMs'] / 1000.0:.2f} |")
        lines.append("")
        if stage["stageTotalsMs"]:
            lines.append("Per-stage totals (summed across sources):")
            lines.append("")
            lines.append("| Stage | Seconds |")
            lines.append("|---|---|")
            for name, value in list(stage["stageTotalsMs"].items())[:15]:
                lines.append(f"| {name} | {value / 1000.0:.2f} |")
            lines.append("")
    else:
        lines.append("## Where the wall-clock went")
        lines.append("")
        lines.append("_No fetch report was found, so stage share is missing from this report._")
        lines.append("")

    lines.append("## Which thread holds the CPU")
    lines.append("")
    coverage = payload.get("coverage") or {}
    if thread_rows:
        lines.append(
            "| Python ident | OS tid | Name | Role | Weight (s) | Share | Samples | Top domain |"
        )
        lines.append("|---|---|---|---|---|---|---|---|")
        for row in thread_rows[:TOP_THREADS]:
            label = f"{row['tid']}"
            native = f"{row['nativeId']}" if row.get("nativeId") else "-"
            name = row["name"] or "(unnamed)"
            share = "sampler" if row["isSampler"] else _fmt_pct(float(row["sharePct"]) or 0.0)
            lines.append(
                f"| {label} | {native} | {name} | {row['role']} | {row['weightSeconds']:.3f} | "
                f"{share} | {row['samples']} | {row['topDomain'] or '-'} |"
            )
        lines.append("")
        lines.append(
            "Roles come from thread names *and* the frames each thread was sampled in: the transport "
            "loop is created unnamed in `transport.py`, so it is only called the loop when its own "
            "frames are in the protocol/TLS domain. `Python ident` is `threading.get_ident()`, which "
            "is **not** an OS thread id; `OS tid` is what `/proc` and the external sampler use, and "
            "it is the join key between the two tables."
        )
        if coverage.get("inProcessCoverageSeconds") is not None:
            lines.append("")
            lines.append(
                f"Coverage: the in-process sampler spans {coverage['inProcessCoverageSeconds']:.0f} s "
                f"(it is killed with the child, so its last periodic dump is the end of its record), "
                f"the external sampler spans {coverage.get('externalCoverageSeconds') or 0:.0f} s, and "
                f"{coverage.get('uncoveredSeconds') or 0:.0f} s of the child's life is covered only "
                "by the external one. A thread whose work happens in that tail will look smaller here "
                "than it is."
            )
    else:
        lines.append("_No in-process samples were found, so no thread breakdown is available._")
    lines.append("")
    if external:
        reconciliation = external.get("reconciliation") or {}
        lines.append("### Independent check: the same child, sampled from outside")
        lines.append("")
        lines.append(
            f"{external.get('tickCount')} ticks over {external.get('spanSeconds')} s "
            f"({external.get('tickRateHz')} Hz, max gap {external.get('maxGapSeconds')} s); "
            f"child CPU {reconciliation.get('processCpuSeconds')} s vs summed threads "
            f"{reconciliation.get('summedThreadCpuSeconds')} s "
            f"({reconciliation.get('deltaPct')}%)."
        )
        lines.append("")
        lines.append("| tid | comm | CPU (s) | Ticks |")
        lines.append("|---|---|---|---|")
        for row in external["threads"][:TOP_THREADS]:
            lines.append(
                f"| {row['tid']} | {row['comm'] or '-'} | {row['cpuSeconds']:.3f} | {row['ticks']} |"
            )
        lines.append("")
        lines.append(
            "Both instruments read the same child, so `tid` here joins to `OS tid` above. They cover "
            "different windows, though, and that asymmetry is the useful part: this table is the whole "
            "process life, the one above is bounded by its last periodic dump."
        )
        lines.append("")

    lines.append("## Which functions burn it")
    lines.append("")
    if buckets:
        lines.append("| Domain | Weight (s) | Share | Frames |")
        lines.append("|---|---|---|---|")
        for row in buckets:
            lines.append(
                f"| {row['domain']} | {row['weightSeconds']:.3f} | {row['sharePct']}% | {row['frames']} |"
            )
        lines.append("")
        unmapped = next((row for row in buckets if row["domain"] == "unmapped"), None)
        if unmapped and float(unmapped["share"]) > UNMAPPED_ALERT_SHARE:
            lines.append(
                f"**Unmapped is material ({unmapped['sharePct']}% > "
                f"{UNMAPPED_ALERT_SHARE * 100:.0f}%):** the top frames below are not in any bucket, so "
                "the table above under-describes the run. They belong in `BUCKET_RULES`."
            )
            lines.append("")
            lines.append("| Unmapped frame | Weight (s) | Samples |")
            lines.append("|---|---|---|")
            for row in payload["unmappedFrames"]:
                lines.append(
                    f"| `{_short(row['frame'])}` | {row['weightSeconds']:.3f} | {row['samples']} |"
                )
            lines.append("")
        lines.append("Top functions by self weight:")
        lines.append("")
        lines.append("| Function | Weight (s) | Samples | Domain |")
        lines.append("|---|---|---|---|")
        for row in payload["topSelf"]:
            lines.append(
                f"| `{_short(row['frame'])}` | {row['weightSeconds']:.3f} | {row['samples']} | "
                f"{classify_frame(row['frame'])} |"
            )
        lines.append("")
        if cumulative:
            lines.append(
                "By call path (cumulative weight), which is what tells protocol work apart from parsing:"
            )
            lines.append("")
            lines.append("| Domain | Weight (s) | Share |")
            lines.append("|---|---|---|")
            for row in cumulative:
                lines.append(
                    f"| {row['domain']} | {row['weightSeconds']:.3f} | {row['sharePct']}% |"
                )
            lines.append("")
    else:
        lines.append("_No function attribution available._")
        lines.append("")

    lines.append("## Verdict")
    lines.append("")
    lines.append(f"**{verdict['shape']}** (confidence: {verdict['confidence']})")
    lines.append("")
    lines.append(verdict["reason"])
    lines.append("")
    lines.append("Evidence:")
    lines.append("")
    for item in verdict["evidence"]:
        lines.append(f"- `{item}`")
    lines.append("")
    lines.append(
        f"Thresholds used: latency-bound when CPU utilisation < {LOW_UTILIZATION * 100:.0f}% of the "
        f"quota (checked first -- on a mostly idle seat the binding constraint is the waiting, not "
        f"the thread); single-thread transport bottleneck when the loop thread holds ≥ "
        f"{LOOP_DOMINANT_SHARE * 100:.0f}% of measured CPU; otherwise thread-contended."
    )
    lines.append("")
    lines.append("Levers this points at (the choice itself is D4, made on this evidence):")
    lines.append("")
    for item in verdict["levers"]:
        lines.append(f"- {item}")
    lines.append("")
    lines.append("### What this does not claim")
    lines.append("")
    lines.append(
        "- Not a production figure: the seat is CPU-capped and the run is one day of live boards."
    )
    lines.append(
        "- Not a speedup: no optimisation is included, and none is implied to have happened."
    )
    lines.append(
        "- Not the whole pipeline: discovery, the admin registry API, finalize I/O and the "
        "browser-fallback Node driver are outside the fetch lane and unattributed here."
    )
    lines.append("")
    return "\n".join(lines)


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--run-dir", required=True, help="artifact directory for one S1 run")
    parser.add_argument(
        "--fetch-report", default="", help="explicit per-source report (.json or .json.gz)"
    )
    parser.add_argument("--cpu-samples", default="", help="explicit cpu-samples-*.json")
    parser.add_argument("--thread-cpu-summary", default="", help="explicit thread-cpu-summary.json")
    parser.add_argument(
        "--wall-clock-seconds", type=float, default=0.0, help="run wall clock, for utilisation"
    )
    parser.add_argument(
        "--cpu-quota", type=float, default=DEFAULT_CPU_QUOTA, help="container CPU limit"
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    run_dir = Path(str(args.run_dir)).expanduser().resolve()
    if not run_dir.is_dir():
        print(f"run dir not found: {run_dir}", file=sys.stderr)
        return 2

    found = discover_inputs(run_dir)
    explicit = {
        "fetchReport": str(args.fetch_report).strip(),
        "cpuSamples": str(args.cpu_samples).strip(),
        "threadCpu": str(args.thread_cpu_summary).strip(),
    }
    resolved: dict[str, Path | None] = {}
    status: dict[str, str] = {}
    for key, value in explicit.items():
        if value:
            path = Path(value).expanduser().resolve()
            resolved[key] = path if path.is_file() else None
            status[key] = "provided" if path.is_file() else "provided but missing"
        else:
            resolved[key] = found[key]
            status[key] = "discovered" if found[key] else "missing"

    samples: dict[str, Any] = {}
    if resolved["cpuSamples"]:
        try:
            samples = json.loads(Path(resolved["cpuSamples"]).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            status["cpuSamples"] = f"unreadable ({exc})"
            samples = {}
    external: dict[str, Any] = {}
    if resolved["threadCpu"]:
        try:
            external = json.loads(Path(resolved["threadCpu"]).read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            status["threadCpu"] = f"unreadable ({exc})"
            external = {}
    stage: dict[str, Any] = {}
    if resolved["fetchReport"]:
        try:
            stage = stage_share(_load_fetch_report(Path(resolved["fetchReport"])))
        except (OSError, json.JSONDecodeError, EOFError) as exc:
            status["fetchReport"] = f"unreadable ({exc})"
            stage = {}

    self_entries = [row for row in (samples.get("selfWeight") or []) if isinstance(row, Mapping)]
    cumulative_entries = [
        row for row in (samples.get("cumulativeWeight") or []) if isinstance(row, Mapping)
    ]
    threads = thread_table(samples)
    table_total = sum(float(row["weightSeconds"]) for row in threads if not row.get("isSampler"))
    external_rows = [row for row in (external.get("threads") or []) if isinstance(row, Mapping)]
    external_total = sum(float(row.get("cpuSeconds") or 0.0) for row in external_rows)
    in_process_seconds = float(samples.get("durationMs") or 0.0) / 1000.0
    external_seconds = float(external.get("spanSeconds") or 0.0)
    coverage = {
        "inProcessCpuSeconds": round(table_total, 3),
        "externalCpuSeconds": round(external_total, 3),
        "inProcessCoverageSeconds": round(in_process_seconds, 1) or None,
        "externalCoverageSeconds": round(external_seconds, 1) or None,
        "uncoveredSeconds": (
            round(max(0.0, external_seconds - in_process_seconds), 1)
            if in_process_seconds and external_seconds
            else None
        ),
    }
    verdict = build_verdict(
        weighting=str(samples.get("weighting") or "unavailable"),
        tables=threads,
        wall_clock_seconds=float(args.wall_clock_seconds) or None,
        cpu_quota=float(args.cpu_quota) or None,
        total_cpu_seconds=external_total or table_total,
        coverage=coverage,
    )

    payload = {
        "runDir": str(run_dir),
        "inputs": {
            "fetchReport": str(resolved["fetchReport"]) if resolved["fetchReport"] else "",
            "cpuSamples": str(resolved["cpuSamples"]) if resolved["cpuSamples"] else "",
            "threadCpu": str(resolved["threadCpu"]) if resolved["threadCpu"] else "",
            "status": status,
        },
        "wallClockSeconds": float(args.wall_clock_seconds) or None,
        "cpuQuota": float(args.cpu_quota) or None,
        "stageShare": stage,
        "cpuSamples": {
            "weighting": samples.get("weighting"),
            "intervalMs": samples.get("intervalMs"),
            "samples": samples.get("samples"),
            "durationMs": samples.get("durationMs"),
            "profile": samples.get("profile"),
        },
        "threads": threads,
        "coverage": coverage,
        "selfBuckets": bucket_weights(self_entries),
        "cumulativeBuckets": bucket_weights(cumulative_entries),
        "topSelf": sorted(
            (
                {
                    "frame": str(row.get("frame") or ""),
                    "weightSeconds": round(float(row.get("weightSeconds") or 0.0), 6),
                    "samples": int(row.get("samples") or 0),
                }
                for row in self_entries
            ),
            key=lambda item: -item["weightSeconds"],
        )[:TOP_FRAMES],
        "unmappedFrames": unmapped_frames(self_entries),
        # Empty when nothing was loaded: a dict of Nones is truthy, and a truthy
        # "external check" section with no numbers in it is worse than none.
        "external": (
            {
                "tickCount": external.get("tickCount"),
                "spanSeconds": external.get("spanSeconds"),
                "maxGapSeconds": external.get("maxGapSeconds"),
                "tickRateHz": external.get("tickRateHz"),
                "malformedLines": external.get("malformedLines"),
                "reconciliation": external.get("reconciliation") or {},
                "threads": list(external.get("threads") or [])[:TOP_THREADS],
            }
            if external
            else {}
        ),
        "verdict": verdict,
    }

    json_path = run_dir / "cpu-attribution.json"
    json_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    markdown_path = run_dir / "CPU-ATTRIBUTION.md"
    markdown_path.write_text(render_markdown(payload), encoding="utf-8")

    print(f"[cpu-attribution] verdict: {verdict['shape']} (confidence {verdict['confidence']})")
    for item in verdict["evidence"]:
        print(f"  {item}")
    print(f"[cpu-attribution] wrote {markdown_path} and {json_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
