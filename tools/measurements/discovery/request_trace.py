"""Per-request host trace for the lane-parity study.

The parity report can name the host that answered 429 for a missing board, but not whether
that 429 was a *rate* that rose with the lane count or an *episode* the arm happened to land
in. This module records every page and probe request an arm makes -- phase, host, status,
start and end -- so `lane_parity trace` can show, per host, the request count, the 429 count
and the window the 429s occupied.

Two measurement seams, both restored after the arm:

* `directory_fetch.fetch_pages_batched` is wrapped so each page callback keeps the job's own
  failure stage (`homepage_fetch`, `gamedevmap_recovery_fetch`) and the status its fetcher
  raised. Homepage and recovery both pass through it, including their sync path.
* `probe_runtime.fetch_text_httpx` is wrapped because the probe's retry ladder calls it
  directly; every attempt is a row, including the ones that fail. The probe's default fetcher
  enters that function, so the shared client path is what is measured.

Nothing in `src/` is modified: the wrappers are installed for the arm and removed in a
`finally`. A 2xx page fetch is recorded as status 200 because its fetcher returns only after
`raise_for_status`; the trace is about the failures, and the statuses that matter are exact.

The episode/spread label is a heuristic, not a statistical test: a host whose 429s occupy at
most a quarter of its own request span is `episode`, one whose 429s occupy at least half is
`spread`, one 429 is `single`, and anything between is `mixed`. The raw window is printed
beside the label so the reader can overrule it.
"""

from __future__ import annotations

import json
import re
import threading
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

TRACE_FILE = "request-trace.jsonl"
PROBE_PHASE = "probe_fetch"
PAGE_PHASE = "page_fetch"
PHASE_ORDER = ("homepage_fetch", "gamedevmap_recovery_fetch", PROBE_PHASE, PAGE_PHASE)
_EPISODE_SHARE = 0.25
_SPREAD_SHARE = 0.5
_MAX_HOST_ROWS = 6


def _host(url: Any) -> str:
    host = urlparse(str(url or "")).netloc.strip().lower()
    return host or "-"


def _status_from_error(exc: BaseException) -> int | None:
    """The HTTP status an exception carries, if it carries one."""
    response = getattr(exc, "response", None)
    code = getattr(response, "status_code", None)
    if code is None:
        code = getattr(exc, "code", None)
    if code is not None:
        try:
            return int(code)
        except (TypeError, ValueError):
            return None
    match = re.search(r"\b(?:HTTP Error |Client error '|Server error ')(\d{3})", str(exc))
    return int(match.group(1)) if match else None


def _phase_for_job(job: Any) -> str:
    if not isinstance(job, dict):
        return PAGE_PHASE
    return str(job.get("failureStage") or PAGE_PHASE).strip() or PAGE_PHASE


class RequestTrace:
    """Rows for one arm, written as JSONL once the arm ends."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._t0 = time.perf_counter()
        self._rows: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    def record(
        self,
        *,
        phase: str,
        url: str,
        status: int | None,
        start_s: float,
        end_s: float,
        error: str = "",
    ) -> None:
        row: dict[str, Any] = {
            "phase": str(phase or PAGE_PHASE),
            "host": _host(url),
            "url": str(url),
            "status": int(status) if status is not None else None,
            "startMs": round(max(0.0, (start_s - self._t0) * 1000.0), 1),
            "endMs": round(max(0.0, (end_s - self._t0) * 1000.0), 1),
        }
        if error:
            row["error"] = str(error)[:120]
        with self._lock:
            self._rows.append(row)

    @property
    def rows(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._rows)

    def write(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        rows = sorted(self.rows, key=lambda row: (row["startMs"], row["endMs"], row["host"]))
        with self.path.open("w", encoding="utf-8") as handle:
            for row in rows:
                handle.write(json.dumps(row, sort_keys=True) + "\n")

    def summary(self) -> dict[str, Any]:
        rows = self.rows
        by_phase: dict[str, dict[str, int]] = {}
        for row in rows:
            bucket = by_phase.setdefault(
                str(row["phase"]), {"requests": 0, "status429": 0, "failed": 0}
            )
            bucket["requests"] += 1
            status = row.get("status")
            if status == 429:
                bucket["status429"] += 1
            if status is None or int(status) >= 400:
                bucket["failed"] += 1
        return {
            "enabled": True,
            "file": self.path.name,
            "requests": len(rows),
            "hosts": len({str(row["host"]) for row in rows}),
            "byPhase": by_phase,
        }


def _wrap_page_sync(
    trace: RequestTrace, fetch: Callable[[Any, str, int], str]
) -> Callable[[Any, str, int], str]:
    def wrapped(job: Any, url: str, timeout_s: int) -> str:
        start = time.perf_counter()
        try:
            text = fetch(job, url, timeout_s)
        except BaseException as exc:
            trace.record(
                phase=_phase_for_job(job),
                url=url,
                status=_status_from_error(exc),
                start_s=start,
                end_s=time.perf_counter(),
                error=type(exc).__name__,
            )
            raise
        trace.record(
            phase=_phase_for_job(job),
            url=url,
            status=200,
            start_s=start,
            end_s=time.perf_counter(),
        )
        return text

    return wrapped


def _wrap_page_async(trace: RequestTrace, fetch: Any) -> Any:
    async def wrapped(client: Any, job: Any, url: str, timeout_s: int) -> str:
        start = time.perf_counter()
        try:
            text = await fetch(client, job, url, timeout_s)
        except BaseException as exc:
            trace.record(
                phase=_phase_for_job(job),
                url=url,
                status=_status_from_error(exc),
                start_s=start,
                end_s=time.perf_counter(),
                error=type(exc).__name__,
            )
            raise
        trace.record(
            phase=_phase_for_job(job),
            url=url,
            status=200,
            start_s=start,
            end_s=time.perf_counter(),
        )
        return text

    return wrapped


@contextmanager
def trace_page_fetches(trace: RequestTrace) -> Iterator[RequestTrace]:
    """Record both page-fetch paths, keeping each job's own failure stage."""
    from src.source_discovery import directory_fetch

    real = directory_fetch.fetch_pages_batched

    def wrapped(timeout_s: int, jobs: list[dict[str, Any]], *args: Any, **kwargs: Any) -> Any:
        if callable(kwargs.get("sync_fetch")):
            kwargs["sync_fetch"] = _wrap_page_sync(trace, kwargs["sync_fetch"])
        if callable(kwargs.get("async_fetch")):
            kwargs["async_fetch"] = _wrap_page_async(trace, kwargs["async_fetch"])
        return real(timeout_s, jobs, *args, **kwargs)

    directory_fetch.fetch_pages_batched = wrapped
    try:
        yield trace
    finally:
        directory_fetch.fetch_pages_batched = real


@contextmanager
def trace_probe_fetches(trace: RequestTrace) -> Iterator[RequestTrace]:
    """Record every probe attempt, including each retry of the same URL."""
    from src.source_discovery import probe_runtime

    real = probe_runtime.fetch_text_httpx

    def wrapped(client: Any, url: str, timeout_s: int) -> str:
        start = time.perf_counter()
        try:
            text = real(client, url, timeout_s)
        except BaseException as exc:
            trace.record(
                phase=PROBE_PHASE,
                url=url,
                status=_status_from_error(exc),
                start_s=start,
                end_s=time.perf_counter(),
                error=type(exc).__name__,
            )
            raise
        trace.record(
            phase=PROBE_PHASE,
            url=url,
            status=200,
            start_s=start,
            end_s=time.perf_counter(),
        )
        return text

    probe_runtime.fetch_text_httpx = wrapped
    try:
        yield trace
    finally:
        probe_runtime.fetch_text_httpx = real


@contextmanager
def trace_requests(trace: RequestTrace) -> Iterator[RequestTrace]:
    with trace_page_fetches(trace), trace_probe_fetches(trace):
        yield trace


def load_trace(study: Path, arm_name: str) -> list[dict[str, Any]]:
    record_path = Path(study) / "arms" / arm_name / "arm.json"
    if not record_path.is_file():
        raise SystemExit(f"missing arm record: {record_path}")
    record = json.loads(record_path.read_text(encoding="utf-8"))
    summary = record.get("requestTrace") or {}
    if not summary.get("enabled"):
        raise SystemExit(f"arm {arm_name!r} has no request trace; re-run it without --no-trace")
    path = Path(study) / "arms" / arm_name / str(summary.get("file") or TRACE_FILE)
    if not path.is_file():
        raise SystemExit(f"arm {arm_name!r}: request trace missing at {path}")
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def host_stats(rows: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    """Per (phase, host): request count, 429 count, failure count and both spans."""
    stats: dict[tuple[str, str], dict[str, Any]] = {}
    for row in rows:
        key = (str(row.get("phase") or PAGE_PHASE), str(row.get("host") or "-"))
        stat = stats.setdefault(
            key,
            {
                "requests": 0,
                "status429": 0,
                "failed": 0,
                "firstMs": None,
                "lastMs": None,
                "first429Ms": None,
                "last429Ms": None,
            },
        )
        stat["requests"] += 1
        start = float(row.get("startMs") or 0.0)
        end = float(row.get("endMs") or start)
        stat["firstMs"] = start if stat["firstMs"] is None else min(stat["firstMs"], start)
        stat["lastMs"] = end if stat["lastMs"] is None else max(stat["lastMs"], end)
        status = row.get("status")
        if status == 429:
            stat["status429"] += 1
            stat["first429Ms"] = (
                start if stat["first429Ms"] is None else min(stat["first429Ms"], start)
            )
            stat["last429Ms"] = end if stat["last429Ms"] is None else max(stat["last429Ms"], end)
        if status is None or int(status) >= 400:
            stat["failed"] += 1
    return stats


def phase_stats(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """One phase's totals plus the window and time bins its 429s occupied."""
    stats: dict[str, Any] = {
        "requests": 0,
        "status429": 0,
        "failed": 0,
        "firstMs": None,
        "lastMs": None,
        "first429Ms": None,
        "last429Ms": None,
        "bins": [0] * 10,
        "hostsRequested": 0,
        "hostsWith429": 0,
        "hostsFully429": 0,
        "hostsPartially429": 0,
    }
    hosts: set[str] = set()
    per_host: dict[str, list[int]] = {}
    for row in rows:
        stats["requests"] += 1
        start = float(row.get("startMs") or 0.0)
        end = float(row.get("endMs") or start)
        stats["firstMs"] = start if stats["firstMs"] is None else min(stats["firstMs"], start)
        stats["lastMs"] = end if stats["lastMs"] is None else max(stats["lastMs"], end)
        host = str(row.get("host") or "-")
        counter = per_host.setdefault(host, [0, 0])
        counter[0] += 1
        status = row.get("status")
        if status == 429:
            counter[1] += 1
            stats["status429"] += 1
            hosts.add(host)
            stats["first429Ms"] = (
                start if stats["first429Ms"] is None else min(stats["first429Ms"], start)
            )
            stats["last429Ms"] = end if stats["last429Ms"] is None else max(stats["last429Ms"], end)
        if status is None or int(status) >= 400:
            stats["failed"] += 1
    stats["hostsRequested"] = len(per_host)
    stats["hostsWith429"] = len(hosts)
    stats["hostsFully429"] = sum(1 for total, hit in per_host.values() if hit == total)
    stats["hostsPartially429"] = sum(1 for total, hit in per_host.values() if 0 < hit < total)
    span = max(0.0, float(stats["lastMs"] or 0.0) - float(stats["firstMs"] or 0.0))
    if stats["status429"] and span > 0:
        for row in rows:
            if row.get("status") != 429:
                continue
            start = float(row.get("startMs") or 0.0)
            index = min(9, max(0, int((start - float(stats["firstMs"])) / span * 10)))
            stats["bins"][index] += 1
    stats["pattern"] = pattern_for(stats)
    return stats


def pattern_for(stat: dict[str, Any]) -> str:
    """Label a host's 429s as an episode or a spread, by the window they occupy."""
    count = int(stat.get("status429") or 0)
    if count == 0:
        return "-"
    if count == 1:
        return "single"
    span = max(0.0, float(stat.get("lastMs") or 0.0) - float(stat.get("firstMs") or 0.0))
    window = max(0.0, float(stat.get("last429Ms") or 0.0) - float(stat.get("first429Ms") or 0.0))
    if span <= 0:
        return "single"
    share = window / span
    if share <= _EPISODE_SHARE:
        return "episode"
    if share >= _SPREAD_SHARE:
        return "spread"
    return "mixed"


def _rate(count: int, requests: int) -> str:
    return f"{100.0 * count / requests:.1f}%" if requests else "-"


def _window(stat: dict[str, Any]) -> str:
    if not stat.get("status429"):
        return "-"
    start = float(stat.get("first429Ms") or 0.0)
    end = float(stat.get("last429Ms") or start)
    return f"{(end - start) / 1000.0:.1f}"


def _phase_window(stat: dict[str, Any]) -> str:
    if not stat.get("status429"):
        return "-"
    span = max(0.0, float(stat.get("lastMs") or 0.0) - float(stat.get("firstMs") or 0.0))
    start = float(stat.get("first429Ms") or 0.0)
    end = float(stat.get("last429Ms") or start)
    share = 100.0 * (end - start) / span if span else 0.0
    active_bins = sum(1 for count in stat.get("bins") or [] if count)
    return (
        f"{(end - start) / 1000.0:.1f}s, {share:.0f}% of span, "
        f"{active_bins}/10 bins, {stat.get('hostsWith429', 0)} hosts"
    )


def build_trace_report(study: Path, specs: list[str]) -> dict[str, Any]:
    """Per-phase, per-host request and 429 rates for the two arms of each pair."""
    study = Path(study)
    pairs: list[dict[str, Any]] = []
    for spec in specs:
        label, names = spec.split(":", 1)
        low_name, high_name = names.split(",", 1)
        low_rows = load_trace(study, low_name)
        high_rows = load_trace(study, high_name)
        low = host_stats(low_rows)
        high = host_stats(high_rows)
        phases: list[dict[str, Any]] = []
        for phase in sorted({key[0] for key in (*low, *high)}):
            low_hosts = {host: stat for (name, host), stat in low.items() if name == phase}
            high_hosts = {host: stat for (name, host), stat in high.items() if name == phase}
            low_phase = [row for row in low_rows if str(row.get("phase")) == phase]
            high_phase = [row for row in high_rows if str(row.get("phase")) == phase]
            low_total = phase_stats(low_phase)
            high_total = phase_stats(high_phase)
            hosts: list[dict[str, Any]] = []
            omitted_single = 0
            for host in sorted(set(low_hosts) | set(high_hosts)):
                low_stat = low_hosts.get(host) or {
                    "requests": 0,
                    "status429": 0,
                    "failed": 0,
                    "firstMs": None,
                    "lastMs": None,
                    "first429Ms": None,
                    "last429Ms": None,
                }
                high_stat = high_hosts.get(host) or {
                    "requests": 0,
                    "status429": 0,
                    "failed": 0,
                    "firstMs": None,
                    "lastMs": None,
                    "first429Ms": None,
                    "last429Ms": None,
                }
                if not low_stat["status429"] and not high_stat["status429"]:
                    continue
                if max(int(low_stat["requests"]), int(high_stat["requests"])) < 2:
                    omitted_single += 1
                    continue
                hosts.append(
                    {
                        "host": host,
                        "low": {**low_stat, "pattern": pattern_for(low_stat)},
                        "high": {**high_stat, "pattern": pattern_for(high_stat)},
                    }
                )
            hosts.sort(
                key=lambda row: (
                    -max(int(row["low"]["status429"]), int(row["high"]["status429"])),
                    row["host"],
                )
            )
            phases.append(
                {
                    "phase": phase,
                    "low": low_total,
                    "high": high_total,
                    "hosts": hosts,
                    "omittedSingleRequestHosts": omitted_single,
                }
            )
        order = {phase: index for index, phase in enumerate(PHASE_ORDER)}
        phases.sort(key=lambda row: (order.get(str(row["phase"]), len(order)), row["phase"]))
        pairs.append(
            {
                "pair": label,
                "low": {"arm": low_name, "requests": len(low_rows)},
                "high": {"arm": high_name, "requests": len(high_rows)},
                "phases": phases,
            }
        )
    return {"pairs": pairs}


def render_trace(report: dict[str, Any]) -> str:
    """A compact table: one block per pair and phase, hosts with a 429 on either side."""
    lines: list[str] = []
    for pair in report["pairs"]:
        lines.append(
            f"pair {pair['pair']}: {pair['low']['arm']} vs {pair['high']['arm']} "
            f"({pair['low']['requests']} / {pair['high']['requests']} requests)"
        )
        for phase in pair["phases"]:
            low, high = phase["low"], phase["high"]
            lines.append(
                f"  {phase['phase']}: {low['requests']} vs {high['requests']} requests, "
                f"429 {low['status429']} ({_rate(low['status429'], low['requests'])}) vs "
                f"{high['status429']} ({_rate(high['status429'], high['requests'])})"
            )
            lines.append(
                f"    hosts: {low['hostsRequested']} vs {high['hostsRequested']} requested, "
                f"{low['hostsFully429']} vs {high['hostsFully429']} fully throttled, "
                f"{low['hostsPartially429']} vs {high['hostsPartially429']} partial"
            )
            lines.append(f"    429 window: {_phase_window(low)} | {_phase_window(high)}")
            if not phase["hosts"]:
                lines.append("    no host was asked more than once on either arm")
                continue
            lines.append(
                "    host | low req/429/rate/window s/pattern | high req/429/rate/window s/pattern"
            )
            for row in phase["hosts"][:_MAX_HOST_ROWS]:
                left, right = row["low"], row["high"]
                lines.append(
                    f"    {row['host']} | {left['requests']}/{left['status429']}/"
                    f"{_rate(left['status429'], left['requests'])}/{_window(left)}/"
                    f"{left['pattern']} | {right['requests']}/{right['status429']}/"
                    f"{_rate(right['status429'], right['requests'])}/{_window(right)}/"
                    f"{right['pattern']}"
                )
            remaining = len(phase["hosts"]) - _MAX_HOST_ROWS
            if remaining > 0:
                lines.append(f"    ... {remaining} more repeated hosts in --json")
    if not lines:
        lines.append("no pairs")
    return "\n".join(lines)
