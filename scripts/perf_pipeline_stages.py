#!/usr/bin/env python3
"""Measure jobs-pipeline per-stage wall-clock, RSS, and CPU on a seeded container.

Runs one `POST /tasks/run-jobs-pipeline` (default preset: smoke) against a
running Baluffo container, samples the container's process tree memory and CPU
from the host while it runs, then cross-references the samples against the
`stageLedger` recorded on the pipeline lifecycle row to compute per-stage
durations / peak RSS / CPU seconds / MiB/s.

Outputs under `_out/perf-pipeline/<run-token>/`:
  - stages.json          canonical per-stage stats (durations + memory + cpu)
  - samples.ndjson       raw timeline (memory + cpu, host-side)
  - report.md            human-readable stage table
  - FINDINGS.md          bottleneck reading of stages.json

Reuses an already-running container when `--reuse-container` finds a healthy
one (default behaviour), starts a fresh one otherwise.

ponytail: stage attributions are interval-aligned against time.monotonic, not
clock-synced to the container — close enough for stage-level signals.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.shared.process_memory import ProcessMemorySampler

DEFAULT_IMAGE = "ghcr.io/deathuman/baluffo:latest"
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "_out" / "perf-pipeline"
DEFAULT_CONTAINER_NAME = "baluffo-perf-pipeline"
DEFAULT_PORT = 8879
DEFAULT_PRESET = "smoke"
DEFAULT_TIMEOUT_S = 3600.0
DEFAULT_POLL_INTERVAL_S = 0.25
DEFAULT_MEMORY_INTERVAL_S = 0.1
DEFAULT_CPU_INTERVAL_S = 0.5

PROFILES: dict[str, dict[str, str]] = {
    # Pipeline workload peaks near ~1.2 GiB RSS during executing_sources
    # against the production-shaped seed — 1 GiB OOMs the fetch child; 1.5 GiB
    # lets the smoke bench complete and is still Pi-class.
    "pi4-tight": {"cpus": "1.5", "memory": "1.5g"},
    # Full production-shaped fetch (500+ loader keys incl. ATS aggregates +
    # browser fallback driver) exceeds 1.5 GiB: the playwright Node driver
    # OOMs at the ceiling. This seat mirrors the uncapped Umbrel deployment
    # (no compose mem_limit) while staying Pi4-class.
    "pi4-roomy": {"cpus": "2.0", "memory": "2.5g"},
    # ponytail: 3.5g probe to verify full 2125 completes after heavy-host caps;
    # uncapped production has no mem_limit, so 3.5g is still Pi-4-class headroom probe.
    "pi4-roomy-3g": {"cpus": "2.0", "memory": "3.5g"},
}


def _utc_now_iso() -> str:
    return datetime.now(UTC).isoformat().replace("+00:00", "Z")


def _generate_run_token() -> str:
    return datetime.now(UTC).strftime("%Y%m%d-%H%M%S-%f")


def _parse_iso(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _docker(*args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        ["docker", *args],
        check=check,
        capture_output=True,
        text=True,
    )


def _stop_container(name: str) -> None:
    _docker("rm", "-f", name, check=False)


def _container_host_pid(name: str) -> int:
    proc = _docker("inspect", "--format", "{{.State.Pid}}", name, check=False)
    if proc.returncode != 0:
        return 0
    text = (proc.stdout or "").strip()
    try:
        return int(text)
    except ValueError:
        return 0


def _container_is_running(name: str) -> bool:
    proc = _docker("inspect", "--format", "{{.State.Running}}", name, check=False)
    return proc.returncode == 0 and (proc.stdout or "").strip().lower() == "true"


def _http_json(
    base_url: str,
    method: str,
    endpoint: str,
    timeout_s: float,
    body: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    url = f"{base_url}{endpoint}"
    data: bytes | None = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    started = time.perf_counter()
    try:
        with urllib.request.urlopen(req, timeout=timeout_s) as resp:
            raw = resp.read()
            duration_ms = int((time.perf_counter() - started) * 1000)
            try:
                parsed = json.loads(raw.decode("utf-8")) if raw else {}
            except json.JSONDecodeError:
                parsed = {}
            return (
                {
                    "ok": 200 <= resp.status < 300,
                    "status": resp.status,
                    "durationMs": duration_ms,
                    "sizeBytes": len(raw),
                },
                parsed if isinstance(parsed, dict) else {},
            )
    except urllib.error.HTTPError as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        try:
            raw = exc.read()
        except OSError:
            raw = b""
        try:
            parsed = json.loads(raw.decode("utf-8")) if raw else {}
        except json.JSONDecodeError:
            parsed = {}
        return (
            {
                "ok": False,
                "status": int(exc.code or 0),
                "durationMs": duration_ms,
                "sizeBytes": len(raw),
                "error": str(exc),
            },
            parsed if isinstance(parsed, dict) else {},
        )
    except (urllib.error.URLError, OSError) as exc:
        duration_ms = int((time.perf_counter() - started) * 1000)
        return (
            {
                "ok": False,
                "status": 0,
                "durationMs": duration_ms,
                "sizeBytes": 0,
                "error": str(exc),
            },
            {},
        )


def _wait_ready(base_url: str, timeout_s: float = 60.0) -> None:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        sample, _ = _http_json(base_url, "GET", "/app/ready", timeout_s=2.0)
        if sample.get("ok"):
            return
        time.sleep(0.5)
    raise TimeoutError(f"container at {base_url} did not become ready within {timeout_s}s")


def _read_proc_stat_seconds(host_pid: int) -> float | None:
    """utime+stime for the container's host root PID.

    Returns None when /proc/<pid> is unavailable (non-Linux host, container
    stopped between samples, or missing permission).
    """
    try:
        with open(f"/proc/{int(host_pid)}/stat", encoding="utf-8") as fh:
            raw = fh.read()
    except (OSError, ValueError):
        return None
    # /proc/<pid>/stat: name may contain spaces (rare); last ')' bounds it
    close_paren = raw.rfind(")")
    if close_paren < 0:
        return None
    fields = raw[close_paren + 1 :].split()
    if len(fields) < 13:
        return None
    try:
        utime = int(fields[11])  # stat field 14 (0-indexed: skip 2 prefix + name)
        stime = int(fields[12])  # stat field 15
    except ValueError:
        return None
    clk = int(os.sysconf("SC_CLK_TCK")) if hasattr(os, "sysconf") else 100
    return (utime + stime) / float(clk or 100)


_ANSI_RE = re.compile(r"\x1b\[[0-9;?]*[A-Za-z]")


def _docker_stats_sampler(container_name: str, interval_s: float) -> dict[str, Any]:
    """Windows/Docker-Desktop-compatible memory sampler via `docker stats`.

    Returns {"samples": [...], "unsupportedReason": ""}. On a Linux host the
    caller should prefer direct /proc sampling instead.
    """
    samples: list[dict[str, Any]] = []
    stop = threading.Event()

    # Format: prerecorded tab-separated. We pick MemUsage + CPUPerc + PIDs.
    fmt = "{{.MemUsage}}\t{{.CPUPerc}}\t{{.PIDs}}"
    proc = subprocess.Popen(
        ["docker", "stats", container_name, "--format", fmt],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )

    def _reader() -> None:
        assert proc.stdout is not None
        while not stop.is_set():
            line = proc.stdout.readline()
            if not line:
                break
            # docker stats emits ANSI cursor-control codes per refresh; strip.
            clean = _ANSI_RE.sub("", line).strip()
            if not clean:
                continue
            t = time.monotonic()
            parts = clean.split("\t")
            if len(parts) < 3:
                continue
            mem_usage, cpu_pct, _pids = parts[0], parts[1], parts[2]
            # MemUsage looks like "42.5MiB / 1GiB" — take first token
            mem_str = mem_usage.split(" / ")[0].strip()
            used_bytes = _parse_human_bytes(mem_str)
            cpu_pct_num = _parse_pct(cpu_pct)
            samples.append(
                {
                    "t": t,
                    "workingSetBytes": used_bytes,
                    "rssBytes": used_bytes,
                    "cpuPercent": cpu_pct_num,
                    "processCount": 0,
                    "processes": [],
                    "unsupportedReason": ""
                    if used_bytes
                    else "could not parse docker stats output",
                }
            )

    thread = threading.Thread(target=_reader, name="docker-stats-sampler", daemon=True)
    thread.start()
    return {"samples": samples, "stop": stop, "proc": proc, "thread": thread}


def _parse_human_bytes(text: str) -> int:
    # ponytail: support the handful of units docker uses — B, KiB/MiB/GiB, KB/MB/GB
    s = text.strip()
    for suffix, mul in (
        ("KiB", 1024),
        ("MiB", 1024**2),
        ("GiB", 1024**3),
        ("KB", 1000),
        ("MB", 1000**2),
        ("GB", 1000**3),
        ("B", 1),
    ):
        if s.endswith(suffix):
            try:
                return int(float(s[: -len(suffix)].strip()) * mul)
            except ValueError:
                return 0
    try:
        return int(float(s))
    except ValueError:
        return 0


def _parse_pct(text: str) -> float:
    try:
        return float(text.strip().rstrip("%")) / 100.0
    except (ValueError, AttributeError):
        return 0.0


class _StampedMemorySampler:
    """Wraps ProcessMemorySampler so each sample carries a monotonic timestamp.

    ponytail: reuses the existing sampler thread verbatim; we just stamp after
    each stop(). For finer-grained per-sample stamping we'd need to subclass
    the sampler thread — YAGNI until per-stage resolution gets grainy.
    """

    def __init__(self, root_pid: int, interval_s: float) -> None:
        self._root_pid = int(root_pid)
        self._interval_s = float(interval_s)
        self._sampler = ProcessMemorySampler(root_pid, interval_s=interval_s)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._samples: list[dict[str, Any]] = []
        self._metadata_cache: dict[int, dict[str, Any]] = {}

    def start(self) -> None:
        from src.shared.process_memory import sample_process_tree

        def _run() -> None:
            while not self._stop_event.is_set():
                t = time.monotonic()
                sample = sample_process_tree(self._root_pid, metadata_cache=self._metadata_cache)
                sample["t"] = t
                self._samples.append(sample)
                self._stop_event.wait(self._interval_s)

        self._thread = threading.Thread(target=_run, name="perf-memory-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> list[dict[str, Any]]:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self._interval_s * 4))
        return list(self._samples)


class _CpuSampler:
    """Reads /proc/<host_pid>/stat utime+stime at intervals and emits deltas.

    Produces entries {t, cpuSecondsDelta} suitable for per-stage summation.
    On non-Linux hosts produces entries with unsupportedReason set.
    """

    def __init__(self, host_pid: int, interval_s: float) -> None:
        self._host_pid = int(host_pid)
        self._interval_s = float(interval_s)
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._samples: list[dict[str, Any]] = []
        self._unsupported: str = ""

    def start(self) -> None:
        if not os.path.isdir(f"/proc/{self._host_pid}"):
            self._unsupported = "cpu sampling requires /proc (linux host)"
            return

        def _run() -> None:
            prev: tuple[float, float] | None = None  # (t, cpu_seconds)
            while not self._stop_event.is_set():
                t = time.monotonic()
                cpu_s = _read_proc_stat_seconds(self._host_pid)
                if cpu_s is not None and prev is not None:
                    delta = cpu_s - prev[1]
                    if delta >= 0:
                        self._samples.append(
                            {"t": t, "cpuSecondsDelta": round(delta, 4), "cumulative": cpu_s}
                        )
                if cpu_s is not None:
                    prev = (t, cpu_s)
                self._stop_event.wait(self._interval_s)

        self._thread = threading.Thread(target=_run, name="perf-cpu-sampler", daemon=True)
        self._thread.start()

    def stop(self) -> dict[str, Any]:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self._interval_s * 4))
        return {
            "samples": list(self._samples),
            "unsupportedReason": self._unsupported,
        }


def _poll_pipeline_task_live(
    base_url: str,
    *,
    run_id_hint: str,
    timeout_s: float,
    interval_s: float,
    fetch_tasks_path: Path | None = None,
) -> dict[str, Any]:
    """Sample /tasks/run-jobs-pipeline-status until the pipeline goes idle.

    Returns stage-transition observations + the terminal sample.

    When ``fetch_tasks_path`` is provided, additionally samples the fetch
    worker's task file each tick and emits ``fetch/<phaseKey>`` observations
    so the ledger records fetch sub-stages without any bridge change.
    ponytail: bench-side enrichment only — no bridge edit needed because the
    fetch worker already publishes taskProgress.phaseKey every poll tick.
    """
    endpoint = "/tasks/run-jobs-pipeline-status"
    deadline = time.monotonic() + timeout_s
    observations: list[dict[str, Any]] = []
    last_stage: str = ""
    last_parsed: dict[str, Any] = {}
    observed_run_id: str = run_id_hint
    last_fetch_phase: str = ""
    # ponytail: the bridge can report the new runId with active=false for a few
    # ticks before registration flips, and jobs-fetch-tasks.json still holds the
    # previous run's phases — never declare idle before seeing this run active.
    seen_active_for_run = False

    def _read_fetch_phase() -> tuple[str, str]:
        if fetch_tasks_path is None or not fetch_tasks_path.is_file():
            return "", ""
        try:
            doc = json.loads(fetch_tasks_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return "", ""
        tp = doc.get("taskProgress") if isinstance(doc, dict) else {}
        if not isinstance(tp, dict):
            return "", ""
        phase = str(tp.get("phaseKey") or "").strip()
        label = str(tp.get("phaseLabel") or "").strip() or phase
        return phase, label

    while time.monotonic() < deadline:
        t = time.monotonic()
        sample, parsed = _http_json(base_url, "GET", endpoint, timeout_s=5.0)
        # fetch sub-stage observation (bench-side, samples jobs-fetch-tasks.json)
        fetch_phase, fetch_label = _read_fetch_phase()
        if fetch_phase and fetch_phase != last_fetch_phase:
            observations.append(
                {
                    "t": t,
                    "stage": f"fetch/{fetch_phase}",
                    "label": fetch_label,
                    "active": True,
                    "runId": observed_run_id,
                    "source": "fetch-tasks.json",
                }
            )
            last_fetch_phase = fetch_phase
        if isinstance(parsed, dict) and parsed:
            stage = str(parsed.get("stage") or "").strip()
            active = bool(parsed.get("active"))
            progress = parsed.get("progress") if isinstance(parsed.get("progress"), dict) else {}
            run_id_from_status = str(parsed.get("runId") or "").strip()
            # ponytail: never override a trigger-supplied pipeline runId —
            # the poll endpoint also surfaces child-task runIds (e.g. fetch)
            # while the pipeline holds the lifecycle row we harvest later.
            if run_id_from_status and not observed_run_id:
                observed_run_id = run_id_from_status
            if stage and stage != last_stage:
                observations.append(
                    {
                        "t": t,
                        "stage": stage,
                        "active": active,
                        "runId": observed_run_id,
                        "progress": {
                            "currentStep": int(progress.get("currentStep") or 0),
                            "totalSteps": int(progress.get("totalSteps") or 0),
                            "label": str(progress.get("label") or ""),
                        },
                    }
                )
                last_stage = stage
            if (
                active
                and run_id_from_status == observed_run_id
                and not str(stage or "").startswith("fetch/")
            ):
                seen_active_for_run = True
            last_parsed = parsed
            # ponytail: only treat as terminal when the bridge explicitly says
            # runId matches AND active is False AND we saw this run active at
            # least once. Empty/error payloads keep polling.
            if (
                not active
                and seen_active_for_run
                and observations
                and run_id_from_status == observed_run_id
            ):
                observations.append(
                    {
                        "t": time.monotonic(),
                        "stage": "idle",
                        "active": False,
                        "runId": observed_run_id,
                    }
                )
                break
        time.sleep(interval_s)
    return {
        "runId": observed_run_id,
        "observations": observations,
        "finalSample": last_parsed,
        "timedOut": time.monotonic() >= deadline,
    }


def _load_lifecycle_row(data_volume: Path, run_id: str) -> dict[str, Any] | None:
    path = data_volume / "admin-task-lifecycle.json"
    if not path.is_file():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    rows = payload.get("rows") if isinstance(payload, dict) else None
    if not isinstance(rows, list):
        return None
    for row in reversed(rows):
        if not isinstance(row, dict):
            continue
        if str(row.get("runId") or "") != run_id:
            continue
        if str(row.get("taskType") or "") != "pipeline":
            continue
        return row
    return None


def _compute_stage_durations(
    ledger: list[dict[str, Any]],
    *,
    started_at: datetime | None,
    finished_at: datetime | None,
) -> list[dict[str, Any]]:
    """Pair consecutive ledger entries; emit [stage, enteredAt, exitedAt, durationMs].

    Stage duration = next entry's enteredAt - this entry's enteredAt.
    The terminal row (completed/canceled/error) marks its own entry timestamp.
    """
    if not ledger:
        return []
    parsed: list[tuple[str, datetime, str]] = []
    for entry in ledger:
        if not isinstance(entry, dict):
            continue
        stage = str(entry.get("stage") or "")
        entered = _parse_iso(entry.get("enteredAt"))
        if not stage or entered is None:
            continue
        parsed.append((stage, entered, str(entry.get("label") or "")))
    if not parsed:
        return []
    rows: list[dict[str, Any]] = []
    for i in range(len(parsed)):
        stage, entered, label = parsed[i]
        if i + 1 < len(parsed):
            _, exited, _ = parsed[i + 1]
        else:
            # Last entry: bounded by job finishedAt when available.
            exited = finished_at or entered
        duration_ms = max(0, int((exited - entered).total_seconds() * 1000))
        rows.append(
            {
                "stage": stage,
                "label": label,
                "enteredAt": entered.isoformat().replace("+00:00", "Z"),
                "exitedAt": exited.isoformat().replace("+00:00", "Z"),
                "durationMs": duration_ms,
            }
        )
    return rows


def _splice_sub_stage_observations(
    stage_rows: list[dict[str, Any]],
    observations: list[dict[str, Any]],
    *,
    anchor_monotonic: float,
    anchor_wall: datetime | None,
) -> list[dict[str, Any]]:
    """Replace a coarse "fetch" stage row with finer sub-stage rows from observations.

    Observations carry monotonic time (``t``) — translate to wall-clock by
    anchoring against the pipeline's startedAt wall time. If we can't anchor
    cleanly, return the original rows untouched.
    """
    if anchor_wall is None:
        return stage_rows
    if not any(str(r.get("stage") or "") == "fetch" for r in stage_rows):
        return stage_rows
    fetch_subs = [
        o
        for o in observations
        if str(o.get("stage") or "").startswith("fetch/") and float(o.get("t") or 0.0) > 0
    ]
    if not fetch_subs:
        return stage_rows

    def _mono_to_wall(t: float) -> datetime:
        return anchor_wall + __import__("datetime").timedelta(seconds=t - anchor_monotonic)

    sub_rows: list[dict[str, Any]] = []
    for i, obs in enumerate(fetch_subs):
        entered = _mono_to_wall(float(obs["t"]))
        if i + 1 < len(fetch_subs):
            exited = _mono_to_wall(float(fetch_subs[i + 1]["t"]))
        else:
            exited = entered  # last fetch sub-stage closes at the next top-level stage
        sub_rows.append(
            {
                "stage": str(obs.get("stage") or ""),
                "label": str(obs.get("label") or ""),
                "enteredAt": entered.isoformat().replace("+00:00", "Z"),
                "exitedAt": exited.isoformat().replace("+00:00", "Z"),
                "durationMs": max(0, int((exited - entered).total_seconds() * 1000)),
                "memory": {"peakRssBytes": 0, "avgRssBytes": 0, "sampleCount": 0},
                "cpu": {"cpuSeconds": 0.0, "sampleCount": 0},
                "rates": {"cpuSecondsPerSecond": 0.0},
            }
        )
    # Find the coarse "fetch" row, replace it with sub-rows, and set the last
    # sub-row's exitedAt to the coarse row's exitedAt so the chain stays
    # continuous.
    out: list[dict[str, Any]] = []
    for row in stage_rows:
        if str(row.get("stage") or "") != "fetch":
            out.append(row)
            continue
        if not sub_rows:
            out.append(row)
            continue
        sub_rows[-1]["exitedAt"] = str(row.get("exitedAt") or "")
        last_exit = _parse_iso(sub_rows[-1]["exitedAt"])
        last_enter = _parse_iso(sub_rows[-1]["enteredAt"])
        if last_exit is not None and last_enter is not None:
            sub_rows[-1]["durationMs"] = max(
                0, int((last_exit - last_enter).total_seconds() * 1000)
            )
        out.extend(sub_rows)
    return out


def _attribute_samples_to_stages(
    stage_rows: list[dict[str, Any]],
    memory_samples: list[dict[str, Any]],
    cpu_samples: list[dict[str, Any]],
    *,
    stage_anchor_monotonic: float | None,
    stage_anchor_wall: datetime | None,
) -> None:
    """Bucket samples into stages and compute per-stage aggregates.

    Uses a single anchor: monotonic capture time anchored to the pipeline
    startedAt wall-clock. Sample offsets from the anchor give their position
    in pipeline-time, which we then bucket against stage durations.

    ponytail: interval alignment is approximate; sub-100ms offsets are noise
    anyway given sample cadence ≥100ms.
    """
    if not stage_rows:
        return
    if stage_anchor_monotonic is None or stage_anchor_wall is None:
        for row in stage_rows:
            row["memory"] = {}
            row["cpu"] = {}
        return

    # Pipeline-time boundaries per stage (seconds since anchor).
    bounds: list[tuple[float, float, dict[str, Any]]] = []
    for row in stage_rows:
        entered = _parse_iso(row.get("enteredAt"))
        exited = _parse_iso(row.get("exitedAt"))
        if entered is None or exited is None:
            continue
        entered_offset = (entered - stage_anchor_wall).total_seconds()
        exited_offset = (exited - stage_anchor_wall).total_seconds()
        bounds.append((entered_offset, exited_offset, row))

    def _bucket(t_monotonic: float) -> dict[str, Any] | None:
        offset = t_monotonic - stage_anchor_monotonic
        for start, end, row in bounds:
            if start <= offset < end:
                return row
        return None

    # Initialize buckets.
    for row in stage_rows:
        row["memory"] = {"peakRssBytes": 0, "avgRssBytes": 0, "sampleCount": 0}
        row["cpu"] = {"cpuSeconds": 0.0, "sampleCount": 0}

    rss_accumulator: dict[int, list[int]] = {i: [] for i in range(len(stage_rows))}
    idx_of_row = {id(row): i for i, row in enumerate(stage_rows)}

    for sample in memory_samples:
        t = float(sample.get("t") or 0.0)
        target = _bucket(t)
        if target is None:
            continue
        idx = idx_of_row.get(id(target))
        if idx is None:
            continue
        rss = int(sample.get("rssBytes") or sample.get("workingSetBytes") or 0)
        rss_accumulator[idx].append(rss)

    cpu_accumulator: dict[int, float] = {i: 0.0 for i in range(len(stage_rows))}
    # Supports two shapes:
    #  - per-PID probes: {"t", "cpuSecondsDelta"}  → sum deltas directly
    #  - docker stats:    {"t", "cpuPercent"}      → integrate pct * dt inside
    #                   the current bucket only. If prev sample was in a
    #                   different bucket, drop that dt (avoids cross-boundary
    #                   attribution drift).
    prev_cpu_sample: dict[str, Any] | None = None
    for sample in sorted(cpu_samples, key=lambda s: float(s.get("t") or 0.0)):
        t = float(sample.get("t") or 0.0)
        target = _bucket(t)
        if target is None:
            prev_cpu_sample = sample
            continue
        idx = idx_of_row.get(id(target))
        if idx is None:
            prev_cpu_sample = sample
            continue
        if "cpuSecondsDelta" in sample:
            cpu_accumulator[idx] += float(sample.get("cpuSecondsDelta") or 0.0)
        elif "cpuPercent" in sample:
            prev_t = float(prev_cpu_sample.get("t") or 0.0) if prev_cpu_sample is not None else None
            if prev_t is not None and t > prev_t and _bucket(prev_t) is target:
                cpu_accumulator[idx] += float(sample.get("cpuPercent") or 0.0) * (t - prev_t)
        prev_cpu_sample = sample

    for i, row in enumerate(stage_rows):
        rss = rss_accumulator[i]
        row["memory"] = {
            "peakRssBytes": max(rss) if rss else 0,
            "avgRssBytes": int(sum(rss) / len(rss)) if rss else 0,
            "sampleCount": len(rss),
        }
        row["cpu"] = {
            "cpuSeconds": round(cpu_accumulator[i], 3),
            "sampleCount": sum(1 for s in cpu_samples if _bucket(float(s.get("t") or 0)) is row),
        }
        duration_s = float(row.get("durationMs") or 0) / 1000.0
        row["rates"] = {
            "cpuSecondsPerSecond": round(cpu_accumulator[i] / duration_s, 3)
            if duration_s > 0
            else 0.0,
        }


def _render_markdown_report(
    *,
    run_token: str,
    profile: str,
    image: str,
    preset: str,
    run_id: str,
    started_at: str,
    finished_at: str,
    total_ms: int,
    terminal: str,
    stage_rows: list[dict[str, Any]],
) -> str:
    lines = [
        f"# Pipeline stage benchmark — {run_token}",
        "",
        f"- image: `{image}`",
        f"- profile: `{profile}`",
        f"- preset: `{preset}`",
        f"- runId: `{run_id}`",
        f"- startedAt: `{started_at}`",
        f"- finishedAt: `{finished_at}`",
        f"- terminal: `{terminal}`",
        f"- total: **{total_ms:,} ms**",
        "",
        "## Per-stage",
        "",
        "| Stage | Duration (ms) | % of total | Peak RSS (MiB) | Avg RSS (MiB) | CPU (s) | CPU/s |",
        "| --- | ---: | ---: | ---: | ---: | ---: | ---: |",
    ]
    mib = 1024 * 1024
    for row in stage_rows:
        duration = int(row.get("durationMs") or 0)
        pct = (100.0 * duration / total_ms) if total_ms else 0.0
        memory = row.get("memory") or {}
        cpu = row.get("cpu") or {}
        rates = row.get("rates") or {}
        lines.append(
            "| {stage} | {dur:,} | {pct:.1f}% | {peak:.1f} | {avg:.1f} | {cpu_s:.2f} | {cpu_rate:.2f} |".format(
                stage=str(row.get("stage") or "?"),
                dur=duration,
                pct=pct,
                peak=int(memory.get("peakRssBytes") or 0) / mib,
                avg=int(memory.get("avgRssBytes") or 0) / mib,
                cpu_s=float(cpu.get("cpuSeconds") or 0.0),
                cpu_rate=float(rates.get("cpuSecondsPerSecond") or 0.0),
            )
        )
    return "\n".join(lines) + "\n"


def _render_findings(
    *,
    run_token: str,
    total_ms: int,
    stage_rows: list[dict[str, Any]],
) -> str:
    if not stage_rows:
        return (
            f"# Pipeline benchmark findings — {run_token}\n\n"
            "No stage ledger captured. Either the run never reached `_mark_stage`\n"
            "(early abort) or the lifecycle row did not persist. Inspect\n"
            "`admin-task-lifecycle.json` from the data volume.\n"
        )
    sorted_by_ms = sorted(stage_rows, key=lambda row: int(row.get("durationMs") or 0), reverse=True)
    dominant = sorted_by_ms[0]
    lines = [
        f"# Pipeline benchmark findings — {run_token}",
        "",
        f"Total: **{total_ms:,} ms** across {len(stage_rows)} stages.",
        "",
        "## Dominant stage",
        "",
        f"- **{dominant['stage']}** consumed **{int(dominant.get('durationMs') or 0):,} ms** "
        f"({100.0 * int(dominant.get('durationMs') or 0) / max(1, total_ms):.1f}% of total)",
        f"- Peak RSS: {int((dominant.get('memory') or {}).get('peakRssBytes') or 0) / (1024 * 1024):.1f} MiB",
        f"- CPU: {float((dominant.get('cpu') or {}).get('cpuSeconds') or 0):.2f} s",
        "",
        "## Stage table (sorted by duration)",
        "",
        "| Rank | Stage | Duration (ms) | Peak RSS (MiB) | CPU (s) |",
        "| ---: | --- | ---: | ---: | ---: |",
    ]
    mib = 1024 * 1024
    for rank, row in enumerate(sorted_by_ms, start=1):
        lines.append(
            f"| {rank} | {row['stage']} | {int(row.get('durationMs') or 0):,} | "
            f"{int((row.get('memory') or {}).get('peakRssBytes') or 0) / mib:.1f} | "
            f"{float((row.get('cpu') or {}).get('cpuSeconds') or 0):.2f} |"
        )
    lines.append("")
    lines.append(
        "Bottleneck decision rule: if the top stage holds >50% of total and is not a known I/O "
        "wall (e.g. sync push over slow uplink), it deserves a focused optimisation PR."
    )
    return "\n".join(lines) + "\n"


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", default=DEFAULT_IMAGE)
    parser.add_argument("--container-name", default=DEFAULT_CONTAINER_NAME)
    parser.add_argument("--data-volume", required=True)
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument(
        "--profile",
        choices=sorted(PROFILES.keys()),
        default="pi4-tight",
    )
    parser.add_argument(
        "--preset",
        default=DEFAULT_PRESET,
        help=f"Pipeline preset body forwarded to /tasks/run-jobs-pipeline (default: {DEFAULT_PRESET}).",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Force recreation of the container (default: reuse if healthy).",
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=DEFAULT_TIMEOUT_S,
        help=f"Pipeline completion timeout, seconds (default: {DEFAULT_TIMEOUT_S}).",
    )
    parser.add_argument(
        "--output",
        default="",
        help=f"Output directory (default: {DEFAULT_OUTPUT_ROOT}/<run-token>).",
    )
    parser.add_argument(
        "--only-sources-file",
        default="",
        help=(
            "Optional path to a newline-separated list of source names. When set, "
            "the bench stages an env file under --output and passes it to "
            "docker run via --env-file so the pipeline fetch child runs only that "
            "subset. Bench-only knob for trimming a large seed data volume without "
            "editing registry files."
        ),
    )
    parser.add_argument(
        "--fetch-max-workers-env",
        default="",
        help=(
            "Optional bench override for BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS "
            "forwarded via --env-file alongside the only-sources list. Used to test "
            "H1 (fetch concurrency drives pi4-tight pressure) vs H2 (per-source peak)."
        ),
    )
    parser.add_argument(
        "--browser-fallback-max-workers-env",
        default="",
        help=(
            "Optional bench override for "
            "BALUFFO_CONTAINER_PIPELINE_BROWSER_FALLBACK_MAX_WORKERS forwarded via "
            "--env-file alongside the only-sources list (capped at 6 by the service)."
        ),
    )
    parser.add_argument(
        "--fetch-max-bytes-env",
        default="",
        help=(
            "Optional bench override for BALUFFO_FETCH_MAX_BYTES forwarded via "
            "--env-file. Caps per-response body reads (httpx + urllib + browser "
            "content); truncated listings retry next run."
        ),
    )
    parser.add_argument(
        "--heap-diagnostics",
        action="store_true",
        help=(
            "Set BALUFFO_FETCH_HEAP_DIAGNOSTICS=1 in the staged env file: sample "
            "global tracemalloc current/peak + top frames every 60s into "
            "<data>/perf-profiles/fetch-heap.jsonl. Distorts wall-clock; diagnosis only."
        ),
    )
    parser.add_argument(
        "--obscura-bin-host-path",
        default="",
        help=(
            "Host directory containing the Linux obscura binary (from releases). "
            "Mounted read-only at /opt/obscura; stages BALUFFO_BROWSER_FALLBACK_"
            "BACKEND=obscura and BALUFFO_OBSCURA_BIN=/opt/obscura/obscura."
        ),
    )
    parser.add_argument(
        "--profile-alloc",
        action="store_true",
        help=(
            "Set BALUFFO_PROFILE_ALLOC=1 in the staged env file. Per-source "
            "tracemalloc capture writes <data>/perf-profiles/allocations.jsonl; "
            "aggregate with scripts/perf_alloc_top.py. Lock-serializes sources: "
            "diagnostic only, wall-clock is not meaningful."
        ),
    )
    return parser.parse_args(argv)


def _stop_samplers(
    memory_sampler: Any | None,
    cpu_sampler: Any | None,
    stats_handle: dict[str, Any] | None,
) -> None:
    for s in (memory_sampler, cpu_sampler):
        if s is not None:
            try:
                s.stop()
            except (AttributeError, RuntimeError):
                pass
    if stats_handle is not None:
        try:
            stats_handle["stop"].set()
            proc = stats_handle.get("proc")
            if proc is not None:
                proc.terminate()
                proc.wait(timeout=3)
        except (subprocess.SubprocessError, OSError):
            pass


def _apply_bench_overrides(data_volume: Path) -> dict[str, str]:
    """Overlay bench-safe settings on top of the seed volume before starting.

    Production-shaped seeds include config that slows the bench without
    informing it (e.g. the 8 000-URL gamedevmap active-audit crawl blocks
    discovery for 15+ min). Bench mode disables gamedevmap *and* other
    long-running audit hooks so the pipeline reaches fetch quickly and the
    A/B comparison focuses on loading_state.

    Returns a mapping of ``{relative_path: original_text}`` so we can restore
    the seed after the run. Caller MUST call :func:`_restore_bench_overrides`
    regardless of outcome.
    """
    overrides: dict[str, str] = {}
    # Disable gamedevmap active audit (dominates discovery when enabled).
    src_discovery_path = data_volume / "source-discovery-config.json"
    if src_discovery_path.exists():
        original = src_discovery_path.read_text(encoding="utf-8")
        try:
            cfg = json.loads(original) if original.strip() else {}
        except json.JSONDecodeError:
            return overrides  # never clobber a malformed config
        cfg.setdefault("gamedevmap", {})["enabled"] = False
        src_discovery_path.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
        overrides[str(src_discovery_path)] = original
    return overrides


def _restore_bench_overrides(overrides: dict[str, str]) -> None:
    for path, original in overrides.items():
        try:
            Path(path).write_text(original, encoding="utf-8")
        except OSError:
            pass


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    data_volume = Path(str(args.data_volume)).expanduser().resolve()
    if not data_volume.is_dir():
        raise SystemExit(f"data volume not found: {data_volume}")

    profile = PROFILES[args.profile]
    run_token = _generate_run_token()
    output_dir = (
        Path(str(args.output)).expanduser().resolve()
        if args.output
        else (DEFAULT_OUTPUT_ROOT / run_token)
    )
    output_dir.mkdir(parents=True, exist_ok=True)

    base_url = f"http://127.0.0.1:{int(args.port)}"
    container_name = str(args.container_name)

    # ponytail: seed data ships production config; the harness should never let
    # production-shaped discovery audits block the bench. Overlay bench-mode
    # overrides before starting the container and restore after.
    overrides_backups = _apply_bench_overrides(data_volume)

    if args.fresh or not _container_is_running(container_name):
        _stop_container(container_name)
        docker_args = [
            "run",
            "-d",
            "--rm",
            "--name",
            container_name,
            "--cpus",
            str(profile["cpus"]),
            "--memory",
            str(profile["memory"]),
            "--memory-swap",
            str(profile["memory"]),
            "-p",
            f"{int(args.port)}:8080",
            "-v",
            f"{str(data_volume)}:/data",
        ]
        # ponytail: --profile-alloc and --fetch-max-workers-env should work even
        # without --only-sources-file (full 2123 run with alloc profiling).
        bench_env_lines: list[str] = []
        only_sources: list[str] = []
        only_sources_path: Path | None = None
        if str(args.only_sources_file or "").strip():
            only_sources_path = Path(str(args.only_sources_file)).expanduser().resolve()
            only_sources = [
                name.strip()
                for name in only_sources_path.read_text(encoding="utf-8").splitlines()
                if name.strip()
            ]
            if only_sources:
                bench_env_lines.append(
                    "BALUFFO_CONTAINER_PIPELINE_ONLY_SOURCES=" + ",".join(only_sources)
                )
        if str(args.fetch_max_workers_env or "").strip():
            bench_env_lines.append(
                "BALUFFO_CONTAINER_PIPELINE_FETCH_MAX_WORKERS="
                + str(args.fetch_max_workers_env).strip()
            )
        if str(args.browser_fallback_max_workers_env or "").strip():
            bench_env_lines.append(
                "BALUFFO_CONTAINER_PIPELINE_BROWSER_FALLBACK_MAX_WORKERS="
                + str(args.browser_fallback_max_workers_env).strip()
            )
        if str(args.fetch_max_bytes_env or "").strip():
            bench_env_lines.append(
                "BALUFFO_FETCH_MAX_BYTES=" + str(args.fetch_max_bytes_env).strip()
            )
        if bool(args.profile_alloc):
            bench_env_lines.append("BALUFFO_PROFILE_ALLOC=1")
        if bool(args.heap_diagnostics):
            bench_env_lines.append("BALUFFO_FETCH_HEAP_DIAGNOSTICS=1")
        if str(args.obscura_bin_host_path or "").strip():
            # ponytail: mount the host-side obscura binary read-only and point
            # the pool's backend switch at the in-container path. The Linux ELF
            # must match the container arch (x86_64) and glibc (>=2.35).
            obscura_host = Path(str(args.obscura_bin_host_path)).expanduser().resolve()
            if not obscura_host.is_dir():
                raise SystemExit(f"obscura bin dir not found: {obscura_host}")
            docker_args.extend(["-v", f"{str(obscura_host)}:/opt/obscura:ro"])
            bench_env_lines.append("BALUFFO_BROWSER_FALLBACK_BACKEND=obscura")
            bench_env_lines.append("BALUFFO_OBSCURA_BIN=/opt/obscura/obscura")
        if bench_env_lines:
            env_file = output_dir / "bench-only-sources.env"
            # ponytail: env-file staging avoids the Windows CreateProcess 32k
            # command-line cap — a 500-name only-sources list is ~36 KB.
            env_file.write_text("\n".join(bench_env_lines) + "\n", encoding="utf-8")
            docker_args.extend(["--env-file", str(env_file)])
            print(
                f"[bench] bench env file: {env_file} "
                f"({len(only_sources)} only-sources, alloc_profile={bool(args.profile_alloc)}, "
                f"maxWorkers={str(args.fetch_max_workers_env or '-')})",
                flush=True,
            )
        docker_args.append(str(args.image))
        _docker(*docker_args)
        starting_fresh = True
    else:
        starting_fresh = False

    try:
        _wait_ready(base_url)
        # Gateway can still answer 504 bridge_degraded briefly after /app/ready flips.
        # Ponytail: bounded retry instead of retry-decorator ceremony.
        gateway_ready_deadline = time.monotonic() + 30.0
        while time.monotonic() < gateway_ready_deadline:
            probe, probe_parsed = _http_json(base_url, "GET", "/ops/health", timeout_s=2.0)
            if probe.get("ok"):
                break
            if probe.get("status") == 504 and probe_parsed.get("gatewayReady") is True:
                time.sleep(0.5)
                continue
            break

        host_pid = _container_host_pid(container_name)
        # ponytail: on Windows+Docker Desktop the container PID lives inside the
        # WSL2 VM — host tasklist/proc can't see it. Fall back to `docker stats`
        # which is the standard way and works on every platform.
        use_docker_stats = os.name == "nt" or host_pid <= 0

        cpu_samples: list[dict[str, Any]] = []
        cpu_unsupported = ""

        if use_docker_stats:
            stats_handle = _docker_stats_sampler(container_name, interval_s=1.0)
            memory_sampler = None
            cpu_sampler = None
        else:
            stats_handle = None
            memory_sampler = _StampedMemorySampler(host_pid, interval_s=DEFAULT_MEMORY_INTERVAL_S)
            cpu_sampler = _CpuSampler(host_pid, interval_s=DEFAULT_CPU_INTERVAL_S)
            memory_sampler.start()
            cpu_sampler.start()

        trigger_started_monotonic = time.monotonic()
        trigger_wall = datetime.now(UTC)

        trigger_sample, trigger_parsed = _http_json(
            base_url,
            "POST",
            "/tasks/run-jobs-pipeline",
            timeout_s=30.0,
            body={"preset": str(args.preset)},
        )
        if not trigger_sample.get("ok"):
            _stop_samplers(
                memory_sampler if not use_docker_stats else None,
                cpu_sampler if not use_docker_stats else None,
                stats_handle if use_docker_stats else None,
            )
            raise RuntimeError(
                f"trigger failed status={trigger_sample.get('status')} "
                f"error={trigger_sample.get('error') or trigger_parsed}"
            )
        run_id = str(trigger_parsed.get("runId") or "").strip()

        poll = _poll_pipeline_task_live(
            base_url,
            run_id_hint=run_id,
            timeout_s=float(args.timeout),
            interval_s=DEFAULT_POLL_INTERVAL_S,
            fetch_tasks_path=data_volume / "jobs-fetch-tasks.json",
        )
        run_id = poll["runId"] or run_id

        if use_docker_stats:
            assert stats_handle is not None
            stop = stats_handle["stop"]
            stop.set()
            proc = stats_handle["proc"]
            try:
                proc.terminate()
                proc.wait(timeout=5)
            except (subprocess.SubprocessError, OSError):
                proc.kill()
            memory_samples = list(stats_handle["samples"])
            # docker stats gives us "cpuPercent" snapshots — integrate over
            # sampling dt to approximate CPU-seconds per stage. Report both.
            cpu_samples = [
                {"t": s["t"], "cpuPercent": s.get("cpuPercent") or 0.0} for s in memory_samples
            ]
            if not memory_samples:
                cpu_unsupported = "docker stats returned no samples"
        else:
            assert memory_sampler is not None and cpu_sampler is not None
            memory_samples = memory_sampler.stop()
            cpu_payload = cpu_sampler.stop()
            cpu_samples = list(cpu_payload.get("samples") or [])
            cpu_unsupported = str(cpu_payload.get("unsupportedReason") or "")
    finally:
        if starting_fresh:
            _stop_container(container_name)
        _restore_bench_overrides(overrides_backups)

    # Wait for the bridge to flush the lifecycle row to disk. Cheap retry loop.
    # ponytail: accept either finished or running rows; if the bridge is still
    # mid-finalize we still harvest stageLedger from the latest heartbeat.
    row = None
    for _ in range(40):
        row = _load_lifecycle_row(data_volume, run_id)
        if row is not None and (
            str(row.get("finishedAt") or "").strip()
            or (row.get("summary") or {}).get("stageLedger")
        ):
            break
        time.sleep(0.5)
    if row is None:
        raise RuntimeError(
            f"no pipeline lifecycle row persisted for run {run_id} "
            f"(checked {data_volume / 'admin-task-lifecycle.json'})"
        )

    summary = row.get("summary") if isinstance(row.get("summary"), dict) else {}
    ledger = summary.get("stageLedger") if isinstance(summary.get("stageLedger"), list) else []
    started_at = _parse_iso(row.get("startedAt"))
    finished_at = _parse_iso(row.get("finishedAt"))
    total_ms = 0
    if started_at is not None and finished_at is not None:
        total_ms = max(0, int((finished_at - started_at).total_seconds() * 1000))

    stage_rows = _compute_stage_durations(ledger, started_at=started_at, finished_at=finished_at)
    # ponytail: when the ledger only carries top-level stages but observations
    # captured fetch sub-phases ("fetch/<phase>"), splice the finer rows in so
    # the report shows actual work breakdown.
    if stage_rows and poll.get("observations"):
        stage_rows = _splice_sub_stage_observations(
            stage_rows,
            list(poll.get("observations") or []),
            anchor_monotonic=trigger_started_monotonic,
            anchor_wall=started_at or trigger_wall,
        )

    # Cross-anchor: use startedAt (wall) <-> trigger_started_monotonic (approx).
    # We didn't observe "startedAt" on the host clock; treat the trigger moment
    # as ~startedAt. Sub-second skew is noise vs stage durations.
    _attribute_samples_to_stages(
        stage_rows,
        memory_samples,
        cpu_samples,
        stage_anchor_monotonic=trigger_started_monotonic,
        stage_anchor_wall=started_at or trigger_wall,
    )

    terminal_reason = str(row.get("terminalReason") or row.get("status") or "")
    truncated = len(ledger) >= 64

    stages_doc = {
        "schemaVersion": 1,
        "runToken": run_token,
        "generated": _utc_now_iso(),
        "image": str(args.image),
        "profile": str(args.profile),
        "preset": str(args.preset),
        "containerName": container_name,
        "hostPid": host_pid,
        "dataVolume": str(data_volume),
        "runId": run_id,
        "startedAt": str(row.get("startedAt") or ""),
        "finishedAt": str(row.get("finishedAt") or ""),
        "terminalReason": terminal_reason,
        "truncated": truncated,
        "totalDurationMs": total_ms,
        "pollTimedOut": bool(poll.get("timedOut")),
        "stageRows": stage_rows,
        "observations": list(poll.get("observations") or []),
        "cpuUnsupportedReason": cpu_unsupported,
    }

    (output_dir / "stages.json").write_text(
        json.dumps(stages_doc, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    with (output_dir / "samples.ndjson").open("w", encoding="utf-8") as fh:
        for sample in memory_samples:
            row_out = {"kind": "memory", **sample}
            fh.write(json.dumps(row_out, ensure_ascii=False) + "\n")
        for sample in cpu_samples:
            row_out = {"kind": "cpu", **sample}
            fh.write(json.dumps(row_out, ensure_ascii=False) + "\n")

    (output_dir / "report.md").write_text(
        _render_markdown_report(
            run_token=run_token,
            profile=str(args.profile),
            image=str(args.image),
            preset=str(args.preset),
            run_id=run_id,
            started_at=str(row.get("startedAt") or ""),
            finished_at=str(row.get("finishedAt") or ""),
            total_ms=total_ms,
            terminal=terminal_reason,
            stage_rows=stage_rows,
        ),
        encoding="utf-8",
    )
    (output_dir / "FINDINGS.md").write_text(
        _render_findings(run_token=run_token, total_ms=total_ms, stage_rows=stage_rows),
        encoding="utf-8",
    )

    print(f"runId={run_id} total={total_ms:,} ms -> {output_dir}", flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
