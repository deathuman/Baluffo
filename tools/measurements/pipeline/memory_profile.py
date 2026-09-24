#!/usr/bin/env python3
"""Sample Linux cgroup and process memory for a Baluffo benchmark run.

This is development-only tooling. It is intentionally dependency-free so it can
be copied or bind-mounted into a running Linux container without changing the
application image or adding runtime dependencies.
"""

from __future__ import annotations

import argparse
import json
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_CGROUP_STAT_KEYS = (
    "anon",
    "file",
    "shmem",
    "slab",
    "kernel",
    "active_anon",
    "inactive_anon",
    "active_file",
    "inactive_file",
    "rss",
    "cache",
)
_PROCESS_STATUS_KEYS = ("VmRSS", "VmHWM", "RssAnon", "RssFile", "RssShmem")
_PROCESS_SMAPS_KEYS = (
    "Rss",
    "Pss",
    "Private_Clean",
    "Private_Dirty",
    "Swap",
)


def _read_int(path: Path) -> int | None:
    try:
        value = path.read_text(encoding="utf-8").strip()
    except (OSError, UnicodeDecodeError):
        return None
    if not value or value == "max":
        return None
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def _read_key_values(path: Path, keys: tuple[str, ...]) -> dict[str, int]:
    result: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return result
    for line in lines:
        key, separator, raw_value = line.partition(" ")
        if not separator or key not in keys:
            continue
        try:
            result[key] = int(raw_value.strip())
        except (TypeError, ValueError, OverflowError):
            continue
    return result


def _cgroup_file(root: Path, *relative_paths: str) -> Path | None:
    for relative_path in relative_paths:
        candidate = root / relative_path
        if candidate.exists():
            return candidate
    return None


def detect_cgroup_version(cgroup_root: Path) -> int:
    if (cgroup_root / "cgroup.controllers").exists() or (cgroup_root / "memory.current").exists():
        return 2
    if (cgroup_root / "memory").is_dir() or (cgroup_root / "memory.usage_in_bytes").exists():
        return 1
    return 0


def _v2_sample(cgroup_root: Path) -> dict[str, Any]:
    events_path = _cgroup_file(cgroup_root, "memory.events", "memory.events.local")
    stat_path = _cgroup_file(cgroup_root, "memory.stat")
    events = (
        _read_key_values(events_path, ("low", "high", "max", "oom", "oom_kill"))
        if events_path
        else {}
    )
    stat = _read_key_values(stat_path, _CGROUP_STAT_KEYS) if stat_path else {}
    return {
        "cgroupVersion": 2,
        "available": (cgroup_root / "memory.current").exists(),
        "memoryCurrentBytes": _read_int(cgroup_root / "memory.current"),
        "memoryMaxBytes": _read_int(cgroup_root / "memory.max"),
        "memoryPeakBytes": _read_int(cgroup_root / "memory.peak"),
        "memorySwapCurrentBytes": _read_int(cgroup_root / "memory.swap.current"),
        "memorySwapMaxBytes": _read_int(cgroup_root / "memory.swap.max"),
        "memoryEvents": events,
        "memoryStat": stat,
    }


def _v1_sample(cgroup_root: Path) -> dict[str, Any]:
    memory_root = cgroup_root / "memory" if (cgroup_root / "memory").is_dir() else cgroup_root
    stat_path = _cgroup_file(memory_root, "memory.stat")
    stat = _read_key_values(stat_path, _CGROUP_STAT_KEYS) if stat_path else {}
    stat.setdefault("anon", stat.get("rss", 0))
    stat.setdefault("file", stat.get("cache", 0))
    return {
        "cgroupVersion": 1,
        "available": (memory_root / "memory.usage_in_bytes").exists(),
        "memoryCurrentBytes": _read_int(memory_root / "memory.usage_in_bytes"),
        "memoryMaxBytes": _read_int(memory_root / "memory.limit_in_bytes"),
        "memoryPeakBytes": _read_int(memory_root / "memory.max_usage_in_bytes"),
        "memorySwapCurrentBytes": None,
        "memorySwapMaxBytes": None,
        "memoryEvents": {
            "failcnt": _read_int(memory_root / "memory.failcnt") or 0,
        },
        "memoryStat": stat,
    }


def sample_cgroup(cgroup_root: Path) -> dict[str, Any]:
    version = detect_cgroup_version(cgroup_root)
    if version == 2:
        return _v2_sample(cgroup_root)
    if version == 1:
        return _v1_sample(cgroup_root)
    return {
        "cgroupVersion": 0,
        "available": False,
        "memoryCurrentBytes": None,
        "memoryMaxBytes": None,
        "memoryPeakBytes": None,
        "memorySwapCurrentBytes": None,
        "memorySwapMaxBytes": None,
        "memoryEvents": {},
        "memoryStat": {},
    }


def _parse_proc_stat(path: Path) -> tuple[int, int] | None:
    try:
        raw = path.read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError):
        return None
    closing = raw.rfind(")")
    if closing < 0:
        return None
    fields = raw[closing + 2 :].split()
    if len(fields) < 2:
        return None
    try:
        return 0, int(fields[1])
    except (TypeError, ValueError, OverflowError):
        return None


def _read_status(path: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return result
    for line in lines:
        key, separator, raw_value = line.partition(":")
        if not separator or key not in _PROCESS_STATUS_KEYS:
            continue
        fields = raw_value.strip().split()
        try:
            value = int(fields[0]) * 1024
        except (IndexError, TypeError, ValueError, OverflowError):
            continue
        result[key] = value
    return result


def _read_smaps(path: Path) -> dict[str, int]:
    result: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeDecodeError):
        return result
    for line in lines:
        key, separator, raw_value = line.partition(":")
        if not separator or key not in _PROCESS_SMAPS_KEYS:
            continue
        fields = raw_value.strip().split()
        try:
            result[key] = int(fields[0]) * 1024
        except (IndexError, TypeError, ValueError, OverflowError):
            continue
    return result


def _read_command(path: Path) -> str:
    try:
        return path.read_bytes().replace(b"\0", b" ").decode("utf-8", "replace").strip()[:240]
    except OSError:
        return ""


def sample_process(pid: int, proc_root: Path = Path("/proc")) -> dict[str, Any] | None:
    process_root = proc_root / str(pid)
    stat = _parse_proc_stat(process_root / "stat")
    if stat is None:
        return None
    status = _read_status(process_root / "status")
    smaps = _read_smaps(process_root / "smaps_rollup")
    cgroup = ""
    try:
        cgroup = (process_root / "cgroup").read_text(encoding="utf-8").strip()[:500]
    except (OSError, UnicodeDecodeError):
        pass
    return {
        "pid": pid,
        "ppid": stat[1],
        "command": _read_command(process_root / "cmdline"),
        "cgroup": cgroup,
        "rssBytes": status.get("VmRSS", 0),
        "hwmBytes": status.get("VmHWM", 0),
        "rssAnonBytes": status.get("RssAnon", 0),
        "rssFileBytes": status.get("RssFile", 0),
        "rssShmemBytes": status.get("RssShmem", 0),
        "pssBytes": smaps.get("Pss", 0),
        "privateCleanBytes": smaps.get("Private_Clean", 0),
        "privateDirtyBytes": smaps.get("Private_Dirty", 0),
        "swapBytes": smaps.get("Swap", 0),
    }


def sample_processes(proc_root: Path = Path("/proc"), *, limit: int = 100) -> list[dict[str, Any]]:
    processes: list[dict[str, Any]] = []
    try:
        entries = list(proc_root.iterdir())
    except OSError:
        return processes
    for entry in entries:
        if not entry.name.isdigit():
            continue
        sample = sample_process(int(entry.name), proc_root)
        if sample is not None:
            processes.append(sample)
    processes.sort(
        key=lambda row: (int(row.get("pssBytes") or 0), int(row.get("rssBytes") or 0)), reverse=True
    )
    return processes[: max(1, int(limit or 100))]


def _read_phase(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.is_file():
        return None
    try:
        raw = path.read_text(encoding="utf-8").strip()
        if raw.startswith("{"):
            value = json.loads(raw)
            return value if isinstance(value, dict) else {"value": value}
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        pass
    return {"value": raw[:240]} if raw else None


def sample_once(
    *,
    cgroup_root: Path,
    proc_root: Path = Path("/proc"),
    phase_file: Path | None = None,
    process_limit: int = 100,
) -> dict[str, Any]:
    sample: dict[str, Any] = {
        "sampledAt": datetime.now(UTC).isoformat(),
        "monotonicSeconds": time.monotonic(),
        "cgroup": sample_cgroup(cgroup_root),
        "processes": sample_processes(proc_root, limit=process_limit),
    }
    phase = _read_phase(phase_file)
    if phase is not None:
        sample["phase"] = phase
    return sample


def run_sampler(
    *,
    output_path: Path,
    cgroup_root: Path,
    proc_root: Path = Path("/proc"),
    interval_s: float = 1.0,
    duration_s: float = 0.0,
    stop_file: Path | None = None,
    phase_file: Path | None = None,
    process_limit: int = 100,
) -> int:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.monotonic()
    count = 0
    with output_path.open("a", encoding="utf-8") as handle:
        while True:
            sample = sample_once(
                cgroup_root=cgroup_root,
                proc_root=proc_root,
                phase_file=phase_file,
                process_limit=process_limit,
            )
            handle.write(json.dumps(sample, sort_keys=True) + "\n")
            handle.flush()
            count += 1
            if duration_s > 0 and time.monotonic() - started >= duration_s:
                break
            if stop_file is not None and stop_file.exists():
                break
            time.sleep(max(0.05, float(interval_s)))
    return count


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--cgroup-root", type=Path, default=Path("/sys/fs/cgroup"))
    parser.add_argument("--proc-root", type=Path, default=Path("/proc"))
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--duration", type=float, default=0.0)
    parser.add_argument("--stop-file", type=Path)
    parser.add_argument("--phase-file", type=Path)
    parser.add_argument("--process-limit", type=int, default=100)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    count = run_sampler(
        output_path=args.output,
        cgroup_root=args.cgroup_root,
        proc_root=args.proc_root,
        interval_s=args.interval,
        duration_s=args.duration,
        stop_file=args.stop_file,
        phase_file=args.phase_file,
        process_limit=args.process_limit,
    )
    print(f"wrote {count} memory samples to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
