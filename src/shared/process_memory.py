"""Best-effort process-tree memory sampling without third-party dependencies."""

from __future__ import annotations

import ctypes
import os
import threading
from collections.abc import Mapping
from typing import Any


def _empty_sample(root_pid: int, *, unsupported_reason: str = "") -> dict[str, Any]:
    return {
        "rootPid": max(0, int(root_pid or 0)),
        "platform": os.name,
        "processCount": 0,
        "skippedProcessCount": 0,
        "workingSetBytes": 0,
        "rssBytes": 0,
        "processes": [],
        "unsupportedReason": unsupported_reason,
    }


def _children_by_parent(rows: Mapping[int, int]) -> dict[int, list[int]]:
    children: dict[int, list[int]] = {}
    for pid, ppid in rows.items():
        children.setdefault(int(ppid), []).append(int(pid))
    return children


def _process_tree(root_pid: int, rows: Mapping[int, int]) -> list[int]:
    root = int(root_pid or 0)
    if root <= 0 or root not in rows:
        return []
    children = _children_by_parent(rows)
    pending = [root]
    seen: set[int] = set()
    while pending:
        pid = pending.pop()
        if pid in seen:
            continue
        seen.add(pid)
        pending.extend(children.get(pid, []))
    return sorted(seen)


def _process_memory_bytes(row: Mapping[str, Any]) -> int:
    return max(int(row.get("workingSetBytes") or 0), int(row.get("rssBytes") or 0))


def _sample_memory_bytes(sample: Mapping[str, Any]) -> int:
    return max(int(sample.get("workingSetBytes") or 0), int(sample.get("rssBytes") or 0))


def _process_category(row: Mapping[str, Any]) -> str:
    name = str(row.get("name") or "").lower()
    image_path = str(row.get("imagePath") or "").lower()
    command_line = str(row.get("commandLine") or "").lower()
    haystack = " ".join((name, image_path, command_line))
    if any(token in haystack for token in ("chrome", "chromium", "msedge", "firefox", "webkit")):
        return "browser"
    if "baluffo" in haystack:
        return "baluffo"
    if any(token in haystack for token in ("python", "python.exe", "py.exe")):
        return "python"
    if any(token in haystack for token in ("node", "node.exe", "npm", "npx")):
        return "node"
    return "other"


def _normalize_process_row(row: Mapping[str, Any]) -> dict[str, Any]:
    working_set = max(0, int(row.get("workingSetBytes") or 0))
    rss = max(0, int(row.get("rssBytes") or 0))
    normalized = {
        "pid": max(0, int(row.get("pid") or 0)),
        "parentPid": max(0, int(row.get("parentPid") or 0)),
        "name": str(row.get("name") or ""),
        "imagePath": str(row.get("imagePath") or ""),
        "commandLine": str(row.get("commandLine") or ""),
        "workingSetBytes": working_set,
        "rssBytes": rss,
        "memoryBytes": max(working_set, rss),
        "skipped": bool(row.get("skipped")),
        "unsupportedReason": str(row.get("unsupportedReason") or ""),
    }
    normalized["category"] = str(row.get("category") or _process_category(normalized))
    return normalized


def _sorted_processes(rows: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    normalized = [_normalize_process_row(row) for row in rows if isinstance(row, Mapping)]
    normalized.sort(key=lambda row: int(row.get("memoryBytes") or 0), reverse=True)
    return normalized


def _cached_process_metadata(
    metadata_cache: dict[int, dict[str, Any]] | None,
    *,
    pid: int,
    name: str,
    image_path_loader: Any,
    command_line_loader: Any | None = None,
) -> dict[str, str]:
    if metadata_cache is not None:
        cached = metadata_cache.get(int(pid))
        if isinstance(cached, dict) and str(cached.get("name") or "") == str(name or ""):
            return {
                "name": str(cached.get("name") or name or ""),
                "imagePath": str(cached.get("imagePath") or ""),
                "commandLine": str(cached.get("commandLine") or ""),
            }
    image_path = str(image_path_loader() or "")
    command_line = str(command_line_loader() or "") if command_line_loader else ""
    metadata = {
        "name": str(name or ""),
        "imagePath": image_path,
        "commandLine": command_line,
    }
    if metadata_cache is not None:
        metadata_cache[int(pid)] = dict(metadata)
    return metadata


def _windows_process_table() -> dict[int, dict[str, Any]]:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    th32cs_snapprocess = 0x00000002
    invalid_handle_value = ctypes.c_void_p(-1).value

    class PROCESSENTRY32W(ctypes.Structure):
        _fields_ = [
            ("dwSize", ctypes.c_ulong),
            ("cntUsage", ctypes.c_ulong),
            ("th32ProcessID", ctypes.c_ulong),
            ("th32DefaultHeapID", ctypes.c_void_p),
            ("th32ModuleID", ctypes.c_ulong),
            ("cntThreads", ctypes.c_ulong),
            ("th32ParentProcessID", ctypes.c_ulong),
            ("pcPriClassBase", ctypes.c_long),
            ("dwFlags", ctypes.c_ulong),
            ("szExeFile", ctypes.c_wchar * 260),
        ]

    snapshot = kernel32.CreateToolhelp32Snapshot(th32cs_snapprocess, 0)
    if not snapshot or snapshot == invalid_handle_value:
        raise OSError(ctypes.get_last_error(), "CreateToolhelp32Snapshot failed")
    try:
        entry = PROCESSENTRY32W()
        entry.dwSize = ctypes.sizeof(PROCESSENTRY32W)
        rows: dict[int, dict[str, Any]] = {}
        ok = kernel32.Process32FirstW(snapshot, ctypes.byref(entry))
        while ok:
            pid = int(entry.th32ProcessID)
            rows[pid] = {
                "pid": pid,
                "parentPid": int(entry.th32ParentProcessID),
                "name": str(entry.szExeFile or ""),
                "imagePath": "",
                "commandLine": "",
            }
            ok = kernel32.Process32NextW(snapshot, ctypes.byref(entry))
        return rows
    finally:
        kernel32.CloseHandle(snapshot)


def _windows_process_image_path(pid: int) -> str:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    process_query_limited_information = 0x1000
    query_full_process_image_name = getattr(kernel32, "QueryFullProcessImageNameW", None)
    if query_full_process_image_name is None:
        return ""
    kernel32.OpenProcess.restype = ctypes.c_void_p
    handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
    if not handle:
        return ""
    try:
        buffer = ctypes.create_unicode_buffer(32768)
        size = ctypes.c_ulong(len(buffer))
        ok = query_full_process_image_name(handle, 0, buffer, ctypes.byref(size))
        return str(buffer.value or "") if ok else ""
    finally:
        kernel32.CloseHandle(handle)


def _windows_working_set_bytes(pid: int) -> int:
    kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
    psapi = ctypes.WinDLL("psapi", use_last_error=True)
    process_query_information = 0x0400
    process_query_limited_information = 0x1000
    process_vm_read = 0x0010

    class PROCESS_MEMORY_COUNTERS_EX(ctypes.Structure):
        _fields_ = [
            ("cb", ctypes.c_ulong),
            ("PageFaultCount", ctypes.c_ulong),
            ("PeakWorkingSetSize", ctypes.c_size_t),
            ("WorkingSetSize", ctypes.c_size_t),
            ("QuotaPeakPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPagedPoolUsage", ctypes.c_size_t),
            ("QuotaPeakNonPagedPoolUsage", ctypes.c_size_t),
            ("QuotaNonPagedPoolUsage", ctypes.c_size_t),
            ("PagefileUsage", ctypes.c_size_t),
            ("PeakPagefileUsage", ctypes.c_size_t),
            ("PrivateUsage", ctypes.c_size_t),
        ]

    kernel32.OpenProcess.restype = ctypes.c_void_p
    handle = kernel32.OpenProcess(process_query_information | process_vm_read, False, int(pid))
    if not handle:
        handle = kernel32.OpenProcess(process_query_limited_information, False, int(pid))
    if not handle:
        raise OSError(ctypes.get_last_error(), f"OpenProcess failed for pid={pid}")
    try:
        counters = PROCESS_MEMORY_COUNTERS_EX()
        counters.cb = ctypes.sizeof(PROCESS_MEMORY_COUNTERS_EX)
        ok = psapi.GetProcessMemoryInfo(
            handle,
            ctypes.byref(counters),
            ctypes.sizeof(PROCESS_MEMORY_COUNTERS_EX),
        )
        if not ok:
            raise OSError(ctypes.get_last_error(), f"GetProcessMemoryInfo failed for pid={pid}")
        return max(0, int(counters.WorkingSetSize))
    finally:
        kernel32.CloseHandle(handle)


def _posix_command_line(pid_name: str) -> str:
    try:
        with open(os.path.join("/proc", pid_name, "cmdline"), "rb") as handle:
            return handle.read().replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
    except OSError:
        return ""


def _posix_image_path(pid_name: str) -> str:
    try:
        return os.readlink(os.path.join("/proc", pid_name, "exe"))
    except OSError:
        return ""


def _posix_process_table(
    metadata_cache: dict[int, dict[str, Any]] | None = None,
) -> dict[int, dict[str, Any]]:
    proc = "/proc"
    if not os.path.isdir(proc):
        raise RuntimeError("/proc is not available")
    rows: dict[int, dict[str, Any]] = {}
    for name in os.listdir(proc):
        if not name.isdigit():
            continue
        stat_path = os.path.join(proc, name, "stat")
        try:
            with open(stat_path, encoding="utf-8") as handle:
                raw = handle.read()
        except OSError:
            continue
        start = raw.find("(")
        end = raw.rfind(")")
        if start < 0 or end < 0:
            continue
        parts = raw[end + 2 :].split()
        if len(parts) < 2:
            continue
        pid = int(name)
        process_name = raw[start + 1 : end]
        metadata = _cached_process_metadata(
            metadata_cache,
            pid=pid,
            name=process_name,
            image_path_loader=lambda pid_name=name: _posix_image_path(pid_name),
            command_line_loader=lambda pid_name=name: _posix_command_line(pid_name),
        )
        rows[pid] = {
            "pid": pid,
            "parentPid": int(parts[1]),
            "name": str(metadata.get("name") or process_name),
            "imagePath": str(metadata.get("imagePath") or ""),
            "commandLine": str(metadata.get("commandLine") or ""),
        }
    return rows


def _posix_rss_bytes(pid: int) -> int:
    page_size = os.sysconf("SC_PAGE_SIZE")
    statm_path = os.path.join("/proc", str(int(pid)), "statm")
    with open(statm_path, encoding="utf-8") as handle:
        raw = handle.read().split()
    if len(raw) < 2:
        return 0
    return max(0, int(raw[1]) * int(page_size))


def sample_process_tree(
    root_pid: int,
    *,
    metadata_cache: dict[int, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    root = int(root_pid or 0)
    if root <= 0:
        return _empty_sample(root, unsupported_reason="missing root pid")
    if os.name == "nt":
        try:
            table = _windows_process_table()
            parents = {pid: int(row.get("parentPid") or 0) for pid, row in table.items()}
            pids = _process_tree(root, parents)
            skipped = 0
            total = 0
            processes: list[dict[str, Any]] = []
            for pid in pids:
                row = dict(table.get(pid) or {"pid": pid, "parentPid": 0})
                metadata = _cached_process_metadata(
                    metadata_cache,
                    pid=pid,
                    name=str(row.get("name") or ""),
                    image_path_loader=lambda current_pid=pid: _windows_process_image_path(
                        current_pid
                    ),
                )
                row["name"] = str(metadata.get("name") or row.get("name") or "")
                row["imagePath"] = str(metadata.get("imagePath") or "")
                row["commandLine"] = str(metadata.get("commandLine") or "")
                try:
                    row["workingSetBytes"] = _windows_working_set_bytes(pid)
                    row["rssBytes"] = 0
                except OSError:
                    skipped += 1
                    row["workingSetBytes"] = 0
                    row["rssBytes"] = 0
                    row["skipped"] = True
                    row["unsupportedReason"] = "memory unavailable"
                normalized = _normalize_process_row(row)
                total += int(normalized.get("workingSetBytes") or 0)
                processes.append(normalized)
            return {
                "rootPid": root,
                "platform": os.name,
                "processCount": len(pids),
                "skippedProcessCount": skipped,
                "workingSetBytes": total,
                "rssBytes": 0,
                "processes": _sorted_processes(processes),
                "unsupportedReason": "",
            }
        except (OSError, TypeError, ValueError) as exc:
            return _empty_sample(root, unsupported_reason=str(exc))
    if os.path.isdir("/proc"):
        try:
            table = _posix_process_table(metadata_cache)
            parents = {pid: int(row.get("parentPid") or 0) for pid, row in table.items()}
            pids = _process_tree(root, parents)
            skipped = 0
            total = 0
            processes: list[dict[str, Any]] = []
            for pid in pids:
                row = dict(table.get(pid) or {"pid": pid, "parentPid": 0})
                try:
                    row["workingSetBytes"] = 0
                    row["rssBytes"] = _posix_rss_bytes(pid)
                except OSError:
                    skipped += 1
                    row["workingSetBytes"] = 0
                    row["rssBytes"] = 0
                    row["skipped"] = True
                    row["unsupportedReason"] = "memory unavailable"
                normalized = _normalize_process_row(row)
                total += int(normalized.get("rssBytes") or 0)
                processes.append(normalized)
            return {
                "rootPid": root,
                "platform": os.name,
                "processCount": len(pids),
                "skippedProcessCount": skipped,
                "workingSetBytes": 0,
                "rssBytes": total,
                "processes": _sorted_processes(processes),
                "unsupportedReason": "",
            }
        except (OSError, TypeError, ValueError) as exc:
            return _empty_sample(root, unsupported_reason=str(exc))
    return _empty_sample(root, unsupported_reason="process-tree memory sampling unsupported")


def summarize_memory_samples(samples: list[dict[str, Any]]) -> dict[str, Any]:
    usable = [sample for sample in samples if not str(sample.get("unsupportedReason") or "")]
    reasons = [
        str(sample.get("unsupportedReason") or "")
        for sample in samples
        if str(sample.get("unsupportedReason") or "")
    ]
    peak_sample = max(usable, key=_sample_memory_bytes) if usable else {}
    peak_processes = _sorted_processes(list(peak_sample.get("processes") or []))
    category_peaks: dict[str, int] = {
        "browser": 0,
        "python": 0,
        "node": 0,
        "baluffo": 0,
        "other": 0,
    }
    top_by_identity: dict[tuple[int, str, str, str], dict[str, Any]] = {}
    for sample in usable:
        category_totals: dict[str, int] = {}
        for process in _sorted_processes(list(sample.get("processes") or [])):
            category = str(process.get("category") or "other")
            bytes_value = _process_memory_bytes(process)
            category_totals[category] = category_totals.get(category, 0) + bytes_value
            identity = (
                int(process.get("pid") or 0),
                str(process.get("name") or ""),
                str(process.get("imagePath") or ""),
                str(process.get("commandLine") or ""),
            )
            existing = top_by_identity.get(identity)
            if existing is None:
                top_by_identity[identity] = {
                    "pid": int(process.get("pid") or 0),
                    "parentPid": int(process.get("parentPid") or 0),
                    "name": str(process.get("name") or ""),
                    "imagePath": str(process.get("imagePath") or ""),
                    "commandLine": str(process.get("commandLine") or ""),
                    "category": category,
                    "peakWorkingSetBytes": int(process.get("workingSetBytes") or 0),
                    "peakRssBytes": int(process.get("rssBytes") or 0),
                    "peakBytes": bytes_value,
                    "sampleCount": 1,
                }
                continue
            existing["sampleCount"] = int(existing.get("sampleCount") or 0) + 1
            if bytes_value > int(existing.get("peakBytes") or 0):
                existing["parentPid"] = int(process.get("parentPid") or 0)
                existing["peakWorkingSetBytes"] = int(process.get("workingSetBytes") or 0)
                existing["peakRssBytes"] = int(process.get("rssBytes") or 0)
                existing["peakBytes"] = bytes_value
        for category, total in category_totals.items():
            category_peaks[category] = max(int(category_peaks.get(category) or 0), int(total))
    top_processes = sorted(
        top_by_identity.values(),
        key=lambda row: int(row.get("peakBytes") or 0),
        reverse=True,
    )[:10]

    def _sample_summary(sample: dict[str, Any]) -> dict[str, Any]:
        if not sample:
            return {}
        processes = _sorted_processes(list(sample.get("processes") or []))
        category_totals: dict[str, int] = {}
        for process in processes:
            category = str(process.get("category") or "other")
            category_totals[category] = category_totals.get(category, 0) + _process_memory_bytes(
                process
            )
        return {
            "rootPid": int(sample.get("rootPid") or 0),
            "platform": str(sample.get("platform") or ""),
            "processCount": int(sample.get("processCount") or 0),
            "workingSetBytes": int(sample.get("workingSetBytes") or 0),
            "rssBytes": int(sample.get("rssBytes") or 0),
            "memoryBytes": _sample_memory_bytes(sample),
            "categoryTotals": category_totals,
            "topProcesses": processes[:5],
        }

    first_sample = usable[0] if usable else {}
    last_sample = usable[-1] if usable else {}
    return {
        "sampleCount": len(usable),
        "peakWorkingSetBytes": max(
            [int(sample.get("workingSetBytes") or 0) for sample in usable] or [0]
        ),
        "peakRssBytes": max([int(sample.get("rssBytes") or 0) for sample in usable] or [0]),
        "maxProcessCount": max([int(sample.get("processCount") or 0) for sample in usable] or [0]),
        "skippedProcessCount": sum(
            int(sample.get("skippedProcessCount") or 0) for sample in usable
        ),
        "unsupportedReason": "" if usable else (reasons[0] if reasons else ""),
        "peakSample": {
            "rootPid": int(peak_sample.get("rootPid") or 0),
            "platform": str(peak_sample.get("platform") or ""),
            "processCount": int(peak_sample.get("processCount") or 0),
            "workingSetBytes": int(peak_sample.get("workingSetBytes") or 0),
            "rssBytes": int(peak_sample.get("rssBytes") or 0),
            "memoryBytes": _sample_memory_bytes(peak_sample),
            "processes": peak_processes,
        }
        if usable
        else {},
        "firstSample": _sample_summary(first_sample),
        "lastSample": _sample_summary(last_sample),
        "topProcesses": top_processes,
        "categoryPeaks": category_peaks if usable else {},
    }


class ProcessMemorySampler:
    def __init__(self, root_pid: int, *, interval_s: float = 0.1) -> None:
        self._root_pid = int(root_pid or 0)
        self._interval_s = max(0.02, float(interval_s or 0.1))
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._samples: list[dict[str, Any]] = []
        self._metadata_cache: dict[int, dict[str, Any]] = {}

    def __enter__(self) -> ProcessMemorySampler:
        self.start()
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.stop()

    def start(self) -> None:
        if self._thread is not None:
            return
        self._thread = threading.Thread(
            target=self._run,
            name="process-memory-sampler",
            daemon=True,
        )
        self._thread.start()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            self._samples.append(
                sample_process_tree(self._root_pid, metadata_cache=self._metadata_cache)
            )
            self._stop_event.wait(self._interval_s)

    def stop(self) -> dict[str, Any]:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join(timeout=max(1.0, self._interval_s * 4))
        if not self._samples:
            self._samples.append(
                sample_process_tree(self._root_pid, metadata_cache=self._metadata_cache)
            )
        return summarize_memory_samples(list(self._samples))
