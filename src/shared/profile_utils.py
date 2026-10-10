"""Shared cProfile capture and stats helpers.

AI boundary owns: profile capture, stats formatting, and profile file naming helpers.
AI boundary implement in: this file for profiler utilities; callers own when profiling is enabled.
AI boundary search before contracts: performance-profile bridge helpers and profiling tests.
AI boundary verify: `npm run lint:repo-guardrails` plus focused profiling tests.
"""

from __future__ import annotations

import cProfile
import io
import json
import os
import pstats
import re
import sys
import threading
import time
import tracemalloc
from collections.abc import Callable, Mapping
from pathlib import Path
from types import FrameType
from typing import Any

_PROFILE_ENABLED_VALUES = {"1", "true", "yes", "on"}
_SAFE_FILENAME_RE = re.compile(r"[^a-zA-Z0-9_.-]+")
_MULTI_UNDERSCORE_RE = re.compile(r"_+")
_PROFILE_LOCK = threading.Lock()


def profile_enabled(env: Mapping[str, str] | None = None) -> bool:
    values = os.environ if env is None else env
    return str(values.get("BALUFFO_PROFILE") or "").strip().lower() in _PROFILE_ENABLED_VALUES


def sanitize_profile_name(value: Any) -> str:
    text = str(value or "").strip()
    text = _SAFE_FILENAME_RE.sub("_", text).strip("._-")
    text = _MULTI_UNDERSCORE_RE.sub("_", text)
    return text or "default"


def profile_output_dir(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    base_dir = Path(str(values.get("BALUFFO_DATA_DIR") or "_out"))
    return base_dir / "perf-profiles"


def runtime_alloc_profile_enabled(env: Mapping[str, str] | None = None) -> bool:
    """``BALUFFO_PROFILE_ALLOC`` gates per-source tracemalloc capture.

    Off by default: tracemalloc adds meaningful CPU overhead on hot paths and
    is unsafe to run nested (we guard with a module-level lock).
    """
    values = os.environ if env is None else env
    return str(values.get("BALUFFO_PROFILE_ALLOC") or "").strip().lower() in _PROFILE_ENABLED_VALUES


def alloc_profile_log_path(env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    base_dir = Path(str(values.get("BALUFFO_DATA_DIR") or "_out"))
    return base_dir / "perf-profiles" / "allocations.jsonl"


_ALLOC_LOCK = threading.Lock()
_TRACEMALLOC_TOP_N = 20


def _format_alloc_frame(stat: tracemalloc.Statistic) -> str:
    frame = stat.traceback[0] if stat.traceback else None
    if frame is None:
        return "<unknown>"
    return f"{frame.filename}:{frame.lineno}"


def run_profiled_alloc[T](
    fn: Callable[..., T],
    *args: Any,
    profile_name: str = "default",
    source_name: str = "",
    **kwargs: Any,
) -> T:
    """Run ``fn`` with tracemalloc on, emitting a JSONL row per invocation.

    Rows land in ``<data_dir>/perf-profiles/allocations.jsonl`` with:
    ``source_name``, ``duration_ms``, ``peak_mib``, ``current_mib``, and
    ``top_frames`` (top N frames by cumulative allocation size, pinned to
    ``file:line`` so they aggregate cleanly across runs).

    ponytail: single global lock means sources serialize when profiling,
    which distorts wall-clock but NOT per-source allocation shape. Only use
    this when diagnosing one source at a time (or in a bench where lock
    distortion is acceptable).
    """
    if not runtime_alloc_profile_enabled():
        return fn(*args, **kwargs)

    with _ALLOC_LOCK:
        started = tracemalloc.is_tracing()
        if not started:
            tracemalloc.start()
        tracemalloc.reset_peak()
        before_cur, _ = tracemalloc.get_traced_memory()
        t0 = os.times()[4] if hasattr(os, "times") else 0.0
        try:
            return fn(*args, **kwargs)
        finally:
            t1 = os.times()[4] if hasattr(os, "times") else 0.0
            after_cur, after_peak = tracemalloc.get_traced_memory()
            snapshot = tracemalloc.take_snapshot()
            top: list[dict[str, Any]] = []
            try:
                stats = snapshot.statistics("filename")
            except Exception:
                stats = []
            for stat in stats[:_TRACEMALLOC_TOP_N]:
                top.append(
                    {
                        "frame": _format_alloc_frame(stat),
                        "size_mib": round(stat.size / (1024 * 1024), 3),
                        "count": stat.count,
                    }
                )
            payload = {
                "profile": profile_name,
                "source": source_name,
                "duration_ms": int(max(0.0, t1 - t0) * 1000),
                "peak_mib": round(after_peak / (1024 * 1024), 3),
                "current_mib": round(after_cur / (1024 * 1024), 3),
                "delta_mib": round((after_cur - before_cur) / (1024 * 1024), 3),
                "top_frames": top,
            }
            try:
                alloc_profile_log_path().parent.mkdir(parents=True, exist_ok=True)
                with alloc_profile_log_path().open("a", encoding="utf-8") as handle:
                    handle.write(json.dumps(payload, ensure_ascii=False) + "\n")
            except OSError:
                pass
            if not started:
                tracemalloc.stop()


# --- Opt-in all-threads sampler ---------------------------------------------
#
# Why sampling and not cProfile: in CPython 3.13 cProfile runs on a single
# ``sys.monitoring`` tool slot, so two profiles cannot be active at once -- a
# per-thread cProfile raises ``ValueError: tool 2 is already in use``, and the
# ``_PROFILE_LOCK`` above is load-bearing for exactly that reason. Taking that
# lock to capture a whole run would also serialize every source, which deletes
# the cross-thread contention such a capture exists to measure. A sampling
# thread reads every thread's Python stack with no hot-path hook, and costs
# nothing at all while disabled.

_SAMPLE_INTERVAL_ENV = "BALUFFO_PROFILE_SAMPLE"
_MAX_SAMPLE_INTERVAL_MS = 2000
_MAX_STACK_DEPTH = 64
_SAMPLE_TOP_N = 200
_SAMPLE_TOP_FRAMES_PER_THREAD = 25
# One minute, not five: the fetch child is killed when the pipeline completes,
# so the exit dump never runs and the last periodic dump is the only record of
# the tail. A 300 s interval cost the finalize phase its attribution entirely
# on the first real run.
_SAMPLE_DUMP_INTERVAL_S = 60.0
_PROC_TASK_DIR_AVAILABLE: bool | None = None


def sample_interval_ms(env: Mapping[str, str] | None = None) -> int:
    """Sampler interval in milliseconds; ``0`` means disabled.

    Unset is disabled. A value that is not a positive integer is also disabled
    rather than defaulted, so a typo in a bench env file cannot start a sampler
    that nobody asked for.
    """
    values = os.environ if env is None else env
    raw = str(values.get(_SAMPLE_INTERVAL_ENV) or "").strip()
    if not raw.lstrip("+").isdigit():
        return 0
    return min(int(raw.lstrip("+")), _MAX_SAMPLE_INTERVAL_MS)


def sample_profile_path(profile_name: str, env: Mapping[str, str] | None = None) -> Path:
    values = os.environ if env is None else env
    base_dir = Path(str(values.get("BALUFFO_DATA_DIR") or "_out"))
    return base_dir / "perf-profiles" / f"cpu-samples-{sanitize_profile_name(profile_name)}.json"


def _clock_ticks_per_second() -> int:
    # ``os.sysconf`` does not exist on Windows (and its stub says so), so it is looked up
    # dynamically: the sampler is stdlib-only and must import on every platform, even
    # where /proc is absent and per-thread CPU falls back to elapsed weighting.
    sysconf = getattr(os, "sysconf", None)
    if sysconf is None:
        return 100
    try:
        return int(sysconf("SC_CLK_TCK"))
    except (AttributeError, OSError, ValueError):
        return 100


def _parse_task_stat_cpu_seconds(raw: bytes | str) -> float | None:
    """``utime + stime`` from one ``/proc/<pid>/task/<tid>/stat`` line.

    ``comm`` may contain spaces and parentheses, so fields are counted from
    after the *last* ``)`` -- splitting the whole line shifts every index. Pure
    and total: a shape we do not recognise returns ``None`` rather than a guess.
    """
    text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
    close = text.rfind(")")
    if close < 0:
        return None
    fields = text[close + 1 :].split()
    if len(fields) < 13:
        return None
    try:
        utime_ticks = int(fields[11])
        stime_ticks = int(fields[12])
    except ValueError:
        return None
    return (utime_ticks + stime_ticks) / _clock_ticks_per_second()


def _proc_thread_cpu_available() -> bool:
    """Whether this platform can report per-thread CPU for our own threads.

    Probed once. Without it, every sample pays a failed lookup on the exception
    path -- which is both wasteful and loud enough in the self-time ranking to
    look like real work.
    """
    global _PROC_TASK_DIR_AVAILABLE
    if _PROC_TASK_DIR_AVAILABLE is None:
        _PROC_TASK_DIR_AVAILABLE = os.path.isdir("/proc/self/task")
    return bool(_PROC_TASK_DIR_AVAILABLE)


def native_thread_ids() -> dict[int, int]:
    """Map Python thread ident -> kernel tid for every live thread.

    Load-bearing: ``sys._current_frames()`` is keyed by ``threading.get_ident()``,
    which is a Python-level identifier and *not* the kernel tid. Reading
    ``/proc/self/task/<ident>/stat`` therefore fails for every thread, and the
    failure looks exactly like a platform without ``/proc`` -- which is how the
    first version of this sampler reported elapsed time as per-thread CPU. The
    kernel tid is on ``Thread.native_id``, and only exists while the thread runs,
    so the map is rebuilt from the live thread table. It is also what lets the
    report join these rows to a sampler reading the same child from outside.
    """
    mapping: dict[int, int] = {}
    for thread in threading.enumerate():
        if thread.ident is not None and thread.native_id is not None:
            mapping[thread.ident] = int(thread.native_id)
    return mapping


def _thread_cpu_seconds(native_id: int) -> float | None:
    """Per-thread CPU seconds, or ``None`` where ``/proc`` is unavailable.

    Linux-only on purpose: the sampler runs inside the fetch child in the
    container, which is where the container's own ``/proc`` describes our
    threads. Elsewhere the sampler still records stacks and weights them by
    elapsed time, and the output says which weighting was used.
    """
    if not _proc_thread_cpu_available():
        return None
    try:
        with open(f"/proc/self/task/{int(native_id)}/stat", "rb") as handle:
            return _parse_task_stat_cpu_seconds(handle.read())
    except OSError:
        return None


def _frame_key(frame: FrameType) -> str:
    """Function-level key: ``filename:firstlineno:name``.

    Deliberately not the *current* line: a hot function would otherwise be split
    across dozens of keys, and the ranking the report needs is by function.
    """
    code = frame.f_code
    filename = str(code.co_filename or "").replace("\\", "/")
    return f"{filename}:{code.co_firstlineno}:{code.co_name}"


def _walk_frame_keys(frame: FrameType | None, *, depth: int = _MAX_STACK_DEPTH) -> list[str]:
    keys: list[str] = []
    current = frame
    while current is not None and len(keys) < depth:
        keys.append(_frame_key(current))
        current = current.f_back
    return keys


class ThreadSampleProfile:
    """Sampling capture across every thread of the process.

    One sample per interval and thread: the innermost frame is attributed the
    interval's weight as *self* time, and every frame in the stack is attributed
    it as *cumulative* time. Weight is the thread's ``/proc`` CPU delta when it
    is readable (so the result is CPU, not wall time) and the interval elapsed
    otherwise. The sampler's own thread is counted in the thread table -- its
    overhead stays visible -- but excluded from the function rankings, which
    would otherwise be dominated by the sampler's own frames.
    """

    def __init__(self, *, profile_name: str, interval_ms: int) -> None:
        self.profile_name = profile_name
        self.interval_ms = interval_ms
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._own_tid = 0
        self._samples = 0
        self._cpu_weighted = False
        self._started_at = 0.0
        self._last_dump_at = 0.0
        self._self_seconds: dict[str, float] = {}
        self._self_samples: dict[str, int] = {}
        self._cumulative_seconds: dict[str, float] = {}
        self._thread_seconds: dict[int, float] = {}
        self._thread_samples: dict[int, int] = {}
        self._thread_self_seconds: dict[int, dict[str, float]] = {}
        self._thread_names: dict[int, str] = {}
        self._thread_native_ids: dict[int, int] = {}
        self._prev_cpu: dict[int, float] = {}
        self._prev_at: dict[int, float] = {}

    def start(self) -> None:
        self._thread = threading.Thread(target=self.run, name="baluffo-cpu-sampler", daemon=True)
        self._thread.start()

    def run(self) -> None:
        self._own_tid = threading.get_ident()
        self._started_at = time.monotonic()
        self._last_dump_at = self._started_at
        interval_s = self.interval_ms / 1000.0
        while not self._stop.wait(interval_s):
            self._sample_once()
            if time.monotonic() - self._last_dump_at >= _SAMPLE_DUMP_INTERVAL_S:
                # ponytail: dump periodically, not only at exit -- a killed
                # container never reaches a ``finally``, and a two-hour capture
                # that yields nothing is worse than a coarser one that survives.
                self.dump()

    def stop(self) -> None:
        self._stop.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=5)

    def _sample_once(self) -> None:
        now = time.monotonic()
        # ponytail: remember every thread's name as we see it. A thread that
        # finishes is gone from enumerate() before the dump, and an unnamed row
        # cannot be told apart from the transport loop.
        native_ids = native_thread_ids()
        for live in threading.enumerate():
            if live.ident is not None:
                self._thread_names[live.ident] = live.name
        for tid, frame in list(sys._current_frames().items()):
            native_id = native_ids.get(tid)
            cpu_seconds = _thread_cpu_seconds(native_id) if native_id is not None else None
            previous_cpu = self._prev_cpu.get(tid)
            if cpu_seconds is not None and previous_cpu is not None:
                weight = max(0.0, cpu_seconds - previous_cpu)
                self._cpu_weighted = True
            else:
                weight = max(0.0, now - self._prev_at.get(tid, now))
            if cpu_seconds is not None:
                self._prev_cpu[tid] = cpu_seconds
            self._prev_at[tid] = now
            self._thread_seconds[tid] = self._thread_seconds.get(tid, 0.0) + weight
            self._thread_samples[tid] = self._thread_samples.get(tid, 0) + 1
            if native_id is not None:
                self._thread_native_ids[tid] = native_id
            if tid == self._own_tid:
                continue
            self._samples += 1
            innermost = _frame_key(frame)
            self._self_seconds[innermost] = self._self_seconds.get(innermost, 0.0) + weight
            self._self_samples[innermost] = self._self_samples.get(innermost, 0) + 1
            thread_self = self._thread_self_seconds.setdefault(tid, {})
            thread_self[innermost] = thread_self.get(innermost, 0.0) + weight
            for key in _walk_frame_keys(frame):
                self._cumulative_seconds[key] = self._cumulative_seconds.get(key, 0.0) + weight

    @staticmethod
    def _ranked(seconds: Mapping[str, float], samples: Mapping[str, int]) -> list[dict[str, Any]]:
        ranked = sorted(seconds.items(), key=lambda item: (-item[1], item[0]))[:_SAMPLE_TOP_N]
        return [
            {
                "frame": frame,
                # Named for what it is: CPU seconds under ``proc-cpu`` weighting,
                # plain elapsed seconds under ``elapsed``. Calling both
                # "cpuSeconds" is how an elapsed-weighted run gets read as CPU.
                "weightSeconds": round(value, 6),
                "samples": int(samples.get(frame, 0)),
            }
            for frame, value in ranked
        ]

    def snapshot(self) -> dict[str, Any]:
        thread_names = {
            **self._thread_names,
            **{t.ident: t.name for t in threading.enumerate() if t.ident is not None},
        }
        threads: list[dict[str, Any]] = []
        for tid in sorted(self._thread_seconds):
            per_thread = self._thread_self_seconds.get(tid) or {}
            threads.append(
                {
                    # Python ident, not the kernel tid: the row also carries
                    # nativeId, which is what /proc and any external sampler use.
                    "tid": tid,
                    "nativeId": self._thread_native_ids.get(tid),
                    "name": thread_names.get(tid, ""),
                    "samples": self._thread_samples.get(tid, 0),
                    "weightSeconds": round(self._thread_seconds[tid], 6),
                    "isSampler": tid == self._own_tid,
                    "topSelfFrames": self._ranked(per_thread, {})[:_SAMPLE_TOP_FRAMES_PER_THREAD],
                }
            )
        return {
            "profile": self.profile_name,
            "intervalMs": self.interval_ms,
            "cpuWeighted": self._cpu_weighted,
            "weighting": "proc-cpu" if self._cpu_weighted else "elapsed",
            "samples": self._samples,
            "durationMs": int(max(0.0, time.monotonic() - self._started_at) * 1000),
            "threads": threads,
            "selfWeight": self._ranked(self._self_seconds, self._self_samples),
            "cumulativeWeight": self._ranked(self._cumulative_seconds, {}),
        }

    def dump(self) -> Path | None:
        path = sample_profile_path(self.profile_name)
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            tmp_path = path.with_name(path.name + ".tmp")
            tmp_path.write_text(
                json.dumps(self.snapshot(), ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8",
            )
            tmp_path.replace(path)
        except OSError:
            return None
        self._last_dump_at = time.monotonic()
        return path


def start_sample_profile(*, profile_name: str = "run") -> ThreadSampleProfile | None:
    """Start the all-threads sampler, or return ``None`` when it is not asked for.

    Disabled, this is one env lookup and no allocation -- the fetch child calls
    it unconditionally at process start.
    """
    interval_ms = sample_interval_ms()
    if interval_ms <= 0:
        return None
    sampler = ThreadSampleProfile(
        profile_name=sanitize_profile_name(profile_name), interval_ms=interval_ms
    )
    sampler.start()
    return sampler


def stop_sample_profile(sampler: ThreadSampleProfile | None) -> Path | None:
    if sampler is None:
        return None
    sampler.stop()
    return sampler.dump()


def _write_profile_outputs(profiler: cProfile.Profile, *, profile_name: str) -> None:
    out_dir = profile_output_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_name = sanitize_profile_name(profile_name)
    profile_path = out_dir / f"{safe_name}.prof"
    profiler.dump_stats(str(profile_path))
    text_stream = io.StringIO()
    try:
        pstats.Stats(str(profile_path), stream=text_stream).sort_stats("cumulative").print_stats(30)
    except (EOFError, OSError, TypeError, ValueError) as exc:
        text_stream.write(f"Unable to render profile summary: {exc}\n")
    (out_dir / f"{safe_name}.prof.txt").write_text(text_stream.getvalue(), encoding="utf-8")


def run_profiled[T](
    fn: Callable[..., T],
    *args: Any,
    profile_name: str = "default",
    **kwargs: Any,
) -> T:
    if not profile_enabled():
        return fn(*args, **kwargs)

    with _PROFILE_LOCK:
        profiler = cProfile.Profile()
        try:
            return profiler.runcall(fn, *args, **kwargs)
        finally:
            _write_profile_outputs(profiler, profile_name=profile_name)


__all__ = [
    "ThreadSampleProfile",
    "alloc_profile_log_path",
    "profile_enabled",
    "profile_output_dir",
    "run_profiled",
    "run_profiled_alloc",
    "runtime_alloc_profile_enabled",
    "sample_interval_ms",
    "sample_profile_path",
    "sanitize_profile_name",
    "start_sample_profile",
    "stop_sample_profile",
]
