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
import threading
import tracemalloc
from collections.abc import Callable, Mapping
from pathlib import Path
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
    "alloc_profile_log_path",
    "profile_enabled",
    "profile_output_dir",
    "run_profiled",
    "run_profiled_alloc",
    "runtime_alloc_profile_enabled",
    "sanitize_profile_name",
]
