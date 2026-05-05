from __future__ import annotations

import cProfile
import io
import os
import pstats
import re
import threading
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


def _write_profile_outputs(profiler: cProfile.Profile, *, profile_name: str) -> None:
    out_dir = profile_output_dir()
    out_dir.mkdir(parents=True, exist_ok=True)
    safe_name = sanitize_profile_name(profile_name)
    profile_path = out_dir / f"{safe_name}.prof"
    profiler.dump_stats(str(profile_path))
    text_stream = io.StringIO()
    try:
        pstats.Stats(str(profile_path), stream=text_stream).sort_stats("cumulative").print_stats(30)
    except Exception as exc:  # pragma: no cover - defensive across Python/profile formats
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
    "profile_enabled",
    "profile_output_dir",
    "run_profiled",
    "sanitize_profile_name",
]
