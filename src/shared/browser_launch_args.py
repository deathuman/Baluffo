"""Chromium launch arguments shared by browser-fallback lanes.

AI boundary owns: Chromium process flags for browser fallback.
AI boundary implement in: this leaf; pooled and unpooled fallback callers import it.
AI boundary search before contracts: browser_fallback_pool and source_check_http.
AI boundary verify: `python -m pytest tests/test_browser_fallback_pool.py -q`.
"""

from __future__ import annotations

import sys

# Umbrel's container policy cannot launch Chromium's GPU helper. The pooled
# headless shell logs `GPU process launch failed: error_code=1002` repeatedly
# and exits through `FATAL:gpu_data_manager_impl_private.cc ... GPU process
# isn't usable` (SIGTRAP) before the first context. `--disable-gpu` alone still
# asks Chromium to launch that helper, so keep the GPU service in the browser
# process on Linux. This is the documented WSL/container workaround; unlike
# `--single-process`, renderer isolation remains intact.
_FALLBACK_CHROMIUM_ARGS: tuple[str, ...] = (
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--renderer-process-limit=2",
    "--js-flags=--max-old-space-size=512",
    "--disable-background-networking",
    "--mute-audio",
)
_LINUX_IN_PROCESS_GPU_ARG = "--in-process-gpu"


def browser_fallback_chromium_args(platform: str | None = None) -> list[str]:
    """Return the launch flags for headless Chromium used by fallback fetches."""
    args = list(_FALLBACK_CHROMIUM_ARGS)
    if (platform if platform is not None else sys.platform).startswith("linux"):
        args.append(_LINUX_IN_PROCESS_GPU_ARG)
    return args


__all__ = ["browser_fallback_chromium_args"]
