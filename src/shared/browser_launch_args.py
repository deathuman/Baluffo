"""Chromium launch arguments shared by browser-fallback lanes.

AI boundary owns: Chromium process flags for browser fallback.
AI boundary implement in: this leaf; pooled and unpooled fallback callers import it.
AI boundary search before contracts: browser_fallback_pool and source_check_http.
AI boundary verify: `python -m pytest tests/test_browser_fallback_pool.py -q`.
"""

from __future__ import annotations

import os
import sys

# Umbrel's container cannot launch Chromium's GPU helper through the zygote
# (SIGTRAP before the first context); `--in-process-gpu` traded that for a
# browser-process SIGSEGV at the first `new_page`. Bypass the zygote on root or
# container Linux only: desktop Linux keeps the zygote that carries the sandbox.
_FALLBACK_CHROMIUM_ARGS: tuple[str, ...] = (
    "--disable-dev-shm-usage",
    "--disable-gpu",
    "--renderer-process-limit=2",
    "--js-flags=--max-old-space-size=512",
    "--disable-background-networking",
    "--mute-audio",
)
_LINUX_NO_ZYGOTE_ARG = "--no-zygote"


def _running_as_root() -> bool:
    geteuid = getattr(os, "geteuid", None)
    return bool(geteuid and geteuid() == 0)


def _running_in_container() -> bool:
    return os.path.exists("/.dockerenv")


def browser_fallback_chromium_args(
    platform: str | None = None,
    *,
    is_root: bool | None = None,
    is_container: bool | None = None,
) -> list[str]:
    """Return the launch flags for headless Chromium used by fallback fetches."""
    args = list(_FALLBACK_CHROMIUM_ARGS)
    resolved_platform = platform if platform is not None else sys.platform
    if not resolved_platform.startswith("linux"):
        return args
    root = _running_as_root() if is_root is None else is_root
    container = _running_in_container() if is_container is None else is_container
    if root or container:
        args.append(_LINUX_NO_ZYGOTE_ARG)
    return args


__all__ = ["browser_fallback_chromium_args"]
