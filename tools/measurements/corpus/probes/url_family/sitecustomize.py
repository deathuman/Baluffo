"""Priced ablation payload: measure the URL-family memo *before* committing to it.

Injected into one A/B arm through ``PYTHONPATH`` (the interpreter imports ``sitecustomize`` before
the driver runs), so an arm can carry a patch while ``src/`` stays byte-identical in both arms.
Unset ``BALUFFO_URL_FAMILY_ABLATION`` and importing this file costs one environment lookup.

**Why a payload rather than a switch in the product.** The point is to price the change before it
is written, so shipping it -- even behind an env gate -- would answer the question with the thing
being asked about. The arms differ by named environment variables only.

**The ablation it carries: ``cached``.** The three pure members of the URL family get a bounded
``lru_cache`` in front of them:

* ``text_utils.normalize_url`` -- 3.2 M calls in the reference subset replay (S1's profile put
  57.8 s cumulative under it, including ~34 s of ``urllib.parse`` self time);
* ``common.url.canonical_url_fingerprint_seed`` -- the 853 k-call wrapper that re-parses what
  ``normalize_url`` already parsed;
* ``common.url.is_public_job_url`` -- 728 k calls, each of which re-parses the URL *and* takes an
  ``ipaddress.ip_address`` ``ValueError`` for every non-IP host.

Each is a function of its argument alone, and each returns an immutable value (``str``/``bool``),
so a memo cannot hand one caller another's mutable state.

The memo is keyed on the **exact argument** and only when it is hashable: an unhashable input
bypasses the cache and runs uncached, so the patched function's semantics are the unpatched
function's semantics, including for the ``Any``-typed callers that pass ``None`` or a mapping. The
bound is the one the shipped version would use (4,096 entries, ``lru_cache`` eviction), and the
probe verifies its own results against the unpatched function for the first few misses, raising
instead of reporting a delta about different work. Everything it counted goes to a marker file.
"""

from __future__ import annotations

import atexit
import json
import os
import sys
from collections.abc import Callable
from functools import lru_cache
from pathlib import Path
from typing import Any

ABLATION_ENV = "BALUFFO_URL_FAMILY_ABLATION"
MARKER_ENV = "BALUFFO_URL_FAMILY_MARKER"
ABLATION_CACHED = "cached"
CACHE_SIZE = 4096
# How many results to check against the unpatched function before trusting the memo.
VERIFY_CALLS = 25
_REPO_ROOT = Path(__file__).resolve().parents[5]


def write_marker(path: Path, payload: dict[str, Any]) -> None:
    """Write the probe's own receipt. Best effort: a measurement must not fail on its receipt."""

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    except OSError:
        pass


class Counter:
    """Per-function call accounting the marker carries."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.calls = 0
        self.misses = 0
        self.hits = 0
        self.distinct = 0
        self.mismatches = 0

    def as_dict(self) -> dict[str, Any]:
        return {
            "calls": self.calls,
            "misses": self.misses,
            "hits": self.hits,
            "distinct": self.distinct,
            "mismatches": self.mismatches,
        }


def _wrap(name: str, original: Callable[..., Any], counters: dict[str, Any]) -> Callable[..., Any]:
    """A bounded memo in front of a pure function, with self-verification and accounting."""

    counter = Counter(name)
    counters[name] = counter
    memo = lru_cache(maxsize=CACHE_SIZE)(original)

    def wrapper(value: Any) -> Any:
        counter.calls += 1
        try:
            hash(value)
        except TypeError:
            # The callers are ``Any``-typed: the rare unhashable argument runs uncached, exactly as
            # it would without the ablation, rather than being coerced into a different call.
            return original(value)
        before = memo.cache_info()
        result = memo(value)
        after = memo.cache_info()
        if after.misses != before.misses:
            counter.misses += 1
            if counter.misses <= VERIFY_CALLS:
                if result != original(value):
                    counter.mismatches += 1
                    raise RuntimeError(
                        f"priced ablation changed {name}'s result; the measurement would be about "
                        "different work, so the arm is failed instead"
                    )
        else:
            counter.hits += 1
        counter.distinct = after.currsize
        return result

    wrapper._baluffo_url_family_probe = name  # type: ignore[attr-defined]
    return wrapper


def install(*, ablation: str, marker_path: Path | None = None) -> dict[str, Any]:
    """Patch the URL family for one process and return the counters it keeps."""

    if ablation != ABLATION_CACHED:
        raise ValueError(f"unknown ablation {ablation!r} (known: {ABLATION_CACHED!r})")
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))
    from src.jobs import text_utils  # noqa: PLC0415 - the probe must patch after the env check
    from src.jobs.common import url as url_module  # noqa: PLC0415

    counters: dict[str, Any] = {"ablation": ablation, "pid": os.getpid()}
    targets = (
        (text_utils, "normalize_url"),
        (url_module, "canonical_url_fingerprint_seed"),
        (url_module, "is_public_job_url"),
    )
    patched = []
    for module, name in targets:
        original = getattr(module, name)
        if getattr(original, "_baluffo_url_family_probe", ""):
            continue
        setattr(module, name, _wrap(name, original, counters))
        patched.append(name)
    counters["patched"] = patched
    if marker_path is not None:
        atexit.register(lambda: write_marker(marker_path, _receipt(counters)))
    return counters


def _receipt(counters: dict[str, Any]) -> dict[str, Any]:
    """The marker's JSON-safe view of the counters (the live dict holds ``Counter`` objects)."""

    return {
        key: value.as_dict() if isinstance(value, Counter) else value
        for key, value in counters.items()
    }


def activate(*, environ: dict[str, str] | None = None) -> dict[str, Any] | None:
    """Arm the probe when the environment asks for it, or do nothing at all when it does not."""

    values = os.environ if environ is None else environ
    ablation = str(values.get(ABLATION_ENV) or "").strip()
    if not ablation:
        return None
    raw_marker = str(values.get(MARKER_ENV) or "").strip()
    marker = Path(raw_marker) / f"probe-{os.getpid()}.json" if raw_marker else None
    return install(ablation=ablation, marker_path=marker)


ARMED: bool = activate() is not None
