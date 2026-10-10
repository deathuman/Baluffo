"""Priced ablation payload: measure a candidate change *before* committing to it.

Injected into one A/B arm through ``PYTHONPATH``: ``sitecustomize`` is imported by the
interpreter before the driver runs, so an arm can carry a patch while ``src/`` stays exactly as
it is in the other arm. Unset ``BALUFFO_ROW_DICT_ABLATION`` and importing this file costs one
environment lookup and returns.

**Why a payload rather than a switch in the product.** The point of the exercise is to price a
change *before* it is committed, so shipping the change -- even behind an env gate -- would
answer the question with the thing being asked about. The arm's environment differs from the
control's by named variables only; the repository's ``src/`` tree is byte-identical in both.

**The ablation it carries: ``cached``.** ``CanonicalJob.to_dict()`` gets a per-instance payload
built once (a plain field->value map, no deep copy) and hands every later caller a fresh
top-level copy of it, instead of running ``dataclasses.asdict`` -- a recursive deep copy of all
40 fields, their lists and their dicts -- on every call. A replay of the reference subset epoch
makes **2,076,173** of those calls for 30,053 rows (~69 per row) across 71 call sites, nearly
all of them reading one or two fields.

Two caveats make the delta it produces a **lower bound** on the candidate's ceiling, which is
the safe direction for a price:

* The probe still allocates a 40-key dict per call and still builds one payload per instance. A
  real implementation that simply reads ``job.source`` at the call site does strictly less.
* The probe shares nested containers (``sourceBundle``, ``locations``, ``availabilityEvidence``)
  with the instance; ``asdict`` deep-copies them. Top-level mutations are isolated (each call
  gets its own dict), nested ones would leak -- so the arms' parity line is the check that no
  caller depends on the deep copy. A divergence there is a finding about the candidate, not a
  broken measurement, and the receipt says so.

Everything the probe did is written to a marker file at exit (call count, builds, distinct
instances, payload mismatches), so an arm that silently failed to patch cannot be read as a
measured zero -- the failure mode S1 was corrected for.
"""

from __future__ import annotations

import atexit
import json
import os
import sys
from pathlib import Path
from typing import Any

ABLATION_ENV = "BALUFFO_ROW_DICT_ABLATION"
MARKER_ENV = "BALUFFO_PRICE_PROBE_MARKER"
ABLATION_CACHED = "cached"
# Enough for a whole-registry replay's live rows plus the merged rebuilds; a bound rather than a
# promise, so a pathological call pattern degrades to the cheap build instead of to unbounded RSS.
MAX_CACHE_ENTRIES = 200_000
# How many payloads to check against ``asdict`` before trusting the cheap build. The comparison is
# the equivalence claim; it is bounded because it costs a deep copy per check.
VERIFY_BUILDS = 25
_REPO_ROOT = Path(__file__).resolve().parents[5]


def cheap_payload(instance: Any, field_names: tuple[str, ...]) -> dict[str, Any]:
    """The field->value map ``asdict`` would return, without copying any of it.

    Same values, same order, same keys: ``asdict`` copies nested containers, this shares them.
    That is the whole ablation, and it is why the payload's nested containers must not be
    mutated in place by a caller -- the parity line is what tests that assumption.
    """

    return {name: getattr(instance, name) for name in field_names}


def write_marker(path: Path, payload: dict[str, Any]) -> None:
    """Write the probe's own receipt. Best effort: a measurement must not fail on its receipt."""

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    except OSError:
        pass


def install(*, ablation: str, marker_path: Path | None = None) -> dict[str, Any]:
    """Patch ``CanonicalJob.to_dict`` for one process and return the counters it keeps.

    Raises rather than falling back when it cannot patch: an arm whose probe never applied would
    otherwise report a delta of zero, which is indistinguishable from "the change is worthless".
    """

    if ablation != ABLATION_CACHED:
        raise ValueError(f"unknown ablation {ablation!r} (known: {ABLATION_CACHED!r})")
    if str(_REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(_REPO_ROOT))
    from src.jobs import models  # noqa: PLC0415 - the probe must patch after the env check

    target = models.CanonicalJob
    if getattr(target.to_dict, "_baluffo_price_probe", "") == ablation:
        return dict(getattr(target.to_dict, "_baluffo_probe_counters", {}))
    field_names = tuple(target.__dataclass_fields__)
    counters: dict[str, Any] = {
        "ablation": ablation,
        "pid": os.getpid(),
        "calls": 0,
        "builds": 0,
        "distinctInstances": 0,
        "cacheOverflow": 0,
        "emptied": False,
        "payloadMismatches": 0,
    }
    cache: dict[int, dict[str, Any]] = {}
    # Strong references on purpose: a cached payload must never be handed to a *different* object
    # that reused a collected id, and a slots dataclass has no ``__weakref__`` to key a weak map.
    keepalive: list[Any] = []

    def cached_to_dict(self: Any) -> dict[str, Any]:
        counters["calls"] += 1
        key = id(self)
        payload = cache.get(key)
        if payload is None:
            payload = cheap_payload(self, field_names)
            if int(counters["builds"]) < VERIFY_BUILDS:
                from dataclasses import asdict  # noqa: PLC0415

                if asdict(self) != payload:
                    counters["payloadMismatches"] += 1
                    raise RuntimeError(
                        "priced ablation changed the payload's values; the measurement would be "
                        "about different work, so the arm is failed instead"
                    )
            counters["builds"] += 1
            if len(cache) >= MAX_CACHE_ENTRIES:
                counters["cacheOverflow"] += 1
                counters["emptied"] = True
                cache.clear()
                keepalive.clear()
            cache[key] = payload
            keepalive.append(self)
            counters["distinctInstances"] = len(cache)
        # A fresh top-level dict per call: the callers that rebind a key of the payload they were
        # handed must not see each other's writes, and ``asdict`` gave each of them its own dict.
        return dict(payload)

    cached_to_dict._baluffo_price_probe = ablation  # type: ignore[attr-defined]
    cached_to_dict._baluffo_probe_counters = counters  # type: ignore[attr-defined]
    target.to_dict = cached_to_dict  # type: ignore[method-assign]
    if marker_path is not None:
        atexit.register(lambda: write_marker(marker_path, counters))
    return counters


def activate(*, environ: dict[str, str] | None = None) -> dict[str, Any] | None:
    """Arm the probe when the environment asks for it, or do nothing at all when it does not."""

    values = os.environ if environ is None else environ
    ablation = str(values.get(ABLATION_ENV) or "").strip()
    if not ablation:
        return None
    raw_marker = str(values.get(MARKER_ENV) or "").strip()
    marker = None
    if raw_marker:
        # Per-process, because one marker path serves every arm of a study.
        marker = Path(raw_marker) / f"probe-{os.getpid()}.json"
    return install(ablation=ablation, marker_path=marker)


ARMED: bool = activate() is not None
