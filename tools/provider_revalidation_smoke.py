#!/usr/bin/env python3
"""Prove a provider feed revalidates instead of re-reading its whole listing.

A provider board may publish its ``ETag`` / ``Last-Modified`` on the GET that carries
the jobs, and nowhere else. This tool runs one registered feed twice against an
isolated state file:

    run 1   forced full read -> the validator must land in jobs-source-state.json
    run 2   the same state, aged past the run freshness window but inside the
            provider stable window -> the decision must be ``revalidate_only``,
            the feed must answer 304, and no listing body may be fetched

It is a smoke check, not a pipeline run: it drives the one provider loader, the
shipped transport and the real state writer, but skips dedup, availability and
output assembly. Point it at any *registered* provider feed; ``--list`` shows what a
given adapter has:

  python tools/provider_revalidation_smoke.py --adapter greenhouse --source atariinc
  python tools/provider_revalidation_smoke.py --adapter lever --list
  python tools/provider_revalidation_smoke.py --adapter greenhouse --all

``--all`` sweeps every feed the adapter registers and classifies each one by what
its own conditional read proved:

  honours        publishes a validator and answers the probe with 304, no body
  ignores        publishes a validator but answers If-None-Match with a body
  probe_refused  publishes a validator but the probe was rejected (e.g. HEAD 405)
  no_probe       publishes a validator but the runner never sends If-None-Match
  no_validator   the full read published no ETag/Last-Modified
  read_failed    the full read failed, so nothing could be asked
  inconclusive   the policy did not reach revalidate_only (e.g. state lost the tag)

A sweep exits 1 only for ``ignores`` -- a feed that publishes a validator and then
ignores it -- because that is the defect a sweep is looking for. The other classes
are reported, not failed: a refused probe is not evidence about If-None-Match.

Everything is written under ``--output-dir`` (default
``_out/provider-revalidation-smoke/<adapter>-<board>``, or ``<adapter>-sweep`` for
``--all``); the tool refuses a path inside the repo's ``data/`` because that is live
state.

A single feed exits 0 only when the validator was stored, the decision was
revalidation, and the second run answered 304 without fetching the listing.
"""

from __future__ import annotations

import argparse
import copy
import importlib
import json
import pkgutil
import sys
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from datetime import UTC, datetime, timedelta
from functools import lru_cache
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.jobs.adapters import default_source_loaders  # noqa: E402
from src.jobs.adapters.plugins import provider_api as provider_package  # noqa: E402
from src.jobs.common import config as common_config  # noqa: E402
from src.jobs.common import diagnostics  # noqa: E402
from src.jobs.common.datetime_utils import parse_datetime  # noqa: E402
from src.jobs.registry import registry_entries  # noqa: E402
from src.jobs.state_incremental import get_incremental_cache_decision  # noqa: E402
from src.jobs.state_source_records import read_source_state, write_source_state  # noqa: E402
from src.jobs.state_source_state import update_source_state_rows  # noqa: E402
from src.jobs.text_utils import clean_text  # noqa: E402
from src.jobs.transport import default_fetch_text  # noqa: E402

# Fields a feed's registry row can be recognized by, most specific first.
IDENTITY_FIELDS = (
    "name",
    "slug",
    "account",
    "org",
    "company",
    "subdomain",
    "studio",
    "board_url",
    "listing_url",
    "base_url",
)

# Clocks a run that happened `minutes` ago would have moved.
AGED_FIELDS = (
    "lastRunAt",
    "lastCheckedAt",
    "lastSuccessAt",
    "lastNonEmptyAt",
    "lastChangedAt",
    "lastSuccessfulFetchAt",
    "lastSeenInFetchAt",
    "nextEligibleCheckAt",
)

STATE_FILE = "jobs-source-state.json"
REVALIDATED_EXCLUSION = "cache_not_modified_304"

OUTCOME_HONOURS = "honours"
OUTCOME_IGNORES = "ignores"
OUTCOME_PROBE_REFUSED = "probe_refused"
OUTCOME_NO_PROBE = "no_probe"
OUTCOME_NO_VALIDATOR = "no_validator"
OUTCOME_READ_FAILED = "read_failed"
OUTCOME_INCONCLUSIVE = "inconclusive"

# Report order: the defect being hunted first, the good news last.
OUTCOME_ORDER = (
    OUTCOME_IGNORES,
    OUTCOME_PROBE_REFUSED,
    OUTCOME_NO_PROBE,
    OUTCOME_NO_VALIDATOR,
    OUTCOME_READ_FAILED,
    OUTCOME_INCONCLUSIVE,
    OUTCOME_HONOURS,
)

RegistryPatch = list[tuple[Any, str, Any]]


def loader_for(adapter: str) -> tuple[str, Callable[..., list[dict[str, Any]]]]:
    """The one source loader that serves this adapter (greenhouse is the odd name)."""
    loaders = dict(default_source_loaders())
    for name in (f"{adapter}_boards", f"{adapter}_sources"):
        if name in loaders:
            return name, loaders[name]
    provider_names = sorted(name for name in loaders if name.endswith(("_boards", "_sources")))
    raise SystemExit(
        f"[smoke] no provider loader for adapter {adapter!r}.\n"
        f"        provider loaders: {', '.join(provider_names)}"
    )


def candidates_for(adapter: str) -> list[dict[str, Any]]:
    """Registered feeds for one provider adapter, pending migrations included."""
    return list(registry_entries(adapter, include_pending_provider_migration=True))


def match_rows(rows: list[dict[str, Any]], query: str) -> list[dict[str, Any]]:
    """Rows a ``--source`` query names: exact identity fields first, then substrings."""
    needle = clean_text(query).casefold()
    if not needle:
        return []
    exact = [
        row
        for row in rows
        if any(clean_text(row.get(field)).casefold() == needle for field in IDENTITY_FIELDS)
    ]
    if exact:
        return exact
    return [
        row
        for row in rows
        if any(needle in clean_text(row.get(field)).casefold() for field in IDENTITY_FIELDS)
    ]


def describe_row(row: dict[str, Any]) -> str:
    url = clean_text(row.get("listing_url") or row.get("board_url") or row.get("careersUrl"))
    keys = ", ".join(
        f"{field}={clean_text(row.get(field))}"
        for field in ("slug", "account", "org", "company", "subdomain")
        if clean_text(row.get(field))
    )
    return f"{clean_text(row.get('name')) or '?'}\t{keys}\t{url}"


@lru_cache(maxsize=1)
def _provider_modules() -> tuple[Any, ...]:
    return tuple(
        importlib.import_module(f"{provider_package.__name__}.{info.name}")
        for info in pkgutil.iter_modules(provider_package.__path__)
    )


def install_single_row_registry(row: dict[str, Any]) -> RegistryPatch:
    """Point every provider runner's registry lookup at the one chosen feed.

    The runners import ``registry_entries`` into their own module namespace, so the
    filter has to be installed per module rather than on the registry leaf. The
    originals come back so a sweep over several feeds can put each one back.
    """

    def _only(_adapter_key: str) -> list[dict[str, Any]]:
        return [dict(row)]

    patched: RegistryPatch = []
    for module in _provider_modules():
        original = module.__dict__.get("registry_entries")
        if callable(original):
            module.__dict__["registry_entries"] = _only
            patched.append((module, "registry_entries", original))
    personio = importlib.import_module("src.jobs.adapters.provider_personio")
    original = personio.__dict__.get("jobs_registry_entries")
    if callable(original):
        personio.__dict__["jobs_registry_entries"] = _only
        patched.append((personio, "jobs_registry_entries", original))
    return patched


def restore_registry(patched: RegistryPatch) -> None:
    for module, attr, original in patched:
        module.__dict__[attr] = original


class ProbeRecorder:
    """The conditional probes provider runners send, with the answer they saw.

    A full read overwrites the report's ``httpStatus`` after a probe it did not
    honour, so a finished report cannot say whether the feed answered 200 or 405.
    Wrapping the lifecycle helper captures that answer at the moment it arrives.
    """

    def __init__(self) -> None:
        self.probes: list[dict[str, Any]] = []
        self._patched: list[tuple[Any, Any]] = []

    def install(self) -> None:
        for module in _provider_modules():
            original = module.__dict__.get("provider_revalidate_not_modified")
            if callable(original):
                module.__dict__["provider_revalidate_not_modified"] = self._wrap(original)
                self._patched.append((module, original))

    def restore(self) -> None:
        for module, original in self._patched:
            module.__dict__["provider_revalidate_not_modified"] = original
        self._patched.clear()

    def reset(self) -> None:
        self.probes.clear()

    def _wrap(self, original: Callable[..., bool]) -> Callable[..., bool]:
        def wrapped(*args: Any, **kwargs: Any) -> bool:
            entry_report = kwargs.get("entry_report")
            asked = isinstance(entry_report, dict) and (
                clean_text(entry_report.get("cacheDecision")) == "revalidate_only"
            )
            handled = original(*args, **kwargs)
            if asked and isinstance(entry_report, dict):
                self.probes.append(
                    {
                        "source": clean_text(entry_report.get("name")),
                        "url": clean_text(kwargs.get("url")),
                        "statusCode": int(entry_report.get("httpStatus") or 0),
                        "notModified": bool(handled),
                    }
                )
            return handled

        return wrapped


@contextmanager
def record_provider_probes() -> Iterator[ProbeRecorder]:
    recorder = ProbeRecorder()
    recorder.install()
    try:
        yield recorder
    finally:
        recorder.restore()


def age_state(rows: dict[str, dict[str, Any]], minutes: int) -> dict[str, dict[str, Any]]:
    """Move a stored state back in time, as a run `minutes` ago would have left it."""
    aged = copy.deepcopy(rows)
    delta = timedelta(minutes=minutes)
    for entry in aged.values():
        for field in AGED_FIELDS:
            parsed = parse_datetime(entry.get(field))
            if parsed is not None:
                entry[field] = (parsed - delta).isoformat()
    return aged


def _run_once(
    *,
    loader: Callable[..., list[dict[str, Any]]],
    loader_name: str,
    source_name: str,
    state_rows: dict[str, dict[str, Any]],
    force_refresh_all: bool,
    fetch_text: Callable[..., str],
    timeout_s: int,
    retries: int,
    backoff_s: float,
) -> dict[str, Any]:
    """One real run of the single feed, counting every listing body it fetches."""
    urls: list[str] = []

    def counting_fetch(url: str, timeout: int, request=None) -> str:
        urls.append(url)
        return fetch_text(url, timeout, request=request)

    diagnostics.SOURCE_DIAGNOSTICS.clear()
    errors: list[str] = []
    jobs: list[dict[str, Any]] = []
    try:
        jobs = loader(
            fetch_text=counting_fetch,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
            source_state_rows=state_rows,
            force_refresh_all=force_refresh_all,
        )
    except Exception as exc:  # noqa: BLE001 - the run's own report carries the diagnosis
        errors.append(f"{type(exc).__name__}: {exc}")
    diag = diagnostics.SOURCE_DIAGNOSTICS.get(loader_name) or {}
    details = [detail for detail in diag.get("details") or [] if isinstance(detail, dict)]
    detail = next(
        (item for item in details if clean_text(item.get("name")) == source_name),
        details[0] if details else {},
    )
    errors.extend(str(item) for item in diag.get("partialErrors") or [])
    return {"detail": detail, "urls": urls, "kept": len(jobs or []), "errors": errors}


def _apply_report(
    *,
    state_rows: dict[str, dict[str, Any]],
    detail: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    return update_source_state_rows(
        source_state_rows=state_rows,
        source_reports=[detail],
        canonical_rows=[],
        finished_at=datetime.now(UTC).isoformat(),
        circuit_breaker_failures=3,
        circuit_breaker_cooldown_minutes=720,
    )


def run_smoke(
    *,
    row: dict[str, Any],
    loader_name: str,
    loader: Callable[..., list[dict[str, Any]]],
    fetch_text: Callable[..., str],
    output_dir: Path,
    age_minutes: int,
    timeout_s: int = 20,
    retries: int = 1,
    backoff_s: float = 0.5,
) -> dict[str, Any]:
    """Run the two-phase cycle and return what happened, with a list of failures."""
    source_name = clean_text(row.get("name"))
    adapter = clean_text(row.get("adapter"))
    output_dir.mkdir(parents=True, exist_ok=True)
    state_path = output_dir / STATE_FILE
    for stale in (state_path, state_path.with_suffix(".json.gz")):
        stale.unlink(missing_ok=True)

    failures: list[str] = []
    first = _run_once(
        loader=loader,
        loader_name=loader_name,
        source_name=source_name,
        state_rows={},
        force_refresh_all=True,
        fetch_text=fetch_text,
        timeout_s=timeout_s,
        retries=retries,
        backoff_s=backoff_s,
    )
    detail = first["detail"]
    tag = clean_text(detail.get("httpEtag")) or clean_text(detail.get("httpLastModified"))
    if not tag:
        failures.append(
            "the full read reported no ETag/Last-Modified; this feed cannot revalidate "
            f"(status={detail.get('status')!r}, httpStatus={detail.get('httpStatus')!r}, "
            f"error={detail.get('error')!r})"
        )

    write_source_state(state_path, _apply_report(state_rows={}, detail=detail))
    saved = read_source_state(state_path)
    saved_entry = saved.get(source_name) or {}
    stored = clean_text(saved_entry.get("lastHttpEtag")) or clean_text(
        saved_entry.get("lastHttpLastModified")
    )
    if tag and not stored:
        failures.append(
            f"the full read reported {tag!r} but {STATE_FILE} stored no validator "
            f"for {source_name!r}"
        )

    # Without a stored validator there is nothing to revalidate with, so the second
    # phase would only report the skip that follows -- one cause, named once.
    decision: dict[str, Any] = {}
    second: dict[str, Any] = {"detail": {}, "urls": [], "kept": 0, "errors": []}
    final_entry: dict[str, Any] = {}
    if not failures:
        aged = age_state(saved, age_minutes)
        write_source_state(state_path, aged)
        reloaded = read_source_state(state_path)
        decision = get_incremental_cache_decision(source_name, reloaded, adapter=adapter)
        if clean_text(decision.get("cacheDecision")) != "revalidate_only":
            failures.append(
                f"decision after ageing {age_minutes} min was {decision.get('cacheDecision')!r} "
                f"({decision.get('cacheDecisionReason')!r}), not 'revalidate_only'"
            )

        second = _run_once(
            loader=loader,
            loader_name=loader_name,
            source_name=source_name,
            state_rows=reloaded,
            force_refresh_all=False,
            fetch_text=fetch_text,
            timeout_s=timeout_s,
            retries=retries,
            backoff_s=backoff_s,
        )
        detail2 = second["detail"]
        if second["urls"]:
            failures.append(
                f"the revalidation run fetched {len(second['urls'])} listing body/bodies: "
                f"{', '.join(second['urls'][:3])}"
            )
        if int(detail2.get("httpStatus") or 0) != 304:
            failures.append(
                f"the conditional probe answered httpStatus={detail2.get('httpStatus')!r}; "
                "the feed does not honour If-None-Match and still needs a full read"
            )
        if clean_text(detail2.get("exclusionReason")) != REVALIDATED_EXCLUSION:
            failures.append(
                "the revalidation run was not recorded as "
                f"{REVALIDATED_EXCLUSION!r} (status={detail2.get('status')!r}, "
                f"exclusionReason={detail2.get('exclusionReason')!r})"
            )

        final_state = _apply_report(state_rows=reloaded, detail=detail2)
        write_source_state(state_path, final_state)
        final_entry = read_source_state(state_path).get(source_name) or {}
        final_tag = clean_text(final_entry.get("lastHttpEtag")) or clean_text(
            final_entry.get("lastHttpLastModified")
        )
        if final_tag != stored:
            failures.append(f"a 304 changed the stored validator from {stored!r} to {final_tag!r}")

    detail2 = second["detail"]

    return {
        "ok": not failures,
        "failures": failures,
        "adapter": adapter,
        "source": source_name,
        "loader": loader_name,
        "ageMinutes": age_minutes,
        "stateFile": str(state_path),
        "run1": {
            "urls": first["urls"],
            "kept": first["kept"],
            "status": detail.get("status"),
            "error": detail.get("error"),
            "httpStatus": detail.get("httpStatus"),
            "httpEtag": detail.get("httpEtag"),
            "httpLastModified": detail.get("httpLastModified"),
            "errors": first["errors"],
        },
        "stored": {
            "lastHttpEtag": saved_entry.get("lastHttpEtag"),
            "lastHttpLastModified": saved_entry.get("lastHttpLastModified"),
            "lastHttpStatus": saved_entry.get("lastHttpStatus"),
            "lastKeptCount": saved_entry.get("lastKeptCount"),
        },
        "decision": decision,
        "run2": {
            "urls": second["urls"],
            "kept": second["kept"],
            "status": detail2.get("status"),
            "error": detail2.get("error"),
            "httpStatus": detail2.get("httpStatus"),
            "exclusionReason": detail2.get("exclusionReason"),
            "cacheDecision": detail2.get("cacheDecision"),
            "cacheDecisionReason": detail2.get("cacheDecisionReason"),
            "errors": second["errors"],
        },
        "storedAfterRun2": {
            "lastHttpEtag": final_entry.get("lastHttpEtag"),
            "lastHttpLastModified": final_entry.get("lastHttpLastModified"),
            "lastHttpStatus": final_entry.get("lastHttpStatus"),
            "cacheDecisionReason": final_entry.get("cacheDecisionReason"),
        },
    }


def classify_outcome(summary: dict[str, Any], probes: list[dict[str, Any]]) -> dict[str, str]:
    """Say what one smoke run proved about a feed's conditional read.

    ``probes`` is what the runner actually sent, in order. The first one that was
    not answered with 304 names the reason: a 2xx body means the feed ignored the
    validator, anything else means it would not answer the question at all.
    """
    run1 = summary.get("run1") or {}
    validator = clean_text(run1.get("httpEtag")) or clean_text(run1.get("httpLastModified"))
    status = int(run1.get("httpStatus") or 0)
    if clean_text(run1.get("status")) == "error" or status >= 400:
        return {
            "outcome": OUTCOME_READ_FAILED,
            "reason": clean_text(run1.get("error"))
            or f"the full read answered httpStatus={status or 'nothing'}",
        }
    if not validator:
        return {
            "outcome": OUTCOME_NO_VALIDATOR,
            "reason": "the full read published no ETag/Last-Modified",
        }
    decision = summary.get("decision") or {}
    if clean_text(decision.get("cacheDecision")) != "revalidate_only":
        if not decision:
            return {
                "outcome": OUTCOME_INCONCLUSIVE,
                "reason": "the state file stored no validator, so there was nothing to ask with",
            }
        if (
            int(run1.get("kept") or 0) <= 0
            and clean_text(decision.get("cacheDecision")) == "skip_fresh"
        ):
            return {
                "outcome": OUTCOME_INCONCLUSIVE,
                "reason": (
                    "the feed kept no jobs, so its empty-source window has not expired; "
                    "no conditional probe is due yet"
                ),
            }
        return {
            "outcome": OUTCOME_INCONCLUSIVE,
            "reason": (
                f"the decision was {decision.get('cacheDecision')!r} "
                f"({decision.get('cacheDecisionReason')!r}), not 'revalidate_only'"
            ),
        }
    probe = _decisive_probe(probes)
    if probe is None:
        return {
            "outcome": OUTCOME_NO_PROBE,
            "reason": "the runner never sent If-None-Match for this feed",
        }
    code = int(probe.get("statusCode") or 0)
    if bool(probe.get("notModified")) or code == 304:
        return {"outcome": OUTCOME_HONOURS, "reason": ""}
    if 200 <= code < 300:
        return {
            "outcome": OUTCOME_IGNORES,
            "reason": f"the probe answered {code} with a body instead of 304",
        }
    return {
        "outcome": OUTCOME_PROBE_REFUSED,
        "reason": f"the probe answered {code or 'nothing'}; the feed cannot be asked",
    }


def _decisive_probe(probes: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The probe that decided the run: the first not answered with 304, else the first."""
    missed = [probe for probe in probes if not bool(probe.get("notModified"))]
    return missed[0] if missed else (probes[0] if probes else None)


def _feed_dir_name(row: dict[str, Any], index: int, used: dict[str, int]) -> str:
    label = (
        clean_text(
            row.get("slug")
            or row.get("account")
            or row.get("subdomain")
            or row.get("company")
            or row.get("name")
        )
        or f"feed-{index}"
    )
    safe = "".join(ch if ch.isalnum() or ch in "-._" else "-" for ch in label)
    base = safe.strip("-._").lower() or f"feed-{index}"
    seen = used.get(base, 0) + 1
    used[base] = seen
    return base if seen == 1 else f"{base}-{seen}"


def sweep_adapter(
    *,
    adapter: str,
    rows: list[dict[str, Any]],
    loader_name: str,
    loader: Callable[..., list[dict[str, Any]]],
    fetch_text: Callable[..., str],
    output_root: Path,
    age_minutes: int,
    timeout_s: int = 20,
    retries: int = 1,
    backoff_s: float = 0.5,
    progress: Callable[[dict[str, Any]], None] | None = None,
) -> dict[str, Any]:
    """Run the two-phase cycle for every feed and classify what each one proved."""
    output_root.mkdir(parents=True, exist_ok=True)
    results: list[dict[str, Any]] = []
    used: dict[str, int] = {}
    with record_provider_probes() as recorder:
        for index, row in enumerate(rows, start=1):
            source_name = clean_text(row.get("name")) or f"feed-{index}"
            recorder.reset()
            feed_dir = output_root / _feed_dir_name(row, index, used)
            summary: dict[str, Any] | None = None
            patched = install_single_row_registry(row)
            try:
                summary = run_smoke(
                    row=row,
                    loader_name=loader_name,
                    loader=loader,
                    fetch_text=fetch_text,
                    output_dir=feed_dir,
                    age_minutes=age_minutes,
                    timeout_s=timeout_s,
                    retries=retries,
                    backoff_s=backoff_s,
                )
            except Exception as exc:  # noqa: BLE001 - one broken feed must not end the sweep
                summary = None
                failure = f"{type(exc).__name__}: {exc}"
            finally:
                restore_registry(patched)
            if summary is None:
                verdict = {"outcome": OUTCOME_READ_FAILED, "reason": failure}
            else:
                (feed_dir / "summary.json").write_text(
                    json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
                )
                verdict = classify_outcome(summary, recorder.probes)
            run1 = (summary or {}).get("run1") or {}
            run2 = (summary or {}).get("run2") or {}
            probe = _decisive_probe(recorder.probes)
            result = {
                "source": source_name,
                "identity": describe_row(row),
                "outcome": verdict["outcome"],
                "reason": verdict["reason"],
                "validator": {
                    "etag": clean_text(run1.get("httpEtag")),
                    "lastModified": clean_text(run1.get("httpLastModified")),
                },
                "probe": dict(probe) if probe else None,
                "run2": {
                    "bodies": len(run2.get("urls") or []),
                    "httpStatus": run2.get("httpStatus"),
                    "exclusionReason": run2.get("exclusionReason"),
                },
                "failures": (summary or {}).get("failures") or [],
                "summaryFile": str(feed_dir / "summary.json") if summary else "",
            }
            results.append(result)
            if progress is not None:
                progress(result)
    counts = {outcome: 0 for outcome in OUTCOME_ORDER}
    for result in results:
        counts[result["outcome"]] = counts.get(result["outcome"], 0) + 1
    offenders = [result["source"] for result in results if result["outcome"] == OUTCOME_IGNORES]
    return {
        "adapter": adapter,
        "loader": loader_name,
        "ageMinutes": age_minutes,
        "feeds": results,
        "counts": counts,
        "offenders": offenders,
        "ok": not offenders,
    }


def _validator_label(record: dict[str, Any]) -> str:
    etag = clean_text(record.get("etag"))
    last_modified = clean_text(record.get("lastModified"))
    if etag:
        return etag if len(etag) <= 30 else f"{etag[:27]}..."
    return last_modified or "-"


def _report_order(result: dict[str, Any]) -> tuple[int, str]:
    outcome = clean_text(result.get("outcome"))
    rank = OUTCOME_ORDER.index(outcome) if outcome in OUTCOME_ORDER else len(OUTCOME_ORDER)
    return rank, clean_text(result.get("source"))


def print_sweep(sweep: dict[str, Any], report_path: Path) -> None:
    print(
        f"[smoke] {sweep['adapter']}: {len(sweep['feeds'])} feeds, "
        f"state aged {sweep['ageMinutes']} min"
    )
    for result in sorted(sweep["feeds"], key=_report_order):
        probe = result.get("probe") or {}
        code = probe.get("statusCode")
        print(
            f"  {result['outcome']:<14} {result['source']:<40} "
            f"{_validator_label(result['validator']):<32} "
            f"probe={code if code is not None else '-':<4} bodies={result['run2']['bodies']}"
        )
        if result["reason"]:
            print(f"  {'':<14} {result['reason']}")
    groups = (
        (OUTCOME_IGNORES, "publish a validator but answer If-None-Match with a body"),
        (OUTCOME_PROBE_REFUSED, "publish a validator but refuse the conditional probe"),
        (OUTCOME_NO_PROBE, "publish a validator but never send If-None-Match"),
    )
    for outcome, phrase in groups:
        names = [r["source"] for r in sweep["feeds"] if r["outcome"] == outcome]
        if names:
            print(f"[smoke] {len(names)} feed(s) {phrase}: {', '.join(names)}")
    counts = ", ".join(f"{key}={value}" for key, value in sweep["counts"].items() if value)
    print(f"[smoke] counts  {counts or 'none'}")
    print(f"[smoke] report  {report_path}")


def _resolve_output_dir(raw: str, adapter: str, row: dict[str, Any], *, label: str = "") -> Path:
    if raw:
        output = Path(raw).expanduser().resolve()
    else:
        slug = label or clean_text(
            row.get("slug")
            or row.get("account")
            or row.get("subdomain")
            or row.get("company")
            or "board"
        )
        output = (ROOT / "_out" / "provider-revalidation-smoke" / f"{adapter}-{slug}").resolve()
    data_dir = (ROOT / "data").resolve()
    if output == data_dir or data_dir in output.parents:
        raise SystemExit(f"[smoke] refusing to write state into live data/: {output}")
    return output


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument("--adapter", required=True, help="provider adapter key, e.g. greenhouse")
    parser.add_argument(
        "--source",
        default="",
        help="registry name, slug, account or subdomain of one feed; see --list",
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="sweep every registered feed and report the ones that ignore If-None-Match",
    )
    parser.add_argument("--list", action="store_true", help="list this adapter's registered feeds")
    parser.add_argument("--output-dir", default="", help="isolated run directory")
    parser.add_argument(
        "--age-minutes",
        type=int,
        default=30,
        help="how old the stored run is when revalidation is attempted",
    )
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--retries", type=int, default=1)
    parser.add_argument("--backoff", type=float, default=0.5)
    parser.add_argument("--json", action="store_true", help="print only the summary JSON")
    args = parser.parse_args(argv)

    loader_name, loader = loader_for(args.adapter)
    rows = candidates_for(args.adapter)
    if args.list:
        if not rows:
            print(f"[smoke] {args.adapter} has no registered feeds")
            return 1
        for row in rows:
            print(describe_row(row))
        return 0
    if args.all and args.source:
        parser.error("--all and --source are exclusive")
    if not args.all and not args.source:
        parser.error("--source or --all is required unless --list is given")

    hot = int(common_config.DEFAULT_HOT_SOURCE_CADENCE_MINUTES)
    stable = int(common_config.DEFAULT_INCREMENTAL_PROVIDER_STABLE_MINUTES)
    if not hot < args.age_minutes < stable:
        parser.error(
            f"--age-minutes must be between {hot} and {stable} (exclusive) so the run is "
            "past the hot-cadence window but inside the provider's stable window"
        )

    if args.all:
        if not rows:
            print(f"[smoke] {args.adapter} has no registered feeds")
            return 1
        output_root = _resolve_output_dir(args.output_dir, args.adapter, {}, label="sweep")
        report_path = output_root / "sweep.json"

        def _progress(result: dict[str, Any]) -> None:
            print(f"  [{result['outcome']}] {result['source']}", file=sys.stderr, flush=True)

        sweep = sweep_adapter(
            adapter=args.adapter,
            rows=rows,
            loader_name=loader_name,
            loader=loader,
            fetch_text=default_fetch_text,
            output_root=output_root,
            age_minutes=args.age_minutes,
            timeout_s=args.timeout,
            retries=args.retries,
            backoff_s=args.backoff,
            progress=_progress,
        )
        sweep["report"] = str(report_path)
        report_path.write_text(json.dumps(sweep, indent=2, ensure_ascii=False), encoding="utf-8")
        if args.json:
            print(json.dumps(sweep, indent=2, ensure_ascii=False))
        else:
            print_sweep(sweep, report_path)
        return 0 if sweep["ok"] else 1

    matches = match_rows(rows, args.source)
    if len(matches) != 1:
        names = ", ".join(clean_text(row.get("name")) or "?" for row in matches[:12])
        print(
            f"[smoke] {args.source!r} matched {len(matches)} feeds under {args.adapter}: "
            f"{names or 'none'}",
            file=sys.stderr,
        )
        return 1
    row = matches[0]

    output_dir = _resolve_output_dir(args.output_dir, args.adapter, row)
    install_single_row_registry(row)
    summary = run_smoke(
        row=row,
        loader_name=loader_name,
        loader=loader,
        fetch_text=default_fetch_text,
        output_dir=output_dir,
        age_minutes=args.age_minutes,
        timeout_s=args.timeout,
        retries=args.retries,
        backoff_s=args.backoff,
    )
    (output_dir / "summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    if args.json:
        print(json.dumps(summary, indent=2, ensure_ascii=False))
    else:
        print(f"[smoke] {summary['adapter']}: {summary['source']} ({summary['loader']})")
        print(
            "  run 1  "
            f"kept={summary['run1']['kept']} http={summary['run1']['httpStatus']} "
            f"etag={summary['run1']['httpEtag']!r}"
        )
        print(
            "  state  "
            f"lastHttpEtag={summary['stored']['lastHttpEtag']!r} "
            f"lastHttpLastModified={summary['stored']['lastHttpLastModified']!r}"
        )
        print(
            f"  after {summary['ageMinutes']} min  "
            f"{summary['decision'].get('cacheDecision')} "
            f"({summary['decision'].get('cacheDecisionReason')})"
        )
        print(
            "  run 2  "
            f"bodies={len(summary['run2']['urls'])} http={summary['run2']['httpStatus']} "
            f"status={summary['run2']['status']} "
            f"exclusionReason={summary['run2']['exclusionReason']!r}"
        )
        print(f"  state file  {summary['stateFile']}")
        if summary["ok"]:
            print("  PASS")
        else:
            for failure in summary["failures"]:
                print(f"  FAIL  {failure}", file=sys.stderr)
    return 0 if summary["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
