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

Everything is written under ``--output-dir`` (default
``_out/provider-revalidation-smoke/<adapter>-<board>``); the tool refuses a path
inside the repo's ``data/`` because that is live state.

Exit 0 only when the validator was stored, the decision was revalidation, and the
second run answered 304 without fetching the listing.
"""

from __future__ import annotations

import argparse
import copy
import importlib
import json
import pkgutil
import sys
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
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


def install_single_row_registry(row: dict[str, Any]) -> None:
    """Point every provider runner's registry lookup at the one chosen feed.

    The runners import ``registry_entries`` into their own module namespace, so the
    filter has to be installed per module rather than on the registry leaf.
    """

    def _only(_adapter_key: str) -> list[dict[str, Any]]:
        return [dict(row)]

    for info in pkgutil.iter_modules(provider_package.__path__):
        module = importlib.import_module(f"{provider_package.__name__}.{info.name}")
        if hasattr(module, "registry_entries"):
            module.__dict__["registry_entries"] = _only
    personio = importlib.import_module("src.jobs.adapters.provider_personio")
    if hasattr(personio, "jobs_registry_entries"):
        personio.__dict__["jobs_registry_entries"] = _only


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


def _resolve_output_dir(raw: str, adapter: str, row: dict[str, Any]) -> Path:
    if raw:
        output = Path(raw).expanduser().resolve()
    else:
        slug = clean_text(
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
    if not args.source:
        parser.error("--source is required unless --list is given")
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

    hot = int(common_config.DEFAULT_HOT_SOURCE_CADENCE_MINUTES)
    stable = int(common_config.DEFAULT_INCREMENTAL_PROVIDER_STABLE_MINUTES)
    if not hot < args.age_minutes < stable:
        parser.error(
            f"--age-minutes must be between {hot} and {stable} (exclusive) so the run is "
            "past the hot-cadence window but inside the provider's stable window"
        )

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
