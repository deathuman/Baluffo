#!/usr/bin/env python3
"""Drain discovery's curated-seed queue, then prove what a fetch collects.

Registering boards is not the same as collecting openings. A curated seed becomes
a registry row only after it survives four gates, and the first measurement showed
almost none of them do: of 513 seeds, 71 were approved on the first run. Measuring
that is the point of this harness, so it runs discovery repeatedly against an
isolated data directory until the backlog stops shrinking.

The repeated runs are not redundant. Discovery throttles what it promotes per run
by design — `ADAPTER_QUEUE_CAPS` is 8 for static, 12 for greenhouse, 10 for ashby,
with a domain cap on top — so a large seed set is *designed* to land over several
cycles. One run tells you the throttle exists; several tell you the drain rate and
whether it reaches zero.

**Three numbers, not one.** Registration alone was previously reported as delivery,
and on 2026-10-04 that mistake shipped: `tools/coverage_drain.py` reported 6,844 of
6,929 openings delivered, and a live fetch collected 0 of them. The three numbers are

    registered      a registry row exists for the board
    adapter_matched that row carries the adapter the curated row declares
    collected       a fetch run kept a non-zero number of its openings

`adapter_matched` is the cheap gate and needs no network. It is what catches a board
that landed under the wrong adapter — Workday registered as `static` collects nothing
because no Workday loader ever sees it. `collected` needs a fetch and is opt-in via
`--verify-collected` because a full run over the curated set takes tens of minutes.

With `--verify-collected` this exits non-zero when boards registered but nothing was
collected, so it is usable as a preflight gate rather than a report that is read
optimistically.

Everything is isolated under `--data-dir`. Discovery has no `--output-dir`, and
`BALUFFO_DATA_DIR` is the only redirect it honours, so an unisolated run writes into
the repo's `data/` and leaves discovery-audit artefacts behind.

Usage:
  python tools/coverage_drain.py --rounds 6 --data-dir _out/coverage/drain
  python tools/coverage_drain.py --rounds 6 --data-dir _out/coverage/drain \
      --verify-collected
"""

from __future__ import annotations

import argparse
import gzip
import json
import os
import shutil
import subprocess
import sys
import time
from collections.abc import Sequence
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from coverage_board_identity import (  # noqa: E402
    ADAPTER_ID_FORMAT,
    build_candidate,
    candidate_identity,
    host_of,
    registry_identity,
)


def _static_pages(row: dict[str, Any]) -> list[str]:
    """The pages the static loader would see on a registry row.

    Mirrors ``src.jobs.adapters.static_runtime._as_pages``: only an existing list survives,
    a bare string is dropped. Duplicated rather than imported because this script runs with
    ``tools/`` on ``sys.path`` and because a measurement tool should not reach into an
    adapter's internals -- but the duplication is the point, since reading ``board_url``
    instead of ``pages`` is what silently left 421 static rows unfetched.
    """
    value = row.get("pages")
    if not isinstance(value, list):
        return []
    return [str(item).strip() for item in value if str(item).strip()]


# The registry is written as a gzipped JSON array; the sibling .jsonl holds the same
# data as a single-line array, so parsing it as newline-delimited silently yields one
# row and every count comes out wrong.
REGISTRY_ACTIVE = "source-registry-active.json.gz"
REGISTRY_PENDING = "source-registry-pending.json.gz"
REPORT = "source-discovery-report.json"
CURATED = "src/curated_coverage_boards.json"
FETCH_REPORT = "jobs-fetch-report.json"
# The fetch writes this gzipped. Reading the bare `.json` name found nothing, so every run
# reported zero collected while the output held 41,277 rows -- a gate that cannot tell
# "collected nothing" from "looked in the wrong file".
FETCH_OUTPUT = "jobs-unified.json.gz"

# Exit code for "boards registered, nothing collected". Distinct from 1 so a caller can
# tell a collection failure from a harness error.
EXIT_NOT_COLLECTED = 3


def board_key(url: Any) -> tuple[str, str]:
    """Comparable ``(host, tenant)`` for a board listing URL.

    Uses the measurement module's own candidate identity, so a curated row and a registry
    row are compared by the same rule. Deriving one side here and reading the other with
    ``registry_identity`` looked equivalent and was not: this returned ``tenant == host``
    where that returned ``tenant == ''``, which reported 57 of 695 boards registered when
    498 were. One rule, applied to both sides, is the whole point.
    """
    _adapter, host, tenant = candidate_identity(build_candidate(str(url or "")))
    return (host, tenant)


def curated_identity(row: dict[str, Any]) -> tuple[str, str]:
    """``(host, tenant)`` for a curated board, whether or not it carries a URL.

    **193 of the 695 curated rows carry no ``listing_url`` at all** — 2,206 openings. They
    identify the way the registry does, by adapter plus tenant:

        {"adapter": "smartrecruiters", "company_id": "Bet3651", ...}

    Keying those on ``listing_url`` resolved every one of them to ``("", "")``, matched
    nothing, and reported them as unregistered — a 197-board, 2,223-opening gap that does
    not exist. It was the fourth wrong number this harness produced from the same mistake:
    applying one identity rule to rows that do not share a shape.

    A URL-bearing board is resolved through ``candidate_identity``; a provider board is
    resolved by building the registry id ``registry_identity`` parses, using the same
    adapter -> tenant-field map the registry itself uses.
    """
    url = str(row.get("listing_url") or row.get("url") or "").strip()
    if url:
        return board_key(url)
    adapter = str(row.get("adapter") or "").strip().lower()
    field = ADAPTER_ID_FORMAT.get(adapter, "listing_url")
    value = str(row.get(field) or "").strip()
    if adapter and field and value:
        parsed = registry_identity(f"{adapter}:{field}:{value}")
        if parsed is not None:
            return (parsed[1], parsed[2])
    # Last resort: any URL-ish field the row happens to carry.
    for fallback in ("api_url", "board_url", "feed_url", "careersUrl"):
        candidate = str(row.get(fallback) or "").strip()
        if candidate.startswith(("http://", "https://")):
            return board_key(candidate)
    return ("", "")


def report_identity(row: dict[str, Any]) -> tuple[str, str]:
    """``(host, tenant)`` for a registration-report row.

    Prefers the identity ``registration_report`` already resolved, and falls back to
    re-deriving it. The fallback keeps hand-built report rows working — several tests
    construct one from ``listing_url`` alone — and it is the same rule either way, which
    is the only thing that matters when a mismatch would read as a coverage finding.
    """
    host = str(row.get("host") or "").strip()
    if host:
        return (host, str(row.get("tenant") or "").strip())
    return curated_identity(row)


def read_registry(data_dir: Path, name: str) -> list[dict[str, Any]]:
    path = data_dir / name
    if not path.exists():
        return []
    try:
        payload = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
    except (OSError, ValueError):
        return []
    rows = payload if isinstance(payload, list) else payload.get("rows") or []
    return [row for row in rows if isinstance(row, dict)]


def read_report(data_dir: Path) -> dict[str, Any]:
    path = data_dir / REPORT
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


_URL_FIELDS = ("board_url", "listing_url", "careersUrl", "api_url", "feed_url", "pages")


def hydrate_registry_urls(rows: list[dict[str, Any]]) -> int:
    """Give a row the URL field its loader reads, recovered from its ``id``.

    Discovery writes registry rows whose only URL is inside ``id``
    (``static:listing_url:https://…``), so a row has nothing to fetch. The static path reads
    ``pages`` specifically — ``build_static_source_context`` calls ``_as_pages(source["pages"])``
    — and ``_as_pages`` returns ``[]`` for anything that is not already a list. Setting
    ``board_url`` is not enough, and cost two runs to learn: 421 static rows reported
    ``request_count: 0`` with ``status: ok`` having never asked.

    Provider rows (``greenhouse:slug:x``, ``ashby:slug:x``) identify by slug and are left
    alone; only rows whose id carries a URL get the field.
    """
    hydrated = 0
    for row in rows:
        if not isinstance(row, dict):
            continue
        adapter = str(row.get("adapter") or "").lower()
        parts = str(row.get("id") or "").split(":", 2)
        if len(parts) < 3:
            continue
        url = parts[2].strip()
        if not url.startswith(("http://", "https://")):
            continue
        if adapter in {"static", "scrapy_static"}:
            # The static loader reads `pages` and nothing else, and `_as_pages` drops a bare
            # string. A row can already carry `board_url` and still be unfetchable, so the
            # guard is per-loader rather than "has any URL field".
            if _static_pages(row):
                continue
            row["pages"] = [url]
            row.setdefault("board_url", url)
            row.setdefault("listing_url", url)
            hydrated += 1
            continue
        if any(row.get(field) for field in _URL_FIELDS):
            continue
        row["board_url"] = url
        row["listing_url"] = url
        hydrated += 1
    return hydrated


def seed_data_dir(data_dir: Path, source_registry: Path | None) -> None:
    """Prepare an isolated data dir, optionally seeded with the live registry.

    Seeding matters: without it discovery sees an empty registry and promotes
    candidates that the running app already serves, which measures nothing useful.
    """
    if data_dir.exists():
        shutil.rmtree(data_dir)
    data_dir.mkdir(parents=True, exist_ok=True)
    if source_registry and source_registry.exists():
        raw = json.loads(source_registry.read_text(encoding="utf-8"))
        rows = raw if isinstance(raw, list) else raw.get("rows") or []
        # Discovery writes rows whose only URL is inside `id`; the runtime fetches from
        # `board_url`/`listing_url`. Without this the static path has nothing to request and
        # every static board reports "fetched, kept nothing" having fetched nothing.
        hydrated = hydrate_registry_urls(rows)
        # The runtime writes gzipped; match that so the seeded file is read the
        # same way the run's own output is.
        payload = json.dumps(rows, ensure_ascii=False).encode("utf-8")
        (data_dir / REGISTRY_ACTIVE).write_bytes(gzip.compress(payload))
        print(f"seeded {len(rows)} registry rows into {data_dir} ({hydrated} URL-hydrated)")


def run_round(data_dir: Path, *, preset: str, timeout: int) -> int:
    # Arguments are written as str() literals. An earlier version interpolated the
    # timeout as a bare int, which produced '--timeout', 15 in the generated source
    # and died with "'int' object is not subscriptable" inside argparse.
    argv = [
        "--mode",
        "static",
        "--no-web-search",
        "--timeout",
        str(timeout),
        "--top",
        "0",
        "--preset",
        preset,
    ]
    script = data_dir / "_run_discovery.py"
    script.write_text(
        "import sys\n"
        f"sys.path.insert(0, {str(ROOT)!r})\n"
        "from src.source_discovery.orchestrator import main\n"
        f"sys.exit(main({argv!r}))\n",
        encoding="utf-8",
    )
    env = {**os.environ, "BALUFFO_DATA_DIR": str(data_dir), "PYTHONIOENCODING": "utf-8"}
    proc = subprocess.run(  # noqa: S603 - fixed argv, no shell
        [sys.executable, str(script)],
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=1800,
        check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr[-2000:])
        raise SystemExit(f"discovery round failed with exit {proc.returncode}")
    return proc.returncode


def round_summary(data_dir: Path) -> dict[str, Any]:
    summary = read_report(data_dir).get("summary") or {}
    return {
        "generated": summary.get("generatedCandidateCount", 0),
        "dedupSkipped": summary.get("skippedDuplicateCount", 0),
        "probeFailed": summary.get("failedProbeCount", 0),
        "healthy": summary.get("healthyCount", 0),
        "deferredByCap": summary.get("discoverableButDeferredCount", 0),
        "queued": summary.get("queuedCandidateCount", 0),
        "approved": summary.get("approvedCandidateCount", 0),
        "active": len(read_registry(data_dir, REGISTRY_ACTIVE)),
        "pending": len(read_registry(data_dir, REGISTRY_PENDING)),
    }


def load_curated() -> list[dict[str, Any]]:
    payload = json.loads((ROOT / CURATED).read_text(encoding="utf-8"))
    rows = payload["boards"] if isinstance(payload, dict) and "boards" in payload else payload
    return [row for row in rows if isinstance(row, dict)]


def collectable_adapters(landing_url: str, landing_tags: set[str], declared: str) -> set[str]:
    """Which loaders can actually read this row, mirroring ``registry_entries``.

    A ``static`` row on a Workday or BambooHR host *is* collected: the runtime migrates
    it into that adapter at read time (``_provider_migration_entry`` in
    ``src/jobs/common/registry.py``). Ignoring that would make this gate fire on boards
    that do collect, which is the other way to be wrong. For every other adapter a
    ``static`` row is only visible to the static loader.
    """
    tags = set(landing_tags)
    if declared in tags:
        tags.add(declared)
    if "static" in landing_tags:
        host = urlparse(landing_url).netloc.lower()
        if host.endswith((".myworkdayjobs.com", ".workday.com", ".bamboohr.com")):
            tags.add("workday" if "workday" in host else "bamboohr")
    return {tag for tag in tags if not tag.startswith("state:")}


def registration_report(
    curated: list[dict[str, Any]], active: list[dict[str, Any]], pending: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    """Per curated board: does a registry row exist, and can a loader read it?

    Identity is ``(host, tenant)`` on both sides, via the same
    :mod:`coverage_board_identity` helpers the audit uses. Keying on the id string or the
    studio label instead is what let a Workday board report as landed under a different
    tenant's row.
    """
    landed: dict[tuple[str, str], dict[str, Any]] = {}
    for state, rows in (("active", active), ("pending", pending)):
        for row in rows:
            identity = registry_identity(str(row.get("id") or ""))
            if identity is None:
                continue
            _, host, tenant = identity
            record = landed.setdefault((host, tenant), {"adapters": set(), "states": set()})
            record["adapters"].add(str(row.get("adapter") or "").lower())
            record["states"].add(state)

    out: list[dict[str, Any]] = []
    for board in curated:
        url = str(board.get("listing_url") or board.get("url") or "")
        adapter = str(board.get("adapter") or "").lower()
        # Resolved from the curated board, not from `url`: 193 of 695 curated rows carry
        # no URL at all, and looking them up by it resolved every one to ("", "") so it
        # matched no registry row and reported a gap of 197 boards / 2,223 openings that
        # does not exist. Measured against the drained registry, those rows match at
        # 190 of 193 -- every provider adapter essentially complete.
        identity = curated_identity(board)
        record = landed.get(identity) or {"adapters": set(), "states": set()}
        states, adapters = record["states"], record["adapters"]
        readable = collectable_adapters(url, adapters, adapter)
        out.append(
            {
                "adapter": adapter,
                "listing_url": url,
                # Stored, not recomputed downstream: a report row does not carry the
                # tenant field `curated_identity` needs, and an empty `host` here would
                # silently read as "no registry row" instead of "not identifiable".
                "host": identity[0],
                "tenant": identity[1],
                "openings": int(board.get("coverageAuditOpenings") or 0),
                "registered": bool(states),
                "state": "active"
                if "active" in states
                else ("pending" if "pending" in states else ""),
                "adapter_matched": adapter in adapters,
                "readable_by": sorted(readable),
                "collectable": adapter in readable,
                "registry_adapters": sorted(adapters),
                "collected": None,
            }
        )
    return out


def run_fetch(data_dir: Path, *, timeout: int, fetch_timeout: int) -> None:
    """Run one real fetch against the isolated data directory."""
    env = {**os.environ, "BALUFFO_DATA_DIR": str(data_dir), "PYTHONIOENCODING": "utf-8"}
    argv = [
        sys.executable,
        str(ROOT / "src" / "jobs_fetcher.py"),
        "--output-dir",
        str(data_dir),
        "--timeout",
        str(fetch_timeout),
        "--force-refresh-all",
        "--ignore-circuit-breaker",
    ]
    proc = subprocess.run(  # noqa: S603 - fixed argv, no shell
        argv,
        cwd=str(ROOT),
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=False,
    )
    if proc.returncode != 0:
        sys.stderr.write(proc.stderr[-2000:])
        raise SystemExit(f"fetch failed with exit {proc.returncode}")


def hydrate_registry_file(data_dir: Path, name: str) -> int:
    """Rewrite a registry file in place with the URL fields the runtime fetches from.

    This has to run *after* the drain rounds and immediately before the fetch: discovery
    owns that file and rewrites it on every round, so hydrating only the seed is not enough
    — the rows the fetch finally reads are the ones discovery last wrote, and those carry no
    URL field at all. The first attempt seeded a hydrated registry and then watched discovery
    overwrite it, which is why the static adapter still made zero requests.
    """
    path = data_dir / name
    if not path.exists():
        return 0
    try:
        payload = json.loads(gzip.decompress(path.read_bytes()).decode("utf-8"))
    except (OSError, ValueError):
        return 0
    rows = payload if isinstance(payload, list) else payload.get("rows") or []
    hydrated = hydrate_registry_urls(rows)
    if hydrated:
        body = json.dumps(rows, ensure_ascii=False).encode("utf-8")
        path.write_bytes(gzip.compress(body))
        # The .jsonl sibling is a single-line array of the same rows; leaving it stale would
        # let a reader pick up the un-hydrated shape.
        sibling = path.with_suffix("").with_suffix(".jsonl")
        if sibling.exists():
            sibling.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
    return hydrated


def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        raw = path.read_bytes()
        if path.suffix == ".gz":
            raw = gzip.decompress(raw)
        return json.loads(raw.decode("utf-8"))
    except (OSError, ValueError, json.JSONDecodeError):
        return None


def _source_key_index(
    report: list[dict[str, Any]],
) -> tuple[
    dict[str, tuple[str, str]],
    list[tuple[str, str, tuple[str, str]]],
    dict[str, str],
]:
    """Map fetch-report source names to boards: ``(by_source, prefix_index, rollup_of)``.

    One rule, two consumers. Attribution and fetch evidence both have to know which
    fetch-report row speaks for which board, and deriving that twice is how the two drifted
    apart before -- the evidence path would have said a board was never asked while the
    attribution path was reading its jobs.
    """
    by_source: dict[str, tuple[str, str]] = {}
    prefix_index: list[tuple[str, str, tuple[str, str]]] = []
    rollup_of: dict[str, str] = {}
    for row in report:
        if not row["registered"]:
            continue
        host, tenant = report_identity(row)
        if not host:
            continue
        key = (host, tenant)
        adapter = str(row.get("adapter") or "").lower()
        rollup = _ROLLUP_SOURCE.get(adapter, "")
        if adapter in {"static", "scrapy_static"}:
            by_source[static_source_name(str(row["listing_url"]))] = key
        else:
            rollup_of.setdefault(rollup, adapter)
            prefix_index.append((host, _posting_prefix(str(row["listing_url"])), key))
    return by_source, prefix_index, rollup_of


# A source row that was never asked is not a source row that found nothing. These are the
# tells, from `static_listing_flow._handle_skip_and_revalidation`: a skip decision, or no
# time spent and nothing fetched. A row that spent seconds and fetched nothing has asked.
_SKIP_DECISIONS = frozenset({"skip_fresh", "cooldown_skip", "skip_revalidate", "skipped"})

FETCH_STATES = (
    "collected",
    "fetched_empty",
    "error",
    "not_selected",
    "rollup_only",
    "no_report",
)


def _classify_source(row: dict[str, Any] | None) -> dict[str, Any]:
    """One source row's fetch evidence, classified.

    The distinction this exists for: ``fetchedCount == 0`` alone cannot tell a board that
    was asked and had nothing from one that was never asked. A board reported as zero on
    the strength of a skip is a measurement artifact wearing a coverage finding's clothes.
    """
    if row is None:
        return {
            "state": "not_selected",
            "kept_count": None,
            "fetched_count": None,
            "duration_ms": None,
            "cache_decision": "",
            "error": "",
        }
    kept = int(row.get("keptCount") or 0)
    fetched = int(row.get("fetchedCount") or 0)
    duration = int(row.get("durationMs") or 0)
    decision = str(((row.get("details") or [{}])[0] or {}).get("cacheDecision") or "")
    error = str(row.get("error") or "")
    if error:
        state = "error"
    elif kept > 0:
        state = "collected"
    elif decision in _SKIP_DECISIONS or (duration == 0 and fetched == 0):
        state = "not_selected"
    else:
        state = "fetched_empty"
    return {
        "state": state,
        "kept_count": kept,
        "fetched_count": fetched,
        "duration_ms": duration,
        "cache_decision": decision,
        "error": error[:120],
    }


def fetch_evidence(data_dir: Path, report: list[dict[str, Any]]) -> dict[tuple[str, str], dict]:
    """Per-board fetch evidence, so "never asked" is separable from "asked and empty".

    Every zero this harness reported was a zero of *kept* jobs, read without the fetch
    evidence beside it. The 205 boards reported here as ``fetched_empty`` were confirmed to
    have asked -- ``cacheDecision: run_now``, 420-5,884 ms spent, ``fetchedCount: 0`` --
    which is the opposite conclusion from the one the kept-count alone supports.

    A provider board cannot be given its own evidence: the adapter fetches every board it
    serves under one rollup row, so per-board evidence does not exist to be had. Those
    boards report ``rollup_only`` rather than inheriting the rollup's state, because the
    rollup keeping jobs says nothing about whether *this* board is among them.
    """
    doc = read_json(data_dir / FETCH_REPORT)
    sources = doc.get("sources") if isinstance(doc, dict) else None
    if not isinstance(sources, list):
        return {}
    by_name = {str(row.get("name") or ""): row for row in sources if isinstance(row, dict)}
    out: dict[tuple[str, str], dict[str, Any]] = {}
    for row in report:
        if not row["registered"]:
            continue
        key = report_identity(row)
        if not key[0]:
            continue
        adapter = str(row.get("adapter") or "").lower()
        if adapter in {"static", "scrapy_static"}:
            out[key] = _classify_source(by_name.get(static_source_name(str(row["listing_url"]))))
            continue
        rollup = _ROLLUP_SOURCE.get(adapter, "")
        evidence = _classify_source(by_name.get(rollup)) if rollup else _classify_source(None)
        evidence = dict(evidence)
        evidence["state"] = "rollup_only" if evidence["state"] != "not_selected" else "no_report"
        evidence["rollup"] = rollup
        out[key] = evidence
    return out


def attribute_collected(
    data_dir: Path, report: list[dict[str, Any]]
) -> tuple[dict[tuple[str, str]], int]:
    """Attribute kept jobs to boards by host + tenant.

    Provider adapters roll their boards up into one fetch-report row, so per-board counts
    cannot come from the report — they come from the output rows, matched the same way
    identity is defined everywhere else. Unmatched jobs are returned rather than dropped:
    an attribution that silently covers only part of the output is a second false green.

    The posting URL lives in ``jobLink``. Provider payloads also carry ``sourceBundle``,
    which names the board id per contributing source, so a job whose posting URL is on a
    CDN still attributes to the board that served it.
    """
    payload = read_json(data_dir / FETCH_OUTPUT)
    if isinstance(payload, dict):
        payload = payload.get("jobs") or payload.get("records") or []
    if not isinstance(payload, list):
        return {}, 0

    # Attribution is by *provenance*, not by where a posting URL happens to live.
    #
    # A job carries the source that fetched it. For a static board that source name is the
    # board's own source id, so the mapping is exact and needs no URL matching at all. For a
    # provider adapter every board collapses into one rollup row (`workday_sources`), so
    # the bundle's posting URL is matched against the board's path prefix -- but only among
    # jobs that rollup actually fetched.
    #
    # Matching globally by URL is what produced the first wrong number in this harness: 70
    # "n-ix jobs" that were Google-Sheet rows pointing at careers.n-ix.com, credited to a
    # board that itself kept zero.
    by_source, prefix_index, rollup_of = _source_key_index(report)

    counts: dict[tuple[str, str], int] = {}
    unmatched = 0
    for job in payload:
        if not isinstance(job, dict):
            continue
        source = str(job.get("source") or "")
        key = by_source.get(source)
        if key is None and source in rollup_of:
            key = _match_bundle(job, prefix_index)
        if key is None:
            unmatched += 1
            continue
        counts[key] = counts.get(key, 0) + 1
    return counts, unmatched


# Provider adapters report one rollup row for every board they serve, so a board's
# collected openings arrive under the adapter's source name rather than its own.
_ROLLUP_SOURCE = {
    "workday": "workday_sources",
    "bamboohr": "bamboohr_sources",
    "greenhouse": "greenhouse_boards",
    "lever": "lever_sources",
    "ashby": "ashby_sources",
    "workable": "workable_sources",
    "smartrecruiters": "smartrecruiters_sources",
    "teamtailor": "teamtailor_sources",
    "personio": "personio_sources",
    "recruitee": "recruitee_sources",
    "jazzhr": "jazzhr_sources",
    "breezy": "breezy_sources",
    "pinpoint": "pinpoint_sources",
    "dayforce": "dayforce_sources",
    "phenom": "phenom_sources",
    "oracle_hcm": "oracle_hcm_sources",
}


def static_source_name(listing_url: str) -> str:
    """The fetch-report source name for a static board, matching the registry's own key."""
    return f"static_source::static:listing_url:{str(listing_url or '').strip()}"


def _posting_prefix(listing_url: str) -> str:
    """Path prefix under which a board serves its postings, lowercased, no trailing slash.

    ``https://careers.wbd.com/careers`` -> ``/careers``. A posting under it
    (``/careers/j/123``) belongs to that board; a posting at ``/other/123`` does not, even
    though the host is identical.
    """
    path = (urlparse(str(listing_url or "")).path or "").strip().lower().rstrip("/")
    return path


def _match_board(url: str, index: list[tuple[str, str, tuple[str, str]]]) -> tuple[str, str] | None:
    """The board whose posting prefix contains this URL, most specific prefix first."""
    if not url:
        return None
    host = host_of(url)
    if not host:
        return None
    path = (urlparse(url).path or "").strip().lower()
    matches = [row for row in index if row[0] == host]
    if not matches:
        return None
    # Longest prefix wins, so a board at /careers/locations/milan is preferred over one at
    # /careers when both could contain the posting.
    matches.sort(key=lambda row: len(row[1]), reverse=True)
    for _candidate_host, prefix, key in matches:
        if prefix and path.startswith(prefix + "/"):
            return key
        if prefix and path == prefix:
            return key
    # A board whose listing URL is the site root owns the whole host.
    for _candidate_host, prefix, key in matches:
        if not prefix:
            return key
    return None


def _job_posting_url(job: dict[str, Any]) -> str:
    for field in ("jobLink", "url", "job_url", "applyUrl"):
        value = str(job.get(field) or "").strip()
        if value.startswith("http"):
            return value
    return ""


def _match_bundle(
    job: dict[str, Any], index: list[tuple[str, str, tuple[str, str]]]
) -> tuple[str, str] | None:
    """Resolve a provider job to a board through its ``sourceBundle``.

    Reached only for jobs whose own source is a provider rollup, so a board is credited
    only with postings that rollup fetched. The bundle's posting URL is matched against the
    board's path prefix; the longest match wins because one board can sit under another.
    """
    bundle = job.get("sourceBundle")
    if not isinstance(bundle, list):
        return None
    for entry in bundle:
        if not isinstance(entry, dict):
            continue
        key = _match_board(_job_posting_url(entry), index)
        if key is not None:
            return key
    return None


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--rounds", type=int, default=5, help="discovery cycles to run")
    parser.add_argument("--preset", default="uncapped", choices=("default", "uncapped"))
    parser.add_argument("--timeout", type=int, default=15, help="per-probe timeout")
    parser.add_argument("--data-dir", type=Path, required=True)
    parser.add_argument(
        "--seed-registry",
        type=Path,
        help="live registry json to seed the isolated run with",
    )
    parser.add_argument("--out", type=Path, help="write the per-round table here")
    parser.add_argument(
        "--verify-collected",
        action="store_true",
        help=(
            "run a real fetch and report collected openings per board. Slower than "
            "registration alone; without it the tool reports registration only and says so."
        ),
    )
    parser.add_argument(
        "--fetch-timeout",
        type=int,
        default=3600,
        help="wall-clock ceiling for the --verify-collected fetch",
    )
    parser.add_argument(
        "--fetch-job-timeout", type=int, default=30, help="per-request timeout for the fetch"
    )
    args = parser.parse_args(argv)

    data_dir = args.data_dir.resolve()
    seed_data_dir(data_dir, args.seed_registry)

    rows: list[dict[str, Any]] = []
    for index in range(1, args.rounds + 1):
        started = time.monotonic()
        run_round(data_dir, preset=args.preset, timeout=args.timeout)
        summary = round_summary(data_dir)
        summary["round"] = index
        summary["seconds"] = round(time.monotonic() - started, 1)
        rows.append(summary)
        print(
            f"round {index}: approved={summary['approved']:>3}  queued={summary['queued']:>3}  "
            f"deferredByCap={summary['deferredByCap']:>4}  probeFailed={summary['probeFailed']:>4}  "
            f"active={summary['active']:>5}  pending={summary['pending']:>4}  "
            f"({summary['seconds']}s)",
            flush=True,
        )
        # A round that neither approves nor defers has stopped making progress.
        if summary["approved"] == 0 and summary["deferredByCap"] == 0:
            print("backlog exhausted: nothing left to promote")
            break

    curated = load_curated()
    report = registration_report(
        curated, read_registry(data_dir, REGISTRY_ACTIVE), read_registry(data_dir, REGISTRY_PENDING)
    )

    unmatched = 0
    if args.verify_collected:
        print("running one fetch to measure collection...", flush=True)
        for registry_file in (REGISTRY_ACTIVE, REGISTRY_PENDING):
            hydrated_now = hydrate_registry_file(data_dir, registry_file)
            if hydrated_now:
                print(f"  hydrated {hydrated_now} {registry_file} row(s) for the fetch", flush=True)
        run_fetch(data_dir, timeout=args.fetch_timeout, fetch_timeout=args.fetch_job_timeout)
        counts, unmatched = attribute_collected(data_dir, report)
        evidence = fetch_evidence(data_dir, report)
        for row in report:
            if not row["registered"]:
                continue
            row["collected"] = counts.get(report_identity(row), 0)
            found = evidence.get(report_identity(row)) or {}
            row["fetch_state"] = found.get("state", "no_report")
            row["fetched_count"] = found.get("fetched_count")
            row["fetch_duration_ms"] = found.get("duration_ms")
            row["cache_decision"] = found.get("cache_decision", "")

    def tally(predicate: Any) -> tuple[int, int]:
        picked = [row for row in report if predicate(row)]
        return len(picked), sum(row["openings"] for row in picked)

    registered_n, registered_o = tally(lambda row: row["registered"])
    active_n, active_o = tally(lambda row: row["state"] == "active")
    matched_n, matched_o = tally(lambda row: row["adapter_matched"])
    readable_n, readable_o = tally(lambda row: row["collectable"])
    collected_n, collected_o = tally(lambda row: (row["collected"] or 0) > 0)

    if args.verify_collected:
        collected_openings = sum(row["collected"] or 0 for row in report)
    else:
        collected_openings = -1

    print()
    print("=== registration ===")
    print(
        f"curated boards          : {len(report)} / {sum(r['openings'] for r in report)} openings"
    )
    print(f"registered (any state)  : {registered_n} / {registered_o}")
    print(f"  of which active       : {active_n} / {active_o}")
    print(f"declared adapter present: {matched_n} / {matched_o}")
    print(f"readable by some loader : {readable_n} / {readable_o}")
    unreadable = [r for r in report if r["registered"] and not r["collectable"]]
    if unreadable:
        print(
            f"REGISTERED BUT UNREADABLE: {len(unreadable)} / {sum(r['openings'] for r in unreadable)}"
        )
        for row in unreadable[:8]:
            print(
                f"    declared={row['adapter']:<12} landed={row['registry_adapters']} "
                f"openings={row['openings']:<5} {row['listing_url'][:52]}"
            )

    if args.verify_collected:
        print()
        print("=== collection (fetch-backed) ===")
        print(f"boards keeping > 0 jobs : {collected_n} / {registered_n}")
        print(f"openings collected      : {collected_openings} / {registered_o}")
        print(f"jobs not attributed     : {unmatched}")
        # A kept count on its own cannot separate "asked and found nothing" from "never
        # asked", and reading it alone is what turned a harness bug into a coverage finding
        # four times. Printed beside the headline, not buried in the JSON.
        states: dict[str, list[int]] = {}
        for row in report:
            if not row["registered"]:
                continue
            bucket = states.setdefault(str(row.get("fetch_state") or "no_report"), [0, 0])
            bucket[0] += 1
            bucket[1] += int(row["openings"])
        print()
        print("=== per-board fetch evidence ===")
        print("  a board only counts as empty if it was actually asked")
        for state in FETCH_STATES:
            if state not in states:
                continue
            count, openings = states[state]
            print(f"  {state:<14} {count:>4} boards  {openings:>6} openings")

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(
            json.dumps(
                {
                    "rounds": rows,
                    "curated": report,
                    "unmatchedJobs": unmatched,
                    "collectedOpenings": collected_openings,
                },
                indent=2,
            ),
            encoding="utf-8",
        )

    first, last = rows[0], rows[-1]
    print()
    print(f"seeds registered      : {first['generated']}")
    print(f"active after round 1  : {first['active']}")
    print(f"active after round {len(rows)}    : {last['active']}")
    print(f"still deferred by cap : {last['deferredByCap']}")
    print(f"probe-failed (stable) : {last['probeFailed']}")

    if not args.verify_collected:
        print()
        print(
            "NOTE: registration only. Collection is unmeasured -- pass --verify-collected "
            "before reporting any board as delivered."
        )
        return 0

    # The gate. Registration without collection is the failure this harness exists to
    # catch, and a report that exits 0 through it is what shipped on 2026-10-04.
    if registered_n > 0 and collected_n == 0:
        print()
        print(
            f"GATE FAILED: {registered_n} boards registered, 0 collected any openings. "
            "Registration is not delivery.",
            file=sys.stderr,
        )
        return EXIT_NOT_COLLECTED
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
