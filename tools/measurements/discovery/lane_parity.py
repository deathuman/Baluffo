#!/usr/bin/env python3
"""Run a counterbalanced lane-parity study over the GameDevMap active audit.

The audit's homepage phase is volume-bound, so raising `activeAuditHomepageFetchConcurrency`
makes it faster. The question this tool answers is the next one: does the wider arm also
change *which boards* the audit validates? On live hosts the answer is confounded, because
per-host 429s move between arms on their own. The study removes what it can and measures the
rest:

* **Same population.** Each arm audits the same first N representative rows in the audit's
  own order. Equality is proven by the digest of `completedUrls` and by the CSV's SHA-256,
  not assumed from the arguments.
* **One setting differs.** `activeAuditHomepageFetchConcurrency`. Per-host concurrency,
  timeout, batch shape, recovery and probe policy are pinned identically.
* **Isolation.** Each arm gets its own `BALUFFO_DATA_DIR` and artifact path, so no arm reads
  another's cache and the repo's `data/` is never touched.
* **Counterbalanced order.** A pair runs low then high, the next runs high then low, so an
  arm's position cannot be mistaken for its lane count.
* **Lane proof.** The artifact's own `summary.phaseRuntime` must show `configuredLanes` equal
  to the setting, with `peakInFlight` reaching it. An arm that does not prove its lane count
  is discarded instead of compared.

Parity is judged per pair on boards (identified by **host + tenant**, never studio label or
id) and on three whole-run failure measures. The tolerance is the study's own same-lane churn
-- the spread between two arms that share a lane count -- rather than a threshold carried over
from an earlier study, because a difference smaller than same-lane churn says nothing. That
floor therefore needs **two arms per lane count**: a study with one pair has no floor, and its
verdict is `inconclusive` rather than every difference being a violation of zero.

Every board that differs is then attributed to the other arm's record for its host: a 429 or
timeout on that host names the failing party, while a board missing from a host that read fine is
a downstream (probe or inference) difference and belongs to a different investigation. A host
failure says where to look, not that the lane count caused it -- a multi-tenant provider also
throttles at one lane count and not at another, so the same 429 can appear in either arm.

Read the failure counts from the artifact's whole-run `failureCounts` and
`failureErrorCounts`, never from its `failures` sample: that list is capped at 200 rows, and
in the first measured pair it understated a 32->64 homepage-failure change by 7x.

Usage::

  python tools/measurements/discovery/lane_parity.py arm \\
      --study _out/perf-runs/discovery/lane-parity --name p1-32 --lanes 32
  python tools/measurements/discovery/lane_parity.py report \\
      --study _out/perf-runs/discovery/lane-parity --pairs p1:p1-32,p1-64 p2:p2-32,p2-64
  python tools/measurements/discovery/lane_parity.py study \\
      --study _out/perf-runs/discovery/lane-parity --pairs 2
  python tools/measurements/discovery/lane_parity.py trace \\
      --study _out/perf-runs/discovery/lane-parity --pairs p1:p1-32,p1-64

`study` runs the counterbalanced sequence as separate processes and then reports, which is the
whole measurement in one command. Arm records are immutable once written: re-running an arm
needs `--force`, because silently replacing one half of a pair would leave the other half
describing different weather.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools.measurements.discovery.request_trace import (  # noqa: E402
    TRACE_FILE,
    RequestTrace,
    build_trace_report,
    render_trace,
    trace_requests,
)

ARTIFACT_NAME = "gamedevmap-active-source-dry-run.json"
HOMEPAGE_STAGE = "homepage_fetch"
RECOVERY_STAGE = "gamedevmap_recovery_fetch"

# The two failure-set measures and the two failure-count measures the rule judges.
FLOOR_KEYS = (
    "boardSymmetricDifference",
    "failureIdentitySymmetricDifference",
    "totalFailureDelta",
    "homepageFailureDelta",
)


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=1, sort_keys=True), encoding="utf-8")


def _digest(values: list[str]) -> str:
    """Stable 16-hex digest of a population, order-insensitive."""
    joined = "\n".join(sorted(values)).encode("utf-8")
    return hashlib.sha256(joined).hexdigest()[:16]


def _arm_config(*, lanes: int, timeout_s: int) -> dict[str, Any]:
    from src.source_discovery.config import load_discovery_config

    cfg = dict(load_discovery_config())
    gamedevmap = dict(cfg.get("gamedevmap") or {})
    gamedevmap.update(
        {
            "enabled": True,
            "activeAuditHomepageFetchConcurrency": lanes,
            "perHostConcurrency": 3,
            "fetchConcurrency": 24,
            "activeAuditBrowserRecoveryLimit": 0,
            "maxHomepageFetches": 0,
            "maxRows": 0,
        }
    )
    cfg["gamedevmap"] = gamedevmap
    cfg["timeoutSeconds"] = timeout_s
    return cfg


def _csv_digest(csv_url: str, timeout_s: int) -> dict[str, Any]:
    from src.source_discovery.web_search_fetch import fetch_text

    text = fetch_text(csv_url, timeout_s)
    return {"sha256": hashlib.sha256(text.encode("utf-8")).hexdigest(), "chars": len(text)}


def _phase(phases: list[dict[str, Any]], name: str, key: str) -> Any:
    return next((row[key] for row in phases if row["phase"] == name), None)


def _arm_record(
    name: str,
    lanes: int,
    args: argparse.Namespace,
    artifact: dict[str, Any],
    csv_meta: dict[str, Any],
    wall_s: float,
    artifact_path: Path,
    request_trace: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """The arm's own receipt: enough to prove the lane knob and pair it, nothing sampled."""
    summary = _as_dict(artifact.get("summary"))
    runtime = _as_dict(artifact.get("runtime"))
    completed = [str(url) for url in _as_list(artifact.get("completedUrls")) if str(url).strip()]
    phases = [
        {
            "phase": str(item.get("phase") or ""),
            "occurrences": int(item.get("occurrences") or 0),
            "wallMs": int(item.get("wallMs") or 0),
            "items": int(item.get("items") or 0),
            "configuredLanes": int(item.get("configuredLanes") or 0),
            "peakInFlight": int(item.get("peakInFlight") or 0),
            "meanInFlight": item.get("meanInFlight"),
            "laneOccupancyPct": item.get("laneOccupancyPct"),
        }
        for item in _as_list(_as_dict(summary.get("phaseRuntime")).get("phases"))
        if isinstance(item, dict)
    ]
    batches = [
        {
            "batch": item.get("batch"),
            "rows": item.get("rows"),
            "homepageFetchMs": item.get("homepageFetchMs"),
            "probeMs": item.get("probeMs"),
            "recoveryWave1FetchMs": item.get("recoveryWave1FetchMs"),
        }
        for item in _as_list(_as_dict(artifact.get("timings")).get("batches"))
        if isinstance(item, dict)
    ]
    record = {
        "name": name,
        "lanes": lanes,
        "timeoutSeconds": int(args.timeout),
        "batchSize": int(args.batch_size),
        "maxBatches": int(args.batches),
        "wallS": wall_s,
        "startedAt": str(artifact.get("startedAt") or ""),
        "finishedAt": str(artifact.get("finishedAt") or ""),
        "csv": csv_meta,
        "runtime": {
            key: runtime.get(key)
            for key in (
                "timeoutSeconds",
                "fetchConcurrency",
                "perHostConcurrency",
                "homepageFetchConcurrency",
                "recoveryFetchConcurrency",
                "recoveryPerHostConcurrency",
            )
        },
        "population": {
            "count": len(completed),
            "digest": _digest(completed),
            "csvRows": summary.get("csvRows"),
            "eligibleRows": summary.get("eligibleRows"),
            "remainingUrls": summary.get("remainingUrls"),
        },
        "counts": {
            key: summary.get(key)
            for key in (
                "activeCandidates",
                "providerCandidates",
                "staticCandidates",
                "rejectedForActivation",
                "zeroJobCandidates",
                "probedCandidates",
                "probeFailures",
                "homepagesFetched",
                "homepageFetchAttempts",
                "recoveryPagesFetched",
                "recoveryFetchAttempts",
                "failures",
                "technicalFailures",
                "coverageMisses",
                "browserRecoveredActiveCandidates",
            )
        },
        "failureCounts": _as_dict(artifact.get("failureCounts")),
        "rejectedReasonCounts": _as_dict(summary.get("rejectedReasonCounts")),
        "phases": phases,
        "batches": batches,
        "artifact": str(artifact_path),
    }
    if request_trace is not None:
        record["requestTrace"] = request_trace
    return record


def run_arm(args: argparse.Namespace) -> dict[str, Any]:
    """Run one isolated arm and write its record. Returns the record."""
    from src.source_discovery.gamedevmap_active_dry_run import (
        run_gamedevmap_active_source_dry_run,
    )

    study = Path(args.study).resolve()
    arm_dir = study / "arms" / args.name
    record_path = arm_dir / "arm.json"
    if record_path.exists() and not args.force:
        raise SystemExit(
            f"arm {args.name!r} already ran ({record_path}). Arms are immutable once run; "
            "pass --force only when re-measuring the whole study."
        )

    data_dir = arm_dir / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    os.environ["BALUFFO_DATA_DIR"] = str(data_dir)
    artifact_path = data_dir / ARTIFACT_NAME

    cfg = _arm_config(lanes=int(args.lanes), timeout_s=int(args.timeout))
    gamedevmap_cfg = _as_dict(cfg.get("gamedevmap"))
    csv_url = str(gamedevmap_cfg.get("csvUrl") or "")
    csv_meta = _csv_digest(csv_url, int(args.timeout))

    trace = None if bool(getattr(args, "no_trace", False)) else RequestTrace(arm_dir / TRACE_FILE)
    started = time.time()
    try:
        if trace is None:
            artifact = run_gamedevmap_active_source_dry_run(
                timeout_s=int(args.timeout),
                config=cfg,
                output_path=artifact_path,
                reset=True,
                batch_size=int(args.batch_size),
                max_batches=int(args.batches),
            )
        else:
            with trace_requests(trace):
                artifact = run_gamedevmap_active_source_dry_run(
                    timeout_s=int(args.timeout),
                    config=cfg,
                    output_path=artifact_path,
                    reset=True,
                    batch_size=int(args.batch_size),
                    max_batches=int(args.batches),
                )
    finally:
        if trace is not None:
            trace.write()
    wall_s = round(time.time() - started, 1)

    record = _arm_record(
        args.name,
        int(args.lanes),
        args,
        artifact,
        csv_meta,
        wall_s,
        artifact_path,
        request_trace=trace.summary() if trace is not None else {"enabled": False},
    )
    _write_json(record_path, record)
    assert_lane_proof(record)
    return record


def assert_lane_proof(record: dict[str, Any]) -> None:
    """Refuse an arm whose artifact does not show the lane count it was given."""
    phase = next((row for row in record["phases"] if row["phase"] == HOMEPAGE_STAGE), None)
    if phase is None:
        raise SystemExit(f"arm {record['name']!r}: no {HOMEPAGE_STAGE} receipt in the artifact")
    if phase["configuredLanes"] != record["lanes"]:
        raise SystemExit(
            f"arm {record['name']!r}: artifact reports {phase['configuredLanes']} configured "
            f"lanes, not {record['lanes']}"
        )
    if phase["peakInFlight"] < record["lanes"]:
        raise SystemExit(
            f"arm {record['name']!r}: peak in flight {phase['peakInFlight']} never reached "
            f"{record['lanes']} lanes, so the arm did not exercise the setting"
        )


def _load_arm(study: Path, name: str) -> dict[str, Any]:
    path = study / "arms" / name / "arm.json"
    if not path.exists():
        raise SystemExit(f"missing arm record: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def _artifact_path(arm: dict[str, Any], study: Path) -> Path:
    """The arm's artifact, by its recorded path and by the layout this tool writes."""
    recorded = Path(str(arm.get("artifact") or ""))
    if recorded.is_file():
        return recorded
    fallback = study / "arms" / str(arm["name"]) / "data" / ARTIFACT_NAME
    if fallback.is_file():
        return fallback
    raise SystemExit(
        f"arm {arm['name']!r}: artifact missing at {recorded} and {fallback}; a study "
        "directory cannot be moved without its arm data"
    )


def _arm_outcomes(arm: dict[str, Any], study: Path) -> dict[str, Any]:
    """Whole-run outcomes, read from the artifact rather than the capped failure sample."""
    from src.source_discovery.core_identity import board_identity_key

    artifact = json.loads(_artifact_path(arm, study).read_text(encoding="utf-8"))
    summary = _as_dict(artifact.get("summary"))
    boards: dict[str, dict[str, Any]] = {}
    for candidate in _as_list(artifact.get("activeCandidates")):
        if not isinstance(candidate, dict):
            continue
        identity = board_identity_key(candidate)
        boards[identity] = {
            "identity": identity,
            "sourceId": str(candidate.get("id") or "").strip(),
            "name": str(candidate.get("name") or "").strip(),
            "studio": str(candidate.get("studio") or "").strip(),
            "adapter": str(candidate.get("adapter") or "").strip(),
            "url": str(
                candidate.get("careersUrl")
                or candidate.get("listing_url")
                or candidate.get("base_url")
                or ""
            ).strip(),
            "endpointUrl": str(
                candidate.get("api_url") or candidate.get("board_url") or ""
            ).strip(),
            "entryUrl": str(candidate.get("sourceDirectoryEntryUrl") or "").strip(),
        }
    error_counts = {
        str(key): int(value or 0)
        for key, value in _as_dict(artifact.get("failureErrorCounts")).items()
    }
    zero_job = [
        {
            "sourceId": str(row.get("id") or "").strip(),
            "name": str(row.get("name") or "").strip(),
            "studio": str(row.get("studio") or "").strip(),
            "jobsFound": row.get("jobsFound"),
        }
        for row in _as_list(artifact.get("zeroJobCandidates"))
        if isinstance(row, dict)
    ]
    rejected = [
        {
            "sourceId": str(row.get("sourceId") or "").strip(),
            "name": str(row.get("name") or "").strip(),
            "adapter": str(row.get("adapter") or "").strip(),
            "url": str(row.get("url") or "").strip(),
            "endpointUrl": str(
                _as_dict(row.get("candidate")).get("api_url")
                or _as_dict(row.get("candidate")).get("board_url")
                or ""
            ).strip(),
            "studio": str(row.get("studio") or "").strip(),
            "reason": str(row.get("reason") or "").strip(),
            "reasonDetail": str(row.get("reasonDetail") or "").strip(),
            "error": str(row.get("error") or "").strip()[:200],
        }
        for row in _as_list(artifact.get("rejectedForActivation"))
        if isinstance(row, dict)
    ]
    failure_counts = _as_dict(artifact.get("failureCounts"))
    return {
        "boards": boards,
        "failureIdentities": set(error_counts),
        "errorCounts": error_counts,
        "totalFailures": int(summary.get("failures") or 0),
        "homepageFailures": int(failure_counts.get(HOMEPAGE_STAGE) or 0),
        "recoveryFailures": int(failure_counts.get(RECOVERY_STAGE) or 0),
        "rejected": rejected,
        "rejectedReasonCounts": _as_dict(summary.get("rejectedReasonCounts")),
        "zeroJob": zero_job,
        "probedCandidates": int(summary.get("probedCandidates") or 0),
        "probeFailures": int(summary.get("probeFailures") or 0),
    }


def _hosts_for(board: dict[str, Any]) -> set[str]:
    hosts = set()
    for key in ("url", "endpointUrl", "entryUrl"):
        host = (urlparse(str(board.get(key) or "")).netloc or "").strip().lower()
        if host:
            hosts.add(host)
    return hosts


def _same_candidate(board: dict[str, Any], other: dict[str, Any]) -> bool:
    """Whether `other` is this board's own candidate, not a sibling's.

    Two Workable tenants of one studio share the studio label and can even share the board
    name, so a studio match is not identity: it lets a sibling's zero-job row stand in for a
    board that was in fact rejected on 429. The probe identity separates them, and is used
    whenever both sides carry one.
    """
    board_id = str(board.get("sourceId") or "").strip()
    other_id = str(other.get("sourceId") or "").strip()
    if board_id and other_id:
        return board_id == other_id
    board_name = str(board.get("name") or "").strip()
    other_name = str(other.get("name") or "").strip()
    return bool(board_name) and board_name == other_name


def _attribute(board: dict[str, Any], other: dict[str, Any]) -> dict[str, Any]:
    """Why this board is missing from `other`: its host failed, or a downstream outcome."""
    hosts = _hosts_for(board)
    hits = [
        {"identity": identity, "count": count}
        for identity, count in other["errorCounts"].items()
        if any(host in identity.lower() for host in hosts)
    ]
    hits.sort(key=lambda row: (-row["count"], row["identity"]))
    rejection = next((row for row in other["rejected"] if _same_candidate(board, row)), None)
    if rejection is None:
        # Probe failures name their endpoint in the error, and that is the only field a
        # provider rejection reliably carries: `studio` and `url` are empty on those rows.
        rejection = next(
            (
                row
                for row in other["rejected"]
                if (row["url"] and row["url"] == board["url"])
                or (row["endpointUrl"] and row["endpointUrl"] == board["endpointUrl"])
            ),
            None,
        )
    if not hits and rejection and rejection["error"]:
        # Probe failures are not in `failureErrorCounts`, so without this a 429 on the
        # board's own endpoint reads as "no host failure" and the loss gets blamed on the
        # probe or on inference. The rejection is the record of that request failing.
        identity = f"{rejection['reason']}|{rejection['error']}"
        if any(host in identity.lower() for host in hosts):
            hits = [{"identity": identity, "count": 1}]
    zero = next((row for row in other["zeroJob"] if _same_candidate(board, row)), None)
    return {
        "hostFailureHits": hits[:3],
        "failedOnHost": bool(hits),
        "homepageFailure": any(hit["identity"].startswith(f"{HOMEPAGE_STAGE}|") for hit in hits),
        "rejection": rejection,
        "zeroJob": zero,
    }


def _pair_report(
    label: str, low: dict[str, Any], high: dict[str, Any], study: Path
) -> dict[str, Any]:
    left = _arm_outcomes(low, study)
    right = _arm_outcomes(high, study)
    boards_low, boards_high = set(left["boards"]), set(right["boards"])
    lost = sorted(boards_low - boards_high)
    gained = sorted(boards_high - boards_low)
    attribution = [
        {
            "presentIn": f"{low['lanes']} only",
            **left["boards"][identity],
            **_attribute(left["boards"][identity], right),
        }
        for identity in lost
    ]
    attribution += [
        {
            "presentIn": f"{high['lanes']} only",
            **right["boards"][identity],
            **_attribute(right["boards"][identity], left),
        }
        for identity in gained
    ]
    return {
        "pair": label,
        "order": [low["name"], high["name"]],
        "lanes": {"low": int(low["lanes"]), "high": int(high["lanes"])},
        "boards": {
            "low": len(boards_low),
            "high": len(boards_high),
            "delta": len(boards_low) - len(boards_high),
            "onlyLow": lost,
            "onlyHigh": gained,
            "symmetricDifference": len(boards_low ^ boards_high),
        },
        "failures": {
            "identitiesLow": len(left["failureIdentities"]),
            "identitiesHigh": len(right["failureIdentities"]),
            "identitySymmetricDifference": len(
                left["failureIdentities"] ^ right["failureIdentities"]
            ),
            "identityOnlyLow": len(left["failureIdentities"] - right["failureIdentities"]),
            "identityOnlyHigh": len(right["failureIdentities"] - left["failureIdentities"]),
            "totalLow": left["totalFailures"],
            "totalHigh": right["totalFailures"],
            "totalDelta": left["totalFailures"] - right["totalFailures"],
            "homepageLow": left["homepageFailures"],
            "homepageHigh": right["homepageFailures"],
            "homepageDelta": left["homepageFailures"] - right["homepageFailures"],
            "recoveryLow": left["recoveryFailures"],
            "recoveryHigh": right["recoveryFailures"],
        },
        "probe": {
            "probedLow": left["probedCandidates"],
            "probedHigh": right["probedCandidates"],
            "failuresLow": left["probeFailures"],
            "failuresHigh": right["probeFailures"],
        },
        "rejectedReasonCounts": {
            "low": left["rejectedReasonCounts"],
            "high": right["rejectedReasonCounts"],
        },
        "timing": {
            "wallSLow": low["wallS"],
            "wallSHigh": high["wallS"],
            "wallSpeedup": round(low["wallS"] / high["wallS"], 3) if high["wallS"] else None,
            "homepageFetchMsLow": _phase(low["phases"], HOMEPAGE_STAGE, "wallMs"),
            "homepageFetchMsHigh": _phase(high["phases"], HOMEPAGE_STAGE, "wallMs"),
            "homepagePeakLow": _phase(low["phases"], HOMEPAGE_STAGE, "peakInFlight"),
            "homepagePeakHigh": _phase(high["phases"], HOMEPAGE_STAGE, "peakInFlight"),
        },
        "population": {
            "low": low["population"],
            "high": high["population"],
            "samePopulation": low["population"]["digest"] == high["population"]["digest"],
        },
        "csvSame": low["csv"]["sha256"] == high["csv"]["sha256"],
        "attribution": attribution,
    }


def _same_lane_churn(
    pairs: list[tuple[str, dict[str, Any], dict[str, Any]]], study: Path
) -> tuple[dict[str, Any], dict[str, int]]:
    """The study's own noise floor, measured between arms that share a lane count."""
    by_lanes: dict[int, list[dict[str, Any]]] = {}
    for _, low, high in pairs:
        for arm in (low, high):
            by_lanes.setdefault(int(arm["lanes"]), []).append(arm)

    churn: dict[str, Any] = {}
    for lanes, arms in sorted(by_lanes.items()):
        if len(arms) < 2:
            churn[str(lanes)] = None
            continue
        outcomes = [_arm_outcomes(arm, study) for arm in arms]
        churn[str(lanes)] = {
            "boardSymmetricDifference": len(
                outcomes[0]["boards"].keys() ^ outcomes[1]["boards"].keys()
            ),
            "failureIdentitySymmetricDifference": len(
                outcomes[0]["failureIdentities"] ^ outcomes[1]["failureIdentities"]
            ),
            "totalFailureDelta": abs(outcomes[0]["totalFailures"] - outcomes[1]["totalFailures"]),
            "homepageFailureDelta": abs(
                outcomes[0]["homepageFailures"] - outcomes[1]["homepageFailures"]
            ),
            "wallSpreadS": round(
                max(arm["wallS"] for arm in arms) - min(arm["wallS"] for arm in arms), 1
            ),
        }
    floor = {
        key: max((row[key] for row in churn.values() if isinstance(row, dict)), default=0)
        for key in FLOOR_KEYS
    }
    return churn, floor


def _violations(pair: dict[str, Any], floor: dict[str, int]) -> list[str]:
    checks = (
        (
            "board symmetric difference",
            pair["boards"]["symmetricDifference"],
            floor["boardSymmetricDifference"],
        ),
        (
            "failure identity difference",
            pair["failures"]["identitySymmetricDifference"],
            floor["failureIdentitySymmetricDifference"],
        ),
        (
            "total failure delta",
            abs(pair["failures"]["totalDelta"]),
            floor["totalFailureDelta"],
        ),
        (
            "homepage failure delta",
            abs(pair["failures"]["homepageDelta"]),
            floor["homepageFailureDelta"],
        ),
    )
    return [
        f"{pair['pair']}: {label} {measured} > floor {allowed}"
        for label, measured, allowed in checks
        if measured > allowed
    ]


def build_report(study: Path, specs: list[str]) -> dict[str, Any]:
    """Judge the counterbalanced pairs and write `study/report.json`."""
    pairs: list[tuple[str, dict[str, Any], dict[str, Any]]] = []
    for spec in specs:
        label, names = spec.split(":", 1)
        low_name, high_name = names.split(",", 1)
        low, high = _load_arm(study, low_name), _load_arm(study, high_name)
        if int(low["lanes"]) >= int(high["lanes"]):
            raise SystemExit(
                f"pair {label}: pass the lower-lane arm first "
                f"({low_name}={low['lanes']}, {high_name}={high['lanes']})"
            )
        pairs.append((label, low, high))

    reports = [_pair_report(label, low, high, study) for label, low, high in pairs]
    churn, floor = _same_lane_churn(pairs, study)
    # One arm per lane count leaves the noise floor unmeasured. Judging anyway would call
    # every difference a violation of a zero floor, so the report refuses to name a winner.
    floor_defined = bool(churn) and all(isinstance(row, dict) for row in churn.values())
    violations = [line for pair in reports for line in _violations(pair, floor)]
    one_way = all(pair["boards"]["delta"] > 0 for pair in reports)
    if not floor_defined:
        verdict = "inconclusive"
    elif violations or one_way:
        verdict = "parity-not-supported"
    else:
        verdict = "parity-supported"
    report = {
        "pairs": reports,
        "sameLaneChurn": churn,
        "floor": floor,
        "floorDefined": floor_defined,
        "violations": violations,
        "totals": {
            "pairs": len(reports),
            "pairsWithBoardLoss": sum(1 for p in reports if p["boards"]["delta"] > 0),
            "pairsWithBoardGain": sum(1 for p in reports if p["boards"]["delta"] < 0),
            "boardsLost": sum(max(0, p["boards"]["delta"]) for p in reports),
            "boardsGained": sum(max(0, -p["boards"]["delta"]) for p in reports),
            "netBoardDelta": sum(p["boards"]["delta"] for p in reports),
            "populationPinned": all(p["population"]["samePopulation"] for p in reports),
            "csvPinned": all(p["csvSame"] for p in reports),
        },
        "verdict": verdict,
    }
    _write_json(study / "report.json", report)
    return report


def _one_line(text: Any, limit: int = 90) -> str:
    """An artifact's error key, collapsed so one attribution stays on one row."""
    return " ".join(str(text).split())[:limit]


def _render_report(report: dict[str, Any]) -> str:
    lines = []
    floor = report["floor"]
    lines.append(
        "same-lane floor: boards {boardSymmetricDifference}, failure identities "
        "{failureIdentitySymmetricDifference}, total failures {totalFailureDelta}, "
        "homepage failures {homepageFailureDelta}".format(**floor)
    )
    if not report["floorDefined"]:
        lines.append(
            "floor undefined: a lane count ran once, so same-lane churn is unmeasured and "
            "the verdict says nothing"
        )
    for pair in report["pairs"]:
        timing = pair["timing"]
        lines.append(
            f"{pair['pair']}: {pair['lanes']['low']} -> {pair['lanes']['high']} lanes, "
            f"boards {pair['boards']['low']} -> {pair['boards']['high']} "
            f"(delta {pair['boards']['delta']:+d}), "
            f"failure identities delta {pair['failures']['identitySymmetricDifference']}, "
            f"total failures {pair['failures']['totalLow']} -> "
            f"{pair['failures']['totalHigh']}, homepage failures "
            f"{pair['failures']['homepageLow']} -> {pair['failures']['homepageHigh']}, "
            f"wall {timing['wallSLow']} -> {timing['wallSHigh']} s"
        )
        for row in pair["attribution"]:
            hits = (
                "; ".join(
                    f"{_one_line(hit['identity'])} x{hit['count']}"
                    for hit in row["hostFailureHits"]
                )
                or "no host failure"
            )
            lines.append(f"  {row['presentIn']}: {row['name']} {row['url']} -- {hits}")
    if report["violations"]:
        lines.append("violations:")
        lines.extend(f"  - {line}" for line in report["violations"])
    totals = report["totals"]
    lines.append(
        f"pooled: {totals['boardsLost']} lost, {totals['boardsGained']} gained, "
        f"net {totals['netBoardDelta']:+d} | population pinned: "
        f"{totals['populationPinned']} | csv pinned: {totals['csvPinned']}"
    )
    lines.append(f"verdict: {report['verdict']}")
    return "\n".join(lines)


def cmd_arm(args: argparse.Namespace) -> int:
    record = run_arm(args)
    print(
        json.dumps(
            {
                "name": record["name"],
                "lanes": record["lanes"],
                "wallS": record["wallS"],
                "population": record["population"],
                "counts": record["counts"],
                "homepageFetch": next(
                    (row for row in record["phases"] if row["phase"] == HOMEPAGE_STAGE),
                    None,
                ),
                "requestTrace": record.get("requestTrace"),
            },
            indent=1,
        )
    )
    return 0


def cmd_report(args: argparse.Namespace) -> int:
    report = build_report(Path(args.study).resolve(), list(args.pairs))
    print(json.dumps(report, indent=1, sort_keys=True) if args.json else _render_report(report))
    return 0


def cmd_trace(args: argparse.Namespace) -> int:
    report = build_trace_report(Path(args.study).resolve(), list(args.pairs))
    print(json.dumps(report, indent=1, sort_keys=True) if args.json else render_trace(report))
    return 0


def cmd_study(args: argparse.Namespace) -> int:
    """Run the counterbalanced sequence in separate processes, then judge it."""
    study = Path(args.study).resolve()
    study.mkdir(parents=True, exist_ok=True)
    specs = []
    for index in range(1, int(args.pairs) + 1):
        first, second = (args.low, args.high) if index % 2 else (args.high, args.low)
        names = {}
        for lanes in (args.low, args.high):
            name = f"p{index}-{lanes}"
            names[lanes] = name
            if (study / "arms" / name / "arm.json").exists() and not args.force:
                raise SystemExit(
                    f"arm {name!r} already ran; pass --force to re-measure the whole study"
                )
        for lanes in (first, second):
            command = [
                sys.executable,
                str(Path(__file__).resolve()),
                "arm",
                "--study",
                str(study),
                "--name",
                names[lanes],
                "--lanes",
                str(lanes),
                "--batches",
                str(args.batches),
                "--batch-size",
                str(args.batch_size),
                "--timeout",
                str(args.timeout),
            ]
            if args.force:
                command.append("--force")
            if getattr(args, "no_trace", False):
                command.append("--no-trace")
            print(f"=== {time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())} arm {names[lanes]}")
            log_path = study / f"arms-{names[lanes]}.log"
            completed = subprocess.run(command, cwd=ROOT, capture_output=True, text=True)
            log_path.write_text(completed.stdout + completed.stderr, encoding="utf-8")
            if completed.returncode != 0:
                print(completed.stderr.strip()[:2000], file=sys.stderr)
                raise SystemExit(f"arm {names[lanes]} failed; see {log_path}")
            for line in completed.stdout.strip().splitlines()[:4]:
                print(f"  {line}")
        specs.append(f"p{index}:{names[args.low]},{names[args.high]}")
    report = build_report(study, specs)
    print(_render_report(report))
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)

    def add_run_flags(target: argparse.ArgumentParser) -> None:
        target.add_argument("--batches", type=int, default=3)
        target.add_argument("--batch-size", type=int, default=1000)
        target.add_argument("--timeout", type=int, default=6)
        target.add_argument("--force", action="store_true", help="Re-run an arm that exists.")
        target.add_argument(
            "--no-trace",
            action="store_true",
            help="Do not record the arm's per-request host trace.",
        )

    arm = sub.add_parser("arm", help="Run one isolated arm and record it.")
    arm.add_argument("--study", required=True)
    arm.add_argument("--name", required=True)
    arm.add_argument("--lanes", type=int, required=True)
    add_run_flags(arm)
    arm.set_defaults(func=cmd_arm)

    report = sub.add_parser("report", help="Judge counterbalanced pairs from arm records.")
    report.add_argument("--study", required=True)
    report.add_argument(
        "--pairs",
        nargs="+",
        required=True,
        help="label:low_arm,high_arm entries; pass the lower-lane arm first.",
    )
    report.add_argument("--json", action="store_true", help="Print the report, not the table.")
    report.set_defaults(func=cmd_report)

    study = sub.add_parser("study", help="Run N counterbalanced pairs, then report.")
    study.add_argument("--study", required=True)
    study.add_argument("--pairs", type=int, default=2, help="Number of counterbalanced pairs.")
    study.add_argument("--low", type=int, default=32)
    study.add_argument("--high", type=int, default=64)
    add_run_flags(study)
    study.set_defaults(func=cmd_study)

    trace = sub.add_parser("trace", help="Per-host request and 429 rates from traced arms.")
    trace.add_argument("--study", required=True)
    trace.add_argument(
        "--pairs",
        nargs="+",
        required=True,
        help="label:low_arm,high_arm entries; pass the lower-lane arm first.",
    )
    trace.add_argument("--json", action="store_true", help="Print the report, not the table.")
    trace.set_defaults(func=cmd_trace)

    args = parser.parse_args(argv)
    return int(args.func(args))


if __name__ == "__main__":
    raise SystemExit(main())
