#!/usr/bin/env python3
"""Drain discovery's curated-seed queue and report what actually lands.

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

Everything is isolated under `--data-dir`. Discovery has no `--output-dir`, and
`BALUFFO_DATA_DIR` is the only redirect it honours, so an unisolated run writes into
the repo's `data/` and leaves discovery-audit artefacts behind.

Usage:
  python tools/coverage_drain.py --rounds 6 --data-dir _out/coverage/drain
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

ROOT = Path(__file__).resolve().parents[1]

# The registry is written as a gzipped JSON array; the sibling .jsonl holds the same
# data as a single-line array, so parsing it as newline-delimited silently yields one
# row and every count comes out wrong.
REGISTRY_ACTIVE = "source-registry-active.json.gz"
REGISTRY_PENDING = "source-registry-pending.json.gz"
REPORT = "source-discovery-report.json"


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
        # The runtime writes gzipped; match that so the seeded file is read the
        # same way the run's own output is.
        payload = json.dumps(rows, ensure_ascii=False).encode("utf-8")
        (data_dir / REGISTRY_ACTIVE).write_bytes(gzip.compress(payload))
        print(f"seeded {len(rows)} registry rows into {data_dir}")


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

    if args.out:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(json.dumps({"rounds": rows}, indent=2), encoding="utf-8")

    first, last = rows[0], rows[-1]
    print()
    print(f"seeds registered      : {first['generated']}")
    print(f"active after round 1  : {first['active']}")
    print(f"active after round {len(rows)}    : {last['active']}")
    print(f"still deferred by cap : {last['deferredByCap']}")
    print(f"probe-failed (stable) : {last['probeFailed']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
