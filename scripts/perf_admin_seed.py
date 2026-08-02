#!/usr/bin/env python3
"""Copy the live Baluffo /data volume into a local benchmark seed directory.

Keeps only the artifacts the Admin page flows actually read/write: registry trees,
fetch/discovery reports, source state, dedup review state, sync/shard manifests,
the runtime jobs feed, and the container runtime SQLite store.

Used by `scripts/perf_admin_flows.py` to run a container against a realistic
production-sized dataset without committing real payloads to git.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Whitelist of files the Admin surface touches. Anything else in /data is ignored.
# Keep this narrow: we want the heavy aggregates, not user credentials or token state.
SEED_FILES = [
    # Registry + conflicts
    "source-registry-active.json",
    "source-registry-pending.json",
    "source-registry-rejected.json",
    "source-registry-metadata.json",
    "source-registry-tombstones.json",
    "registry-conflicts-adjudication.json",
    "registry-conflicts-summary.json",
    "registry-conflicts-full.json",
    # Jobs + fetch
    "jobs-fetch-report.json",
    "jobs-fetch-report-summary.json",
    "jobs-fetch-tasks.json",
    "jobs-source-state.json",
    "jobs-lifecycle-state.json",
    "jobs-unified.json",
    "jobs-unified.json.gz",
    "jobs-unified-light.json",
    "jobs-unified-light.json.gz",
    # Discovery
    "source-discovery-candidates.json",
    "source-discovery-config.json",
    "source-discovery-report.json",
    "source-discovery.log",
    # Source policy + dedup
    "source-policy.json",
    "dedup-review-state.json",
    # Sync
    "source-sync-state.json",
    "source-sync-shard-manifest.json",
    "source-sync-log.json",
    # Runtime store + activity
    "baluffo-runtime.db",
    "admin-run-history.json",
]

DEFAULT_OUT = REPO_ROOT / "_out" / "perf-admin-flows" / "seed-data"


def _copy_named(source_dir: Path, target_dir: Path, name: str) -> bool:
    source = source_dir / name
    target = target_dir / name
    if not source.is_file():
        return False
    target.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, target)
    return True


def seed_volume(source_dir: Path, target_dir: Path) -> dict[str, int]:
    if not source_dir.is_dir():
        raise SystemExit(f"source data dir not found: {source_dir}")
    target_dir.mkdir(parents=True, exist_ok=True)
    copied = 0
    for name in SEED_FILES:
        copied += 1 if _copy_named(source_dir, target_dir, name) else 0
    return {"filesCopied": copied, "target": str(target_dir)}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-volume-path",
        required=True,
        help="Path to the live Baluffo data directory (Umbrel: /home/umbrel/app_data/baluffo/data).",
    )
    parser.add_argument(
        "--output",
        default=str(DEFAULT_OUT),
        help=f"Benchmark seed volume output directory (default: {DEFAULT_OUT}).",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)
    result = seed_volume(Path(args.from_volume_path).resolve(), Path(args.output).resolve())
    print(f"seeded {result['filesCopied']} file(s) into {result['target']}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
