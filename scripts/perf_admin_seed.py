#!/usr/bin/env python3
"""Copy a Baluffo data volume into a local benchmark seed directory.

Keeps only the artifacts the Admin page flows actually read/write: registry trees,
fetch/discovery reports, source state, dedup review state, sync/shard manifests,
the runtime jobs feed, and the container runtime SQLite store.

Used by `scripts/perf_admin_flows.py` to run a container against a realistic
production-sized dataset without committing real payloads to git.

``--from-volume-path`` points at the *data* directory, not a repo root: pass
``data`` for this checkout, or ``/home/umbrel/app_data/baluffo/data`` for a live
Umbrel host. Individual artifacts are accepted as either the plain file or its
``.gz`` sibling, because a container ``/data`` keeps them flat while this repo's
``data/`` stores some registry and feed artifacts gzip-on-disk.
"""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path
from typing import Any

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
    "jobs-unified-light.json",
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


def _copy_named(source_dir: Path, target_dir: Path, name: str) -> str:
    """Copy one whitelist entry, preferring the plain file and falling back to .gz.

    Returns the relative name copied, or "" when neither variant exists. A repo
    checkout's ``data/`` stores some of these artifacts gzip-on-disk under a
    ``.json`` logical name, while a live container ``/data`` keeps them flat, so
    accepting either variant is what keeps both layouts working.
    """
    for candidate in (name, f"{name}.gz"):
        source = source_dir / candidate
        if not source.is_file():
            continue
        target = target_dir / candidate
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(source, target)
        return candidate
    return ""


def _looks_like_repo_root_with_data_child(path: Path) -> bool:
    """True when `path` looks like a repo checkout root rather than a data dir."""
    if (path / "data").is_dir() and (path / "package.json").is_file():
        return True
    return (path / "data").is_dir() and not any((path / n).exists() for n in SEED_FILES)


def seed_volume(source_dir: Path, target_dir: Path) -> dict[str, Any]:
    if not source_dir.is_dir():
        raise SystemExit(f"source data dir not found: {source_dir}")
    if _looks_like_repo_root_with_data_child(source_dir):
        raise SystemExit(
            f"source data dir looks like a repo checkout root, not a Baluffo data dir: "
            f"{source_dir}\n"
            f"Pass the data directory itself, e.g. --from-volume-path data "
            f"(or --from-volume-path {source_dir / 'data'})."
        )
    target_dir.mkdir(parents=True, exist_ok=True)
    copied: list[str] = []
    missing: list[str] = []
    for name in SEED_FILES:
        landed = _copy_named(source_dir, target_dir, name)
        if landed:
            copied.append(landed)
        elif name not in missing:
            missing.append(name)
    if not copied:
        raise SystemExit(
            f"seeded 0 files from {source_dir} into {target_dir}\n"
            f"None of the {len(SEED_FILES)} whitelisted artifacts were found. "
            f"Check that --from-volume-path points at a Baluffo data directory."
        )
    return {
        "filesCopied": len(copied),
        "copied": copied,
        "missing": missing,
        "target": str(target_dir),
    }


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--from-volume-path",
        required=True,
        help=(
            "Path to the Baluffo data directory containing the artifacts. "
            "For a live Umbrel host that is "
            "/home/umbrel/app_data/baluffo/data; for this repo checkout pass "
            "`data` (not the repo root)."
        ),
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
    if result["missing"]:
        preview = ", ".join(result["missing"][:6])
        suffix = (
            "" if len(result["missing"]) <= 6 else f", ... (+{len(result['missing']) - 6} more)"
        )
        print(
            f"note: {len(result['missing'])} whitelisted artifact(s) were absent from the "
            f"source: {preview}{suffix}",
            flush=True,
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
