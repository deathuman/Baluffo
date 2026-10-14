"""CLI maintenance tool to safely reclaim space from baluffo-runtime.db and updater downloads.

Identifies orphaned feed generations, verifies current generation integrity,
purges dead snapshots, runs SQLite VACUUM, and cleans obsolete updater archives.
Default mode is dry-run. Pass --apply to perform modifications.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _sha256_json(value: Any) -> str:
    return hashlib.sha256(_json_dumps(value).encode("utf-8")).hexdigest()


def compute_generation_hash(conn: sqlite3.Connection, generation: str) -> tuple[int, str]:
    cur = conn.cursor()
    cur.execute(
        """
        SELECT job_key, payload_json
        FROM jobs
        WHERE feed_generation = ?
        ORDER BY row_ordinal ASC
        """,
        (generation,),
    )
    rows = cur.fetchall()
    row_count = len(rows)
    hash_val = _sha256_json([r[1] for r in rows])
    return row_count, hash_val


def inspect_database(db_path: Path) -> dict[str, Any]:
    if not db_path.exists():
        raise FileNotFoundError(f"Database file not found: {db_path}")

    db_size = db_path.stat().st_size
    wal_path = db_path.with_name(f"{db_path.name}-wal")
    wal_size = wal_path.stat().st_size if wal_path.exists() else 0

    conn = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    cur = conn.cursor()

    cur.execute("SELECT current_generation, row_count, row_hash FROM job_feed_state WHERE id = 1")
    state_row = cur.fetchone()
    if not state_row or not state_row[0]:
        conn.close()
        raise RuntimeError("No current_generation found in job_feed_state")

    current_gen = str(state_row[0])
    expected_row_count = int(state_row[1] or 0)
    expected_row_hash = str(state_row[2] or "")

    cur.execute("SELECT COUNT(DISTINCT feed_generation) FROM jobs")
    distinct_generations_count = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COUNT(*) FROM jobs WHERE feed_generation = ?", (current_gen,))
    active_jobs_count = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COUNT(*) FROM job_sources WHERE feed_generation = ?", (current_gen,))
    active_sources_count = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COUNT(*) FROM jobs WHERE feed_generation != ?", (current_gen,))
    dead_jobs_count = int(cur.fetchone()[0] or 0)

    cur.execute("SELECT COUNT(*) FROM job_sources WHERE feed_generation != ?", (current_gen,))
    dead_sources_count = int(cur.fetchone()[0] or 0)

    cur.execute("PRAGMA page_count")
    page_count = int(cur.fetchone()[0] or 0)
    cur.execute("PRAGMA page_size")
    page_size = int(cur.fetchone()[0] or 0)
    cur.execute("PRAGMA freelist_count")
    freelist_count = int(cur.fetchone()[0] or 0)

    conn.close()

    return {
        "db_path": str(db_path),
        "db_size_bytes": db_size,
        "wal_size_bytes": wal_size,
        "current_generation": current_gen,
        "expected_row_count": expected_row_count,
        "expected_row_hash": expected_row_hash,
        "distinct_generations_count": distinct_generations_count,
        "active_jobs_count": active_jobs_count,
        "active_sources_count": active_sources_count,
        "dead_jobs_count": dead_jobs_count,
        "dead_sources_count": dead_sources_count,
        "page_count": page_count,
        "page_size": page_size,
        "freelist_count": freelist_count,
        "freelist_bytes": freelist_count * page_size,
    }


def inspect_updater_downloads(data_dir: Path) -> list[dict[str, Any]]:
    downloads_dir = data_dir / "updater" / "downloads"
    if not downloads_dir.exists():
        return []
    zips = list(downloads_dir.glob("*.zip"))
    return [
        {"path": str(z), "name": z.name, "size_bytes": z.stat().st_size}
        for z in sorted(zips, key=lambda p: p.stat().st_mtime)
    ]


def reclaim(
    data_dir: Path,
    *,
    apply: bool = False,
    run_vacuum: bool = True,
    prune_updater: bool = True,
) -> dict[str, Any]:
    db_path = data_dir / "baluffo-runtime.db"
    pre_info = inspect_database(db_path)
    updater_zips = inspect_updater_downloads(data_dir)

    print("=== Baluffo Runtime Storage Reclamation ===")
    print(f"Data directory:     {data_dir}")
    print(f"Database path:      {db_path}")
    print(
        f"Database size:      {pre_info['db_size_bytes'] / (1024 * 1024):.2f} MB ({pre_info['db_size_bytes'] / (1024 * 1024 * 1024):.2f} GB)"
    )
    print(f"WAL size:           {pre_info['wal_size_bytes'] / (1024 * 1024):.2f} MB")
    print(f"Total generations:  {pre_info['distinct_generations_count']}")
    print(f"Active generation:  {pre_info['current_generation']}")
    print(
        f"Active rows:        {pre_info['active_jobs_count']} jobs, {pre_info['active_sources_count']} sources"
    )
    print(
        f"Dead rows to purge: {pre_info['dead_jobs_count']} jobs, {pre_info['dead_sources_count']} sources"
    )
    print(f"Freelist bytes:     {pre_info['freelist_bytes'] / (1024 * 1024):.2f} MB")

    if updater_zips:
        total_zip_bytes = sum(z["size_bytes"] for z in updater_zips)
        print(
            f"\nUpdater downloads:  {len(updater_zips)} archives ({total_zip_bytes / (1024 * 1024):.2f} MB)"
        )
        for z in updater_zips:
            print(f"  - {z['name']}: {z['size_bytes'] / (1024 * 1024):.2f} MB")

    if not apply:
        print("\n[DRY RUN] No changes applied. Re-run with --apply to execute cleanup.")
        return {"dry_run": True, "pre": pre_info, "updater_zips": updater_zips}

    print("\n[APPLYING] Beginning purge and compaction...")

    conn = sqlite3.connect(str(db_path), timeout=60.0)
    conn.execute("PRAGMA busy_timeout = 60000")
    conn.execute("PRAGMA foreign_keys = ON")

    # Verify active generation before deletion
    current_gen = pre_info["current_generation"]
    cnt_before, hash_before = compute_generation_hash(conn, current_gen)
    if cnt_before != pre_info["active_jobs_count"]:
        conn.close()
        raise RuntimeError(
            f"Active jobs count mismatch: {cnt_before} != {pre_info['active_jobs_count']}"
        )

    print("Purging orphaned generations from jobs and job_sources...")
    conn.execute("BEGIN IMMEDIATE")
    cur = conn.cursor()
    cur.execute("DELETE FROM job_sources WHERE feed_generation != ?", (current_gen,))
    cur.execute("DELETE FROM jobs WHERE feed_generation != ?", (current_gen,))
    conn.commit()
    print("Purge committed.")

    # Verify active generation after deletion
    cnt_after, hash_after = compute_generation_hash(conn, current_gen)
    if cnt_after != cnt_before or hash_after != hash_before:
        conn.close()
        raise RuntimeError("Integrity violation: active generation changed during orphan purge!")
    print(
        f"Integrity verified: active generation '{current_gen}' has {cnt_after} rows, matching hash."
    )

    if run_vacuum:
        print("Running SQLite VACUUM to reclaim free pages to OS (this may take a few seconds)...")
        conn.execute("VACUUM")
        print("VACUUM completed.")

    print("Running PRAGMA wal_checkpoint(TRUNCATE)...")
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")

    print("Running PRAGMA quick_check...")
    cur = conn.cursor()
    cur.execute("PRAGMA quick_check")
    qc = cur.fetchone()[0]
    if qc != "ok":
        conn.close()
        raise RuntimeError(f"PRAGMA quick_check failed: {qc}")
    print("PRAGMA quick_check: OK")

    conn.close()

    # Prune updater zips if requested (keep the latest 1 if present)
    pruned_zips = []
    if prune_updater and len(updater_zips) > 0:
        # Keep the newest zip, remove earlier ones
        to_remove = updater_zips[:-1] if len(updater_zips) > 1 else updater_zips
        for z in to_remove:
            p = Path(z["path"])
            try:
                p.unlink(missing_ok=True)
                pruned_zips.append(z)
                print(f"Removed old updater zip: {z['name']}")
            except OSError as err:
                print(f"Failed to remove {z['name']}: {err}")

    post_info = inspect_database(db_path)
    reclaimed_bytes = pre_info["db_size_bytes"] - post_info["db_size_bytes"]
    pct = (reclaimed_bytes / max(1, pre_info["db_size_bytes"])) * 100.0

    print("\n=== Reclamation Summary ===")
    print(
        f"Previous DB size:  {pre_info['db_size_bytes'] / (1024 * 1024):.2f} MB ({pre_info['db_size_bytes'] / (1024 * 1024 * 1024):.2f} GB)"
    )
    print(
        f"New DB size:       {post_info['db_size_bytes'] / (1024 * 1024):.2f} MB ({post_info['db_size_bytes'] / (1024 * 1024 * 1024):.2f} GB)"
    )
    print(
        f"Reclaimed space:   {reclaimed_bytes / (1024 * 1024):.2f} MB ({reclaimed_bytes / (1024 * 1024 * 1024):.2f} GB) [{pct:.1f}% reduction]"
    )
    print(f"Distinct generations left: {post_info['distinct_generations_count']}")

    return {
        "applied": True,
        "pre": pre_info,
        "post": post_info,
        "reclaimed_bytes": reclaimed_bytes,
        "pruned_zips": pruned_zips,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Reclaim space in baluffo-runtime.db")
    default_data_dir = os.environ.get("APPDATA", "")
    default_path = (Path(default_data_dir) / "Baluffo") if default_data_dir else Path("data")

    parser.add_argument(
        "--data-dir",
        type=Path,
        default=default_path,
        help="Path to the Baluffo data directory (default: %%APPDATA%%\\Baluffo)",
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Actually execute purge, vacuum, and updater cleanup (default is dry-run)",
    )
    parser.add_argument(
        "--no-vacuum",
        action="store_true",
        help="Skip VACUUM step",
    )
    parser.add_argument(
        "--no-prune-updater",
        action="store_true",
        help="Skip pruning updater downloads",
    )

    args = parser.parse_args()
    try:
        reclaim(
            args.data_dir,
            apply=args.apply,
            run_vacuum=not args.no_vacuum,
            prune_updater=not args.no_prune_updater,
        )
        return 0
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
