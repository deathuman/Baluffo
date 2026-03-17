"""
Benchmark discovery probe phase under different concurrency settings.

Runs source discovery with a limited candidate set (sheet + no web search,
gamesmap disabled) and measures wall time and outcomes for several
concurrency presets. Use results to tune BALUFFO_DISCOVERY_PROBE_* env vars.

Usage:
  python scripts/benchmark_discovery_probe.py [--rows N] [--timeout T]

Writes nothing to the real data dir; uses a temp dir for the run.
"""

from __future__ import annotations

import json
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# Presets: (total, static, provider, teamtailor). Defaults in source_discovery match "moderate".
PRESETS = {
    "conservative": (10, 5, 10, 8),
    "moderate": (25, 10, 25, 15),
    "high": (50, 10, 50, 25),
    "aggressive": (80, 20, 80, 40),
    "max": (120, 30, 120, 60),
}


def _ensure_json_array(path: Path, default: list) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(json.dumps(default), encoding="utf-8")


def run_one_preset(
    preset_name: str,
    total: int,
    static: int,
    provider: int,
    teamtailor: int,
    sheet_max_rows: int,
    timeout_s: int,
    root: Path,
) -> dict:
    from src import source_discovery as sd

    env_keys = (
        "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TOTAL",
        "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_STATIC",
        "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_PROVIDER",
        "BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TEAMTAILOR",
        "BALUFFO_SHEET_DIRECTORY_MAX_ROWS",
    )
    prev = {k: os.environ.get(k) for k in env_keys}
    try:
        os.environ["BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TOTAL"] = str(total)
        os.environ["BALUFFO_DISCOVERY_PROBE_CONCURRENCY_STATIC"] = str(static)
        os.environ["BALUFFO_DISCOVERY_PROBE_CONCURRENCY_PROVIDER"] = str(provider)
        os.environ["BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TEAMTAILOR"] = str(teamtailor)
        os.environ["BALUFFO_SHEET_DIRECTORY_MAX_ROWS"] = str(sheet_max_rows)

        paths = (
            sd.ACTIVE_PATH,
            sd.PENDING_PATH,
            sd.REJECTED_PATH,
            sd.DISCOVERY_CANDIDATES_PATH,
            sd.DISCOVERY_REPORT_PATH,
        )
        orig = list(paths)
        sd.ACTIVE_PATH = root / "active.json"
        sd.PENDING_PATH = root / "pending.json"
        sd.REJECTED_PATH = root / "rejected.json"
        sd.DISCOVERY_CANDIDATES_PATH = root / "candidates.json"
        sd.DISCOVERY_REPORT_PATH = root / "report.json"
        # Reset registries so this preset sees a clean state (same candidate set every run).
        _ensure_json_array(sd.ACTIVE_PATH, [])
        _ensure_json_array(sd.PENDING_PATH, [])
        _ensure_json_array(sd.REJECTED_PATH, [])

        try:
            t0 = time.perf_counter()
            report = sd.run_discovery(
                timeout_s=timeout_s,
                top_n=0,
                mode="dynamic",
                include_web_search=False,
                discovery_config={"gamesmap": {"enabled": False}},
            )
            elapsed = time.perf_counter() - t0
        finally:
            sd.ACTIVE_PATH, sd.PENDING_PATH, sd.REJECTED_PATH, sd.DISCOVERY_CANDIDATES_PATH, sd.DISCOVERY_REPORT_PATH = orig
    finally:
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v

    s = report.get("summary") or {}
    return {
        "preset": preset_name,
        "total": total,
        "static": static,
        "provider": provider,
        "teamtailor": teamtailor,
        "wall_seconds": round(elapsed, 1),
        "found": int(s.get("foundEndpointCount") or 0),
        "probed": int(s.get("probedCandidateCount") or 0),
        "queued": int(s.get("queuedCandidateCount") or 0),
        "failed_probes": int(s.get("failedProbeCount") or 0),
        "probe_misses": int(s.get("probeMissCount") or 0),
        "deferred": int(s.get("discoverableButDeferredCount") or 0),
    }


def main() -> int:
    import argparse
    p = argparse.ArgumentParser(description="Benchmark discovery probe concurrency.")
    p.add_argument("--rows", type=int, default=35, help="Max sheet rows (candidate cap)")
    p.add_argument("--timeout", type=int, default=12, help="Request timeout per URL (s)")
    p.add_argument("--presets", nargs="*", default=list(PRESETS), help="Preset names to run")
    args = p.parse_args()

    from tests.helpers.temp_paths import workspace_tmpdir

    rows = max(1, args.rows)
    timeout_s = max(5, args.timeout)
    presets_to_run = [x for x in args.presets if x in PRESETS]
    if not presets_to_run:
        presets_to_run = list(PRESETS)

    print(f"Discovery probe benchmark (sheet_max_rows={rows}, timeout_s={timeout_s})")
    print("Presets:", ", ".join(presets_to_run))
    print()

    results = []
    for name in presets_to_run:
        total, static, provider, tt = PRESETS[name]
        # Fresh temp dir per preset so every run sees the same candidate set (no prior pending).
        with workspace_tmpdir("discovery-bench") as root:
            print(f"  Running preset: {name} (total={total}, static={static}) ...", flush=True)
            try:
                r = run_one_preset(name, total, static, provider, tt, rows, timeout_s, root)
                results.append(r)
                print(f"    -> {r['wall_seconds']}s, probed={r['probed']}, failed={r['failed_probes']}, queued={r['queued']}")
            except Exception as e:
                print(f"    -> ERROR: {e}")
                results.append({
                    "preset": name,
                    "total": total,
                    "static": static,
                    "provider": provider,
                    "teamtailor": tt,
                    "wall_seconds": None,
                    "found": None,
                    "probed": None,
                    "queued": None,
                    "failed_probes": None,
                    "probe_misses": None,
                    "deferred": None,
                    "error": str(e),
                })

    # Summary table
    print()
    print("Results:")
    print("-" * 100)
    h = ("preset", "total", "static", "wall_s", "probed", "failed", "queued", "deferred")
    print(f"  {h[0]:<14} {h[1]:>6} {h[2]:>6} {h[3]:>8} {h[4]:>7} {h[5]:>6} {h[6]:>6} {h[7]:>8}")
    print("-" * 100)
    for r in results:
        if r.get("error"):
            print(f"  {r['preset']:<14} ERROR: {r['error'][:50]}")
            continue
        print(
            f"  {r['preset']:<14} {r['total']:>6} {r['static']:>6} "
            f"{r['wall_seconds']:>8.1f} {r['probed']:>7} {r['failed_probes']:>6} {r['queued']:>6} {r['deferred']:>8}"
        )
    print("-" * 100)

    # Recommendation: prefer preset with lowest wall time while failed_probes is not much worse than conservative
    valid = [r for r in results if not r.get("error") and r.get("wall_seconds") is not None]
    if valid:
        by_time = sorted(valid, key=lambda x: (x["wall_seconds"], -x["queued"]))
        best = by_time[0]
        print()
        print("Recommendation: Code defaults use 'moderate' (25/10/25/15). For faster runs try 'high' or 'aggressive'.")
        print(f"  Fastest this run: {best['preset']} ({best['wall_seconds']}s, failed={best['failed_probes']}, queued={best['queued']})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
