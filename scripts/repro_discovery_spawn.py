#!/usr/bin/env python3
"""Reproduce admin-bridge style discovery spawn and detect 'stuck on Initializing'.

This mirrors TaskLaunchApi.run_background_script env for discovery:
  BALUFFO_DATA_DIR, BALUFFO_DISCOVERY_RUN_ID, BALUFFO_DISCOVERY_STARTED_AT,
  BALUFFO_DISCOVERY_REPORT_PATH, BALUFFO_DISCOVERY_LOG_PATH

Usage (dev child, from repo root):
  python scripts/repro_discovery_spawn.py --root . --data-dir data

Usage (packaged portable, Windows):
  python scripts/repro_discovery_spawn.py ^
    --exe _out/runs/<run>/build/portable/Baluffo.exe ^
    --root _out/runs/<run>/build/portable/ship/app/versions/<ver> ^
    --data-dir data

Exit codes: 0 = report advanced beyond Initializing within timeout; 2 = stuck; 1 = error.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
INITIALIZING = "initializing scan"


def _read_report(path: Path) -> dict[str, Any]:
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return raw if isinstance(raw, dict) else {}


def _phase_snapshot(report: dict[str, Any]) -> tuple[str, str, str]:
    summ = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    tp = report.get("taskProgress") if isinstance(report.get("taskProgress"), dict) else {}
    life = {}
    rt = report.get("runtime")
    if isinstance(rt, dict):
        life = rt.get("lifecycle") if isinstance(rt.get("lifecycle"), dict) else {}
    phase_label = str(tp.get("phaseLabel") or summ.get("phaseLabel") or "").strip()
    phase_key = str(tp.get("phaseKey") or summ.get("phaseKey") or "").strip()
    heartbeat = str(life.get("heartbeatAt") or "").strip()
    return phase_label, phase_key, heartbeat


def _seed_report(*, report_path: Path, run_id: str, started_at: str) -> None:
    from src.contracts import SCHEMA_VERSION
    from src.source_discovery.reporting import build_discovery_task_progress

    summary = {
        "foundEndpointCount": 0,
        "probedCandidateCount": 0,
        "queuedCandidateCount": 0,
        "phase": "starting",
        "phaseKey": "starting",
        "phaseLabel": "Initializing scan",
    }
    payload = {
        "schemaVersion": SCHEMA_VERSION,
        "runId": run_id,
        "mode": "dynamic",
        "startedAt": started_at,
        "finishedAt": "",
        "summary": summary,
        "runtime": {
            "lifecycle": {
                "owner": "discovery_report",
                "heartbeatAt": started_at,
            },
        },
        "taskProgress": build_discovery_task_progress(summary=summary, finished=False),
        "candidates": [],
        "failures": [],
        "topFailures": [],
        "outputs": {},
    }
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--root",
        type=Path,
        default=ROOT,
        help="Repo root (dev) or ship active version dir containing src/ (portable)",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=ROOT / "data",
        help="Directory to copy registry JSON from (source-discovery-config, active, pending, rejected)",
    )
    parser.add_argument(
        "--exe",
        type=Path,
        default=None,
        help="Baluffo.exe (frozen). If omitted, uses current Python on src/source_discovery.py.",
    )
    parser.add_argument("--timeout", type=float, default=45.0, help="Seconds to wait for progress")
    parser.add_argument("--poll", type=float, default=0.25, help="Poll interval seconds")
    parser.add_argument(
        "--quick",
        action="store_true",
        help="Shorter discovery: --top 3 --timeout 45 --no-web-search",
    )
    args = parser.parse_args()
    if str(ROOT) not in sys.path:
        sys.path.insert(0, str(ROOT))

    work = Path(tempfile.mkdtemp(prefix="baluffo_repro_"))
    try:
        for name in (
            "source-registry-active.json",
            "source-registry-pending.json",
            "source-registry-rejected.json",
            "source-discovery-config.json",
        ):
            src = args.data_dir / name
            dst = work / name
            if src.exists():
                shutil.copy2(src, dst)
            elif name.endswith("pending.json") or name.endswith("rejected.json"):
                dst.write_text("[]", encoding="utf-8")
            elif name.endswith("config.json"):
                dst.write_text("{}", encoding="utf-8")

        run_id = f"repro_{uuid.uuid4().hex[:12]}"
        started_at = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime()) + "Z"
        report_path = work / "source-discovery-report.json"
        log_path = work / "source-discovery.log"

        _seed_report(report_path=report_path, run_id=run_id, started_at=started_at)
        snap0 = _phase_snapshot(_read_report(report_path))
        hb0 = snap0[2]

        child_env = os.environ.copy()
        child_env["BALUFFO_DATA_DIR"] = str(work.resolve())
        child_env["PYTHONUNBUFFERED"] = "1"
        child_env["BALUFFO_DISCOVERY_RUN_ID"] = run_id
        child_env["BALUFFO_DISCOVERY_STARTED_AT"] = started_at
        child_env["BALUFFO_DISCOVERY_REPORT_PATH"] = str(report_path.resolve())
        child_env["BALUFFO_DISCOVERY_LOG_PATH"] = str(log_path.resolve())

        root = args.root.resolve()
        top = "3" if args.quick else "5"
        child_timeout = "120" if args.quick else "90"
        disc_args = [
            "--mode",
            "dynamic",
            "--preset",
            "default",
            "--top",
            top,
            "--timeout",
            child_timeout,
            "--no-web-search",
        ]

        if args.exe:
            cmd = [
                str(args.exe.resolve()),
                "__child_script__",
                "--root",
                str(root),
                "--script",
                "source_discovery.py",
                "--",
                *disc_args,
            ]
        else:
            script = root / "src" / "source_discovery.py"
            if not script.exists():
                print(f"error: missing {script}", file=sys.stderr)
                return 1
            cmd = [sys.executable, "-u", str(script), *disc_args]

        print("workdir:", work)
        print("cmd:", " ".join(cmd))
        proc = subprocess.Popen(
            cmd,
            cwd=str(root),
            env=child_env,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        deadline = time.monotonic() + float(args.timeout)
        advanced = False
        last_snap: tuple[str, str, str] = snap0
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                break
            rep = _read_report(report_path)
            last_snap = _phase_snapshot(rep)
            label_l, key, hb = last_snap
            label_l_norm = label_l.strip().lower()
            progressed = hb and hb != hb0
            past_init = label_l_norm and label_l_norm != INITIALIZING
            past_starting = key.strip().lower() not in {"", "starting"}
            if progressed or past_init or past_starting:
                advanced = True
                break
            time.sleep(max(0.05, float(args.poll)))

        rc = proc.poll()
        if rc is None:
            proc.terminate()
            try:
                rc = proc.wait(timeout=8)
            except subprocess.TimeoutExpired:
                proc.kill()
                try:
                    rc = proc.wait(timeout=3)
                except subprocess.TimeoutExpired:
                    rc = -1

        print("exit:", rc)
        print("last_phase:", last_snap)
        print("advanced_beyond_initializing:", advanced)
        if log_path.exists():
            tail = log_path.read_text(encoding="utf-8", errors="replace").splitlines()[-12:]
            print("--- log tail ---")
            print("\n".join(tail))

        if rc not in (None, 0) and not advanced:
            print("error: child exited early without updating report", file=sys.stderr)
            return 1
        if not advanced:
            print(
                f"error: report stayed on initial phase for {args.timeout}s "
                f"(heartbeat advanced={hb0 != last_snap[2]})",
                file=sys.stderr,
            )
            return 2
        return 0
    finally:
        try:
            shutil.rmtree(work, ignore_errors=True)
        except OSError:
            pass


if __name__ == "__main__":
    raise SystemExit(main())
