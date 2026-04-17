#!/usr/bin/env python3
"""Run cold and warm packaged startup probes while reusing a single rebuilt exe."""

from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PACKAGED_SMOKE_SCRIPT = ROOT / "src" / "packaged_desktop_smoke.py"
PAIR_ARTIFACT_ROOT = ROOT / ".tmp" / "packaged-desktop-smoke-pair"
PAIR_RETENTION_RUNS = 2
DEFAULT_RUNTIME_TIMEOUT_S = 60.0


def generate_pair_run_token(*, now: datetime | None = None) -> str:
    resolved_now = now if isinstance(now, datetime) else datetime.now(UTC)
    return resolved_now.strftime("%Y%m%d-%H%M%S-%f")


def prune_pair_artifacts(root: Path, *, current_run_dir: Path) -> None:
    artifact_root = Path(root).expanduser().resolve()
    current = Path(current_run_dir).expanduser().resolve()
    artifact_root.mkdir(parents=True, exist_ok=True)
    run_dirs = [path for path in artifact_root.iterdir() if path.is_dir()]
    run_dirs.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    keep_limit = max(1, int(PAIR_RETENTION_RUNS))
    kept = 0
    for run_dir in run_dirs:
        resolved = run_dir.resolve()
        if resolved == current:
            continue
        if kept < keep_limit - 1:
            kept += 1
            continue
        shutil.rmtree(run_dir, ignore_errors=True)


def read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def resolve_built_exe(cold_report_path: Path, cold_artifacts_dir: Path) -> Path:
    if cold_report_path.exists():
        report = read_json(cold_report_path)
        exe_token = str(report.get("exePath") or "").strip()
        if exe_token:
            exe_path = Path(exe_token).expanduser().resolve()
            if exe_path.is_file():
                return exe_path
    fallback = cold_artifacts_dir / "portable-build" / "Baluffo.exe"
    if fallback.is_file():
        return fallback.resolve()
    raise RuntimeError(
        f"Cold startup probe did not produce a reusable portable exe under {cold_artifacts_dir}."
    )


def run_packaged_probe(args: list[str]) -> subprocess.CompletedProcess[Any]:
    command = [sys.executable, str(PACKAGED_SMOKE_SCRIPT), *args]
    return subprocess.run(command, cwd=ROOT, check=False)


def run_startup_probe_pair(*, runtime_timeout_s: float = DEFAULT_RUNTIME_TIMEOUT_S) -> int:
    run_root = PAIR_ARTIFACT_ROOT / generate_pair_run_token()
    cold_artifacts_dir = run_root / "cold"
    warm_artifacts_dir = run_root / "warm"
    cold_report_path = run_root / "cold-report.json"
    warm_report_path = run_root / "warm-report.json"
    run_root.mkdir(parents=True, exist_ok=True)
    prune_pair_artifacts(PAIR_ARTIFACT_ROOT, current_run_dir=run_root)

    print("Running cold startup probe with a single rebuild...")
    cold_result = run_packaged_probe(
        [
            "--rebuild",
            "--startup-probe",
            "--profile-only",
            "--profile-mode",
            "cold",
            "--runtime-timeout",
            str(runtime_timeout_s),
            "--artifacts-dir",
            str(cold_artifacts_dir),
            "--report-path",
            str(cold_report_path),
        ]
    )

    try:
        reused_exe = resolve_built_exe(cold_report_path, cold_artifacts_dir)
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        return int(cold_result.returncode or 1)

    print(f"Reusing packaged exe for warm startup probe: {reused_exe}")
    warm_result = run_packaged_probe(
        [
            "--exe-path",
            str(reused_exe),
            "--startup-probe",
            "--profile-only",
            "--profile-mode",
            "warm",
            "--runtime-timeout",
            str(runtime_timeout_s),
            "--artifacts-dir",
            str(warm_artifacts_dir),
            "--report-path",
            str(warm_report_path),
        ]
    )

    print(f"Cold report: {cold_report_path}")
    print(f"Warm report: {warm_report_path}")
    return 0 if cold_result.returncode == 0 and warm_result.returncode == 0 else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run cold and warm packaged startup probes while reusing a single rebuild."
    )
    parser.add_argument("--runtime-timeout", type=float, default=DEFAULT_RUNTIME_TIMEOUT_S)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return run_startup_probe_pair(runtime_timeout_s=float(args.runtime_timeout))


if __name__ == "__main__":
    raise SystemExit(main())
