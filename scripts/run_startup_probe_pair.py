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


def startup_pair_paths(
    *,
    artifact_root: Path = PAIR_ARTIFACT_ROOT,
    run_token: str = "",
) -> dict[str, Path]:
    token = str(run_token or generate_pair_run_token()).strip()
    run_root = Path(artifact_root) / token
    return {
        "runRoot": run_root,
        "coldArtifactsDir": run_root / "cold",
        "warmArtifactsDir": run_root / "warm",
        "coldReportPath": run_root / "cold-report.json",
        "warmReportPath": run_root / "warm-report.json",
    }


def cold_startup_probe_args(paths: dict[str, Path], *, runtime_timeout_s: float) -> list[str]:
    return [
        "--rebuild",
        "--startup-probe",
        "--profile-only",
        "--profile-mode",
        "cold",
        "--runtime-timeout",
        str(runtime_timeout_s),
        "--artifacts-dir",
        str(paths["coldArtifactsDir"]),
        "--report-path",
        str(paths["coldReportPath"]),
    ]


def warm_startup_probe_args(
    paths: dict[str, Path],
    *,
    reused_exe: Path,
    runtime_timeout_s: float,
) -> list[str]:
    return [
        "--exe-path",
        str(reused_exe),
        "--startup-probe",
        "--profile-only",
        "--profile-mode",
        "warm",
        "--runtime-timeout",
        str(runtime_timeout_s),
        "--artifacts-dir",
        str(paths["warmArtifactsDir"]),
        "--report-path",
        str(paths["warmReportPath"]),
    ]


def packaged_probe_command(args: list[str]) -> list[str]:
    return [sys.executable, str(PACKAGED_SMOKE_SCRIPT), *args]


def run_packaged_probe(args: list[str]) -> subprocess.CompletedProcess[Any]:
    command = packaged_probe_command(args)
    return subprocess.run(command, cwd=ROOT, check=False)


def write_startup_pair_summary(
    path: Path,
    *,
    paths: dict[str, Path],
    cold_exit_code: int,
    warm_exit_code: int,
    reused_exe: Path | None,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ok": int(cold_exit_code or 0) == 0 and int(warm_exit_code or 0) == 0,
        "runRoot": str(paths["runRoot"]),
        "coldReportPath": str(paths["coldReportPath"]),
        "warmReportPath": str(paths["warmReportPath"]),
        "coldArtifactsDir": str(paths["coldArtifactsDir"]),
        "warmArtifactsDir": str(paths["warmArtifactsDir"]),
        "reusedExe": str(reused_exe or ""),
        "coldExitCode": int(cold_exit_code or 0),
        "warmExitCode": int(warm_exit_code or 0),
    }
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def run_startup_probe_pair(
    *,
    runtime_timeout_s: float = DEFAULT_RUNTIME_TIMEOUT_S,
    artifact_root: Path = PAIR_ARTIFACT_ROOT,
    summary_path: Path | None = None,
) -> int:
    paths = startup_pair_paths(artifact_root=artifact_root)
    run_root = paths["runRoot"]
    run_root.mkdir(parents=True, exist_ok=True)
    prune_pair_artifacts(artifact_root, current_run_dir=run_root)

    print("Running cold startup probe with a single rebuild...")
    cold_result = run_packaged_probe(
        cold_startup_probe_args(paths, runtime_timeout_s=runtime_timeout_s)
    )

    reused_exe: Path | None = None
    try:
        reused_exe = resolve_built_exe(paths["coldReportPath"], paths["coldArtifactsDir"])
    except RuntimeError as exc:
        print(str(exc), file=sys.stderr)
        if summary_path is not None:
            write_startup_pair_summary(
                summary_path,
                paths=paths,
                cold_exit_code=int(cold_result.returncode or 1),
                warm_exit_code=1,
                reused_exe=None,
            )
        return int(cold_result.returncode or 1)

    print(f"Reusing packaged exe for warm startup probe: {reused_exe}")
    warm_result = run_packaged_probe(
        warm_startup_probe_args(
            paths,
            reused_exe=reused_exe,
            runtime_timeout_s=runtime_timeout_s,
        )
    )

    if summary_path is not None:
        write_startup_pair_summary(
            summary_path,
            paths=paths,
            cold_exit_code=int(cold_result.returncode or 0),
            warm_exit_code=int(warm_result.returncode or 0),
            reused_exe=reused_exe,
        )
    print(f"Cold report: {paths['coldReportPath']}")
    print(f"Warm report: {paths['warmReportPath']}")
    return 0 if cold_result.returncode == 0 and warm_result.returncode == 0 else 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run cold and warm packaged startup probes while reusing a single rebuild."
    )
    parser.add_argument("--runtime-timeout", type=float, default=DEFAULT_RUNTIME_TIMEOUT_S)
    parser.add_argument("--artifact-root", default=str(PAIR_ARTIFACT_ROOT))
    parser.add_argument("--summary-path", default="")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary_path = Path(str(args.summary_path)) if str(args.summary_path or "").strip() else None
    return run_startup_probe_pair(
        runtime_timeout_s=float(args.runtime_timeout),
        artifact_root=Path(str(args.artifact_root)),
        summary_path=summary_path,
    )


if __name__ == "__main__":
    raise SystemExit(main())
