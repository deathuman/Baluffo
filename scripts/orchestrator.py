#!/usr/bin/env python3
import argparse
import hashlib
import json
import locale
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pydantic import ValidationError as PydanticValidationError

from src.core.schemas import ManifestSchema

# Colors
C_GREEN = "\033[92m"
C_RED = "\033[91m"
C_RESET = "\033[0m"


def _safe_print(text: str) -> None:
    try:
        print(text)
        return
    except UnicodeEncodeError:
        print(
            text.encode(sys.stdout.encoding or "utf-8", errors="replace").decode(
                sys.stdout.encoding or "utf-8", errors="replace"
            )
        )


def color_msg(msg: str, color_code: str) -> str:
    if sys.stdout.isatty():
        return f"{color_code}{msg}{C_RESET}"
    return msg


# Paths
SRC = ROOT / "src"
OUT = ROOT / "_out"
RUNS = OUT / "runs"
LATEST = OUT / "latest"
STATE = OUT / ".state"
MANIFEST_PATH = OUT / "LATEST_MANIFEST.json"


def get_src_hash() -> str:
    """Calculates a single hash for all files in src/."""
    hasher = hashlib.sha256()
    # Also include the build scripts themselves in the hash
    scripts_dir = ROOT / "scripts"
    paths = list(SRC.rglob("*")) + list(scripts_dir.glob("build_*.py"))

    for path in sorted(paths):
        if path.is_file() and "__pycache__" not in str(path) and ".pytest_cache" not in str(path):
            try:
                hasher.update(path.read_bytes())
            except OSError:
                pass
    return hasher.hexdigest()


def ensure_dirs():
    """Ensures necessary directories exist."""
    RUNS.mkdir(parents=True, exist_ok=True)
    STATE.mkdir(parents=True, exist_ok=True)


def rotate_history(max_runs: int = 5):
    """Keeps only the last N runs in _out/runs/."""
    try:
        run_items = sorted(
            [d for d in RUNS.iterdir() if d.is_dir()], key=lambda x: x.stat().st_mtime
        )
        if len(run_items) > max_runs:
            for old_run in run_items[0 : len(run_items) - max_runs]:
                print(f">>> Rotating: Purging old run {old_run.name}")
                shutil.rmtree(old_run, ignore_errors=True)
    except Exception as e:
        print(f"!!! History rotation failed: {e}")


def sync_latest(run_dir: Path):
    """Syncs the latest run directory to _out/latest (physical copy)."""
    try:
        if LATEST.exists():
            shutil.rmtree(LATEST)
        shutil.copytree(run_dir, LATEST)
        print(">>> Latest run mirrored to: _out/latest/")
    except Exception as e:
        print(f"!!! Latest sync failed: {e}")


def update_manifest(
    status: str, summary: str, artifacts: dict[str, Any] | None = None, run_id: str | None = None
):
    """Updates the machine-readable manifest HUD. Validates shape with ManifestSchema before writing."""
    manifest = {
        "last_run_id": run_id or "",
        "last_run_time": time.strftime("%Y-%m-%d %H:%M:%S"),
        "status": status,
        "summary": summary,
        "src_hash": get_src_hash(),
        "artifacts_root": f"_out/runs/{run_id}" if run_id else "",
        "artifacts": artifacts or {},
    }
    manifest_model = ManifestSchema.model_validate(manifest)
    MANIFEST_PATH.write_text(manifest_model.model_dump_json(indent=2), encoding="utf-8")


def run_proc(command: list[str], name: str, allow_stream: bool = False) -> tuple[bool, str]:
    _safe_print(f">>> [{name}] Running: {' '.join(command)}")
    full_output = []
    try:
        # Use shell=True for npm on Windows to avoid FileNotFoundError
        use_shell = command[0] == "npm" and os.name == "nt"

        # We use bufsize=0 to avoid buffering for real-time dots
        process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding=locale.getpreferredencoding(False) or "utf-8",
            errors="replace",
            shell=use_shell,
            bufsize=1,  # Line buffered
            universal_newlines=True,
        )

        # Read character by character to catch dots immediately
        # Note: This can be tricky with different line endings and buffering.
        # We'll read line by line but look for dots at the start of lines or specific markers.
        # Actually, let's try reading character by character from the raw stream if possible.

        progress_chars = {".", "F", "E", "s", "x", "X", "!"}

        while True:
            if process.stdout is None:
                break
            char = process.stdout.read(1)
            if not char and process.poll() is not None:
                break
            if char:
                full_output.append(char)
                if allow_stream and char in progress_chars:
                    print(char, end="", flush=True)

        process.wait()
        output_str = "".join(full_output)

        # Ensure we are on a new line after the dots
        _safe_print("")

        if process.returncode != 0:
            _safe_print(color_msg(f"X [{name}] Failed with exit code {process.returncode}", C_RED))
            if output_str.strip():
                for line in output_str.strip().splitlines()[-30:]:
                    _safe_print(f"    {line}")
            return False, output_str
        _safe_print(color_msg(f"OK [{name}] Success", C_GREEN))
        return True, output_str
    except Exception as e:
        _safe_print(color_msg(f"X [{name}] Subprocess error: {e}", C_RED))
        return False, str(e)


def build(
    args: argparse.Namespace, run_dir: Path | None = None, is_verify: bool = False
) -> tuple[bool, Path | None]:
    """Orchestrates the build process with skipping logic."""
    ensure_dirs()
    current_hash = get_src_hash()
    hash_file = STATE / "src.hash"

    # Check if we can skip the build
    if not args.force and hash_file.exists() and hash_file.read_text().strip() == current_hash:
        print(">>> Build SKIPPED: Code hash matches last successful build.")
        if MANIFEST_PATH.exists():
            try:
                manifest = json.loads(MANIFEST_PATH.read_text())
                manifest_model = ManifestSchema.model_validate(manifest)
                if manifest_model.status == "success" and manifest_model.artifacts_root:
                    prev_run = ROOT / manifest_model.artifacts_root
                    if prev_run.exists():
                        return True, prev_run
            except (json.JSONDecodeError, PydanticValidationError, TypeError):
                pass
        # If manifest missing or invalid but hash matched, we still rebuild to be safe

    if not run_dir:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        run_dir = RUNS / timestamp
        run_dir.mkdir(parents=True, exist_ok=True)

    build_dir = run_dir / "build"

    # 1. Build Ship Bundle
    ship_out = build_dir / "ship"
    ok, log = run_proc(
        [sys.executable, "scripts/build_ship_bundle.py", "--output-dir", str(ship_out)],
        "ShipBundle",
    )
    if not ok:
        update_manifest("failure", f"Ship bundle build failed: {log[:200]}", run_id=run_dir.name)
        return False, run_dir

    # 2. Build Portable EXE
    exe_out = build_dir / "portable"
    ok, log = run_proc(
        [
            sys.executable,
            "scripts/build_portable_exe.py",
            "--output-dir",
            str(exe_out),
            "--skip-zip",
        ],
        "PortableEXE",
    )
    if not ok:
        update_manifest("failure", f"Portable EXE build failed: {log[:200]}", run_id=run_dir.name)
        return False, run_dir

    hash_file.write_text(current_hash)

    if not is_verify:
        sync_latest(run_dir)
        rotate_history()
        update_manifest(
            "success",
            "Build completed successfully",
            artifacts={
                "exe": "build/portable/Baluffo.exe",
                "ship": "build/ship",
                "py_tests_status": "not_run",
                "node_tests_status": "not_run",
                "py_tests_ok": False,
                "node_tests_ok": False,
            },
            run_id=run_dir.name,
        )

    return True, run_dir


def verify(args: argparse.Namespace):
    """Full verification: Build + Unit + Smoke."""
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    run_dir = RUNS / timestamp
    run_dir.mkdir(parents=True, exist_ok=True)

    # 0. Pre-commit gate
    ok_precommit, _log_precommit = run_proc(
        ["npm", "run", "lint:precommit"], "PreCommit", allow_stream=True
    )

    # Run build (might return a previous successful run_dir if skipped)
    ok, effective_build_run_dir = build(args, run_dir=run_dir, is_verify=True)
    if not ok:
        return

    test_dir = run_dir / "test"

    # 1. Python Unit Tests
    ok_py, log_py = run_proc(["npm", "run", "test:py:extended"], "PyTests", allow_stream=True)

    # 2. Frontend Unit Tests (Node)
    ok_node, log_node = run_proc(["npm", "run", "test:unit"], "NodeTests", allow_stream=True)

    # 3. Packaged Smoke Tests
    smoke_dir = test_dir / "smoke"
    # Important: effective_build_run_dir handles the case where build was skipped
    exe_path = (effective_build_run_dir or run_dir) / "build" / "portable" / "Baluffo.exe"

    if not exe_path.exists():
        print(f"!!! SmokeTest Failed: Executable not found at {exe_path}")
        ok_smoke, log_smoke = False, f"Executable missing: {exe_path}"
    else:
        ok_smoke, log_smoke = run_proc(
            [
                sys.executable,
                "src/packaged_desktop_smoke.py",
                "--exe-path",
                str(exe_path),
                "--artifacts-dir",
                str(smoke_dir),
                "--report-path",
                str(smoke_dir / "report.json"),
            ],
            "SmokeTest",
            allow_stream=True,
        )

    total_ok = ok_precommit and ok_py and ok_node and ok_smoke
    status = "success" if total_ok else "failure"
    summary = "Verification PASSED" if total_ok else "Verification FAILED"

    precommit_status = "passed" if ok_precommit else "failed"
    py_status = "passed" if ok_py else "failed"
    node_status = "passed" if ok_node else "failed"
    artifacts = {
        "exe": f"{Path(effective_build_run_dir or run_dir).relative_to(ROOT)}/build/portable/Baluffo.exe",
        "smoke_report": "test/smoke/report.json",
        "precommit_status": precommit_status,
        "py_tests_status": py_status,
        "node_tests_status": node_status,
        "precommit_ok": precommit_status == "passed",
        "py_tests_ok": py_status == "passed",
        "node_tests_ok": node_status == "passed",
    }

    sync_latest(run_dir)
    rotate_history()
    update_manifest(status, summary, artifacts=artifacts, run_id=run_dir.name)

    final_color = C_GREEN if total_ok else C_RED
    indicator = "OK" if total_ok else "X"
    _safe_print(color_msg(f"\n{indicator} {summary} (ID: {run_dir.name})", final_color))


def main():
    parser = argparse.ArgumentParser(description="Baluffo AI-Native Orchestrator")
    subparsers = parser.add_subparsers(dest="command")

    build_parser = subparsers.add_parser("build", help="Orchestrate build")
    build_parser.add_argument(
        "--force", action="store_true", help="Force build even if hash matches"
    )

    verify_parser = subparsers.add_parser("verify", help="Full verification (Build + Test)")
    verify_parser.add_argument(
        "--force", action="store_true", help="Force build even if hash matches"
    )
    verify_parser.add_argument("--full", action="store_true", help="Always run full smoke tests")

    args = parser.parse_args()

    if args.command == "build":
        build(args)
    elif args.command == "verify":
        verify(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
