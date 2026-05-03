from __future__ import annotations

import argparse
import os
import subprocess
import sys
from pathlib import Path
from shutil import which

ROOT = Path(__file__).resolve().parents[1]
HOOK_PATH = ROOT / ".githooks" / "pre-push"


def _build_env(args: argparse.Namespace) -> dict[str, str]:
    env = os.environ.copy()
    if args.full_ci:
        env["PRE_PUSH_FULL_CI"] = "1"
    if args.warm_hooks:
        env["PRE_PUSH_WARM_HOOKS"] = "1"
    if args.timing_log:
        env["PRE_PUSH_TIMING_LOG"] = "1"
    if args.timing_log_path:
        env["PRE_PUSH_TIMING_LOG_PATH"] = args.timing_log_path
    return env


def _resolve_sh() -> str:
    sh_path = which("sh")
    if sh_path:
        return sh_path
    git_sh = Path("C:/Program Files/Git/bin/sh.exe")
    if git_sh.exists():
        return str(git_sh)
    print("ERROR: could not locate `sh` for running the tracked pre-push hook.", file=sys.stderr)
    print("Install Git for Windows or make `sh` available on PATH.", file=sys.stderr)
    raise SystemExit(1)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run the tracked pre-push hook with synthetic refs."
    )
    parser.add_argument("--remote-name", default="origin")
    parser.add_argument("--remote-url", default="origin")
    parser.add_argument("--local-ref", default="refs/heads/main")
    parser.add_argument("--local-sha", default="HEAD")
    parser.add_argument("--remote-ref", default="refs/heads/main")
    parser.add_argument("--remote-sha", default="0000000000000000000000000000000000000000")
    parser.add_argument("--full-ci", action="store_true")
    parser.add_argument("--warm-hooks", action="store_true")
    parser.add_argument("--timing-log", action="store_true")
    parser.add_argument("--timing-log-path")
    args = parser.parse_args()

    completed = subprocess.run(
        [sys.executable, "-m", "scripts.check_python_version"],
        cwd=ROOT,
        check=False,
    )
    if completed.returncode != 0:
        return completed.returncode

    hook_input = f"{args.local_ref} {args.local_sha} {args.remote_ref} {args.remote_sha}\n"
    hook_completed = subprocess.run(
        [_resolve_sh(), str(HOOK_PATH), args.remote_name, args.remote_url],
        cwd=ROOT,
        env=_build_env(args),
        input=hook_input,
        text=True,
        check=False,
    )
    return hook_completed.returncode


if __name__ == "__main__":
    raise SystemExit(main())
