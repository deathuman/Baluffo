#!/usr/bin/env python3
"""Promote a published container image and prove the host now runs it.

The Umbrel channel updates by image tag, not by git tag, so a host can sit one
release behind while every local check is green. This command pulls the compose
project, recreates it, then polls ``/ops/health`` until the running
``appVersion`` and ``build.commit`` match the image that was just pulled.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from collections.abc import Callable, Sequence
from pathlib import Path
from typing import Any
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[2]


def build_commands(compose_dir: Path) -> list[list[str]]:
    """The two compose commands, ordered; the project directory makes cwd irrelevant."""
    prefix = ["docker", "compose", "--project-directory", str(compose_dir)]
    return [prefix + ["pull"], prefix + ["up", "-d"]]


def health_matches(payload: dict[str, Any], *, version: str, commit: str) -> bool:
    if str(payload.get("appVersion") or "") != version:
        return False
    if not commit:
        return True
    build = payload.get("build")
    running = str(build.get("commit") or "") if isinstance(build, dict) else ""
    return running == commit


def fetch_health(base_url: str, *, timeout_s: float = 5.0) -> dict[str, Any]:
    url = base_url.rstrip("/") + "/ops/health"
    with urlopen(url, timeout=timeout_s) as response:  # noqa: S310 - caller-supplied host
        payload = json.loads(response.read().decode("utf-8"))
    return payload if isinstance(payload, dict) else {}


def promote(
    *,
    compose_dir: Path,
    base_url: str,
    version: str,
    commit: str,
    timeout_s: float,
    dry_run: bool,
    run: Callable[[Sequence[str]], int] = lambda command: (
        subprocess.run(list(command), check=False).returncode
    ),
    health: Callable[[], dict[str, Any]] | None = None,
    sleep: Callable[[float], None] = time.sleep,
    now: Callable[[], float] = time.monotonic,
) -> int:
    if health is None:
        health = lambda: fetch_health(base_url)  # noqa: E731 - local default closes over the arg
    commands = build_commands(compose_dir)
    for command in commands:
        print("+", " ".join(command))
    if dry_run:
        return 0
    for command in commands:
        if run(command) != 0:
            print(f"command failed: {' '.join(command)}", file=sys.stderr)
            return 1

    deadline = now() + max(1.0, timeout_s)
    last: dict[str, Any] = {}
    while now() < deadline:
        try:
            last = health()
        except (OSError, ValueError, RuntimeError) as exc:
            print(f"health probe failed: {exc}", file=sys.stderr)
            last = {}
        if health_matches(last, version=version, commit=commit):
            print(f"running {version} ({commit or 'commit unchecked'})")
            return 0
        sleep(2.0)

    print(
        f"host did not reach {version} ({commit or 'commit unchecked'}) before the deadline; "
        f"last health: appVersion={last.get('appVersion')!r} build={last.get('build')!r}",
        file=sys.stderr,
    )
    return 1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--compose-dir", default=str(ROOT / "deathuman-baluffo"), help="Compose project directory."
    )
    parser.add_argument("--base-url", default="http://127.0.0.1:8877", help="Running app origin.")
    parser.add_argument("--expected-version", required=True, help="Version the image should run.")
    parser.add_argument("--expected-commit", default="", help="Full commit hash to verify.")
    parser.add_argument(
        "--timeout", type=float, default=120.0, help="Health wait budget in seconds."
    )
    parser.add_argument("--dry-run", action="store_true", help="Print the commands and stop.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    return promote(
        compose_dir=Path(args.compose_dir).expanduser().resolve(),
        base_url=str(args.base_url),
        version=str(args.expected_version).strip(),
        commit=str(args.expected_commit).strip(),
        timeout_s=float(args.timeout),
        dry_run=bool(args.dry_run),
    )


if __name__ == "__main__":
    raise SystemExit(main())
