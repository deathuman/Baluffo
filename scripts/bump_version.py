#!/usr/bin/env python3
"""Bump the Baluffo version across every release artifact.

Keeps the five version-bearing files in sync so a release bump is one command
instead of five hand-edits (the exact drift that caused the v0.2.139 release
notes mismatch):

  - src/app_version.py
  - deathuman-baluffo/umbrel-app.yml
  - deathuman-baluffo/docker-compose.yml
  - docs/CHANGELOG.md  (moves the [Unreleased] section into a dated release)
  - dist/baluffo-ship/app/current.txt

It also regenerates release-notes.md from the new changelog section via
scripts/extract_release_notes.py so the package artifact matches.

Usage:
  python scripts/bump_version.py 0.2.140
  python scripts/bump_version.py 0.2.140 --date 2026-08-26
  python scripts/bump_version.py 0.2.140 --dry-run
"""

from __future__ import annotations

import argparse
import datetime as _datetime
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.python_version_guard import ensure_required_python

VERSION_RE = re.compile(r"^\d+\.\d+\.\d+$")


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def bump_app_version(text: str, version: str) -> str:
    if f'APP_VERSION = "{version}"' in text:
        return text
    return re.sub(r'APP_VERSION = "\d+\.\d+\.\d+"', f'APP_VERSION = "{version}"', text, count=1)


def bump_umbrel_app(text: str, version: str) -> str:
    return re.sub(
        r'^(version: )"\d+\.\d+\.\d+"', rf'\g<1>"{version}"', text, count=1, flags=re.MULTILINE
    )


def bump_docker_compose(text: str, version: str) -> str:
    return re.sub(
        r"(ghcr\.io/deathuman/baluffo:)\d+\.\d+\.\d+",
        rf"\g<1>{version}",
        text,
        count=1,
    )


def bump_changelog(text: str, version: str, date: str) -> str:
    lines = text.splitlines()
    unreleased_idx = next(
        (i for i, line in enumerate(lines) if line.strip().startswith("## [Unreleased]")), None
    )
    if unreleased_idx is None:
        raise ValueError("docs/CHANGELOG.md has no '## [Unreleased]' section.")
    next_idx = next(
        (i for i in range(unreleased_idx + 1, len(lines)) if lines[i].startswith("## [")),
        len(lines),
    )
    if next_idx == unreleased_idx + 1:
        raise ValueError("docs/CHANGELOG.md '## [Unreleased]' has no following release section.")

    preamble = "\n".join(lines[: unreleased_idx + 1])
    body = "\n".join(lines[unreleased_idx + 1 : next_idx]).strip()
    rest = "\n".join(lines[next_idx:])
    return f"{preamble}\n\n## [{version}] - {date}\n{body}\n\n{rest}\n"


def bump_current_txt(version: str) -> str:
    return f"{version}\n"


def main(argv: list[str] | None = None) -> int:
    ensure_required_python()
    parser = argparse.ArgumentParser(
        description="Bump the Baluffo version across release artifacts."
    )
    parser.add_argument("version")
    parser.add_argument("--date", default=_datetime.date.today().isoformat())
    parser.add_argument(
        "--dry-run", action="store_true", help="Print changes without writing files."
    )
    args = parser.parse_args(argv)

    version = args.version.strip()
    if not VERSION_RE.match(version):
        print(f"error: version must look like X.Y.Z (got {version!r})", file=sys.stderr)
        return 2

    app_version_path = ROOT / "src" / "app_version.py"
    umbrel_app_path = ROOT / "deathuman-baluffo" / "umbrel-app.yml"
    compose_path = ROOT / "deathuman-baluffo" / "docker-compose.yml"
    changelog_path = ROOT / "docs" / "CHANGELOG.md"
    current_txt_path = ROOT / "dist" / "baluffo-ship" / "app" / "current.txt"

    current = re.search(r'APP_VERSION = "([^"]+)"', _read(app_version_path))
    current_version = current.group(1) if current else None
    if current_version == version:
        print(f"error: version is already {version}.", file=sys.stderr)
        return 2

    targets = [
        (app_version_path, bump_app_version(_read(app_version_path), version)),
        (umbrel_app_path, bump_umbrel_app(_read(umbrel_app_path), version)),
        (compose_path, bump_docker_compose(_read(compose_path), version)),
        (changelog_path, bump_changelog(_read(changelog_path), version, args.date)),
        (current_txt_path, bump_current_txt(version)),
    ]

    changed = [(path, new_text) for path, new_text in targets if _read(path) != new_text]
    if not changed:
        print("nothing to change.")
        return 0

    for path, new_text in changed:
        print(f"{'[dry-run] would write' if args.dry_run else 'writing'} {path.relative_to(ROOT)}")
        if not args.dry_run:
            _write(path, new_text)

    if args.dry_run:
        return 0

    extract = ROOT / "scripts" / "extract_release_notes.py"
    if extract.exists():
        print(f"regenerating release-notes.md for {version}")
        subprocess.run([sys.executable, str(extract), "--version", version], check=True)

    print(f"bumped {current_version} -> {version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
