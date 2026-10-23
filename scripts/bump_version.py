#!/usr/bin/env python3
"""Bump the Baluffo version across every release artifact.

Keeps the version-bearing files in sync so a release bump is one command instead
of three hand-edits (the exact drift that caused the v0.2.139 release notes
mismatch):

  - src/app_version.py
  - docs/CHANGELOG.md  (moves the [Unreleased] section into a dated release)
  - dist/baluffo-ship/app/current.txt

Before writing anything it compares the version every present artifact declares.
A half-applied earlier bump (app version moved but the changelog or the packaged
`current.txt` left behind) would otherwise be carried forward silently, leaving a
release whose notes describe a version the app never declared. Mismatches are
reported and stop the bump; `--allow-drift` proceeds and rewrites them all.

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
APP_VERSION_RE = re.compile(r'APP_VERSION = "([^"]+)"')

# Every artifact that declares the version, in the order a bump touches them.
# Used only to detect drift: the bump helpers below own the rewriting.
_RELEASE_ARTIFACTS: tuple[tuple[str, str], ...] = (
    ("src/app_version.py", r'APP_VERSION = "([^"]+)"'),
    ("docs/CHANGELOG.md", r"^## \[(\d+\.\d+\.\d+)\]"),
    ("dist/baluffo-ship/app/current.txt", r"^(\d+\.\d+\.\d+)\s*$"),
)


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def _write(path: Path, text: str) -> None:
    # newline="\n" keeps the file LF on Windows. Without it, write_text() applies
    # os.linesep translation and emits CRLF, which the repo (LF-only via
    # .gitattributes) then fails on `ruff format --check` until the file is
    # renormalized -- a confusing failure that looks unrelated to the bump.
    path.write_text(text, encoding="utf-8", newline="\n")


def read_declared_versions(root: Path) -> list[tuple[str, str | None]]:
    """Return ``(artifact, declared version)`` for every present release artifact.

    An unbuilt ``current.txt`` is absent from the list: there is nothing to
    disagree with.
    """
    declared: list[tuple[str, str | None]] = []
    for rel_path, pattern in _RELEASE_ARTIFACTS:
        path = root / rel_path
        if not path.is_file():
            continue
        match = re.search(pattern, _read(path), flags=re.MULTILINE)
        declared.append((rel_path, match.group(1) if match else None))
    return declared


def version_drift(declared: list[tuple[str, str | None]]) -> list[tuple[str, str]]:
    """Return the artifacts whose declared version disagrees with ``src/app_version.py``."""
    reference = dict(declared).get("src/app_version.py")
    if reference is None:
        return []
    return [
        (artifact, version) for artifact, version in declared if version not in (None, reference)
    ]


def report_drift(drift: list[tuple[str, str]], reference: str | None) -> None:
    """Explain an incomplete earlier bump on stderr, naming each artifact's version."""
    print(
        f"error: version drift detected (src/app_version.py declares {reference}):", file=sys.stderr
    )
    for artifact, version in drift:
        print(f"  - {artifact}: {version}", file=sys.stderr)
    print(
        "A previous bump reached only some artifacts. Finish or revert it before bumping\n"
        "again, so this release does not carry the older version forward.\n"
        "Re-run with --allow-drift to bump anyway; every present artifact is rewritten.",
        file=sys.stderr,
    )


def bump_app_version(text: str, version: str) -> str:
    if f'APP_VERSION = "{version}"' in text:
        return text
    return re.sub(r'APP_VERSION = "\d+\.\d+\.\d+"', f'APP_VERSION = "{version}"', text, count=1)


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
    # The blank line after the heading matches every hand-written section: a
    # version heading followed directly by `### Changed` breaks the file's shape.
    return f"{preamble}\n\n## [{version}] - {date}\n\n{body}\n\n{rest}\n"


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
    parser.add_argument(
        "--allow-drift",
        action="store_true",
        help="Bump even when the artifacts disagree, rewriting them all to the new version.",
    )
    args = parser.parse_args(argv)

    version = args.version.strip()
    if not VERSION_RE.match(version):
        print(f"error: version must look like X.Y.Z (got {version!r})", file=sys.stderr)
        return 2

    app_version_path = ROOT / "src" / "app_version.py"
    changelog_path = ROOT / "docs" / "CHANGELOG.md"
    current_txt_path = ROOT / "dist" / "baluffo-ship" / "app" / "current.txt"

    current = APP_VERSION_RE.search(_read(app_version_path))
    current_version = current.group(1) if current else None

    drift = version_drift(read_declared_versions(ROOT))
    if drift:
        report_drift(drift, current_version)
        if not args.allow_drift:
            return 2

    if current_version == version:
        print(f"error: version is already {version}.", file=sys.stderr)
        return 2

    targets = [
        (app_version_path, bump_app_version(_read(app_version_path), version)),
        (changelog_path, bump_changelog(_read(changelog_path), version, args.date)),
    ]
    if current_txt_path.is_file():
        # A clean checkout has not been packaged, so it has no `current.txt` to
        # move. Creating one would invent an artifact that
        # `read_declared_versions` deliberately ignores when it is absent.
        targets.append((current_txt_path, bump_current_txt(version)))

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
