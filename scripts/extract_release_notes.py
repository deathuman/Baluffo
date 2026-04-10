#!/usr/bin/env python3
"""Extract the top versioned changelog section into a release-notes artifact."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app_version import APP_VERSION
from src.python_version_guard import ensure_required_python


def extract_release_notes(changelog_text: str, version: str) -> str:
    lines = changelog_text.splitlines()
    top_version_heading_index = None
    top_version_heading = ""

    for index, line in enumerate(lines):
        if line.startswith("## ["):
            top_version_heading_index = index
            top_version_heading = line.strip()
            break

    if top_version_heading_index is None:
        raise ValueError("Could not find a versioned changelog section to extract.")

    expected_heading = f"## [{version}]"
    if not top_version_heading.startswith(expected_heading):
        raise ValueError(
            f"Top changelog section must be {expected_heading!r}, found {top_version_heading!r}."
        )

    end_index = len(lines)
    for index in range(top_version_heading_index + 1, len(lines)):
        if lines[index].startswith("## "):
            end_index = index
            break

    extracted = "\n".join(lines[top_version_heading_index:end_index]).rstrip()
    if not extracted:
        raise ValueError(f"Changelog section {expected_heading!r} was empty.")
    return f"{extracted}\n"


def build_release_notes(changelog_path: Path, version: str, output_path: Path) -> Path:
    changelog_text = changelog_path.read_text(encoding="utf-8")
    release_notes = extract_release_notes(changelog_text, version)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(release_notes, encoding="utf-8")
    return output_path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract release notes from docs/CHANGELOG.md.")
    parser.add_argument("--version", default=APP_VERSION)
    parser.add_argument("--changelog", default=str(ROOT / "docs" / "CHANGELOG.md"))
    parser.add_argument("--output", default="release-notes.md")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    ensure_required_python()
    args = parse_args(argv)
    output_path = build_release_notes(
        Path(args.changelog).expanduser().resolve(),
        str(args.version).strip(),
        Path(args.output).expanduser().resolve(),
    )
    print(str(output_path))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
