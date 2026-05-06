#!/usr/bin/env python3
"""Audit gzip-backed JSON artifact storage policy."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.shared.json_io import is_gzip_backed_json_name


def find_plain_gzip_backed_json(root: Path) -> list[Path]:
    root = Path(root)
    if not root.exists():
        return []
    return sorted(
        path
        for path in root.rglob("*.json")
        if path.is_file() and is_gzip_backed_json_name(path.name)
    )


def audit_roots(roots: list[Path]) -> dict[str, Any]:
    violations: list[str] = []
    scanned_roots: list[str] = []
    for root in roots:
        resolved = Path(root).expanduser()
        scanned_roots.append(str(resolved))
        violations.extend(str(path) for path in find_plain_gzip_backed_json(resolved))
    return {
        "ok": not violations,
        "scannedRoots": scanned_roots,
        "violationCount": len(violations),
        "violations": violations,
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "roots",
        nargs="*",
        default=["data", "_out"],
        help="Roots to scan for plain JSON files that should be gzip-backed.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print a machine-readable JSON report.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    report = audit_roots([Path(root) for root in args.roots])
    if bool(args.json):
        print(json.dumps(report, indent=2, ensure_ascii=False))
    elif report["ok"]:
        print("OK: no plain gzip-backed JSON artifacts found.")
    else:
        print(f"Found {report['violationCount']} plain gzip-backed JSON artifact(s):")
        for path in report["violations"]:
            print(path)
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
