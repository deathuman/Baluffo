from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.jobs.location_bucket_manifest import (
    build_unknown_country_bucket_manifest,
    check_manifest_against_csv,
    load_manifest,
    load_rows_from_csv,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build or check representative samples for city + unknown-country buckets."
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    build_parser = subparsers.add_parser("build", help="Build a bucket manifest from a CSV export.")
    build_parser.add_argument("--input-csv", required=True, help="Path to jobs-unified.csv.")
    build_parser.add_argument(
        "--output-json", required=True, help="Path to write the manifest JSON."
    )

    check_parser = subparsers.add_parser(
        "check", help="Check manifest representative rows against a candidate CSV export."
    )
    check_parser.add_argument(
        "--manifest-json", required=True, help="Path to a manifest JSON file."
    )
    check_parser.add_argument(
        "--candidate-csv", required=True, help="Path to a candidate CSV export."
    )
    check_parser.add_argument(
        "--output-json", default="", help="Optional path to write the check report."
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "build":
        rows = load_rows_from_csv(Path(args.input_csv))
        manifest = build_unknown_country_bucket_manifest(rows)
        output_path = Path(args.output_json)
        output_path.write_text(json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8")
        print(str(output_path))
        return 0

    manifest = load_manifest(Path(args.manifest_json))
    rows = load_rows_from_csv(Path(args.candidate_csv))
    report = check_manifest_against_csv(manifest, rows)
    payload = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output_json:
        output_path = Path(args.output_json)
        output_path.write_text(payload, encoding="utf-8")
        print(str(output_path))
    else:
        print(payload)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
