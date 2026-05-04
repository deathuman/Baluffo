from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from source_registry_identity import source_identity


class SourceSyncValidationError(RuntimeError):
    pass


def _load_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise SourceSyncValidationError(f"{path}: file not found") from exc
    except json.JSONDecodeError as exc:
        raise SourceSyncValidationError(
            f"{path}: invalid JSON at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise SourceSyncValidationError(message)


def _validate_row_buckets(snapshot: dict[str, Any], *, path: Path) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for bucket in ("active", "pending"):
        rows = snapshot[bucket]
        _require(isinstance(rows, list), f"{path}: {bucket} must be an array")
        for index, row in enumerate(rows):
            _require(isinstance(row, dict), f"{path}: {bucket}[{index}] must be an object")
            row_id = source_identity(row)
            _require(bool(row_id), f"{path}: {bucket}[{index}] has no canonical source identity")
            if row_id in seen:
                duplicates.add(row_id)
            else:
                seen.add(row_id)
    _require(
        not duplicates,
        f"{path}: duplicate canonical source identity across active/pending: {', '.join(sorted(duplicates))}",
    )


def validate_source_sync_snapshot(snapshot: Any, schema: dict[str, Any], *, path: Path) -> None:
    _require(isinstance(schema, dict), f"{path}: schema must be a JSON object")
    _require(schema.get("type") == "object", f"{path}: schema must describe a top-level object")
    properties = schema.get("properties")
    _require(isinstance(properties, dict), f"{path}: schema properties missing")

    _require(isinstance(snapshot, dict), f"{path}: snapshot must be a JSON object")
    unexpected = sorted(key for key in snapshot.keys() if key not in properties)
    _require(
        not unexpected,
        f"{path}: unknown top-level key(s): {', '.join(unexpected)}",
    )
    required = [str(item) for item in schema.get("required") or []]
    missing = [key for key in required if key not in snapshot]
    _require(not missing, f"{path}: missing required top-level key(s): {', '.join(missing)}")

    schema_version_schema = properties.get("schemaVersion")
    expected_version = None
    if isinstance(schema_version_schema, dict):
        expected_version = schema_version_schema.get("const")
    if expected_version is not None:
        _require(
            snapshot.get("schemaVersion") == expected_version,
            f"{path}: schemaVersion must be {expected_version}",
        )

    generated_at = snapshot.get("generatedAt")
    _require(
        isinstance(generated_at, str) and generated_at.strip(),
        f"{path}: generatedAt must be a non-empty string",
    )

    source_schema = properties.get("source") if isinstance(properties, dict) else None
    source = snapshot.get("source")
    _require(isinstance(source, dict), f"{path}: source must be an object")
    if isinstance(source_schema, dict):
        source_properties = source_schema.get("properties")
        if isinstance(source_properties, dict):
            unexpected_source_keys = sorted(
                key for key in source.keys() if key not in source_properties
            )
            _require(
                not unexpected_source_keys,
                f"{path}: source contains unknown key(s): {', '.join(unexpected_source_keys)}",
            )
        source_required = [str(item) for item in source_schema.get("required") or []]
        missing_source = [key for key in source_required if key not in source]
        _require(
            not missing_source,
            f"{path}: source missing required key(s): {', '.join(missing_source)}",
        )
    source_name = source.get("name")
    _require(
        isinstance(source_name, str) and source_name.strip(),
        f"{path}: source.name must be a non-empty string",
    )

    _validate_row_buckets(snapshot, path=path)


def validate_source_sync_file(snapshot_path: Path, schema_path: Path) -> None:
    schema = _load_json(schema_path)
    snapshot = _load_json(snapshot_path)
    validate_source_sync_snapshot(snapshot, schema, path=snapshot_path)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Validate canonical source-sync snapshot files.")
    parser.add_argument(
        "snapshots",
        nargs="+",
        type=Path,
        help="One or more source-sync snapshot files to validate.",
    )
    parser.add_argument(
        "--schema",
        type=Path,
        default=ROOT / "schemas" / "source-sync.schema.json",
        help="Path to the source-sync JSON schema file.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        for snapshot_path in args.snapshots:
            validate_source_sync_file(snapshot_path, args.schema)
            print(f"{snapshot_path}: ok")
    except SourceSyncValidationError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
