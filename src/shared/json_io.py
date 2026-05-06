from __future__ import annotations

import gzip
import json
import re
import shutil
from pathlib import Path
from typing import Any

PIPELINE_GZIP_JSON_NAMES = {
    "jobs-lifecycle-state.json",
    "jobs-source-state.json",
    "jobs-unified-light.json",
    "jobs-unified.json",
}

_PIPELINE_GZIP_ARCHIVE_NAME = re.compile(r"^jobs-lifecycle-archive-\d{4}\.json$")


def is_gzip_backed_json_name(name: str) -> bool:
    base_name = str(name or "").removesuffix(".gz")
    return base_name in PIPELINE_GZIP_JSON_NAMES or bool(
        _PIPELINE_GZIP_ARCHIVE_NAME.fullmatch(base_name)
    )


def _json_candidates(path: Path) -> list[Path]:
    path = Path(path)
    if not is_gzip_backed_json_name(path.name):
        return [path]
    if path.suffix == ".gz":
        return [path, path.with_suffix("")]
    return [path.with_name(path.name + ".gz"), path]


def gzip_backed_json_storage_path(path: Path) -> Path:
    path = Path(path)
    if path.suffix == ".gz" or not is_gzip_backed_json_name(path.name):
        return path
    return path.with_name(path.name + ".gz")


def existing_json_candidate(path: Path) -> Path | None:
    for candidate in _json_candidates(Path(path)):
        if candidate.exists():
            return candidate
    return None


def _read_json_path(path: Path) -> Any:
    if path.suffix == ".gz":
        with gzip.open(path, mode="rt", encoding="utf-8") as handle:
            return json.load(handle)
    return json.loads(path.read_text(encoding="utf-8"))


def read_json_text(path: Path) -> str:
    if path.suffix == ".gz":
        with gzip.open(path, mode="rt", encoding="utf-8") as handle:
            return handle.read()
    return Path(path).read_text(encoding="utf-8")


def write_json_text(path: Path, text: str) -> Path:
    target = gzip_backed_json_storage_path(Path(path))
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.suffix == ".gz":
        with gzip.open(target, mode="wt", encoding="utf-8") as handle:
            handle.write(text)
    else:
        target.write_text(text, encoding="utf-8")
    return target


def copy_json_file_to_storage(source: Path, target: Path) -> Path:
    source = Path(source)
    resolved_target = gzip_backed_json_storage_path(Path(target))
    resolved_target.parent.mkdir(parents=True, exist_ok=True)
    if source.suffix == resolved_target.suffix:
        shutil.copy2(source, resolved_target)
        return resolved_target
    write_json_text(resolved_target, read_json_text(source))
    return resolved_target


def read_json(path: Path, fallback: Any) -> Any:
    try:
        for candidate in _json_candidates(Path(path)):
            if candidate.exists():
                return _read_json_path(candidate)
        return fallback
    except (OSError, json.JSONDecodeError):
        return fallback


def read_json_object(
    path: Path,
    fallback: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = read_json(path, dict(fallback or {}))
    return payload if isinstance(payload, dict) else dict(fallback or {})
