"""Filesystem-backed compressed evidence archive manifest helpers."""

from __future__ import annotations

import gzip
import hashlib
import json
import os
from collections.abc import Callable, Iterable, Mapping
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

MANIFEST_NAME = "evidence-archive-manifest.json"
DEFAULT_TOTAL_BUDGET_BYTES = 500 * 1024 * 1024
DEFAULT_PER_RUN_WARNING_BYTES = 25 * 1024 * 1024
DEFAULT_RETENTION_DAYS = 90
EVIDENCE_SCHEMA_VERSION = 1


def _now_iso() -> str:
    return datetime.now(UTC).isoformat()


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_segment(value: Any, *, fallback: str) -> str:
    text = "".join(
        char if char.isalnum() or char in {"-", "_", "."} else "_" for char in _clean_text(value)
    )
    return text.strip("._") or fallback


def _parse_iso(value: Any) -> datetime | None:
    text = _clean_text(value)
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _json_dumps(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _load_json_object(path: Path, default: dict[str, Any]) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return dict(default)
    return dict(payload) if isinstance(payload, dict) else dict(default)


def _atomic_write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f"{path.name}.tmp")
    tmp.write_bytes(payload)
    os.replace(tmp, path)


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    body = json.dumps(payload, indent=2, ensure_ascii=False).encode("utf-8")
    _atomic_write_bytes(path, body)


class EvidenceArchiveStore:
    """Writes compressed runtime evidence and maintains a bounded manifest."""

    def __init__(
        self,
        data_dir: Path | str,
        *,
        now_iso: Callable[[], str] = _now_iso,
        total_budget_bytes: int = DEFAULT_TOTAL_BUDGET_BYTES,
        per_run_warning_bytes: int = DEFAULT_PER_RUN_WARNING_BYTES,
        retention_days: int = DEFAULT_RETENTION_DAYS,
    ) -> None:
        self.data_dir = Path(data_dir).expanduser().resolve()
        self.manifest_path = self.data_dir / MANIFEST_NAME
        self._now_iso = now_iso
        self.total_budget_bytes = max(0, int(total_budget_bytes))
        self.per_run_warning_bytes = max(0, int(per_run_warning_bytes))
        self.retention_days = max(0, int(retention_days))

    def load_manifest(self) -> dict[str, Any]:
        payload = _load_json_object(
            self.manifest_path,
            {"schemaVersion": EVIDENCE_SCHEMA_VERSION, "archives": []},
        )
        archives = payload.get("archives")
        return {
            "schemaVersion": EVIDENCE_SCHEMA_VERSION,
            "updatedAt": _clean_text(payload.get("updatedAt")),
            "archives": [dict(row) for row in archives if isinstance(row, dict)]
            if isinstance(archives, list)
            else [],
        }

    def write_archive(
        self,
        *,
        run_id: str,
        kind: str,
        payload: Any,
        retention_class: str = "default",
        pinned: bool = False,
        active_run_ids: Iterable[str] = (),
    ) -> dict[str, Any]:
        safe_run_id = _safe_segment(run_id, fallback="run")
        safe_kind = _safe_segment(kind, fallback="evidence")
        relative_path = Path("artifacts") / "fetch" / safe_run_id / f"{safe_kind}.json.gz"
        archive_path = self._resolve_archive_path(relative_path)
        compressed = gzip.compress(_json_dumps(payload).encode("utf-8"), mtime=0)
        sha256 = hashlib.sha256(compressed).hexdigest()
        _atomic_write_bytes(archive_path, compressed)
        created_at = self._now_iso()
        entry: dict[str, Any] = {
            "runId": _clean_text(run_id) or safe_run_id,
            "kind": _clean_text(kind) or safe_kind,
            "path": relative_path.as_posix(),
            "sizeBytes": len(compressed),
            "sha256": sha256,
            "createdAt": created_at,
            "retentionClass": _clean_text(retention_class) or "default",
            "pinned": bool(pinned),
        }
        if self.per_run_warning_bytes and len(compressed) > self.per_run_warning_bytes:
            entry["warning"] = "archive_size_warning"
        manifest = self.load_manifest()
        manifest["archives"] = [
            row
            for row in manifest.get("archives", [])
            if not (
                _clean_text(row.get("runId")) == _clean_text(entry.get("runId"))
                and _clean_text(row.get("kind")) == _clean_text(entry.get("kind"))
            )
        ]
        manifest["archives"].append(entry)
        self._save_manifest(manifest)
        self.enforce_retention(active_run_ids=active_run_ids)
        return entry

    def enforce_retention(self, *, active_run_ids: Iterable[str] = ()) -> dict[str, Any]:
        manifest = self.load_manifest()
        active_ids = {_clean_text(run_id) for run_id in active_run_ids if _clean_text(run_id)}
        archives = [dict(row) for row in manifest.get("archives", []) if isinstance(row, dict)]
        now = _parse_iso(self._now_iso()) or datetime.now(UTC)
        deleted: list[dict[str, Any]] = []

        def removable(row: Mapping[str, Any]) -> bool:
            return not bool(row.get("pinned")) and _clean_text(row.get("runId")) not in active_ids

        def delete_row(row: Mapping[str, Any]) -> None:
            path = self._resolve_archive_path(Path(_clean_text(row.get("path"))))
            try:
                path.unlink()
            except FileNotFoundError:
                pass
            deleted.append(dict(row))

        remaining: list[dict[str, Any]] = []
        expired_candidates: list[dict[str, Any]] = []
        retention_delta = timedelta(days=self.retention_days)
        for row in archives:
            created_at = _parse_iso(row.get("createdAt"))
            expired = (
                self.retention_days > 0
                and created_at is not None
                and now - created_at > retention_delta
            )
            if expired and removable(row):
                expired_candidates.append(row)
            else:
                remaining.append(row)
        for row in sorted(expired_candidates, key=lambda item: _clean_text(item.get("createdAt"))):
            delete_row(row)

        remaining = [row for row in remaining if row not in deleted]
        total_bytes = sum(max(0, int(row.get("sizeBytes") or 0)) for row in remaining)
        if self.total_budget_bytes and total_bytes > self.total_budget_bytes:
            candidates = [row for row in remaining if removable(row)]
            for row in sorted(candidates, key=lambda item: _clean_text(item.get("createdAt"))):
                if total_bytes <= self.total_budget_bytes:
                    break
                delete_row(row)
                total_bytes -= max(0, int(row.get("sizeBytes") or 0))
            remaining = [row for row in remaining if row not in deleted]
        if self.total_budget_bytes and total_bytes > self.total_budget_bytes:
            candidates = [row for row in remaining if removable(row)]
            for row in sorted(
                candidates,
                key=lambda item: max(0, int(item.get("sizeBytes") or 0)),
                reverse=True,
            ):
                if total_bytes <= self.total_budget_bytes:
                    break
                delete_row(row)
                total_bytes -= max(0, int(row.get("sizeBytes") or 0))
            remaining = [row for row in remaining if row not in deleted]

        manifest["archives"] = remaining
        self._save_manifest(manifest)
        return {"deletedCount": len(deleted), "deleted": deleted, "totalSizeBytes": total_bytes}

    def _save_manifest(self, manifest: Mapping[str, Any]) -> None:
        payload = {
            "schemaVersion": EVIDENCE_SCHEMA_VERSION,
            "updatedAt": self._now_iso(),
            "archives": list(manifest.get("archives") or []),
        }
        _atomic_write_json(self.manifest_path, payload)

    def _resolve_archive_path(self, relative_path: Path) -> Path:
        if relative_path.is_absolute():
            raise ValueError("archive path must be relative")
        resolved = (self.data_dir / relative_path).resolve()
        data_root = self.data_dir.resolve()
        try:
            resolved.relative_to(data_root)
        except ValueError as exc:
            raise ValueError(f"archive path escapes data dir: {relative_path}") from exc
        return resolved
