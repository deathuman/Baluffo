"""The JSON source table must not rebuild its whole registry on every open.

Measured on the Pi: `/registry/sources` took 6.7s even at limitPerBucket=1, so
the cost was the full normalize + safe-demotion derivation over 3.9k rows, not
row rendering. These tests pin the input-signature cache and the empty-tombstone
short-circuit that removes the per-row URL fingerprint pass.
"""

from __future__ import annotations

import json
import types
from pathlib import Path
from typing import Any, cast

import pytest

from src.bridge.registry_service import RegistryPaths, RegistryService
from src.bridge.registry_tombstones import filter_tombstoned_rows


class _JsonAuthorityRuntime:
    """Minimal runtime-store stub: only the authority-mode read is exercised."""

    def __init__(self) -> None:
        self.store = types.SimpleNamespace(
            get_authority_modes=lambda: {"sourceRegistry": "json"},
        )


def _write(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text(json.dumps(rows), encoding="utf-8")


def _service(tmp_path: Path) -> tuple[RegistryService, RegistryPaths]:
    paths = RegistryPaths(
        active=tmp_path / "source-registry-active.json",
        pending=tmp_path / "source-registry-pending.json",
        rejected=tmp_path / "source-registry-rejected.json",
    )
    _write(
        paths.active,
        [
            {
                "id": "active-1",
                "name": "Active",
                "adapter": "static",
                "listing_url": "https://a.example/careers",
            }
        ],
    )
    _write(
        paths.pending,
        [{"id": "pending-1", "name": "Pending", "adapter": "greenhouse", "slug": "p1"}],
    )
    _write(paths.rejected, [])
    (tmp_path / "source-registry-tombstones.json").write_text("{}", encoding="utf-8")
    (tmp_path / "jobs-source-state.json").write_text("{}", encoding="utf-8")
    service = RegistryService(
        paths=paths,
        default_active=[],
        normalize_manual_static=lambda row: row,
        runtime_store_factory=cast(Any, _JsonAuthorityRuntime),
    )
    return service, paths


def test_table_cache_skips_normalization_on_repeat_open(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    service, _ = _service(tmp_path)
    calls = 0
    original = service.normalize_state

    def counting(state: dict[str, Any]) -> dict[str, list[dict[str, Any]]]:
        nonlocal calls
        calls += 1
        return original(state)

    monkeypatch.setattr(service, "normalize_state", counting)

    first = service.get_compact_table_payload(
        buckets=["pending", "active", "rejected"], limit_per_bucket=10
    )
    second = service.get_compact_table_payload(
        buckets=["pending", "active", "rejected"], limit_per_bucket=10
    )

    assert calls == 1
    assert [row["id"] for row in second["sources"]["pending"]] == ["pending-1"]
    assert second["summary"]["activeCount"] == first["summary"]["activeCount"] == 1


def test_table_cache_invalidates_when_registry_rows_change(tmp_path: Path) -> None:
    service, paths = _service(tmp_path)
    service.get_compact_table_payload(buckets=["pending"], limit_per_bucket=10)

    _write(
        paths.pending,
        [
            {"id": "pending-1", "name": "Pending", "adapter": "greenhouse", "slug": "p1"},
            {"id": "pending-2", "name": "Second", "adapter": "lever", "account": "p2"},
        ],
    )

    payload = service.get_compact_table_payload(buckets=["pending"], limit_per_bucket=10)
    assert [row["id"] for row in payload["sources"]["pending"]] == ["pending-1", "pending-2"]


def test_empty_tombstones_skip_row_fingerprints(monkeypatch: Any) -> None:
    def _boom(row: dict[str, Any]) -> str:
        raise AssertionError("URL fingerprint must not be computed without tombstones")

    monkeypatch.setattr("src.bridge.registry_tombstones.source_url_fingerprint", _boom)
    rows = [{"id": "a", "name": "A", "adapter": "static"}]
    assert filter_tombstoned_rows(rows, {}) == rows
