"""Runtime definition-consistency tests for the lean registry split.

The batch-3 trap: a direct write to the runtime ``.json.gz`` that renames row
ids leaves the new ids definition-less in ``source-registry-metadata.json.gz``
— ``load_json_array`` merges zero sparse fields, the pipeline joins zero pages,
and the source silently fetches nothing while reporting ``status ok``.

These tests pin the merged-view invariant end to end on a temp data dir.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.source_registry_io_load import load_json_array
from src.source_registry_io_save import save_json_atomic

FULL_ROW = {
    "id": "static:listing_url:https://old.example/jobs",
    "name": "Old Example (GameDevMap)",
    "studio": "Old Example",
    "adapter": "static",
    "listing_url": "https://old.example/jobs",
    "pages": ["https://old.example/jobs"],
    "careersUrl": "https://old.example/jobs",
    "registryState": "active",
    "pendingReason": "",
    "stateChangedAt": "2026-09-07T00:00:00Z",
    "stateChangedBy": "test",
}


@pytest.fixture()
def registry_dir(tmp_path: Path) -> Path:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    save_json_atomic(data_dir / "source-registry-active.json", [dict(FULL_ROW)])
    return data_dir


def _active(data_dir: Path) -> list[dict]:
    return load_json_array(data_dir / "source-registry-active.json", [])


def _static_pages(data_dir: Path) -> dict[str, list]:
    return {
        str(r.get("id")): list(r.get("pages") or [])
        for r in _active(data_dir)
        if str(r.get("id", "")).startswith("static:")
    }


def test_roundtrip_preserves_definition(registry_dir: Path) -> None:
    pages = _static_pages(registry_dir)
    assert pages["static:listing_url:https://old.example/jobs"] == ["https://old.example/jobs"]


def test_sanctioned_rename_keeps_definition(registry_dir: Path) -> None:
    """The save_json_atomic path: rename the id and the metadata map follows."""
    rows = _active(registry_dir)
    row = rows[0]
    row["id"] = "static:listing_url:https://new.example/jobs"
    row["listing_url"] = "https://new.example/jobs"
    row["pages"] = ["https://new.example/jobs"]
    row["careersUrl"] = "https://new.example/jobs"
    save_json_atomic(registry_dir / "source-registry-active.json", rows)

    pages = _static_pages(registry_dir)
    assert pages == {"static:listing_url:https://new.example/jobs": ["https://new.example/jobs"]}


def test_direct_gz_rename_loses_definition_is_detected_by_guardrail(
    registry_dir: Path,
) -> None:
    """Reproduce the batch-3 failure mode, then confirm the guardrail catches it.

    A direct .gz write (bypassing save_json_atomic) that replaces a row with a
    lean new-id row leaves the new id without a metadata entry; the merged view
    then has no definition at all (the exact batch-3 executor mistake). The
    repo guardrail (list_definitionless_static_rows) must flag the shape.
    """
    import gzip

    rows = _active(registry_dir)
    lean = {k: rows[0][k] for k in ("name", "adapter", "studio", "registryState")}
    lean["id"] = "static:listing_url:https://renamed.example/jobs"
    gz_path = registry_dir / "source-registry-active.json.gz"
    with gzip.open(gz_path, "wt", encoding="utf-8") as f:
        json.dump([lean], f, ensure_ascii=False)

    pages = _static_pages(registry_dir)
    assert pages == {"static:listing_url:https://renamed.example/jobs": []}

    from tools.repo_health.source_registry_duplicate_url_policy import (
        list_definitionless_static_rows,
    )

    failures = list_definitionless_static_rows(_active(registry_dir))
    assert len(failures) == 1
    assert "static:listing_url:https://renamed.example/jobs" in failures[0]


def test_seed_guardrail_flags_definitionless_row(tmp_path: Path) -> None:
    from tools.repo_health.source_registry_duplicate_url_policy import (
        check_active_seed_definitions,
    )

    seed_dir = tmp_path / "data" / "defaults"
    seed_dir.mkdir(parents=True)
    seed = seed_dir / "source-registry-active.seed.json"
    seed.write_text(
        json.dumps(
            [
                {
                    "id": "static:listing_url:https://x.example/jobs",
                    "adapter": "static",
                    "name": "X",
                    "studio": "X",
                    "registryState": "active",
                }
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    failures = check_active_seed_definitions(tmp_path)
    assert len(failures) == 1
    assert "definition-less" in failures[0]
