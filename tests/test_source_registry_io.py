from __future__ import annotations

from pathlib import Path
from typing import Any

import src.source_registry_io as registry_io


def test_lean_registry_metadata_write_lock_does_not_fail_required_registry_write(
    tmp_path: Path,
    monkeypatch,
) -> None:
    active_path = tmp_path / "source-registry-active.json"
    metadata_name = "source-registry-metadata.json.gz"
    original_replace = registry_io._replace_path_with_retry

    def replace_with_locked_metadata(
        tmp: Path,
        target: Path,
        *,
        policy: str = registry_io._WRITE_POLICY_REQUIRED,
    ) -> bool:
        if target.name == metadata_name:
            return registry_io._finish_write_failure(PermissionError("locked metadata"), policy)
        return original_replace(tmp, target, policy=policy)

    monkeypatch.setattr(registry_io, "_replace_path_with_retry", replace_with_locked_metadata)
    monkeypatch.setattr(registry_io, "DATA_DIR", tmp_path)

    payload: list[dict[str, Any]] = [
        {
            "id": "static:example.com",
            "name": "Example",
            "adapter": "static",
            "url": "https://example.com/jobs",
        }
    ]

    registry_io.save_json_atomic(active_path, payload)

    assert registry_io.load_json_array(active_path, []) == payload
