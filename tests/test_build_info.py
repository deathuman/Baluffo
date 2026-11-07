from __future__ import annotations

import json
from pathlib import Path

from src import build_info


def test_build_info_prefers_environment(monkeypatch, tmp_path: Path) -> None:
    stamp = tmp_path / "build-info.json"
    stamp.write_text(
        json.dumps({"commit": "from-file", "builtAt": "2026-01-01T00:00:00Z"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(build_info, "_candidate_paths", lambda: (stamp,))
    monkeypatch.setenv(build_info.BUILD_COMMIT_ENV, "from-env")
    monkeypatch.setenv(build_info.BUILD_BUILT_AT_ENV, "2026-02-02T00:00:00Z")

    assert build_info.read_build_info() == {
        "commit": "from-env",
        "builtAt": "2026-02-02T00:00:00Z",
    }


def test_build_info_reads_the_packaged_stamp(monkeypatch, tmp_path: Path) -> None:
    stamp = tmp_path / "build-info.json"
    stamp.write_text(
        json.dumps({"commit": "abc123", "builtAt": "2026-03-03T00:00:00Z"}),
        encoding="utf-8",
    )
    monkeypatch.setattr(build_info, "_candidate_paths", lambda: (stamp,))
    monkeypatch.delenv(build_info.BUILD_COMMIT_ENV, raising=False)
    monkeypatch.delenv(build_info.BUILD_BUILT_AT_ENV, raising=False)

    assert build_info.read_build_info() == {
        "commit": "abc123",
        "builtAt": "2026-03-03T00:00:00Z",
    }


def test_build_info_is_empty_without_a_stamp(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setattr(build_info, "_candidate_paths", lambda: (tmp_path / "missing.json",))
    monkeypatch.delenv(build_info.BUILD_COMMIT_ENV, raising=False)
    monkeypatch.delenv(build_info.BUILD_BUILT_AT_ENV, raising=False)

    assert build_info.read_build_info() == {"commit": "", "builtAt": ""}
