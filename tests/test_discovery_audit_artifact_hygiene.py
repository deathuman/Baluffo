from __future__ import annotations

from pathlib import Path

from tests.helpers.discovery_artifact_hygiene import (
    DISCOVERY_AUDIT_ARTIFACT_NAMES,
    changed_discovery_audit_artifacts,
    format_discovery_audit_change,
    snapshot_discovery_audit_artifacts,
)


def test_discovery_audit_snapshot_detects_new_artifact(tmp_path: Path) -> None:
    before = snapshot_discovery_audit_artifacts(tmp_path)
    assert before == {name: (False, 0, 0) for name in DISCOVERY_AUDIT_ARTIFACT_NAMES}

    artifact = tmp_path / "data" / "gameprog-discovery-audit.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("{}", encoding="utf-8")

    changed = changed_discovery_audit_artifacts(tmp_path, before)
    assert changed == ["gameprog-discovery-audit.json"]
    assert str(artifact) in format_discovery_audit_change(tmp_path, changed)


def test_discovery_audit_snapshot_ignores_unchanged_artifact(tmp_path: Path) -> None:
    artifact = tmp_path / "data" / "gamesmap-discovery-audit.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("{}", encoding="utf-8")
    before = snapshot_discovery_audit_artifacts(tmp_path)

    assert changed_discovery_audit_artifacts(tmp_path, before) == []


def test_discovery_audit_snapshot_detects_deletion(tmp_path: Path) -> None:
    artifact = tmp_path / "data" / "web-search-discovery-audit.json"
    artifact.parent.mkdir(parents=True)
    artifact.write_text("{}", encoding="utf-8")
    before = snapshot_discovery_audit_artifacts(tmp_path)
    artifact.unlink()

    assert changed_discovery_audit_artifacts(tmp_path, before) == [
        "web-search-discovery-audit.json"
    ]
