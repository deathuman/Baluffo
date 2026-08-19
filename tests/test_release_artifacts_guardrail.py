from __future__ import annotations

import json
import zipfile

from src.app_version import APP_VERSION
from tools.repo_health import release_artifacts_policy


def _build_ship_bundle(tmp_path, version: str) -> None:
    current_path = tmp_path / "dist" / "baluffo-ship" / "app" / "current.txt"
    current_path.parent.mkdir(parents=True)
    current_path.write_text(f"{version}\n", encoding="utf-8")


def _build_portable_zip(tmp_path, version: str) -> None:
    zip_path = tmp_path / "dist" / f"baluffo-portable-{APP_VERSION}.zip"
    zip_path.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "w") as archive:
        archive.writestr("ship/app/current.txt", f"{version}\n")


def _build_manifest(tmp_path, version: str) -> None:
    manifest_path = tmp_path / "dist" / "baluffo-desktop-update-manifest.json"
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(
        json.dumps({"version": version, "channel": "stable"}), encoding="utf-8"
    )


def test_ship_bundle_absent_is_clean(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(release_artifacts_policy, "ROOT", tmp_path)
    assert release_artifacts_policy.check_ship_bundle_embedded_version() == []


def test_ship_bundle_matching_version_is_clean(tmp_path, monkeypatch) -> None:
    _build_ship_bundle(tmp_path, APP_VERSION)
    monkeypatch.setattr(release_artifacts_policy, "ROOT", tmp_path)
    assert release_artifacts_policy.check_ship_bundle_embedded_version() == []


def test_ship_bundle_stale_version_fails(tmp_path, monkeypatch) -> None:
    _build_ship_bundle(tmp_path, "0.0.0")
    monkeypatch.setattr(release_artifacts_policy, "ROOT", tmp_path)
    failures = release_artifacts_policy.check_ship_bundle_embedded_version()
    assert len(failures) == 1
    assert "baluffo-ship/app/current.txt" in failures[0]
    assert APP_VERSION in failures[0]


def test_portable_zip_absent_is_clean(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(release_artifacts_policy, "ROOT", tmp_path)
    assert release_artifacts_policy.check_portable_zip_embedded_version() == []


def test_portable_zip_matching_version_is_clean(tmp_path, monkeypatch) -> None:
    _build_portable_zip(tmp_path, APP_VERSION)
    monkeypatch.setattr(release_artifacts_policy, "ROOT", tmp_path)
    assert release_artifacts_policy.check_portable_zip_embedded_version() == []


def test_portable_zip_stale_version_fails(tmp_path, monkeypatch) -> None:
    _build_portable_zip(tmp_path, "0.0.0")
    monkeypatch.setattr(release_artifacts_policy, "ROOT", tmp_path)
    failures = release_artifacts_policy.check_portable_zip_embedded_version()
    assert len(failures) == 1
    assert f"baluffo-portable-{APP_VERSION}.zip" in failures[0]


def test_manifest_absent_is_clean(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(release_artifacts_policy, "ROOT", tmp_path)
    assert release_artifacts_policy.check_desktop_update_manifest_version() == []


def test_manifest_matching_version_is_clean(tmp_path, monkeypatch) -> None:
    _build_manifest(tmp_path, APP_VERSION)
    monkeypatch.setattr(release_artifacts_policy, "ROOT", tmp_path)
    assert release_artifacts_policy.check_desktop_update_manifest_version() == []


def test_manifest_stale_version_fails(tmp_path, monkeypatch) -> None:
    _build_manifest(tmp_path, "0.0.0")
    monkeypatch.setattr(release_artifacts_policy, "ROOT", tmp_path)
    failures = release_artifacts_policy.check_desktop_update_manifest_version()
    assert len(failures) == 1
    assert "baluffo-desktop-update-manifest.json" in failures[0]
    assert APP_VERSION in failures[0]


def test_release_group_is_registered() -> None:
    from tools.repo_health import repo_guardrails

    assert "release" in repo_guardrails.GROUPS
    assert repo_guardrails.GROUP_RUNNERS["release"] is repo_guardrails.run_release_group
