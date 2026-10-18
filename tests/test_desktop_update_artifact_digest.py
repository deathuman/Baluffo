"""Tests for the reused downloaded-artifact digest behind desktop update status."""

import hashlib
import json
import os
import threading
import time
from pathlib import Path

import pytest

from src.ship import desktop_update_state as update_state
from src.ship.desktop_update_service import DesktopUpdateService
from src.ship.desktop_update_shared import DesktopUpdatePaths, write_json_atomic
from tests.helpers.temp_paths import workspace_tmpdir


def _hash_counter(monkeypatch: pytest.MonkeyPatch) -> list[Path]:
    calls: list[Path] = []
    real_compute = update_state.compute_sha256

    def counting(path: Path) -> str:
        calls.append(Path(path))
        return real_compute(path)

    monkeypatch.setattr(update_state, "compute_sha256", counting)
    return calls


def test_artifact_digest_is_reused_while_the_artifact_is_unchanged(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tmp_path / "baluffo-portable-0.3.021.zip"
    payload = b"portable artifact bytes"
    artifact.write_bytes(payload)
    calls = _hash_counter(monkeypatch)

    first = update_state.cached_artifact_sha256(artifact)
    second = update_state.cached_artifact_sha256(artifact)

    assert first == second == hashlib.sha256(payload).hexdigest()
    assert calls == [artifact]


def test_artifact_digest_is_recomputed_when_the_artifact_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tmp_path / "baluffo-portable-0.3.021.zip"
    artifact.write_bytes(b"portable artifact bytes")
    calls = _hash_counter(monkeypatch)
    first = update_state.cached_artifact_sha256(artifact)

    artifact.write_bytes(b"a different set of portable artifact bytes")
    second = update_state.cached_artifact_sha256(artifact)

    assert first != second
    assert len(calls) == 2


def test_artifact_digest_is_recomputed_when_only_the_timestamp_changes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tmp_path / "baluffo-portable-0.3.021.zip"
    artifact.write_bytes(b"portable artifact bytes")
    calls = _hash_counter(monkeypatch)
    update_state.cached_artifact_sha256(artifact)

    stat = artifact.stat()
    os.utime(artifact, ns=(stat.st_atime_ns + 1_000_000_000, stat.st_mtime_ns + 1_000_000_000))
    update_state.cached_artifact_sha256(artifact)

    assert len(calls) == 2


def _blocking_hash(monkeypatch: pytest.MonkeyPatch) -> tuple[threading.Event, threading.Event]:
    """Make the digest read hang until the test releases it."""
    started = threading.Event()
    release = threading.Event()
    real_compute = update_state.compute_sha256

    def blocking(path: Path) -> str:
        started.set()
        assert release.wait(timeout=10.0) is True
        return real_compute(path)

    monkeypatch.setattr(update_state, "compute_sha256", blocking)
    return started, release


def test_budgeted_lookup_never_blocks_on_the_artifact_read(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tmp_path / "baluffo-portable-0.3.021.zip"
    artifact.write_bytes(b"portable artifact bytes")
    started, release = _blocking_hash(monkeypatch)

    began = time.perf_counter()
    assert update_state.artifact_sha256_within_budget(artifact, budget_ms=20) is None
    elapsed = time.perf_counter() - began

    assert elapsed < 1.0, "a status poll must not wait for the artifact read"
    assert started.wait(timeout=5.0) is True
    release.set()
    assert update_state.artifact_sha256_within_budget(artifact, budget_ms=5000) == (
        hashlib.sha256(b"portable artifact bytes").hexdigest()
    )


def test_warmed_artifact_digest_is_reused_by_the_next_lookup(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    artifact = tmp_path / "baluffo-portable-0.3.021.zip"
    payload = b"portable artifact bytes"
    artifact.write_bytes(payload)
    calls = _hash_counter(monkeypatch)

    assert update_state.warm_artifact_sha256(artifact) is True
    digest = update_state.artifact_sha256_within_budget(artifact, budget_ms=5000)

    assert digest == hashlib.sha256(payload).hexdigest()
    assert calls == [artifact]


def test_status_reconciliation_keeps_the_last_status_while_the_digest_reads(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        paths = DesktopUpdatePaths.from_data_dir(Path(tmp) / "data")
        paths.downloads_dir.mkdir(parents=True, exist_ok=True)
        artifact = paths.downloads_dir / "baluffo-portable-0.3.021.zip"
        artifact.write_bytes(b"portable artifact bytes")
        expected = update_state.compute_sha256(artifact)
        manifest = {
            "version": "0.3.021",
            "portable_artifact": {
                "url": "https://example.com/releases/baluffo-portable-0.3.021.zip",
                "sha256": expected,
            },
        }
        prior = update_state.default_status_payload(current_version="0.3.020")
        prior.update({"downloadState": "downloaded", "installState": "ready"})
        _started, release = _blocking_hash(monkeypatch)

        budgeted = update_state._reconcile_downloaded_artifact_status(
            paths=paths,
            manifest=manifest,
            status=prior,
            verify_budget_ms=20,
        )

        # Unverified is not the same as missing: the poll must not flip the state.
        assert budgeted["downloadState"] == "downloaded"
        assert budgeted["installState"] == "ready"

        release.set()
        verified = update_state._reconcile_downloaded_artifact_status(
            paths=paths,
            manifest=manifest,
            status=prior,
        )

    assert verified["downloadState"] == "downloaded"
    assert verified["installState"] == "ready"
    assert verified["downloadedZipPath"] == str(artifact)


def test_status_payload_warms_the_artifact_digest_before_the_first_poll(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with workspace_tmpdir("desktop-update-warm") as tmp:
        payload = b"portable artifact bytes"
        service = DesktopUpdateService(
            data_dir=Path(tmp) / "data",
            current_version_getter=lambda: "0.3.020",
        )
        paths = service.paths
        paths.downloads_dir.mkdir(parents=True, exist_ok=True)
        artifact = paths.downloads_dir / "baluffo-portable-0.3.021.zip"
        artifact.write_bytes(payload)
        write_json_atomic(
            paths.manifest_cache_path,
            {
                "manifest": {
                    "version": "0.3.021",
                    "portable_artifact": {
                        "url": "https://example.com/releases/baluffo-portable-0.3.021.zip",
                        "sha256": hashlib.sha256(payload).hexdigest(),
                    },
                }
            },
        )
        calls = _hash_counter(monkeypatch)

        assert service.warm_downloaded_artifact()["requested"] is True
        status = service.get_status_payload(verify_budget_ms=5000)

    assert status["downloadState"] == "downloaded"
    assert status["installState"] == "ready"
    assert calls == [artifact], "the warm read must be the only hash of the artifact"


def test_warming_skips_a_missing_downloaded_artifact(tmp_path: Path) -> None:
    service = DesktopUpdateService(
        data_dir=tmp_path / "data",
        current_version_getter=lambda: "0.3.020",
    )

    assert service.warm_downloaded_artifact() == {
        "requested": False,
        "reason": "no_cached_manifest",
    }


def test_status_reconciliation_reuses_the_digest_across_polls(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    with workspace_tmpdir("desktop-update") as tmp:
        paths = DesktopUpdatePaths.from_data_dir(Path(tmp) / "data")
        paths.downloads_dir.mkdir(parents=True, exist_ok=True)
        payload = b"portable artifact bytes"
        artifact = paths.downloads_dir / "baluffo-portable-0.3.021.zip"
        artifact.write_bytes(payload)
        manifest = {
            "version": "0.3.021",
            "portable_artifact": {
                "url": "https://example.com/releases/baluffo-portable-0.3.021.zip",
                "sha256": update_state.compute_sha256(artifact),
            },
        }
        calls = _hash_counter(monkeypatch)

        statuses = [
            update_state._reconcile_downloaded_artifact_status(
                paths=paths,
                manifest=manifest,
                status=update_state.default_status_payload(current_version="0.3.020"),
            )
            for _ in range(3)
        ]

    assert len(calls) == 1
    for status in statuses:
        assert status["downloadState"] == "downloaded"
        assert status["installState"] == "ready"
        assert status["downloadedBytes"] == len(payload)
        assert status["downloadPercent"] == 100
        assert status["downloadedZipPath"] == str(artifact)
    assert json.loads(json.dumps(statuses[0])) == statuses[0]
