from __future__ import annotations

import json
from pathlib import Path

from scripts.provider_migration_staging_refresh import refresh_provider_migration_staging
from src.source_registry_io import load_json_array, load_runtime_evidence_array


def _write_json(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload), encoding="utf-8")


def _read_json(path: Path) -> object:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_base_registry(data_dir: Path) -> None:
    _write_json(data_dir / "source-registry-active.json", [])
    _write_json(data_dir / "source-registry-pending.json", [])
    _write_json(data_dir / "source-registry-rejected.json", [])
    _write_json(data_dir / "source-registry-tombstones.json", {})


def test_provider_migration_refresh_updates_stale_report_and_pending(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "_out"
    _write_base_registry(data_dir)
    _write_json(
        data_dir / "source-discovery-report.json",
        {"summary": {"queuedCandidateCount": 1}, "candidateReview": {}},
    )
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "sourceIdentity": "static:studio",
                "name": "Static Studio",
                "adapter": "static",
                "listing_url": "https://studio.example/careers",
                "detectedProviderUrl": "https://boards.greenhouse.io/staticstudio",
                "jobsFound": 3,
            }
        ],
    )

    audit = refresh_provider_migration_staging(
        data_dir,
        out_dir,
        apply_pending=True,
        dry_run=False,
        at="2026-05-19T12:00:00+00:00",
    )

    report = _read_json(data_dir / "source-discovery-report.json")
    provider_migration = report["candidateReview"]["providerMigration"]
    candidates = load_runtime_evidence_array(data_dir / "source-discovery-candidates.json", [])
    pending = load_json_array(data_dir / "source-registry-pending.json", [])

    assert audit["summary"]["stagedProviderCandidateCount"] == 1
    assert audit["summary"]["affectedPendingCount"] == 1
    assert audit["affectedPendingIds"] == ["greenhouse:slug:staticstudio"]
    assert provider_migration["stageableProviderCandidateCount"] == 1
    assert provider_migration["stagedProviderCandidateCount"] == 1
    assert any(row.get("id") == "greenhouse:slug:staticstudio" for row in candidates)
    pending_row = pending[0]
    assert pending_row["id"] == "greenhouse:slug:staticstudio"
    assert pending_row["adapter"] == "greenhouse"
    assert pending_row["pendingReason"] == "provider_migration_candidate"
    assert pending_row["candidateState"] == "staged_provider_candidate"
    assert pending_row["createdFromAdvisory"] is True
    assert pending_row["migrationSourceIdentity"] == "static:studio"


def test_provider_migration_refresh_dry_run_writes_only_audit(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "_out"
    _write_base_registry(data_dir)
    _write_json(
        data_dir / "source-discovery-report.json",
        {"summary": {"queuedCandidateCount": 1}, "candidateReview": {}},
    )
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "sourceIdentity": "static:studio",
                "name": "Static Studio",
                "adapter": "static",
                "listing_url": "https://studio.example/careers",
                "detectedProviderUrl": "https://boards.greenhouse.io/staticstudio",
                "jobsFound": 3,
            }
        ],
    )
    before = {
        path.name: path.read_text(encoding="utf-8")
        for path in (
            data_dir / "source-discovery-report.json",
            data_dir / "source-discovery-candidates.json",
            data_dir / "source-registry-pending.json",
        )
    }

    audit = refresh_provider_migration_staging(
        data_dir,
        out_dir,
        dry_run=True,
        at="2026-05-19T12:00:00+00:00",
    )

    after = {
        path.name: path.read_text(encoding="utf-8")
        for path in (
            data_dir / "source-discovery-report.json",
            data_dir / "source-discovery-candidates.json",
            data_dir / "source-registry-pending.json",
        )
    }
    assert audit["summary"]["dryRun"] is True
    assert audit["summary"]["dataArtifactsWritten"] is False
    assert after == before
    assert (out_dir / "provider-migration-staging-refresh.json").exists()


def test_provider_migration_refresh_apply_pending_skips_blocked_rows(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "_out"
    _write_json(
        data_dir / "source-registry-active.json",
        [{"id": "greenhouse:slug:duplicateactive", "adapter": "greenhouse"}],
    )
    _write_json(
        data_dir / "source-registry-pending.json",
        [{"id": "greenhouse:slug:duplicatepending", "adapter": "greenhouse"}],
    )
    _write_json(data_dir / "source-registry-rejected.json", [])
    _write_json(data_dir / "source-registry-tombstones.json", {})
    _write_json(data_dir / "source-discovery-report.json", {"candidateReview": {}})
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "sourceIdentity": "static:dupe-active",
                "name": "Duplicate Active",
                "adapter": "static",
                "listing_url": "https://dupe-active.example/jobs",
                "detectedProviderUrl": "https://boards.greenhouse.io/duplicateactive",
                "duplicateOfActiveSource": True,
                "jobsFound": 3,
            },
            {
                "sourceIdentity": "static:dupe-pending",
                "name": "Duplicate Pending",
                "adapter": "static",
                "listing_url": "https://dupe-pending.example/jobs",
                "detectedProviderUrl": "https://boards.greenhouse.io/duplicatepending",
                "duplicateOfPendingSource": True,
                "jobsFound": 3,
            },
            {
                "sourceIdentity": "static:unsupported",
                "name": "Unsupported Provider",
                "adapter": "static",
                "listing_url": "https://unsupported.example/jobs",
                "detectedProviderUrl": "https://jobs.jobvite.com/unsupported",
                "jobsFound": 1,
            },
            {
                "sourceIdentity": "static:weak",
                "name": "Weak Static",
                "adapter": "static",
                "listing_url": "https://weak.example/jobs",
                "jobsFound": 0,
            },
        ],
    )

    audit = refresh_provider_migration_staging(
        data_dir,
        out_dir,
        apply_pending=True,
        dry_run=False,
        at="2026-05-19T12:00:00+00:00",
    )

    pending = load_json_array(data_dir / "source-registry-pending.json", [])
    blocker_counts = audit["stagingBlockerCounts"]

    assert audit["summary"]["stagedProviderCandidateCount"] == 0
    assert audit["summary"]["affectedPendingCount"] == 0
    assert [row["id"] for row in pending] == ["greenhouse:slug:duplicatepending"]
    assert blocker_counts["duplicate_active"] == 1
    assert blocker_counts["duplicate_pending"] == 1
    assert blocker_counts["unsupported_provider"] >= 1
    assert blocker_counts["insufficient_evidence"] >= 1


def test_provider_migration_refresh_skips_tombstoned_candidates(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    out_dir = tmp_path / "_out"
    _write_base_registry(data_dir)
    _write_json(
        data_dir / "source-registry-tombstones.json",
        {"static:tombstoned": {"sourceId": "static:tombstoned", "reason": "deleted"}},
    )
    _write_json(data_dir / "source-discovery-report.json", {"candidateReview": {}})
    _write_json(
        data_dir / "source-discovery-candidates.json",
        [
            {
                "id": "static:tombstoned",
                "name": "Tombstoned Static",
                "adapter": "static",
                "listing_url": "https://tombstoned.example/jobs",
                "detectedProviderUrl": "https://boards.greenhouse.io/tombstoned",
                "jobsFound": 3,
            }
        ],
    )

    audit = refresh_provider_migration_staging(
        data_dir,
        out_dir,
        apply_pending=True,
        dry_run=False,
        at="2026-05-19T12:00:00+00:00",
    )

    assert audit["summary"]["tombstonedCandidateCount"] == 1
    assert audit["summary"]["stagedProviderCandidateCount"] == 0
    assert audit["summary"]["dataArtifactsWritten"] is True
    assert audit["providerMigration"]["totalCandidates"] == 0
    assert load_json_array(data_dir / "source-registry-pending.json", []) == []
