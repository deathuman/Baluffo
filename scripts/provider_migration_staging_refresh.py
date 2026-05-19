#!/usr/bin/env python3
"""Focused provider-migration staging refresh from existing discovery evidence."""

from __future__ import annotations

import argparse
import gzip
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.shared.utils import now_iso
from src.source_discovery.provider_migration_advisory import (
    build_provider_migration_payload,
    stage_provider_candidates_with_diagnostics,
)
from src.source_registry_identity import (
    ensure_source_id,
    source_identity,
    source_url_fingerprint,
)
from src.source_registry_io import (
    load_json_array,
    load_json_object,
    load_runtime_evidence,
    load_runtime_evidence_array,
    save_json_atomic,
)
from src.source_registry_state import (
    hide_repeated_zero_job_pending,
    transition_registry_to_pending,
)

SCHEMA_VERSION = "1.0"
AUDIT_NAME = "provider-migration-staging-refresh.json"
PENDING_REASON = "provider_migration_candidate"
STAGED_ACTOR = "provider_migration_advisory"


def _json_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [dict(row) for row in payload if isinstance(row, dict)]
    if isinstance(payload, dict):
        return [dict(row) for row in payload.values() if isinstance(row, dict)]
    return []


def _runtime_artifact_status(path: Path, expected_type: type) -> str:
    existing = path if path.exists() else path.with_name(path.name + ".gz")
    if not existing.exists():
        return "missing"
    try:
        if existing.suffix == ".gz":
            with gzip.open(existing, "rt", encoding="utf-8") as fh:
                payload = json.load(fh)
        else:
            payload = json.loads(existing.read_text(encoding="utf-8"))
    except (OSError, UnicodeDecodeError, json.JSONDecodeError):
        return "malformed"
    return "ok" if isinstance(payload, expected_type) else "malformed"


def _discovery_rows(
    report_payload: dict[str, Any],
    candidate_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows = [*_json_rows(report_payload.get("candidates")), *candidate_rows]
    seen: set[str] = set()
    out: list[dict[str, Any]] = []
    for row in rows:
        key = source_identity(row)
        if key in seen:
            continue
        seen.add(key)
        out.append(dict(row))
    return out


def _tombstone_identity_tokens(tombstones: dict[str, Any]) -> tuple[set[str], set[str]]:
    identities: set[str] = set()
    fingerprints: set[str] = set()
    for key, raw in tombstones.items():
        key_text = str(key or "").strip().lower()
        if key_text:
            identities.add(key_text)
        row = raw if isinstance(raw, dict) else {}
        for field in ("id", "sourceId", "sourceIdentity"):
            value = str(row.get(field) or "").strip().lower()
            if value:
                identities.add(value)
        source = row.get("source")
        if isinstance(source, dict):
            identities.add(source_identity(source))
            fingerprint = source_url_fingerprint(source)
            if fingerprint:
                fingerprints.add(fingerprint)
        fingerprint = str(
            row.get("sourceUrlFingerprint")
            or row.get("source_url_fingerprint")
            or row.get("urlFingerprint")
            or ""
        ).strip()
        if fingerprint:
            fingerprints.add(fingerprint)
    return identities, fingerprints


def _is_tombstoned(
    row: dict[str, Any], tombstone_identities: set[str], tombstone_fingerprints: set[str]
) -> bool:
    if source_identity(row) in tombstone_identities:
        return True
    for field in ("id", "sourceId", "sourceIdentity"):
        value = str(row.get(field) or "").strip().lower()
        if value and value in tombstone_identities:
            return True
    fingerprint = source_url_fingerprint(row)
    return bool(fingerprint and fingerprint in tombstone_fingerprints)


def _filter_tombstoned_rows(
    rows: list[dict[str, Any]], tombstones: dict[str, Any]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    tombstone_identities, tombstone_fingerprints = _tombstone_identity_tokens(tombstones)
    kept: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for row in rows:
        if _is_tombstoned(row, tombstone_identities, tombstone_fingerprints):
            skipped.append(dict(row))
        else:
            kept.append(dict(row))
    return kept, skipped


def _merge_rows_preserving_existing(
    existing_rows: list[dict[str, Any]], new_rows: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    out = [dict(row) for row in existing_rows if isinstance(row, dict)]
    seen = {source_identity(row) for row in out}
    for row in new_rows:
        if not isinstance(row, dict):
            continue
        identity = source_identity(row)
        if identity in seen:
            continue
        seen.add(identity)
        out.append(ensure_source_id(row))
    return out


def _pending_registry_row(row: dict[str, Any], *, at: str) -> dict[str, Any]:
    pending = transition_registry_to_pending(
        dict(row),
        reason=PENDING_REASON,
        actor=STAGED_ACTOR,
        at=at,
    )
    pending["candidateState"] = "staged_provider_candidate"
    pending["createdFromAdvisory"] = True
    return hide_repeated_zero_job_pending(pending, at=at)


def _provider_migration_summary(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "totalCandidates": int(payload.get("totalCandidates") or 0),
        "stageableProviderCandidateCount": int(payload.get("stageableProviderCandidateCount") or 0),
        "stagedProviderCandidateCount": int(payload.get("stagedProviderCandidateCount") or 0),
        "stagingSkippedCount": int(payload.get("stagingSkippedCount") or 0),
        "stagingBlockerCounts": dict(payload.get("stagingBlockerCounts") or {}),
    }


def _compact_examples(rows: list[dict[str, Any]], *, limit: int = 8) -> list[dict[str, Any]]:
    examples: list[dict[str, Any]] = []
    for row in rows[:limit]:
        examples.append(
            {
                key: row.get(key)
                for key in (
                    "id",
                    "sourceIdentity",
                    "name",
                    "adapter",
                    "slug",
                    "account",
                    "company_id",
                    "api_url",
                    "feed_url",
                    "board_url",
                    "listing_url",
                    "migrationSourceIdentity",
                    "detectedProviderFamily",
                    "detectedProviderId",
                    "candidateState",
                )
                if row.get(key) not in (None, "")
            }
        )
    return examples


def refresh_provider_migration_staging(
    data_dir: Path,
    out_dir: Path,
    *,
    apply_pending: bool = False,
    dry_run: bool = True,
    at: str | None = None,
) -> dict[str, Any]:
    """Refresh provider-migration staging evidence from current local artifacts."""

    if apply_pending and dry_run:
        raise ValueError("--apply-pending and --dry-run are mutually exclusive")

    generated_at = str(at or now_iso())
    data_dir = Path(data_dir)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    report_path = data_dir / "source-discovery-report.json"
    candidates_path = data_dir / "source-discovery-candidates.json"
    active_path = data_dir / "source-registry-active.json"
    pending_path = data_dir / "source-registry-pending.json"
    tombstones_path = data_dir / "source-registry-tombstones.json"
    audit_path = out_dir / AUDIT_NAME

    report_status = _runtime_artifact_status(report_path, dict)
    candidates_status = _runtime_artifact_status(candidates_path, list)
    report_payload = load_runtime_evidence(report_path, {})
    candidate_rows = load_runtime_evidence_array(candidates_path, [])
    active_rows = load_json_array(active_path, [])
    pending_rows = load_json_array(pending_path, [])
    tombstones = load_json_object(tombstones_path, {})

    discovery_rows = _discovery_rows(report_payload, candidate_rows)
    eligible_rows, tombstoned_rows = _filter_tombstoned_rows(discovery_rows, tombstones)
    missing_or_malformed_candidates = candidates_status in {"missing", "malformed"}
    fallback_required = missing_or_malformed_candidates or report_status == "malformed"

    stage_result = stage_provider_candidates_with_diagnostics(
        eligible_rows,
        active_rows=active_rows,
        pending_rows=pending_rows,
        seen_rows=candidate_rows,
        at=generated_at,
    )
    staged_rows = [
        dict(row)
        for row in stage_result.get("staged", [])
        if isinstance(row, dict)
        and not _is_tombstoned(
            row,
            *_tombstone_identity_tokens(tombstones),
        )
    ]
    diagnostics = [
        dict(row) for row in stage_result.get("diagnostics", []) if isinstance(row, dict)
    ]
    provider_migration = build_provider_migration_payload(
        eligible_rows,
        active_rows=active_rows,
        pending_rows=pending_rows,
        at=generated_at,
    )

    existing_pending_ids = {source_identity(row) for row in pending_rows}
    pending_provider_rows = [
        _pending_registry_row(row, at=generated_at)
        for row in staged_rows
        if source_identity(row) not in existing_pending_ids
    ]
    affected_pending_ids = [source_identity(row) for row in pending_provider_rows]
    merged_candidates = _merge_rows_preserving_existing(candidate_rows, staged_rows)
    merged_pending = _merge_rows_preserving_existing(pending_rows, pending_provider_rows)

    updated_report = dict(report_payload)
    candidate_review = dict(updated_report.get("candidateReview") or {})
    candidate_review["providerMigration"] = provider_migration
    updated_report["candidateReview"] = candidate_review
    updated_report["providerMigrationStagingRefresh"] = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": generated_at,
        "applyPending": bool(apply_pending),
        "dryRun": bool(dry_run),
        "stagedProviderCandidateCount": len(staged_rows),
        "affectedPendingIds": affected_pending_ids,
    }

    blocker_counts: Counter[str] = Counter()
    for diagnostic in diagnostics:
        blockers = diagnostic.get("providerStagingBlockers")
        if not isinstance(blockers, list):
            continue
        blocker_counts.update(str(blocker) for blocker in blockers if str(blocker or "").strip())

    written_artifacts: list[str] = []
    data_artifacts_written = bool(apply_pending and not dry_run and not fallback_required)
    if data_artifacts_written:
        save_json_atomic(report_path, updated_report)
        written_artifacts.append(str(report_path))
        save_json_atomic(candidates_path, merged_candidates)
        written_artifacts.append(str(candidates_path))
        save_json_atomic(pending_path, merged_pending)
        written_artifacts.append(str(pending_path))

    summary = {
        "applyPending": bool(apply_pending),
        "dryRun": bool(dry_run or not apply_pending),
        "fallbackRequired": bool(fallback_required),
        "inputCandidateCount": len(discovery_rows),
        "eligibleCandidateCount": len(eligible_rows),
        "tombstonedCandidateCount": len(tombstoned_rows),
        "stagedProviderCandidateCount": len(staged_rows),
        "skippedCandidateCount": int(provider_migration.get("stagingSkippedCount") or 0),
        "pendingBeforeCount": len(pending_rows),
        "pendingAfterCount": len(merged_pending) if data_artifacts_written else len(pending_rows),
        "affectedPendingCount": len(affected_pending_ids),
        "reportUpdated": data_artifacts_written,
        "candidatesUpdated": data_artifacts_written,
        "pendingUpdated": data_artifacts_written and bool(affected_pending_ids),
        "dataArtifactsWritten": data_artifacts_written,
    }
    warnings: list[str] = []
    if report_status == "missing":
        warnings.append("source-discovery-report.json is missing; a minimal report can be created.")
    elif report_status == "malformed":
        warnings.append(
            "source-discovery-report.json is malformed; provider review was not written."
        )
    if candidates_status == "missing":
        warnings.append("source-discovery-candidates.json is missing; run full discovery first.")
    elif candidates_status == "malformed":
        warnings.append("source-discovery-candidates.json is malformed; run full discovery first.")
    if fallback_required:
        warnings.append(
            "Fast refresh cannot safely update data artifacts from missing or malformed discovery inputs; run full discovery."
        )

    audit_written_artifacts = [*written_artifacts, str(audit_path)]
    audit = {
        "schemaVersion": SCHEMA_VERSION,
        "generatedAt": generated_at,
        "inputs": {
            "dataDir": str(data_dir),
            "outDir": str(out_dir),
            "sourceDiscoveryReport": {"path": str(report_path), "status": report_status},
            "sourceDiscoveryCandidates": {
                "path": str(candidates_path),
                "status": candidates_status,
            },
            "sourceRegistryActive": {"path": str(active_path), "rowCount": len(active_rows)},
            "sourceRegistryPending": {"path": str(pending_path), "rowCount": len(pending_rows)},
            "sourceRegistryTombstones": {
                "path": str(tombstones_path),
                "rowCount": len(tombstones),
            },
        },
        "summary": summary,
        "providerMigration": _provider_migration_summary(provider_migration),
        "stagingBlockerCounts": dict(sorted(blocker_counts.items())),
        "stagingBlockerExamples": provider_migration.get("stagingBlockerExamples") or [],
        "stagedExamples": _compact_examples(staged_rows),
        "affectedPendingIds": affected_pending_ids,
        "mutation": {
            "readOnly": not data_artifacts_written,
            "runtimeArtifactsWritten": data_artifacts_written,
            "activeRegistryMutated": False,
            "rejectedRegistryMutated": False,
            "sourceSyncMutated": False,
            "migrationLinksApplied": False,
            "staticRowsHidden": False,
            "staticRowsDeleted": False,
        },
        "writtenArtifacts": audit_written_artifacts,
        "fallbackCommand": "python src/source_discovery.py" if fallback_required else "",
        "warnings": warnings,
    }
    save_json_atomic(audit_path, audit)
    return audit


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Refresh provider migration staging diagnostics from existing discovery candidates."
        )
    )
    parser.add_argument("--data-dir", default="data", help="Runtime data directory.")
    parser.add_argument("--out-dir", default="_out", help="Audit output directory.")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Write only the _out audit summary; this is the default without --apply-pending.",
    )
    parser.add_argument(
        "--apply-pending",
        action="store_true",
        help=(
            "Update source-discovery-report.json, source-discovery-candidates.json, "
            "and source-registry-pending.json with staged provider migration candidates."
        ),
    )
    args = parser.parse_args(argv)
    if args.apply_pending and args.dry_run:
        parser.error("--apply-pending and --dry-run are mutually exclusive")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    audit = refresh_provider_migration_staging(
        Path(args.data_dir),
        Path(args.out_dir),
        apply_pending=bool(args.apply_pending),
        dry_run=not bool(args.apply_pending),
    )
    print(json.dumps(audit["summary"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
