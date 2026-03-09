#!/usr/bin/env python3
"""Desktop backup end-to-end validator.

Runs isolated export/import validation scenarios and writes a machine-readable report.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from scripts.local_data_store import LocalDataPaths, LocalDataStore


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize_iso_text(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).isoformat()
    except ValueError:
        return text


def _sha256_bytes(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _json_dumps_sorted(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=False)


def _job_fingerprint(row: Dict[str, Any]) -> str:
    return str(row.get("jobKey") or "").strip()


def _attachment_fingerprint(row: Dict[str, Any]) -> str:
    job_key = str(row.get("jobKey") or "").strip()
    attachment_id = str(row.get("id") or "").strip()
    if job_key and attachment_id:
        return f"{job_key}::{attachment_id}"
    return "|".join(
        [
            job_key,
            str(row.get("name") or "").strip().lower(),
            str(row.get("type") or "").strip().lower(),
            str(int(row.get("size") or 0)),
            str(row.get("createdAt") or "").strip(),
        ]
    )


def _activity_fingerprint(row: Dict[str, Any]) -> str:
    details = row.get("details") if isinstance(row.get("details"), dict) else {}
    return "|".join(
        [
            str(row.get("type") or "").strip(),
            str(row.get("jobKey") or "").strip(),
            str(row.get("title") or "").strip(),
            str(row.get("company") or "").strip(),
            str(row.get("createdAt") or "").strip(),
            _json_dumps_sorted(details),
        ]
    )


def _normalize_job(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "jobKey": str(row.get("jobKey") or ""),
        "title": str(row.get("title") or ""),
        "company": str(row.get("company") or ""),
        "sector": str(row.get("sector") or ""),
        "companyType": str(row.get("companyType") or ""),
        "city": str(row.get("city") or ""),
        "country": str(row.get("country") or ""),
        "workType": str(row.get("workType") or ""),
        "contractType": str(row.get("contractType") or ""),
        "jobLink": str(row.get("jobLink") or ""),
        "profession": str(row.get("profession") or ""),
        "isCustom": bool(row.get("isCustom")),
        "customSourceLabel": str(row.get("customSourceLabel") or ""),
        "reminderAt": _normalize_iso_text(row.get("reminderAt")),
        "contactedAt": _normalize_iso_text(row.get("contactedAt")),
        "updatedBy": str(row.get("updatedBy") or ""),
        "applicationStatus": str(row.get("applicationStatus") or ""),
        "notes": str(row.get("notes") or ""),
        "attachmentsCount": int(row.get("attachmentsCount") or 0),
        "savedAt": _normalize_iso_text(row.get("savedAt")),
        "phaseTimestamps": {str(k): _normalize_iso_text(v) for k, v in dict(row.get("phaseTimestamps") or {}).items()},
    }


def _normalize_attachment(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "id": str(row.get("id") or ""),
        "jobKey": str(row.get("jobKey") or ""),
        "name": str(row.get("name") or ""),
        "type": str(row.get("type") or ""),
        "size": int(row.get("size") or 0),
        "createdAt": _normalize_iso_text(row.get("createdAt")),
    }


def _normalize_activity(row: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": str(row.get("type") or ""),
        "jobKey": str(row.get("jobKey") or ""),
        "title": str(row.get("title") or ""),
        "company": str(row.get("company") or ""),
        "createdAt": _normalize_iso_text(row.get("createdAt")),
        "details": dict(row.get("details") or {}),
    }


@dataclass
class Snapshot:
    jobs: Dict[str, Dict[str, Any]]
    attachments: Dict[str, Dict[str, Any]]
    activity: Dict[str, Dict[str, Any]]
    attachment_hashes: Dict[str, str]


def _capture_snapshot(store: LocalDataStore, uid: str) -> Snapshot:
    jobs_rows = store.list_saved_jobs(uid)
    jobs = {_job_fingerprint(row): _normalize_job(row) for row in jobs_rows}

    attachments: Dict[str, Dict[str, Any]] = {}
    attachment_hashes: Dict[str, str] = {}
    for job in jobs_rows:
        job_key = str(job.get("jobKey") or "")
        for row in store.list_attachments_for_job(uid, job_key):
            fp = _attachment_fingerprint(row)
            attachments[fp] = _normalize_attachment(row)
            try:
                raw, _mime, _name = store.get_attachment_blob(uid, job_key, str(row.get("id") or ""))
                attachment_hashes[fp] = _sha256_bytes(raw)
            except (FileNotFoundError, PermissionError, ValueError):
                # JSON backups can restore metadata-only attachments without local file bytes.
                continue

    activity_rows = store.list_activity_for_user(uid, 2_000)
    activity = {_activity_fingerprint(row): _normalize_activity(row) for row in activity_rows}

    return Snapshot(
        jobs=jobs,
        attachments=attachments,
        activity=activity,
        attachment_hashes=attachment_hashes,
    )


def _diff_maps(
    before: Dict[str, Dict[str, Any]],
    after: Dict[str, Dict[str, Any]],
    label: str,
) -> List[Dict[str, Any]]:
    mismatches: List[Dict[str, Any]] = []
    before_keys = set(before.keys())
    after_keys = set(after.keys())
    if label.startswith("attachments"):
        # Reconcile attachment id renames by metadata signature.
        before_only = sorted(before_keys - after_keys)
        after_only = sorted(after_keys - before_keys)
        before_sig: Dict[str, str] = {}
        for key in before_only:
            row = before.get(key) or {}
            sig = "|".join(
                [
                    str(row.get("jobKey") or ""),
                    str(row.get("name") or "").lower(),
                    str(row.get("type") or "").lower(),
                    str(int(row.get("size") or 0)),
                    str(row.get("createdAt") or ""),
                ]
            )
            before_sig[sig] = key
        for key in after_only:
            row = after.get(key) or {}
            sig = "|".join(
                [
                    str(row.get("jobKey") or ""),
                    str(row.get("name") or "").lower(),
                    str(row.get("type") or "").lower(),
                    str(int(row.get("size") or 0)),
                    str(row.get("createdAt") or ""),
                ]
            )
            match = before_sig.get(sig)
            if not match:
                continue
            before_keys.discard(match)
            after_keys.discard(key)
            before_keys.add(key)
            before[key] = dict(before.get(match) or {})
            before[key]["id"] = str(after.get(key, {}).get("id") or before[key].get("id") or "")

    for key in sorted(before_keys - after_keys):
        mismatches.append({"kind": f"{label}_missing_after_import", "key": key})
    for key in sorted(after_keys - before_keys):
        mismatches.append({"kind": f"{label}_unexpected_after_import", "key": key})
    for key in sorted(before_keys & after_keys):
        left = before[key]
        right = after[key]
        field_names = sorted(set(left.keys()) | set(right.keys()))
        for field_name in field_names:
            if field_name == "id" and label.startswith("attachments"):
                # Attachment ids may be renamed on import collision; key/fingerprint and metadata are authoritative.
                continue
            lv = left.get(field_name)
            rv = right.get(field_name)
            if isinstance(lv, dict) or isinstance(rv, dict):
                lv_text = _json_dumps_sorted(lv or {})
                rv_text = _json_dumps_sorted(rv or {})
                if lv_text != rv_text:
                    mismatches.append(
                        {
                            "kind": f"{label}_field_mismatch",
                            "key": key,
                            "fieldPath": field_name,
                            "before": lv,
                            "after": rv,
                        }
                    )
                continue
            if lv != rv:
                mismatches.append(
                    {
                        "kind": f"{label}_field_mismatch",
                        "key": key,
                        "fieldPath": field_name,
                        "before": lv,
                        "after": rv,
                    }
                )
    return mismatches


def _diff_attachment_hashes(before: Dict[str, str], after: Dict[str, str]) -> List[Dict[str, Any]]:
    mismatches: List[Dict[str, Any]] = []
    before_keys = set(before.keys())
    after_keys = set(after.keys())
    for key in sorted(before_keys - after_keys):
        mismatches.append({"kind": "attachment_hash_missing_after_import", "key": key})
    for key in sorted(after_keys - before_keys):
        mismatches.append({"kind": "attachment_hash_unexpected_after_import", "key": key})
    for key in sorted(before_keys & after_keys):
        if before[key] != after[key]:
            mismatches.append(
                {
                    "kind": "attachment_hash_mismatch",
                    "key": key,
                    "before": before[key],
                    "after": after[key],
                }
            )
    return mismatches


def _seed_profile_data(store: LocalDataStore, uid: str) -> Tuple[List[str], List[str]]:
    job_a = store.save_job_for_user(
        uid,
        {
            "title": "Gameplay Engineer",
            "company": "Studio One",
            "sector": "Game",
            "companyType": "Game",
            "city": "Amsterdam",
            "country": "NL",
            "workType": "Hybrid",
            "contractType": "Full-time",
            "jobLink": "https://example.com/jobs/gameplay-engineer",
            "profession": "Engineering",
            "notes": "Initial recruiter ping.",
            "applicationStatus": "applied",
            "phaseTimestamps": {
                "bookmark": "2026-03-08T08:00:00.000Z",
                "applied": "2026-03-08T09:00:00.000Z",
            },
            "reminderAt": "2026-03-20T09:00:00.000Z",
            "contactedAt": "2026-03-08T10:00:00.000Z",
            "updatedBy": "validator_seed",
            "isCustom": False,
        },
    )
    store.update_application_status(
        uid,
        job_a,
        "interview_1",
        {"preserveTimestamp": "2026-03-10T11:30:00.000Z"},
    )
    store.update_job_notes(uid, job_a, "Interview scheduled for Tuesday.")

    job_b = store.save_job_for_user(
        uid,
        {
            "title": "Technical Artist",
            "company": "Studio Two",
            "sector": "Game",
            "companyType": "Game",
            "city": "Utrecht",
            "country": "NL",
            "workType": "Remote",
            "contractType": "Contract",
            "jobLink": "https://example.com/jobs/tech-artist",
            "profession": "Art",
            "notes": "Need portfolio update.",
            "applicationStatus": "bookmark",
            "phaseTimestamps": {"bookmark": "2026-03-07T10:00:00.000Z"},
            "reminderAt": "2026-03-18T08:30:00.000Z",
            "updatedBy": "validator_seed",
            "isCustom": True,
            "customSourceLabel": "Manual",
        },
    )

    att_a = store.add_attachment_for_job(
        uid,
        job_a,
        {"name": "resume-a.txt", "type": "text/plain", "size": 14},
        "data:text/plain;base64,SGVsbG8gUmVzdW1lIEE=",
    )
    att_b = store.add_attachment_for_job(
        uid,
        job_b,
        {"name": "portfolio-b.txt", "type": "text/plain", "size": 17},
        "data:text/plain;base64,UG9ydGZvbGlvIHRleHQgQg==",
    )
    return [job_a, job_b], [att_a, att_b]


def _wipe_profile(store: LocalDataStore, uid: str, profile_name: str) -> str:
    store.wipe_account_admin("1234", uid)
    recreated = store.sign_in(profile_name)
    return str(recreated.get("uid") or "")


def run_validation(data_dir: Path) -> Dict[str, Any]:
    profile_name = "Backup Validation Profile"
    if data_dir.exists():
        shutil.rmtree(data_dir, ignore_errors=True)
    data_dir.mkdir(parents=True, exist_ok=True)

    paths = LocalDataPaths.from_data_dir(data_dir)
    store = LocalDataStore(paths)
    user = store.sign_in(profile_name)
    uid = str(user.get("uid") or "")
    if not uid:
        raise RuntimeError("Could not create isolated validation profile.")

    scenarios: List[Dict[str, Any]] = []
    overall_ok = True

    # Scenario A: JSON export/import (without files)
    _seed_profile_data(store, uid)
    before_a = _capture_snapshot(store, uid)
    payload_a = store.export_profile_data(uid, include_files=False)
    payload_a_has_blobs = any(bool(att.get("blobDataUrl")) for att in payload_a.get("attachments") or [])
    uid = _wipe_profile(store, uid, profile_name)
    import_result_a = store.import_profile_data(uid, payload_a)
    after_a = _capture_snapshot(store, uid)
    mismatches_a = []
    mismatches_a.extend(_diff_maps(before_a.jobs, after_a.jobs, "jobs"))
    mismatches_a.extend(_diff_maps(before_a.attachments, after_a.attachments, "attachments_metadata"))
    mismatches_a.extend(_diff_maps(before_a.activity, after_a.activity, "activity"))
    scenario_a_ok = not mismatches_a and not payload_a_has_blobs
    overall_ok = overall_ok and scenario_a_ok
    scenarios.append(
        {
            "name": "scenario_a_json_no_files",
            "ok": scenario_a_ok,
            "payloadIncludesFiles": bool(payload_a.get("includesFiles")),
            "payloadHasAttachmentBlobs": payload_a_has_blobs,
            "importResult": import_result_a,
            "mismatches": mismatches_a,
        }
    )

    # Reset to a clean profile between scenarios so metadata-only attachment rows from Scenario A do not affect Scenario B.
    uid = _wipe_profile(store, uid, profile_name)

    # Scenario B: ZIP/files export/import (include files)
    _seed_profile_data(store, uid)
    before_b = _capture_snapshot(store, uid)
    payload_b = store.export_profile_data(uid, include_files=True)
    payload_b_has_blobs = all(bool(att.get("blobDataUrl")) for att in payload_b.get("attachments") or [])
    uid = _wipe_profile(store, uid, profile_name)
    import_result_b = store.import_profile_data(uid, payload_b)
    after_b = _capture_snapshot(store, uid)
    mismatches_b = []
    mismatches_b.extend(_diff_maps(before_b.jobs, after_b.jobs, "jobs"))
    mismatches_b.extend(_diff_maps(before_b.attachments, after_b.attachments, "attachments_metadata"))
    mismatches_b.extend(_diff_maps(before_b.activity, after_b.activity, "activity"))
    mismatches_b.extend(_diff_attachment_hashes(before_b.attachment_hashes, after_b.attachment_hashes))
    scenario_b_ok = not mismatches_b and payload_b_has_blobs
    overall_ok = overall_ok and scenario_b_ok
    scenarios.append(
        {
            "name": "scenario_b_with_files",
            "ok": scenario_b_ok,
            "payloadIncludesFiles": bool(payload_b.get("includesFiles")),
            "payloadHasAttachmentBlobs": payload_b_has_blobs,
            "importResult": import_result_b,
            "mismatches": mismatches_b,
        }
    )

    # Scenario C: duplicate + malformed rows should preserve semantics with warnings.
    baseline_c = _capture_snapshot(store, uid)
    payload_c = store.export_profile_data(uid, include_files=True)
    if payload_c.get("savedJobs"):
        payload_c["savedJobs"].append(dict(payload_c["savedJobs"][0]))
    payload_c["attachments"] = list(payload_c.get("attachments") or []) + [
        {
            "id": "att_malformed",
            "name": "orphan.bin",
            "type": "application/octet-stream",
            "size": 3,
            "blobDataUrl": "data:application/octet-stream;base64,AAAA",
        }
    ]
    import_result_c = store.import_profile_data(uid, payload_c)
    after_c = _capture_snapshot(store, uid)
    mismatches_c = []
    mismatches_c.extend(_diff_maps(baseline_c.jobs, after_c.jobs, "jobs"))
    mismatches_c.extend(_diff_maps(baseline_c.attachments, after_c.attachments, "attachments_metadata"))
    mismatches_c.extend(_diff_maps(baseline_c.activity, after_c.activity, "activity"))
    scenario_c_ok = not mismatches_c and len(import_result_c.get("warnings") or []) > 0
    overall_ok = overall_ok and scenario_c_ok
    scenarios.append(
        {
            "name": "scenario_c_duplicates_and_malformed",
            "ok": scenario_c_ok,
            "importResult": import_result_c,
            "mismatches": mismatches_c,
        }
    )

    return {
        "generatedAt": now_iso(),
        "ok": overall_ok,
        "schemaVersion": 2,
        "backupSchemaVersion": 2,
        "dataDir": str(data_dir),
        "scenarios": scenarios,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate desktop backup export/import end-to-end.")
    parser.add_argument(
        "--data-dir",
        default="data/.backup-validation-tmp",
        help="Isolated data directory used for validation.",
    )
    parser.add_argument(
        "--report-path",
        default="data/backup-validation-report.json",
        help="Path to write JSON validation report.",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir).resolve()
    report_path = Path(args.report_path).resolve()
    report_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        report = run_validation(data_dir)
    except Exception as exc:  # pragma: no cover - failsafe reporting
        report = {
            "generatedAt": now_iso(),
            "ok": False,
            "schemaVersion": 2,
            "backupSchemaVersion": 2,
            "dataDir": str(data_dir),
            "error": str(exc),
            "scenarios": [],
        }

    report_path.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Backup validation report written to: {report_path}")
    return 0 if bool(report.get("ok")) else 1


if __name__ == "__main__":
    raise SystemExit(main())
