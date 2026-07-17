from pathlib import Path

from src.jobs.availability_identity import write_identity_quarantine
from src.jobs.common.url import fingerprint_url
from src.local_data_store import LocalDataPaths, LocalDataStore
from src.local_data_store_shared import custom_job_availability_id
from tests.helpers.temp_paths import workspace_tmpdir


def test_saved_availability_attention_is_idempotent_and_does_not_reorder_user_activity() -> None:
    with workspace_tmpdir("local-data-store-availability") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        uid = str(store.sign_in("Availability")["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Engine Programmer",
                "company": "Studio Availability",
                "jobLink": "https://example.com/jobs/engine",
                "availabilityId": "availability_engine",
            },
        )
        before_activity = store.list_saved_jobs(uid)[0]["lastActivityAt"]
        transition = {
            "availabilityId": "availability_engine",
            "availabilityStatus": "unavailable",
            "availabilityCheckedAt": "2026-07-10T10:00:00+00:00",
            "availabilityTransitionId": "availability_event_closed_1",
        }

        assert store.project_availability_transition(transition) == 1
        assert store.project_availability_transition(transition) == 0
        assert store.get_availability_attention(uid)["count"] == 1
        row = store.list_saved_jobs(uid)[0]
        assert row["lastActivityAt"] == before_activity
        assert row["systemActivityAt"] == "2026-07-10T10:00:00+00:00"
        assert row["pipelinePhase"] == "bookmark"
        assert row["outcomeStatus"] == "active"

        assert (
            store.acknowledge_availability_attention(
                uid, transition_id="availability_event_closed_1"
            )
            == 1
        )
        assert store.get_availability_attention(uid)["count"] == 0

        backup = store.export_profile_data(uid)
        assert backup["schemaVersion"] == 4
        backed_up = next(row for row in backup["savedJobs"] if row["jobKey"] == job_key)
        assert backed_up["availabilityId"] == "availability_engine"
        assert backed_up["availabilityAttention"]["events"][0]["transitionId"] == (
            "availability_event_closed_1"
        )


def test_shared_availability_transition_creates_one_unread_alert_per_profile() -> None:
    with workspace_tmpdir("local-data-store-shared-availability") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        uid = str(store.sign_in("Shared Availability")["uid"])
        shared_url = "https://jobs.example.com/openings/42"
        for key_salt in ("first", "second"):
            store.save_job_for_user(
                uid,
                {
                    "title": f"Engine Programmer {key_salt}",
                    "company": "Studio Shared",
                    "jobLink": shared_url,
                    "isCustom": True,
                    "keySalt": key_salt,
                },
            )
        transition = {
            "availabilityId": custom_job_availability_id(shared_url),
            "availabilityStatus": "unavailable",
            "availabilityCheckedAt": "2026-07-10T10:00:00+00:00",
            "availabilityTransitionId": "availability_event_shared_closed",
        }

        assert store.project_availability_transition(transition) == 2
        rows = store.list_saved_jobs(uid)
        events = [row["availabilityAttention"]["events"][0] for row in rows]
        assert sum(bool(event["alert"]) for event in events) == 1
        assert store.get_availability_attention(uid)["count"] == 1


def test_terminal_saved_job_gets_timeline_without_unread_alert_and_report_restores() -> None:
    with workspace_tmpdir("local-data-store-availability-terminal") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        uid = str(store.sign_in("Availability Terminal")["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Technical Artist",
                "company": "Studio Terminal",
                "jobLink": "https://example.com/jobs/art",
                "availabilityId": "availability_art",
            },
        )
        store.update_application_tracking(
            uid, job_key, {"outcomeStatus": "rejected"}, {"override": True}
        )
        store.project_availability_transition(
            {
                "availabilityId": "availability_art",
                "availabilityStatus": "unavailable",
                "availabilityCheckedAt": "2026-07-10T10:00:00+00:00",
                "availabilityTransitionId": "availability_event_art_closed",
            }
        )
        assert store.get_availability_attention(uid)["count"] == 0
        assert any(
            row["type"] == "job_availability_unavailable"
            for row in store.list_activity_for_user(uid, 100)
        )

        assert store.manage_availability_report(uid, job_key, action="report")["hidden"] is True
        store.project_availability_transition(
            {
                "availabilityId": "availability_art",
                "availabilityStatus": "available",
                "availabilityCheckedAt": "2026-07-12T10:00:00+00:00",
                "availabilityTransitionId": "availability_event_art_live",
            }
        )
        row = store.list_saved_jobs(uid)[0]
        assert row["availabilityAttention"]["hiddenByReport"] is False
        assert row["outcomeStatus"] == "rejected"


def test_definitive_live_shadow_evidence_restores_profile_report_and_notifies() -> None:
    with workspace_tmpdir("local-data-store-availability-shadow-restore") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        uid = str(store.sign_in("Availability Restore")["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Gameplay Programmer",
                "company": "Studio Restore",
                "jobLink": "https://example.com/jobs/gameplay",
                "availabilityId": "availability_restore",
            },
        )
        store.manage_availability_report(uid, job_key, action="report")

        assert (
            store.restore_reported_jobs_for_live(
                "availability_restore", checked_at="2026-07-12T10:00:00+00:00"
            )
            == 1
        )
        row = store.list_saved_jobs(uid)[0]
        assert row["availabilityAttention"]["hiddenByReport"] is False
        assert store.get_availability_attention(uid)["count"] == 1


def test_custom_jobs_get_stable_private_monitoring_identity_and_scoped_manifest() -> None:
    with workspace_tmpdir("local-data-store-custom-availability") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        first_uid = str(store.sign_in("Custom Availability One")["uid"])
        store.save_job_for_user(
            first_uid,
            {
                "title": "Engine Programmer",
                "company": "Studio Custom",
                "jobLink": "https://jobs.example.com/openings/42/?utm_source=newsletter&team=engine#apply",
                "isCustom": True,
            },
        )
        expected_id = custom_job_availability_id("https://jobs.example.com/openings/42?team=engine")
        assert store.list_saved_jobs(first_uid)[0]["availabilityId"] == expected_id

        second_uid = str(store.sign_in("Custom Availability Two")["uid"])
        store.save_job_for_user(
            second_uid,
            {
                "title": "Engine Programmer Copy",
                "company": "Studio Custom",
                "jobLink": "https://JOBS.example.com/openings/42?team=engine&utm_medium=social",
                "isCustom": True,
            },
        )
        store.save_job_for_user(
            second_uid,
            {
                "title": "Private Draft",
                "company": "Studio Custom",
                "jobLink": "http://localhost/jobs/7",
                "isCustom": True,
                "keySalt": "private",
            },
        )

        rows = store.list_saved_jobs(second_uid)
        assert next(row for row in rows if row["title"] == "Private Draft")["availabilityId"] == ""
        manifest = store.build_availability_priority_manifest()
        assert manifest["schemaVersion"] == 2
        custom_rows = [row for row in manifest["rows"] if row["scope"] == "custom_saved"]
        assert custom_rows == [
            {
                "availabilityId": expected_id,
                "jobLink": "https://jobs.example.com/openings/42?team=engine",
                "priority": "saved_daily",
                "scope": "custom_saved",
            }
        ]


def test_custom_job_url_edit_replaces_monitoring_identity_and_backup_import_repairs_it() -> None:
    with workspace_tmpdir("local-data-store-custom-availability-edit") as tmp:
        store = LocalDataStore(LocalDataPaths.from_data_dir(Path(tmp) / "data"))
        uid = str(store.sign_in("Custom Availability Edit")["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Technical Artist",
                "company": "Studio Edit",
                "jobLink": "https://jobs.example.com/one",
                "isCustom": True,
            },
        )
        original_id = store.list_saved_jobs(uid)[0]["availabilityId"]
        store.project_availability_transition(
            {
                "availabilityId": original_id,
                "availabilityStatus": "unavailable",
                "availabilityTransitionId": "custom_old_unavailable",
                "availabilityCheckedAt": "2026-07-12T10:00:00+00:00",
            }
        )
        store.manage_availability_report(uid, job_key, action="report")
        store.save_job_for_user(
            uid,
            {
                "jobKey": job_key,
                "title": "Technical Artist",
                "company": "Studio Edit",
                "jobLink": "https://jobs.example.com/two",
                "isCustom": True,
            },
        )
        replacement = store.list_saved_jobs(uid)[0]
        assert replacement["availabilityId"] != original_id
        assert replacement["availabilityId"] == custom_job_availability_id(replacement["jobLink"])
        assert replacement["availabilityAttention"] == {}

        backup = store.export_profile_data(uid)
        backup["savedJobs"][0]["availabilityId"] = "stale_custom_id"
        store.import_profile_data(uid, backup)
        repaired = store.list_saved_jobs(uid)[0]
        assert repaired["availabilityId"] == custom_job_availability_id(repaired["jobLink"])


def test_availability_overlay_joins_canonical_and_private_custom_state_by_exact_id() -> None:
    with workspace_tmpdir("local-data-store-availability-overlay") as tmp:
        data_dir = Path(tmp) / "data"
        store = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
        uid = str(store.sign_in("Availability Overlay")["uid"])
        store.save_job_for_user(
            uid,
            {
                "title": "Canonical Role",
                "company": "Studio",
                "jobLink": "https://jobs.example.com/canonical",
                "availabilityId": "availability_canonical_1",
            },
        )
        store.save_job_for_user(
            uid,
            {
                "title": "Custom Role",
                "company": "Studio",
                "jobLink": "https://jobs.example.com/custom",
                "isCustom": True,
            },
        )
        custom = next(row for row in store.list_saved_jobs(uid) if row["isCustom"])
        from src.jobs.state_lifecycle import write_job_lifecycle_state

        write_job_lifecycle_state(
            data_dir / "jobs-lifecycle-state.json",
            {
                "canonical": {
                    "availabilityId": "availability_canonical_1",
                    "availabilityStatus": "unavailable",
                    "availabilityCheckedAt": "2026-07-12T10:00:00+00:00",
                    "availabilityEvidence": {
                        "kind": "direct_closed",
                        "confidence": "definitive",
                        "checkedAt": "2026-07-12T10:00:00+00:00",
                        "source": "jobs.example.com",
                    },
                }
            },
        )
        write_job_lifecycle_state(
            data_dir / "local-user-data" / "jobs-custom-availability-state.json",
            {
                custom["availabilityId"]: {
                    "availabilityId": custom["availabilityId"],
                    "availabilityStatus": "verification_overdue",
                    "availabilityCheckedAt": "2026-07-13T10:00:00+00:00",
                }
            },
        )

        overlay = store.get_availability_overlay(uid)
        assert {row["availabilityStatus"] for row in overlay["rows"]} == {
            "unavailable",
            "verification_overdue",
        }
        assert all("jobLink" not in row for row in overlay["rows"])


def test_repaired_identity_migration_uses_exact_url_and_clears_current_attention() -> None:
    with workspace_tmpdir("local-data-store-identity-repair") as tmp:
        data_dir = Path(tmp) / "data"
        store = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
        uid = str(store.sign_in("Identity Repair")["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Canonical Role",
                "company": "Studio",
                "jobLink": "https://jobs.example.com/openings/42",
                "availabilityId": "availability_contaminated",
            },
        )
        custom_key = store.save_job_for_user(
            uid,
            {
                "title": "Custom Role",
                "company": "Studio",
                "jobLink": "https://jobs.example.com/custom/1",
                "isCustom": True,
            },
        )
        store.project_availability_transition(
            {
                "availabilityId": "availability_contaminated",
                "availabilityStatus": "unavailable",
                "availabilityCheckedAt": "2026-07-16T10:00:00+00:00",
                "availabilityTransitionId": "availability_old_transition",
            }
        )
        store.manage_availability_report(uid, job_key, action="report")
        write_identity_quarantine(
            data_dir / "jobs-availability-identity-quarantine.json",
            {
                "availability_contaminated": {
                    "detectedAt": "2026-07-16T12:00:00+00:00",
                    "reason": "cross_url_identity_collision",
                    "replacementAvailabilityIds": ["availability_repaired"],
                    "replacementIdentities": [
                        {
                            "availabilityId": "availability_repaired",
                            "urlFingerprints": [
                                fingerprint_url("https://jobs.example.com/openings/42")
                            ],
                        }
                    ],
                }
            },
            updated_at="2026-07-16T12:00:00+00:00",
        )

        result = store.reconcile_repaired_availability_identities()
        rows = {row["jobKey"]: row for row in store.list_saved_jobs(uid)}

        assert result == {"rebound": 1, "unmonitored": 0}
        repaired = rows[job_key]
        assert repaired["availabilityId"] == "availability_repaired"
        assert repaired["availabilityAttention"]["events"] == []
        assert repaired["availabilityAttention"]["localReport"] == {}
        assert repaired["availabilityAttention"]["hiddenByReport"] is False
        assert rows[custom_key]["availabilityId"] == custom_job_availability_id(
            "https://jobs.example.com/custom/1"
        )
