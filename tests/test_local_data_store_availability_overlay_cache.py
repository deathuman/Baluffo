"""Cache behaviour for the saved availability overlay projection.

The overlay used to parse the whole gzip-backed lifecycle artifact on every poll,
and that parse holds the GIL for its full duration, stalling unrelated routes.
These cases pin the reuse and the invalidation that replaced it.
"""

from pathlib import Path

from src.local_data_store import LocalDataPaths, LocalDataStore
from tests.helpers.temp_paths import workspace_tmpdir


def test_availability_overlay_caches_projection_until_lifecycle_artifact_changes(
    monkeypatch,
) -> None:
    from src import local_data_store_availability as availability_mod
    from src.jobs.state_lifecycle import write_job_lifecycle_state

    with workspace_tmpdir("local-data-store-availability-overlay-cache") as tmp:
        data_dir = Path(tmp) / "data"
        store = LocalDataStore(LocalDataPaths.from_data_dir(data_dir))
        uid = str(store.sign_in("Overlay Cache")["uid"])
        job_key = store.save_job_for_user(
            uid,
            {
                "title": "Canonical Role",
                "company": "Studio",
                "jobLink": "https://jobs.example.com/cached",
                "availabilityId": "availability_cached_1",
            },
        )
        state_path = data_dir / "jobs-lifecycle-state.json"
        write_job_lifecycle_state(
            state_path,
            {
                "canonical": {
                    "availabilityId": "availability_cached_1",
                    "availabilityStatus": "unavailable",
                    "availabilityCheckedAt": "2026-07-12T10:00:00+00:00",
                }
            },
        )

        # A fresh workspace dir means a cold key, so this first call parses.
        reads: list[Path] = []
        real_read = availability_mod.read_job_lifecycle_state

        def counting_read(path):
            reads.append(Path(path))
            return real_read(path)

        monkeypatch.setattr(availability_mod, "read_job_lifecycle_state", counting_read)

        first = store.get_availability_overlay(uid)
        assert first["rows"] == [
            {
                "jobKey": job_key,
                "availabilityId": "availability_cached_1",
                "availabilityStatus": "unavailable",
                "availabilityCheckedAt": "2026-07-12T10:00:00+00:00",
                "availabilityVerifiedAt": "",
                "availabilityUnavailableAt": "",
                "availabilityEvidence": {},
            }
        ]
        # One read each for the canonical artifact and the per-profile custom state.
        assert len(reads) == 2

        # A second poll answers from the projection instead of re-parsing.
        assert store.get_availability_overlay(uid) == first
        assert len(reads) == 2

        write_job_lifecycle_state(
            state_path,
            {
                "canonical": {
                    "availabilityId": "availability_cached_1",
                    "availabilityStatus": "available",
                    "availabilityCheckedAt": "2026-07-14T10:00:00+00:00",
                    "availabilityEvidence": {
                        "kind": "direct_live",
                        "source": "jobs.example.com",
                    },
                },
                # The extra row also changes the artifact's byte size, so the
                # fingerprint moves even if mtime resolution were coarse.
                "other": {
                    "availabilityId": "availability_cached_2",
                    "availabilityStatus": "available",
                },
            },
        )

        refreshed = store.get_availability_overlay(uid)
        row = refreshed["rows"][0]
        assert row["jobKey"] == job_key
        # Key order is the response contract the frontend already reads.
        assert list(row) == [
            "jobKey",
            "availabilityId",
            "availabilityStatus",
            "availabilityCheckedAt",
            "availabilityVerifiedAt",
            "availabilityUnavailableAt",
            "availabilityEvidence",
        ]
        assert row["availabilityStatus"] == "available"
        assert row["availabilityCheckedAt"] == "2026-07-14T10:00:00+00:00"
        assert row["availabilityEvidence"].get("kind") == "direct_live"
        assert "jobLink" not in row
        assert len(reads) == 4
