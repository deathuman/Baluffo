import json
import threading
import time
from pathlib import Path

import pytest

import src.bridge.job_availability_service as availability_service_module
from src.bridge.job_availability_service import JobAvailabilityService
from src.bridge.routes.get_routes import handle_get
from src.bridge.routes.post_routes import handle_post
from src.jobs.availability_tombstones import (
    read_availability_tombstones,
    write_availability_tombstones,
)
from src.jobs.state_lifecycle import read_job_lifecycle_state, write_job_lifecycle_state
from src.shared.json_io import read_json
from src.storage import BaluffoStore, JobRuntimeStore
from tests.helpers.bridge_api import FakeDesktopLocalDataStore, FakeHandler, make_stub_bridge_api


def _canonical_feed_row(availability_id: str = "availability_1") -> dict:
    return {
        "id": "job-1",
        "title": "Engine Programmer",
        "company": "Studio",
        "city": "Rome",
        "country": "Italy",
        "workType": "Hybrid",
        "contractType": "Full-time",
        "jobLink": "https://example.com/jobs/1",
        "sector": "Games",
        "profession": "engine-programmer",
        "source": "fixture",
        "sourceJobId": "1",
        "availabilityId": availability_id,
        "availabilityStatus": "available",
        "sourceBundle": [],
    }


def test_scheduled_sweep_drains_the_full_bounded_plan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    rows = [
        {"availabilityId": f"availability_{index}", "jobLink": f"https://e{index}.test/job"}
        for index in range(30)
    ]
    (data_dir / "jobs-availability-sweep-plan.json").write_text(
        json.dumps({"rows": rows}), encoding="utf-8"
    )
    service = JobAvailabilityService(data_dir=data_dir, local_store_factory=lambda: None)
    started_ids: list[str] = []

    def fake_start(payload):
        availability_id = str(payload.get("availabilityId") or "")
        started_ids.append(availability_id)
        return {"started": True, "runId": f"run_{availability_id}"}

    monkeypatch.setattr(service, "start", fake_start)
    result = service.start_sweep_from_plan(max_concurrent=4)
    deadline = time.monotonic() + 2.0
    while len(started_ids) < len(rows) and time.monotonic() < deadline:
        time.sleep(0.01)

    assert result["started"] == 4
    assert result["queued"] == 26
    assert len(started_ids) == 30


def _wait_for_terminal_status(service: JobAvailabilityService, run_id: str) -> dict:
    deadline = time.monotonic() + 2.0
    while time.monotonic() < deadline:
        status = service.status(run_id)
        if status.get("status") != "running":
            return status
        time.sleep(0.001)
    raise AssertionError(f"availability task {run_id} did not finish")


def test_job_availability_public_routes(tmp_path: Path) -> None:
    api = make_stub_bridge_api(tmp_path, FakeDesktopLocalDataStore())
    api.start_job_availability_check = lambda payload: {
        "started": True,
        "runId": "availability_run_1",
        "availabilityId": payload["availabilityId"],
    }
    api.get_job_availability_check_status = lambda run_id: {
        "ok": True,
        "runId": run_id,
        "status": "succeeded",
    }

    post_handler = FakeHandler()
    assert handle_post(
        post_handler,
        api=api,
        path="/tasks/job-availability-check",
        payload={"availabilityId": "availability_1"},
    )
    assert post_handler.sent[-1]["payload"]["runId"] == "availability_run_1"

    get_handler = FakeHandler()
    assert handle_get(
        get_handler,
        api=api,
        path="/tasks/job-availability-check-status",
        query={"runId": ["availability_run_1"]},
    )
    assert get_handler.sent[-1]["payload"]["status"] == "succeeded"


def test_background_check_removes_closed_row_and_projects_saved_transition(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_job_lifecycle_state(
        data_dir / "jobs-lifecycle-state.json",
        {
            "job-1": {
                "status": "active",
                "availabilityId": "availability_1",
                "availabilityStatus": "available",
                "availabilityVerifiedAt": "2026-07-01T10:00:00+00:00",
                "title": "Engine Programmer",
                "company": "Studio",
                "jobLink": "https://example.com/jobs/1",
                "source": "fixture",
            }
        },
    )
    (data_dir / "jobs-unified.json").write_text(
        json.dumps([_canonical_feed_row()]), encoding="utf-8"
    )
    with BaluffoStore(data_dir) as storage:
        storage.set_authority_mode("jobsFeed", "sqlite", reason="test-cutover")
        JobRuntimeStore(storage).replace_feed(
            run_id="seed", rows=read_json(data_dir / "jobs-unified.json", [])
        )
    completed = threading.Event()

    class Validator:
        def check(self, _url: str):
            completed.set()
            return {
                "kind": "direct_closed",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
                "httpStatus": 410,
            }

    class Store:
        def __init__(self) -> None:
            self.entries = []

        def project_availability_transition(self, entry):
            self.entries.append(dict(entry))

    store = Store()
    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: store,
        validator=Validator(),
        enforce_direct=True,
    )
    started = service.start({"availabilityId": "availability_1"})
    assert started["started"] is True
    assert completed.wait(1.0)
    status = _wait_for_terminal_status(service, started["runId"])

    assert status["status"] == "succeeded"
    assert status["result"]["availabilityStatus"] == "unavailable"
    lifecycle = read_job_lifecycle_state(data_dir / "jobs-lifecycle-state.json")
    assert lifecycle["job-1"]["availabilityClosureOrigin"] == "direct"
    assert read_json(data_dir / "jobs-unified.json", []) == []
    tombstones = read_availability_tombstones(data_dir / "jobs-availability-tombstones.json")
    assert tombstones["availability_1"]["canonicalRow"]["workType"] == "Hybrid"
    with BaluffoStore(data_dir) as storage:
        assert JobRuntimeStore(storage).current_rows() == []
        assert storage.get_authority_modes()["jobsFeed"] == "sqlite"
    assert not (data_dir / "jobs-unified.csv").exists()
    assert store.entries[0]["availabilityTransitionId"]


def test_failed_lifecycle_commit_restores_sqlite_generation_and_feed_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    original_entry = {
        "status": "active",
        "availabilityId": "availability_rollback",
        "availabilityStatus": "available",
        "title": "Rendering Engineer",
        "company": "Studio",
        "jobLink": "https://example.com/jobs/rollback",
        "source": "fixture",
    }
    write_job_lifecycle_state(
        data_dir / "jobs-lifecycle-state.json", {"job-rollback": original_entry}
    )
    original_lifecycle = read_job_lifecycle_state(data_dir / "jobs-lifecycle-state.json")
    rollback_row = _canonical_feed_row("availability_rollback")
    rollback_row.update(
        {
            "title": "Rendering Engineer",
            "jobLink": "https://example.com/jobs/rollback",
        }
    )
    original_feed = json.dumps([rollback_row], separators=(",", ":"))
    (data_dir / "jobs-unified.json").write_text(original_feed, encoding="utf-8")
    with BaluffoStore(data_dir) as storage:
        storage.set_authority_mode("jobsFeed", "sqlite", reason="test-cutover")
        runtime = JobRuntimeStore(storage)
        original_generation = runtime.replace_feed(
            run_id="seed", rows=read_json(data_dir / "jobs-unified.json", [])
        ).generation

    class Validator:
        def check(self, _url: str):
            return {
                "kind": "direct_closed",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
                "httpStatus": 410,
            }

    real_write = write_job_lifecycle_state
    calls = 0

    def fail_once(path: Path, rows: dict) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("lifecycle locked")
        real_write(path, rows)

    monkeypatch.setattr(availability_service_module, "write_job_lifecycle_state", fail_once)
    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: None,
        validator=Validator(),
        enforce_direct=True,
    )
    started = service.start({"availabilityId": "availability_rollback"})
    status = _wait_for_terminal_status(service, started["runId"])

    assert status["status"] == "failed"
    assert (data_dir / "jobs-unified.json").read_text(encoding="utf-8") == original_feed
    assert not (data_dir / "jobs-unified-light.json").exists()
    assert not (data_dir / "jobs-unified-startup.json").exists()
    assert read_job_lifecycle_state(data_dir / "jobs-lifecycle-state.json") == original_lifecycle
    with BaluffoStore(data_dir) as storage:
        runtime = JobRuntimeStore(storage)
        assert runtime.current_generation() == original_generation
        assert runtime.current_rows() == read_json(data_dir / "jobs-unified.json", [])
        assert storage.get_authority_modes()["jobsFeed"] == "sqlite"


def test_background_live_check_restores_row_missing_from_feed(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_job_lifecycle_state(
        data_dir / "jobs-lifecycle-state.json",
        {
            "job-1": {
                "status": "likely_removed",
                "availabilityId": "availability_1",
                "availabilityStatus": "unavailable",
                "availabilityClosureOrigin": "source_absent",
                "title": "Engine Programmer",
                "company": "Studio",
                "jobLink": "https://example.com/jobs/1",
                "source": "fixture",
                "sourceJobId": "1",
                "postedAt": "2026-07-01T10:00:00+00:00",
            }
        },
    )
    for name in ("jobs-unified.json", "jobs-unified-light.json", "jobs-unified-startup.json"):
        (data_dir / name).write_text("[]", encoding="utf-8")
    write_availability_tombstones(
        data_dir / "jobs-availability-tombstones.json",
        {
            "availability_1": {
                "canonicalRow": _canonical_feed_row(),
                "retiredAt": "2026-07-02T10:00:00+00:00",
                "reason": "source_absent",
            }
        },
        updated_at="2026-07-02T10:00:00+00:00",
    )
    with BaluffoStore(data_dir) as storage:
        storage.set_authority_mode("jobsFeed", "json", reason="test-json-fallback")

    class Validator:
        def check(self, _url: str):
            return {
                "kind": "direct_live",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
                "httpStatus": 200,
            }

    class Store:
        def project_availability_transition(self, _entry):
            return 1

        def restore_reported_jobs_for_live(self, _availability_id, *, checked_at):
            return int(bool(checked_at))

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=Store,
        validator=Validator(),
        enforce_direct=True,
    )
    started = service.start({"availabilityId": "availability_1"})
    status = _wait_for_terminal_status(service, started["runId"])

    restored = read_json(data_dir / "jobs-unified.json", [])
    assert status["status"] == "succeeded"
    assert restored[0]["availabilityId"] == "availability_1"
    assert restored[0]["availabilityStatus"] == "available"
    assert restored[0]["workType"] == "Hybrid"
    assert restored[0]["profession"] == "engine-programmer"
    assert not (data_dir / "jobs-unified.csv").exists()


def test_json_reconciliation_missing_private_feed_fails_without_state_changes(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    state_path = data_dir / "jobs-lifecycle-state.json"
    original = {
        "status": "active",
        "availabilityId": "availability_1",
        "availabilityStatus": "available",
        "jobLink": "https://example.com/jobs/1",
    }
    write_job_lifecycle_state(state_path, {"job-1": original})
    original_state = read_job_lifecycle_state(state_path)

    class Validator:
        def check(self, _url: str):
            return {
                "kind": "direct_closed",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
                "httpStatus": 410,
            }

    projected: list[dict] = []

    class Store:
        def project_availability_transition(self, entry):
            projected.append(dict(entry))

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=Store,
        validator=Validator(),
        enforce_direct=True,
    )
    started = service.start({"availabilityId": "availability_1"})
    status = _wait_for_terminal_status(service, started["runId"])

    assert status["status"] == "failed"
    assert read_job_lifecycle_state(state_path) == original_state
    assert projected == []
    assert not (data_dir / "jobs-availability-history.json").exists()
    assert not (data_dir / "jobs-availability-tombstones.json").exists()


def test_live_reopening_without_canonical_tombstone_fails_closed(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    state_path = data_dir / "jobs-lifecycle-state.json"
    original = {
        "status": "likely_removed",
        "availabilityId": "availability_1",
        "availabilityStatus": "unavailable",
        "availabilityClosureOrigin": "source_absent",
        "jobLink": "https://example.com/jobs/1",
    }
    write_job_lifecycle_state(state_path, {"job-1": original})
    original_state = read_job_lifecycle_state(state_path)
    (data_dir / "jobs-unified.json").write_text("[]", encoding="utf-8")

    class Validator:
        def check(self, _url: str):
            return {
                "kind": "direct_live",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
                "httpStatus": 200,
            }

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: None,
        validator=Validator(),
        enforce_direct=True,
    )
    started = service.start({"availabilityId": "availability_1"})
    status = _wait_for_terminal_status(service, started["runId"])

    assert status["status"] == "failed"
    assert read_job_lifecycle_state(state_path) == original_state
    assert read_json(data_dir / "jobs-unified.json", None) == []


def test_duplicate_background_checks_reuse_the_active_run(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_job_lifecycle_state(
        data_dir / "jobs-lifecycle-state.json",
        {
            "job-1": {
                "availabilityId": "availability_1",
                "availabilityStatus": "available",
                "jobLink": "https://example.com/jobs/1",
            }
        },
    )
    entered = threading.Event()
    release = threading.Event()

    class Validator:
        def check(self, _url: str):
            entered.set()
            assert release.wait(1.0)
            return {
                "kind": "direct_live",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
            }

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: object(),
        validator=Validator(),
        enforce_direct=False,
    )
    first = service.start({"availabilityId": "availability_1"})
    assert entered.wait(1.0)
    second = service.start({"availabilityId": "availability_1"})
    release.set()

    assert second["started"] is True
    assert second["reused"] is True
    assert second["runId"] == first["runId"]


def test_older_direct_evidence_cannot_overwrite_newer_lifecycle_state(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    state_path = data_dir / "jobs-lifecycle-state.json"
    original = {
        "status": "active",
        "availabilityId": "availability_1",
        "availabilityStatus": "available",
        "availabilityCheckedAt": "2026-07-09T10:00:00+00:00",
        "jobLink": "https://example.com/jobs/1",
    }
    write_job_lifecycle_state(state_path, {"job-1": original})
    entered = threading.Event()
    release = threading.Event()

    class Validator:
        def check(self, _url: str):
            entered.set()
            assert release.wait(1.0)
            return {
                "kind": "direct_live",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
            }

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: object(),
        validator=Validator(),
        enforce_direct=True,
    )
    started = service.start({"availabilityId": "availability_1"})
    assert entered.wait(1.0)
    write_job_lifecycle_state(
        state_path,
        {
            "job-1": {
                **original,
                "status": "likely_removed",
                "availabilityStatus": "unavailable",
                "availabilityCheckedAt": "2026-07-11T10:00:00+00:00",
                "availabilityUnavailableAt": "2026-07-11T10:00:00+00:00",
            }
        },
    )
    release.set()
    status = _wait_for_terminal_status(service, started["runId"])

    assert status["result"]["applied"] is False
    lifecycle = read_job_lifecycle_state(state_path)
    assert lifecycle["job-1"]["availabilityStatus"] == "unavailable"
    assert lifecycle["job-1"]["availabilityCheckedAt"] == "2026-07-11T10:00:00+00:00"


def test_post_pipeline_publication_projects_transitions_before_sweep(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_job_lifecycle_state(
        data_dir / "jobs-lifecycle-state.json",
        {
            "job-1": {
                "availabilityId": "availability_1",
                "availabilityStatus": "unavailable",
                "availabilityTransitionId": "availability_event_1",
                "availabilityCheckedAt": "2026-07-10T10:00:00+00:00",
            }
        },
    )
    (data_dir / "jobs-availability-sweep-plan.json").write_text('{"rows":[]}', encoding="utf-8")

    class Store:
        def __init__(self) -> None:
            self.entries = []

        def project_availability_transitions(self, entries):
            self.entries = list(entries)
            return 1

    store = Store()
    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: store,
        enforce_direct=False,
    )
    result = service.post_pipeline_publication({"runId": "pipeline_1"})

    assert result["projected"] == 1
    assert result["sweep"]["started"] == 0
    assert store.entries[0]["availabilityTransitionId"] == "availability_event_1"


def test_post_pipeline_publication_still_starts_sweep_when_projection_fails(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_job_lifecycle_state(
        data_dir / "jobs-lifecycle-state.json",
        {
            "job-1": {
                "availabilityId": "availability_1",
                "availabilityStatus": "available",
                "jobLink": "https://example.com/jobs/1",
            }
        },
    )
    (data_dir / "jobs-availability-sweep-plan.json").write_text(
        '{"rows":[{"availabilityId":"availability_1"}]}', encoding="utf-8"
    )

    class Validator:
        def check(self, _url: str):
            return {
                "kind": "network_error",
                "confidence": "unknown",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
            }

    def failing_store():
        raise OSError("profile store unavailable")

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=failing_store,
        validator=Validator(),
        enforce_direct=False,
    )
    result = service.post_pipeline_publication({"runId": "pipeline_1"})

    assert result["projected"] == 0
    assert result["projectionError"] == "OSError"
    assert result["sweep"]["started"] == 1


def test_post_pipeline_publication_projects_when_identity_migration_fails(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_job_lifecycle_state(
        data_dir / "jobs-lifecycle-state.json",
        {
            "job-1": {
                "availabilityId": "availability_1",
                "availabilityStatus": "available",
            }
        },
    )
    (data_dir / "jobs-availability-sweep-plan.json").write_text('{"rows":[]}', encoding="utf-8")

    class Store:
        def reconcile_repaired_availability_identities(self):
            raise OSError("private quarantine unavailable")

        def project_availability_transitions(self, _entries):
            return 1

    service = JobAvailabilityService(data_dir=data_dir, local_store_factory=Store)
    result = service.post_pipeline_publication({"runId": "pipeline_1"})

    assert result["identityMigrationError"] == "OSError"
    assert result["projected"] == 1


def test_custom_saved_check_uses_private_ledger_and_never_changes_public_artifacts(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    availability_id = "availability_custom_123"
    (data_dir / "jobs-availability-priority.json").write_text(
        '{"schemaVersion":2,"rows":['
        '{"availabilityId":"availability_custom_123",'
        '"jobLink":"https://jobs.example.com/custom/123",'
        '"priority":"saved_daily","scope":"custom_saved"}]}',
        encoding="utf-8",
    )
    public_feed = '[{"availabilityId":"availability_public","title":"Public Role"}]'
    public_history = '{"schemaVersion":1,"rows":[]}'
    (data_dir / "jobs-unified.json").write_text(public_feed, encoding="utf-8")
    (data_dir / "jobs-availability-history.json").write_text(public_history, encoding="utf-8")

    class Validator:
        def check(self, url: str):
            assert url == "https://jobs.example.com/custom/123"
            return {
                "kind": "direct_closed",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "jobs.example.com",
                "httpStatus": 410,
            }

    class Store:
        def __init__(self) -> None:
            self.entries = []

        def project_availability_transition(self, entry):
            self.entries.append(dict(entry))

    store = Store()
    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: store,
        validator=Validator(),
        enforce_direct=True,
    )
    started = service.start({"availabilityId": availability_id})
    status = _wait_for_terminal_status(service, started["runId"])

    assert status["result"]["availabilityStatus"] == "unavailable"
    private = read_job_lifecycle_state(service.custom_lifecycle_path)
    assert private[availability_id]["availabilityStatus"] == "unavailable"
    assert store.entries[0]["availabilityId"] == availability_id
    assert (data_dir / "jobs-unified.json").read_text(encoding="utf-8") == public_feed
    assert (data_dir / "jobs-availability-history.json").read_text(
        encoding="utf-8"
    ) == public_history


def test_custom_check_refreshes_bridge_owned_manifest_before_resolving_url(
    tmp_path: Path,
) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()

    class Validator:
        def check(self, url: str):
            assert url == "https://jobs.example.com/custom/new"
            return {
                "kind": "direct_unverified",
                "confidence": "unknown",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "jobs.example.com",
            }

    class Store:
        def build_availability_priority_manifest(self):
            return {
                "schemaVersion": 2,
                "rows": [
                    {
                        "availabilityId": "availability_custom_new",
                        "jobLink": "https://jobs.example.com/custom/new",
                        "priority": "saved_daily",
                        "scope": "custom_saved",
                    }
                ],
            }

    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=Store,
        validator=Validator(),
        enforce_direct=False,
    )
    started = service.start({"availabilityId": "availability_custom_new"})
    status = _wait_for_terminal_status(service, started["runId"])

    assert status["status"] == "succeeded"
    manifest = read_json(data_dir / "jobs-availability-priority.json", {})
    assert manifest["rows"][0]["scope"] == "custom_saved"


def test_shadow_live_check_does_not_restore_profile_reports(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    write_job_lifecycle_state(
        data_dir / "jobs-lifecycle-state.json",
        {
            "job-1": {
                "availabilityId": "availability_1",
                "availabilityStatus": "unavailable",
                "jobLink": "https://example.com/jobs/1",
            }
        },
    )

    class Validator:
        def check(self, _url: str):
            return {
                "kind": "direct_live",
                "confidence": "definitive",
                "checkedAt": "2026-07-10T10:00:00+00:00",
                "source": "example.com",
            }

    class Store:
        def __init__(self) -> None:
            self.restored = 0

        def restore_reported_jobs_for_live(self, _availability_id, *, checked_at):
            self.restored += int(bool(checked_at))

    store = Store()
    service = JobAvailabilityService(
        data_dir=data_dir,
        local_store_factory=lambda: store,
        validator=Validator(),
        enforce_direct=False,
    )
    started = service.start({"availabilityId": "availability_1"})
    _wait_for_terminal_status(service, started["runId"])

    assert store.restored == 0
    lifecycle = read_job_lifecycle_state(data_dir / "jobs-lifecycle-state.json")
    assert lifecycle["job-1"]["availabilityStatus"] == "unavailable"
