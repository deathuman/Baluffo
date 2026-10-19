from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from src.bridge.fetch_report_summary import (
    FETCH_REPORT_SUMMARY_FILE_NAME,
    write_fetch_report_summary_artifact,
)
from src.bridge.task_launch_api import TaskLaunchApi
from src.pipeline_io import write_atomic_if_changed
from src.shared.json_io import read_json
from src.ship import jobs_first_run_state as feed_state
from tests.bridge.test_task_launch_bootstrap import (
    _save_json_atomic,
    _successful_bootstrap_report,
    _task_launch_api,
    _write_bootstrap_artifacts,
)
from tests.helpers.temp_paths import workspace_tmpdir


def _record_gate_reads(monkeypatch: pytest.MonkeyPatch, reads: list[Path]) -> None:
    real_read_json = feed_state.read_json

    def recording_read_json(path: Any, default: Any = None) -> Any:
        reads.append(Path(path))
        return real_read_json(path, default)

    monkeypatch.setattr(feed_state, "read_json", recording_read_json)


def _fail_bootstrap_promotion(api: TaskLaunchApi, staging_dir: Path) -> None:
    def fail_merge(_staging_dir: Path) -> None:
        raise OSError("state locked")

    # setattr, not attribute assignment: the test double replaces a bound method, and a
    # plain assignment needs `type: ignore[assignment]` because the function is unbound.
    setattr(api, "_merge_bootstrap_state_artifacts", fail_merge)  # noqa: B010
    closed = api._close_bootstrap_from_staging(  # noqa: SLF001
        run_id="jobs_bootstrap_test",
        staging_dir=staging_dir,
        report_shell=api._bootstrap_report_shell(  # noqa: SLF001
            run_id="jobs_bootstrap_test",
            started_at="2026-05-17T12:00:00+00:00",
            schema_version=1,
        ),
        normalize_fetch_report_contract=lambda payload: payload,
        save_json_atomic=_save_json_atomic,
        finish_lifecycle_run=lambda *_args, **_kwargs: {},
        fail_lifecycle_run=lambda *_args, **_kwargs: {},
    )
    assert closed is True


def _staged_bootstrap(api: TaskLaunchApi) -> Path:
    staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_test")  # noqa: SLF001
    staging_dir.mkdir(parents=True)
    _write_bootstrap_artifacts(staging_dir)
    return staging_dir


def test_jobs_bootstrap_promotion_publishes_the_summary_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("task-launch-bootstrap-sidecar") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = _staged_bootstrap(api)

        api._promote_bootstrap_output(  # noqa: SLF001
            staging_dir=staging_dir,
            report=_successful_bootstrap_report(),
            normalize_fetch_report_contract=lambda payload: payload,
            save_json_atomic=_save_json_atomic,
        )

        report_path = data_dir / "jobs-fetch-report.json"
        sidecar_path = data_dir / FETCH_REPORT_SUMMARY_FILE_NAME
        sidecar = read_json(sidecar_path, {})
        assert sidecar["summary"]["outputCount"] == 1
        assert sidecar["runId"] == "jobs_bootstrap_test"
        assert sidecar["finishedAt"] == "2026-05-17T12:00:10+00:00"

        reads: list[Path] = []
        _record_gate_reads(monkeypatch, reads)
        assert feed_state.has_successful_runtime_jobs_report(data_dir) is True
        assert reads[0] == sidecar_path
        assert report_path not in reads


def test_jobs_bootstrap_promotion_survives_an_unwritable_summary_sidecar(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("task-launch-bootstrap-sidecar-failure") as data_dir:
        bridge_calls: list[tuple[str, str, dict[str, Any]]] = []
        api = _task_launch_api(data_dir, bridge_calls=bridge_calls)
        staging_dir = _staged_bootstrap(api)

        def fail_sidecar(*_args: Any, **_kwargs: Any) -> dict[str, Any]:
            raise OSError("sidecar disk full")

        monkeypatch.setattr(
            "src.bridge.task_launch_api_bootstrap.write_fetch_report_summary_artifact",
            fail_sidecar,
        )

        promoted = api._promote_bootstrap_output(  # noqa: SLF001
            staging_dir=staging_dir,
            report=_successful_bootstrap_report(),
            normalize_fetch_report_contract=lambda payload: payload,
            save_json_atomic=_save_json_atomic,
        )

        assert promoted["summary"]["outputCount"] == 1
        assert read_json(data_dir / "jobs-fetch-report.json", {})["summary"]["status"] == "ok"
        assert not (data_dir / FETCH_REPORT_SUMMARY_FILE_NAME).exists()
        assert bridge_calls[-1][:2] == ("warning", "bootstrap_report_summary_write_failed")


def test_jobs_bootstrap_failure_publishes_a_sidecar_that_fails_the_gate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("task-launch-bootstrap-failure-sidecar") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = _staged_bootstrap(api)

        _fail_bootstrap_promotion(api, staging_dir)

        report_path = data_dir / "jobs-fetch-report.json"
        sidecar_path = data_dir / FETCH_REPORT_SUMMARY_FILE_NAME
        sidecar = read_json(sidecar_path, {})
        assert sidecar["summary"]["status"] == "error"
        assert sidecar["runId"] == "jobs_bootstrap_test"
        assert sidecar["finishedAt"] == "2026-05-17T12:00:00+00:00"

        reads: list[Path] = []
        _record_gate_reads(monkeypatch, reads)
        assert feed_state.has_successful_runtime_jobs_report(data_dir) is False
        assert reads[0] == sidecar_path
        assert report_path not in reads


def test_jobs_bootstrap_failure_sidecar_supersedes_a_stale_success_sidecar() -> None:
    with workspace_tmpdir("task-launch-bootstrap-stale-sidecar") as data_dir:
        api = _task_launch_api(data_dir)
        write_atomic_if_changed(data_dir / "jobs-unified.json", '[{"id":"old-job"}]')
        write_atomic_if_changed(data_dir / "jobs-unified-light.json", '[{"id":"old-job"}]')
        _save_json_atomic(data_dir / "jobs-fetch-report.json", _successful_bootstrap_report())
        write_fetch_report_summary_artifact(
            data_dir / "jobs-fetch-report.json",
            _successful_bootstrap_report(),
        )
        assert feed_state.has_successful_runtime_jobs_report(data_dir) is True

        staging_dir = _staged_bootstrap(api)
        _fail_bootstrap_promotion(api, staging_dir)

        assert feed_state.has_successful_runtime_jobs_report(data_dir) is False
