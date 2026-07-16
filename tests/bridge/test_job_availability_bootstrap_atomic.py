from __future__ import annotations

import threading
import time
from contextlib import contextmanager
from pathlib import Path

import pytest

import src.bridge.task_launch_api as task_launch_api_module
from src.bridge.task_launch_jobs_feed import jobs_feed_reconciliation_transaction
from src.pipeline_io import write_atomic_if_changed
from src.shared.json_io import read_json
from tests.bridge.test_task_launch_bootstrap import (
    _save_json_atomic,
    _task_launch_api,
    _write_bootstrap_artifacts,
)
from tests.helpers.temp_paths import workspace_tmpdir


def test_jobs_bootstrap_promotion_waits_for_availability_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("task-launch-bootstrap-reconciliation-lock") as data_dir:
        api = _task_launch_api(data_dir)
        staging_dir = api._bootstrap_staging_dir("jobs_bootstrap_test")  # noqa: SLF001
        staging_dir.mkdir(parents=True)
        _write_bootstrap_artifacts(staging_dir)
        write_atomic_if_changed(data_dir / "jobs-unified.json", '[{"id":"old-job"}]')
        attempting_lock = threading.Event()
        real_transaction = jobs_feed_reconciliation_transaction

        @contextmanager
        def observed_transaction(target: Path):
            attempting_lock.set()
            with real_transaction(target):
                yield

        monkeypatch.setattr(
            task_launch_api_module,
            "jobs_feed_reconciliation_transaction",
            observed_transaction,
        )
        results: list[bool] = []
        failures: list[BaseException] = []

        def close_bootstrap() -> None:
            try:
                results.append(
                    api._close_bootstrap_from_staging(  # noqa: SLF001
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
                )
            except BaseException as exc:  # pragma: no cover - surfaced below
                failures.append(exc)

        with real_transaction(data_dir):
            thread = threading.Thread(target=close_bootstrap)
            thread.start()
            assert attempting_lock.wait(1.0)
            time.sleep(0.05)
            assert results == []
            assert read_json(data_dir / "jobs-unified.json", [])[0]["id"] == "old-job"

        thread.join(timeout=2.0)
        assert not thread.is_alive()
        assert failures == []
        assert results == [True]
        assert read_json(data_dir / "jobs-unified.json", [])[0]["id"] == "job-1"
