from __future__ import annotations

from typing import Any

from tests.bridge.test_task_launch_bootstrap import _save_json_atomic, _task_launch_api
from tests.helpers.temp_paths import workspace_tmpdir


def test_jobs_bootstrap_repairs_missing_feed_after_full_pipeline_success() -> None:
    """A past pipeline success must not block rebuilding a feed that is gone."""
    with workspace_tmpdir("task-launch-bootstrap-repair-missing-feed") as data_dir:
        api = _task_launch_api(data_dir)
        api._start_bootstrap_lifecycle_watch = lambda **_kwargs: None  # type: ignore[method-assign]  # noqa: SLF001
        called = False

        def run_background_script(*_args: Any, **_kwargs: Any) -> int:
            nonlocal called
            called = True
            return 1234

        result = api.start_jobs_bootstrap_task(
            {"source": "jobs_first_run"},
            normalize_fetch_report_contract=lambda payload: payload,
            run_background_script=run_background_script,
            save_json_atomic=_save_json_atomic,
            schema_version=1,
            get_lifecycle_run_history_rows=lambda: [
                {
                    "type": "pipeline",
                    "status": "ok",
                    "finishedAt": "2026-05-17T13:00:00+00:00",
                }
            ],
        )

        assert result["started"] is True
        assert called is True
