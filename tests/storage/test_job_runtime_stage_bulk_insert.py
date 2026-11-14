from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pytest

from src.storage import BaluffoStore, JobRuntimeStore
from tests.helpers.temp_paths import workspace_tmpdir


def _row(index: int) -> dict[str, Any]:
    return {
        "id": index,
        "title": f"Job {index}",
        "company": "Studio",
        "city": "Amsterdam",
        "country": "Netherlands",
        "jobLink": f"https://example.test/jobs/{index}",
        "source": "Example",
        "sourceJobId": f"job-{index}",
        "status": "active",
        "firstSeenAt": "2026-05-12T10:00:00Z",
        "lastSeenAt": "2026-05-12T10:00:00Z",
        "sourceBundle": [],
    }


def test_stage_feed_uses_one_bulk_insert_scope_and_one_final_checkpoint(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with workspace_tmpdir("job-runtime-stage-bulk-insert") as data_dir:
        with BaluffoStore(data_dir) as store:
            entered: list[str] = []
            checkpoints: list[str] = []
            original_bulk_mode = store.bulk_insert_mode
            original_checkpoint = store.checkpoint_required

            @contextmanager
            def tracking_bulk_mode() -> Iterator[None]:
                entered.append("entered")
                with original_bulk_mode():
                    yield

            def tracking_checkpoint(*, mode: str) -> dict[str, Any]:
                checkpoints.append(mode)
                return original_checkpoint(mode=mode)

            monkeypatch.setattr(store, "bulk_insert_mode", tracking_bulk_mode)
            monkeypatch.setattr(store, "checkpoint_required", tracking_checkpoint)

            runtime = JobRuntimeStore(
                store,
                now_iso=lambda: "2026-05-12T10:00:00Z",
                batch_size=2,
            )
            summary = runtime.stage_feed(
                run_id="fetch_bulk",
                rows=[_row(1), _row(2), _row(3)],
            )

            assert summary.row_count == 3
            assert entered == ["entered"]
            assert checkpoints == ["TRUNCATE"]
            assert store._bulk_write_mode is False
