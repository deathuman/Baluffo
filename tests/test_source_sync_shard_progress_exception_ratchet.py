from __future__ import annotations

import pytest

from src.source_sync_shard import _emit_pull_progress, _emit_push_progress


def test_shard_progress_callbacks_suppress_expected_sink_failures() -> None:
    def _fail_runtime(**_kwargs: object) -> None:
        raise RuntimeError("progress sink unavailable")

    def _fail_os(**_kwargs: object) -> None:
        raise OSError("progress sink closed")

    _emit_pull_progress(
        _fail_runtime,
        phase_label="Downloading shard 1 of 1",
        counts={"shardCount": 1},
        ratio=0.5,
    )
    _emit_push_progress(
        _fail_os,
        phase_label="Uploading shard 1 of 1",
        counts={"changedShardCount": 1},
        ratio=0.5,
    )


def test_shard_progress_callbacks_do_not_suppress_unexpected_failures() -> None:
    def _unexpected(**_kwargs: object) -> None:
        raise AssertionError("unexpected progress bug")

    with pytest.raises(AssertionError, match="unexpected progress bug"):
        _emit_pull_progress(
            _unexpected,
            phase_label="Downloading shard 1 of 1",
            counts={"shardCount": 1},
            ratio=0.5,
        )

    with pytest.raises(AssertionError, match="unexpected progress bug"):
        _emit_push_progress(
            _unexpected,
            phase_label="Uploading shard 1 of 1",
            counts={"changedShardCount": 1},
            ratio=0.5,
        )
