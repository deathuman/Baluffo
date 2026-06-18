from __future__ import annotations

from types import SimpleNamespace

import pytest

import src.source_sync_shard as shard_mod


class _FakeFuture:
    def __init__(self) -> None:
        self.cancelled = False

    def cancel(self) -> bool:
        self.cancelled = True
        return True


class _FakeExecutor:
    instances: list[_FakeExecutor] = []

    def __init__(self, *, max_workers: int) -> None:
        self.max_workers = max_workers
        self.futures: list[_FakeFuture] = []
        type(self).instances.append(self)

    def __enter__(self) -> _FakeExecutor:
        return self

    def __exit__(self, *_args: object) -> bool:
        return False

    def submit(self, *_args: object, **_kwargs: object) -> _FakeFuture:
        future = _FakeFuture()
        self.futures.append(future)
        return future


def test_read_sharded_snapshot_cancels_futures_on_base_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    manifest = {
        "generatedAt": "2026-06-18T00:00:00+00:00",
        "source": {"name": "unit-test"},
        "shards": [
            {
                "bucket": "active",
                "key": "a",
                "path": "baluffo/source-sync/shards/active/a.json.gz",
                "sha256": "0" * 64,
                "sizeBytes": 10,
                "rowCount": 1,
            }
        ],
    }

    monkeypatch.setattr(
        shard_mod,
        "read_manifest",
        lambda *_args, **_kwargs: {
            "manifest": manifest,
            "sha": "manifest-sha",
            "manifestSizeBytes": 100,
        },
    )
    monkeypatch.setattr(shard_mod, "ThreadPoolExecutor", _FakeExecutor)

    def raise_interrupt(_futures: object) -> list[object]:
        raise KeyboardInterrupt

    monkeypatch.setattr(shard_mod, "as_completed", raise_interrupt)

    with pytest.raises(KeyboardInterrupt):
        shard_mod.read_sharded_snapshot(
            SimpleNamespace(),
            SimpleNamespace(),
            max_workers=1,
            opener=object(),
        )

    assert len(_FakeExecutor.instances) == 1
    assert [future.cancelled for future in _FakeExecutor.instances[0].futures] == [True]
