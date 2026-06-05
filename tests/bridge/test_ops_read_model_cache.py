from __future__ import annotations

import threading
import time
from concurrent.futures import ThreadPoolExecutor

import pytest

from src.bridge.ops_read_model_cache import OpsReadModelCache


def test_ops_read_model_cache_coalesces_concurrent_misses() -> None:
    cache = OpsReadModelCache()
    calls = 0
    gate = threading.Barrier(4)

    def builder() -> dict[str, object]:
        nonlocal calls
        calls += 1
        time.sleep(0.05)
        return {"value": calls, "items": []}

    def read() -> dict[str, object]:
        gate.wait(timeout=2)
        return cache.get_or_build(
            "expensive",
            signature=("same",),
            builder=builder,
            ttl_s=5,
            operation_label="test.expensive",
        )

    with ThreadPoolExecutor(max_workers=4) as executor:
        results = list(executor.map(lambda _index: read(), range(4)))

    assert calls == 1
    assert results == [{"value": 1, "items": []}] * 4
    results[0]["items"].append("mutated")
    assert results[1]["items"] == []


def test_ops_read_model_cache_waiters_receive_builder_error() -> None:
    cache = OpsReadModelCache()
    calls = 0
    gate = threading.Barrier(3)

    def builder() -> dict[str, object]:
        nonlocal calls
        calls += 1
        time.sleep(0.05)
        raise RuntimeError("boom")

    def read() -> str:
        gate.wait(timeout=2)
        with pytest.raises(RuntimeError, match="boom"):
            cache.get_or_build(
                "failing",
                signature=("same",),
                builder=builder,
                ttl_s=5,
                operation_label="test.failing",
            )
        return "failed"

    with ThreadPoolExecutor(max_workers=3) as executor:
        assert list(executor.map(lambda _index: read(), range(3))) == ["failed"] * 3

    assert calls == 1
