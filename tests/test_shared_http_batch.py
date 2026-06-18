import builtins
import importlib
import threading
import time

import pytest

from src.shared import http_batch

fetch_pages_batched = http_batch.fetch_pages_batched


def test_optional_httpx_import_failure_falls_back_to_sync_path(monkeypatch) -> None:
    real_import = builtins.__import__

    def fake_import(name, globals_=None, locals_=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "httpx":
            raise ModuleNotFoundError("No module named 'httpx'")
        return real_import(name, globals_, locals_, fromlist, level)

    with monkeypatch.context() as context:
        context.setattr(builtins, "__import__", fake_import)
        reloaded = importlib.reload(http_batch)

    try:
        assert reloaded.httpx is None
    finally:
        importlib.reload(http_batch)


def test_optional_httpx_import_does_not_hide_unexpected_failures(monkeypatch) -> None:
    real_import = builtins.__import__

    def fake_import(name, globals_=None, locals_=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "httpx":
            raise RuntimeError("unexpected httpx import bug")
        return real_import(name, globals_, locals_, fromlist, level)

    try:
        with monkeypatch.context() as context:
            context.setattr(builtins, "__import__", fake_import)
            with pytest.raises(RuntimeError, match="unexpected httpx import bug"):
                importlib.reload(http_batch)
    finally:
        importlib.reload(http_batch)


def test_fetch_pages_batched_preserves_order_and_respects_limits_on_sync_path() -> None:
    jobs = [
        {"url": "https://a.example/slow", "payload": {"id": "a-slow"}},
        {"url": "https://a.example/fast", "payload": {"id": "a-fast"}},
        {"url": "https://b.example/fast", "payload": {"id": "b-fast"}},
        {"url": "https://c.example/fail", "payload": {"id": "c-fail"}},
    ]
    delays = {
        "https://a.example/slow": 0.08,
        "https://a.example/fast": 0.01,
        "https://b.example/fast": 0.02,
        "https://c.example/fail": 0.02,
    }
    progress_calls: list[tuple[int, int]] = []
    lock = threading.Lock()
    active = 0
    max_active = 0
    host_active: dict[str, int] = {}
    host_max: dict[str, int] = {}

    def fake_fetch(job: dict[str, object], url: str, _: int) -> str:
        nonlocal active, max_active
        _ = job
        host = url.split("/")[2]
        with lock:
            active += 1
            max_active = max(max_active, active)
            host_active[host] = host_active.get(host, 0) + 1
            host_max[host] = max(host_max.get(host, 0), host_active[host])
        try:
            time.sleep(delays[url])
            if url.endswith("/fail"):
                raise RuntimeError("boom")
            return f"<html>{url}</html>"
        finally:
            with lock:
                active -= 1
                host_active[host] = max(0, host_active.get(host, 1) - 1)

    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=fake_fetch,
        total_concurrency=3,
        per_host_concurrency=1,
        progress_callback=lambda completed, total: progress_calls.append((completed, total)),
    )

    assert [str((row.get("payload") or {}).get("id") or "") for row in results] == [
        "a-slow",
        "a-fast",
        "b-fast",
        "c-fail",
    ]
    assert [bool(row.get("ok")) for row in results] == [True, True, True, False]
    assert str(results[0].get("text") or "").startswith("<html>")
    assert "boom" in str(results[3].get("error") or "")
    assert max_active <= 3
    assert all(count <= 1 for count in host_max.values())
    assert progress_calls[-1] == (4, 4)


def test_fetch_pages_batched_does_not_hide_unexpected_fetch_bug() -> None:
    jobs = [{"url": "https://a.example/bug", "payload": {"id": "a"}}]

    def fake_fetch(_job: dict[str, object], _url: str, _timeout_s: int) -> str:
        raise AssertionError("unexpected fetch bug")

    with pytest.raises(AssertionError, match="unexpected fetch bug"):
        fetch_pages_batched(
            5,
            jobs,
            sync_fetch=fake_fetch,
            total_concurrency=1,
            per_host_concurrency=1,
        )


def test_fetch_pages_batched_treats_missing_fixture_fetch_as_row_failure() -> None:
    jobs = [{"url": "https://a.example/missing", "payload": {"id": "a"}}]

    def fake_fetch(_job: dict[str, object], url: str, _timeout_s: int) -> str:
        raise KeyError(url)

    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=fake_fetch,
        total_concurrency=1,
        per_host_concurrency=1,
    )

    assert results[0]["ok"] is False
    assert "https://a.example/missing" in str(results[0]["error"])


def test_fetch_pages_batched_ignores_expected_progress_callback_failure() -> None:
    jobs = [{"url": "https://a.example/ok", "payload": {"id": "a"}}]

    def fake_fetch(_job: dict[str, object], url: str, _timeout_s: int) -> str:
        return f"<html>{url}</html>"

    def fail_progress(_completed: int, _total: int) -> None:
        raise RuntimeError("progress sink unavailable")

    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=fake_fetch,
        total_concurrency=1,
        per_host_concurrency=1,
        progress_callback=fail_progress,
    )

    assert results[0]["ok"] is True


def test_fetch_pages_batched_does_not_hide_unexpected_progress_callback_bug() -> None:
    jobs = [{"url": "https://a.example/ok", "payload": {"id": "a"}}]

    def fake_fetch(_job: dict[str, object], url: str, _timeout_s: int) -> str:
        return f"<html>{url}</html>"

    def fail_progress(_completed: int, _total: int) -> None:
        raise AssertionError("unexpected progress bug")

    with pytest.raises(AssertionError, match="unexpected progress bug"):
        fetch_pages_batched(
            5,
            jobs,
            sync_fetch=fake_fetch,
            total_concurrency=1,
            per_host_concurrency=1,
            progress_callback=fail_progress,
        )


def test_fetch_pages_batched_uses_async_fetch_when_provided() -> None:
    jobs = [
        {"url": "https://async.example/a", "payload": {"id": "a"}},
        {"url": "https://async.example/b", "payload": {"id": "b"}},
    ]
    sync_calls: list[str] = []
    async_calls: list[str] = []

    def fake_sync_fetch(job: dict[str, object], url: str, _: int) -> str:
        _ = job
        sync_calls.append(url)
        raise AssertionError("sync path should not be used when async_fetch is provided")

    async def fake_async_fetch(client, job: dict[str, object], url: str, _: int) -> str:
        _ = client
        _ = job
        async_calls.append(url)
        await __import__("asyncio").sleep(0.01)
        return f"async:{url}"

    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=fake_sync_fetch,
        async_fetch=fake_async_fetch,
        total_concurrency=2,
        per_host_concurrency=2,
    )

    assert sync_calls == []
    assert async_calls == ["https://async.example/a", "https://async.example/b"]
    assert [str(row.get("text") or "") for row in results] == [
        "async:https://async.example/a",
        "async:https://async.example/b",
    ]
