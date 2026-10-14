import ast
import builtins
import importlib
import inspect
import threading
import time

import pytest

from src.shared import http_batch
from tests.helpers.concurrency import BlockingActiveCounter

fetch_pages_batched = http_batch.fetch_pages_batched


def _is_none_literal(node: ast.AST) -> bool:
    return isinstance(node, ast.Constant) and node.value is None


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
    progress_calls: list[tuple[int, int]] = []
    lock = threading.Lock()
    active = 0
    max_active = 0
    host_active: dict[str, int] = {}
    host_max: dict[str, int] = {}
    fetches = BlockingActiveCounter(auto_release_at=2)

    def fake_fetch(job: dict[str, object], url: str, _: int) -> str:
        nonlocal active, max_active
        host = url.split("/")[2]
        with lock:
            active += 1
            max_active = max(max_active, active)
            host_active[host] = host_active.get(host, 0) + 1
            host_max[host] = max(host_max.get(host, 0), host_active[host])
        fetches.enter()
        try:
            fetches.wait_released()
            if url.endswith("/fail"):
                raise RuntimeError("boom")
            return f"<html>{url}</html>"
        finally:
            fetches.exit()
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


def test_fetch_pages_batched_async_path_isolates_httpx_invalid_url_per_page() -> None:
    # Regression: a redirect with a malformed Location raised httpx.InvalidURL
    # (an Exception subclass, not HTTPError) outside the expected-exceptions
    # tuple and killed the whole discovery worker mid-audit. It must degrade to
    # a per-row failure instead.
    import httpx

    jobs = [
        {"url": "https://ok.example/a", "payload": {"id": "a"}},
        {"url": "https://bad.example/redirect", "payload": {"id": "b"}},
        {"url": "https://ok.example/c", "payload": {"id": "c"}},
    ]

    async def fake_async_fetch(_client, job: dict[str, object], url: str, _timeout_s: int) -> str:
        await __import__("asyncio").sleep(0)
        payload = job.get("payload")
        payload_id = payload.get("id") if isinstance(payload, dict) else None
        if str(payload_id) == "b":
            raise httpx.InvalidURL("For absolute URLs, path must be empty or begin with '/'")
        return f"<html>{url}</html>"

    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=lambda _job, _url, _t: (_ for _ in ()).throw(AssertionError("sync path unused")),
        async_fetch=fake_async_fetch,
        total_concurrency=2,
        per_host_concurrency=2,
    )

    assert [bool(row.get("ok")) for row in results] == [True, False, True]
    assert "For absolute URLs" in str(results[1].get("error") or "")


def test_sync_path_runs_at_the_configured_concurrency_beyond_the_default_executor_ceiling() -> None:
    """The lane count is the caller's budget, not `min(32, cpu_count + 4)`.

    The sync path used `asyncio.to_thread`, i.e. the event loop's shared default executor, so 40
    configured lanes ran as at most 32 (20 on a 16-core box) with no error and no log line -- and
    whether a caller got that cap or its real lane count came down to whether its fetcher happened
    to be `fetch_text` by identity.
    """
    lane_count = 40
    jobs = [{"url": f"https://lane{i}.example/", "payload": {"i": i}} for i in range(lane_count)]
    fetches = BlockingActiveCounter()
    released = threading.Event()
    thread_names: set[str] = set()
    lock = threading.Lock()

    def fake_fetch(_job: dict[str, object], url: str, _timeout_s: int) -> str:
        with lock:
            thread_names.add(threading.current_thread().name)
        fetches.enter()
        try:
            # Block only long enough for every lane to come alive at once: a batch that cannot
            # reach its lane count then fails below with the count it did reach, rather than
            # hanging here or raising a message-free assert out of a pool thread.
            if fetches.peak >= lane_count:
                released.set()
            released.wait(timeout=1.0)
            return f"<html>{url}</html>"
        finally:
            fetches.exit()

    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=fake_fetch,
        total_concurrency=lane_count,
        per_host_concurrency=lane_count,
    )

    assert len(results) == lane_count
    assert all(row["ok"] for row in results)
    assert fetches.peak == lane_count, (
        f"peak in-flight was {fetches.peak} of {lane_count} configured lanes"
    )
    assert thread_names and all(name.startswith("page-fetch") for name in thread_names), (
        f"fetches must run on this module's own pool, saw {sorted(thread_names)}"
    )


def test_shared_page_batch_module_never_submits_to_an_executor_it_did_not_size() -> None:
    """The cap was one call, so it must not come back as a call on any path.

    `asyncio.to_thread` submits to the event loop's shared default executor, `min(32, cpu_count +
    4)`, so a batch that uses it runs at the machine's worker count instead of the caller's lane
    count -- silently, and for every caller whose fetcher is not `fetch_text` by identity. The lane
    contracts above can only see the paths they drive; this reads every call in the module,
    including ones added later.
    """
    tree = ast.parse(inspect.getsource(http_batch))
    offenders: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        call = func.attr if isinstance(func, ast.Attribute) else str(getattr(func, "id", ""))
        if call == "to_thread":
            offenders.append("asyncio.to_thread() submits to the loop's shared default executor")
        elif call == "run_in_executor" and node.args and _is_none_literal(node.args[0]):
            offenders.append("run_in_executor(None, ...) submits to the shared default executor")
        elif call == "ThreadPoolExecutor":
            keywords = [keyword for keyword in node.keywords if keyword.arg]
            if "max_workers" not in {keyword.arg for keyword in keywords}:
                offenders.append(
                    "ThreadPoolExecutor() without max_workers uses its own default size"
                )
            prefix = next(
                (
                    str(keyword.value.value)
                    for keyword in keywords
                    if keyword.arg == "thread_name_prefix"
                    and isinstance(keyword.value, ast.Constant)
                ),
                "",
            )
            if not prefix.startswith("page-fetch"):
                offenders.append(
                    "ThreadPoolExecutor() must keep its page-fetch thread prefix, which the lane "
                    "contracts key on to tell this module's pool from the shared default executor"
                )
    assert not offenders, (
        "src/shared/http_batch.py may only run sync fetches on a pool it sizes to the batch: "
        + "; ".join(offenders)
    )


def test_sync_path_deadline_does_not_wait_for_a_straggler_thread() -> None:
    """A bounded batch must return at its deadline even on the sync path.

    Sync work cannot be cancelled, so the pool is abandoned (`wait=False`): its thread finishes on
    its own budget and its result is discarded. Waiting instead -- which is what the shared default
    executor does when the loop closes -- would make the total-batch deadline a fiction for every
    caller that passes a plain callable.
    """
    jobs = [{"url": f"https://stall{i}.example/", "payload": {"i": i}} for i in range(2)]

    def hanging_fetch(_job: dict[str, object], _url: str, _timeout_s: int) -> str:
        time.sleep(5.0)
        return "<html>late</html>"

    started = time.perf_counter()
    results = fetch_pages_batched(
        1,
        jobs,
        sync_fetch=hanging_fetch,
        total_concurrency=2,
        per_host_concurrency=2,
    )
    elapsed = time.perf_counter() - started

    assert elapsed < 4.0, f"batch took {elapsed:.1f}s against its 2 s deadline"
    assert [row["ok"] for row in results] == [False, False]
    assert all("total batch deadline exceeded" in str(row["error"]) for row in results)


def test_fetch_pages_batched_uses_async_fetch_when_provided() -> None:
    jobs = [
        {"url": "https://async.example/a", "payload": {"id": "a"}},
        {"url": "https://async.example/b", "payload": {"id": "b"}},
    ]
    sync_calls: list[str] = []
    async_calls: list[str] = []

    def fake_sync_fetch(job: dict[str, object], url: str, _: int) -> str:
        sync_calls.append(url)
        raise AssertionError("sync path should not be used when async_fetch is provided")

    async def fake_async_fetch(client, job: dict[str, object], url: str, _: int) -> str:
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


def test_real_http_batch_host_ready_lets_fast_hosts_finish_before_slow_host() -> None:
    """Direct-coverage: the SHIPPED fetch_pages_batched uses host-ready acquisition.

    Reviewed in P3 as the one gap in the fairness experiment: that test models the scheduler's
    semaphore order locally instead of driving the real function, so a future revert of the
    `async with host_sem: async with total_sem` ordering in `_fetch_one` would go undetected.
    This test drives the real `fetch_pages_batched` with a grouped fixture: one slow host and
    several fast hosts, slow jobs first in the list. Under host-ready, the fast-host jobs finish
    while the slow host is still running its serial work; under total-before-host they are starved
    and finish only as slow slots free. It fails if the ordering ever regresses.
    """
    import time

    slow_delay = 0.25
    fast_cap = 2
    slow_host = "slow.example"
    fast_hosts = [f"fast-{i}.example" for i in range(3)]

    # Slow jobs first in the list reproduces the worst-case starvation input.
    slow_jobs = [{"url": f"https://{slow_host}/s{i}", "payload": {}} for i in range(6)]
    fast_jobs = [
        {"url": f"https://{host}/f{i}", "payload": {}} for host in fast_hosts for i in range(4)
    ]
    jobs = slow_jobs + fast_jobs

    fast_done_at: dict[str, float] = {}
    slow_done_at: dict[str, float] = {}
    started = time.perf_counter()

    async def fake_async_fetch(_client, job: dict[str, object], url: str, _t: int) -> str:
        if slow_host in url:
            await __import__("asyncio").sleep(slow_delay)
            slow_done_at[url] = time.perf_counter() - started
        else:
            fast_done_at[url] = time.perf_counter() - started
        return f"<html>{url}</html>"

    results = fetch_pages_batched(
        5,
        jobs,
        sync_fetch=lambda _job, _url, _t: (_ for _ in ()).throw(AssertionError("sync unused")),
        async_fetch=fake_async_fetch,
        total_concurrency=6,
        per_host_concurrency=fast_cap,
    )

    assert len(results) == len(jobs)
    assert all(row.get("ok") for row in results)
    assert len(fast_done_at) == len(fast_jobs)
    assert len(slow_done_at) == len(slow_jobs)

    # Host-ready: fast-host jobs complete promptly (well before slow serial throughput ends).
    # 6 slow jobs at .25s with per_host_cap=2 => 3 waves => ~0.75s+ of slow serial work.
    # Fast jobs finish in a single early wave, so their max completion must be far below that.
    fast_max = max(fast_done_at.values())
    slow_max = max(slow_done_at.values())
    # A starved (total-before-host) ordering would push fast_max up to ~slow_max; host-ready
    # must keep it below half the slow completion time. Generous margin to avoid flake.
    assert fast_max < slow_max * 0.5, (
        f"fast-host max completion {fast_max:.3f}s not early relative to slow max {slow_max:.3f}s "
        f"— total-before-host starvation likely regressed"
    )
