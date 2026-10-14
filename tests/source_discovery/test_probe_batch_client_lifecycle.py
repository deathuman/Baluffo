"""The probe batch's shared httpx client, and who owns it when a fetch outlives its own deadline.

`run_bounded_probe_batch_async` hands every default-path fetch one shared `httpx.Client`. A sync fetch
runs in a pool thread and cannot be cancelled, so a fetch whose per-call `asyncio.wait_for` bound has
fired keeps reading after the batch returns -- and the batch used to let a `with httpx.Client(...)`
block retire the client at that point. Measured on a loopback dripper
(`_out/perf-runs/discovery/probe_client_lifecycle_probe.py --arm eager`): the client closed with one
fetch still inside it, and that request died with `ReadError: [WinError 10038] An operation was
attempted on something that is not a socket` at the same instant the batch returned. `httpx.Client
.__exit__` is what makes that possible: in httpx 0.28 it closes the transport *without* calling
`close()`.

These two tests hold a fetch past its bound on each exit from the batch -- the normal one and the
deadline one, where the probe task itself is cancelled -- and assert the order of events. Both are shown
to fail against the old eager close by
`_out/perf-runs/discovery/verify_client_lifecycle_test_discriminates.py`.

The rest of the batch's contracts (the per-call and batch bounds, the default httpx path, the lane
count) live in `test_probe_runtime_bounded_fetch.py`; the permit order lives in
`test_probe_batch_permits.py`.

Test style follows the repo convention (`asyncio.run` inside sync test functions; there are no
`async def test_` functions in this suite).
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

from src.source_discovery import probe_runtime


async def _probe_calls_fetcher_once(
    row: dict[str, Any],
    timeout_s: int,
    *,
    fetcher: Any,
    **_kwargs: Any,
) -> tuple[bool, int, str]:
    """Probe body used here: one fetch of the row's own URL, then success."""
    await fetcher(f"https://example.invalid/{row.get('id')}", timeout_s)
    return True, 1, ""


# --- the client lifecycle: the batch's client is closed only after its last fetch leaves ----------

# Longer than the per-call bound below, so the fetch is still inside the client when the batch returns.
_ABANDONED_FETCH_S = 1.5


class _RecordingClient:
    """Stands in for the batch's `httpx.Client`: records whether it was closed, when, and by whom."""

    def __init__(self, **_kwargs: Any) -> None:
        self.closed = False
        self.closed_at: float | None = None
        self.closed_from: str | None = None

    def close(self) -> None:
        self.closed = True
        self.closed_at = time.perf_counter()
        self.closed_from = threading.current_thread().name


def test_the_shared_client_closes_only_after_its_last_fetch_has_left(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The client every fetch shares is closed after the last one leaves it, never under one.

    The fetch here outlives the batch on purpose: its per-call bound fires at `timeout_s` and the
    thread keeps going, which is the state the batch cannot cancel. The assertions are the order of
    events -- the batch returns with the fetch still inside the client, the client is still open at
    that moment and when the fetch exits, and it closes afterwards, from the fetch's own thread (no
    parked waiter, and nothing double-closes).
    """
    clients: list[_RecordingClient] = []

    def client_factory(**_kwargs: Any) -> _RecordingClient:
        client = _RecordingClient(**_kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr(probe_runtime.httpx, "Client", client_factory)

    release = threading.Event()
    fetch_exited = threading.Event()
    state: dict[str, Any] = {"entered_at": None, "exit_at": None, "closed_before_use": None}

    def fake_sync_fetch(client: _RecordingClient, url: str, timeout_s: int) -> str:
        state["entered_at"] = time.perf_counter()
        state["closed_before_use"] = client.closed
        try:
            release.wait(timeout=_ABANDONED_FETCH_S + 6)
            state["closed_while_inside"] = client.closed
            return f"<html>{url}</html>"
        finally:
            state["exit_at"] = time.perf_counter()
            fetch_exited.set()

    monkeypatch.setattr(probe_runtime, "fetch_text_httpx", fake_sync_fetch)

    candidates = [{"id": "c0", "adapter": "static"}]
    elapsed = 0.0

    async def _inner() -> list[Any]:
        nonlocal elapsed
        started = time.perf_counter()
        try:
            return await asyncio.wait_for(
                probe_runtime.run_bounded_probe_batch_async(
                    candidates,
                    timeout_s=1,
                    fetcher=probe_runtime.fetch_text,  # IS default_fetcher -> the batch owns a client
                    async_probe=_probe_calls_fetcher_once,
                    default_fetcher=probe_runtime.fetch_text,
                ),
                timeout=30,
            )
        finally:
            elapsed = time.perf_counter() - started

    results = asyncio.run(_inner())

    assert len(results) == 1, "one result per candidate is the contract"
    _row, ok, _jobs, error, _ms = results[0]
    assert not ok and "timed out" in str(error), (
        "the abandoned fetch is still reported as a per-candidate timeout"
    )
    assert elapsed < _ABANDONED_FETCH_S, (
        f"the batch took {elapsed:.2f}s; a fetch that outlives its bound must not hold the batch open"
    )
    in_flight = len(clients) == 1 and not clients[0].closed
    assert in_flight, (
        "the batch returned with its client already closed while the fetch was still in it"
    )
    assert state["closed_before_use"] is False, "the fetch must not start against a closed client"

    release.set()
    assert fetch_exited.wait(timeout=5), "the abandoned fetch never finished"

    assert state.get("closed_while_inside") is False, (
        "the client was closed while a fetch was still inside it: a sync request cannot be cancelled, "
        "so the close has to wait for it"
    )
    deadline = time.perf_counter() + 2.0
    while not clients[0].closed and time.perf_counter() < deadline:
        time.sleep(0.01)
    client = clients[0]
    assert client.closed, "the client must still be closed once its last fetch has left"
    assert client.closed_at is not None and client.closed_at >= state["exit_at"], (
        "the close must not precede the fetch that was inside the client"
    )
    assert str(client.closed_from or "").startswith("probe-fetch"), (
        f"the last fetch out closes the client itself; closed from {client.closed_from!r} instead, "
        f"which would mean a waiter thread is parked on it"
    )


def test_the_batch_deadline_also_waits_for_outstanding_fetches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The other exit from the batch: the deadline cancels tasks with fetches still in the client.

    `_cancel_pending` cancels the tasks, not the threads behind them, so this is the exit where the
    most work is abandoned at once -- and the client must outlive those fetches here too.
    """
    clients: list[_RecordingClient] = []

    def client_factory(**_kwargs: Any) -> _RecordingClient:
        client = _RecordingClient(**_kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr(probe_runtime.httpx, "Client", client_factory)

    release = threading.Event()
    fetch_entered = threading.Event()
    fetch_exited = threading.Event()
    state: dict[str, Any] = {"closed_while_inside": None}

    def fake_sync_fetch(client: _RecordingClient, url: str, timeout_s: int) -> str:
        fetch_entered.set()
        try:
            release.wait(timeout=_ABANDONED_FETCH_S + 6)
            state["closed_while_inside"] = client.closed
            return f"<html>{url}</html>"
        finally:
            fetch_exited.set()

    monkeypatch.setattr(probe_runtime, "fetch_text_httpx", fake_sync_fetch)

    async def probe_that_outlives_the_deadline(
        row: dict[str, Any], timeout_s: int, *, fetcher: Any, **_kwargs: Any
    ) -> tuple[bool, int, str]:
        try:
            await fetcher(str(row.get("id")), timeout_s)
        except TimeoutError:
            pass  # the per-call bound fired; the thread behind it is still parked in the client
        await asyncio.sleep(10)  # keep the task alive past the batch's own deadline
        return True, 1, ""

    candidates = [{"id": "c0", "adapter": "static"}]
    elapsed = 0.0

    async def _inner() -> list[Any]:
        nonlocal elapsed
        started = time.perf_counter()
        try:
            return await asyncio.wait_for(
                probe_runtime.run_bounded_probe_batch_async(
                    candidates,
                    timeout_s=1,
                    fetcher=probe_runtime.fetch_text,  # IS default_fetcher -> the batch owns a client
                    async_probe=probe_that_outlives_the_deadline,
                    default_fetcher=probe_runtime.fetch_text,
                ),
                timeout=30,
            )
        finally:
            elapsed = time.perf_counter() - started

    results = asyncio.run(_inner())

    assert fetch_entered.is_set(), "the fetch must have started for this to be the case under test"
    assert len(results) == 1 and not results[0][1], "a deadline leaves a failure row per candidate"
    assert "total probe deadline exceeded" in str(results[0][3]), results
    assert elapsed < 5, f"the batch hit its own deadline but took {elapsed:.1f}s to return"
    assert len(clients) == 1, "the freed client must be the refutable one"
    assert clients[0].closed is False, (
        "the deadline exit closed the client while a fetch was still inside it"
    )

    release.set()
    assert fetch_exited.wait(timeout=5), "the abandoned fetch never finished"
    assert state["closed_while_inside"] is False, "the client was closed under the abandoned fetch"
    deadline = time.perf_counter() + 2.0
    while not clients[0].closed and time.perf_counter() < deadline:
        time.sleep(0.01)
    assert clients[0].closed, "the client must still be closed once its last fetch has left"
