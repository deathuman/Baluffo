"""The late-entry interleaving: a thread that enters the pool but not `in_flight` tracking before the
batch deadline closes the client under it.

`run_bounded_probe_batch_async` has two `_InFlightFetches` counters: `pool_in_flight` tracks threads
running in the pool, `in_flight` tracks threads that are actively using the shared httpx client. A
thread that has called `pool_in_flight.enter()` but not yet `in_flight.enter()` is invisible to
`retire_when_idle`, so a batch deadline that fires while a thread is in that gap sees the in_flight
count as zero and closes the client — and the thread's subsequent `in_flight.enter()` races against a
closed socket.

These tests reproduce that gap at the `_InFlightFetches` unit level (the gap is between the two
enter calls, so the unit is the correct scope for the discriminating oracle).

Test style follows the repo convention (`asyncio.run` inside sync test functions; there are no
`async def test_` functions in this suite).
"""

from __future__ import annotations

import threading
import time

import httpx

from src.source_discovery.probe_runtime import _InFlightFetches

_LONG_S = 30.0


def test_late_entry_gap_must_not_close_client() -> None:
    """Simulate the gap using _InFlightFetches directly.

    Two threads: thread A enters and leaves (simulating a completed fetch), thread B is in the gap
    (has not called in_flight.enter yet). ``retire_when_idle`` sees count=0 and must NOT close the
    client because thread B is about to enter.
    """
    in_flight = _InFlightFetches()
    client = httpx.Client()
    assert not client.is_closed

    about_to_enter = threading.Event()
    proceed = threading.Event()
    entered_in_flight = threading.Event()
    release_inside = threading.Event()
    left_in_flight = threading.Event()

    def thread_a() -> None:
        in_flight.enter()
        try:
            _ = client.get("http://localhost:1", timeout=0.1)
        except Exception:
            pass  # Connection refused/error is expected
        finally:
            in_flight.leave()

    def thread_b() -> None:
        about_to_enter.set()
        assert proceed.wait(timeout=_LONG_S), "thread B never got the proceed signal"
        # The gap is HERE: between about_to_enter and in_flight.enter().
        # If retire_when_idle closed the client during this window, the check below catches it.
        assert not client.is_closed, "client was closed before thread B could enter in_flight"
        in_flight.enter()
        entered_in_flight.set()
        # Hold inside in_flight until the main thread has verified the client is open.
        assert release_inside.wait(timeout=_LONG_S), "thread B was never released from in_flight"
        # Even if connection fails, the client must be open.
        assert not client.is_closed, "client was closed when thread B tried to use it"
        in_flight.leave()
        left_in_flight.set()

    # Thread A enters and leaves in_flight.
    ta = threading.Thread(target=thread_a, name="test-thread-a")
    ta.start()
    ta.join(timeout=10)
    assert not ta.is_alive(), "thread A did not finish"

    # Thread B signals it is in the gap (about to enter).
    tb = threading.Thread(target=thread_b, name="test-thread-b")
    tb.start()
    assert about_to_enter.wait(timeout=10), "thread B never reached the gap"

    # retire_when_idle with count=0 AND pool_still_active=True.
    # In the real batch code, pool_still_active is True when pool_in_flight.outstanding() > 0,
    # which means a thread may be about to enter in_flight. The old code would close immediately,
    # but the fix defers the close via _retiring so the late-entering thread finds the client open.
    in_flight.retire_when_idle(client, defer_if_pool_active=True)

    # Let thread B proceed. It should find the client open (the defer worked).
    proceed.set()
    assert entered_in_flight.wait(timeout=10), "thread B never entered in_flight"
    # Thread B is now inside in_flight (between enter() and leave()), waiting on release_inside.
    # The client must still be open: retire_when_idle deferred, and the thread is actively using it.
    assert not client.is_closed, (
        "client was closed while thread B was inside in_flight: "
        "the defer should have kept it open until the last leave()"
    )
    # Release thread B so it can leave in_flight.
    release_inside.set()
    assert left_in_flight.wait(timeout=10), "thread B never left in_flight"
    tb.join(timeout=10)
    assert not tb.is_alive(), "thread B did not finish"
    # Thread B's leave() closed the client (last-fetch-out). This is correct: the close happened
    # after the thread was done, not before it could enter.
    assert client.is_closed, "client must be closed after the last thread leaves in_flight"


def test_genuinely_idle_client_must_still_close() -> None:
    """Control: when no thread is in the gap, retire_when_idle MUST close the client."""
    in_flight = _InFlightFetches()
    client = httpx.Client()
    assert not client.is_closed

    def worker() -> None:
        in_flight.enter()
        try:
            time.sleep(0.01)
        finally:
            in_flight.leave()

    t = threading.Thread(target=worker)
    t.start()
    t.join(timeout=10)

    assert in_flight.outstanding() == 0
    in_flight.retire_when_idle(client)
    assert client.is_closed, "a genuinely idle client was not closed by retire_when_idle"
