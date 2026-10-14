"""The probe batch's two permit seams, and which one carries a wait.

`run_bounded_probe_batch_async` bounds a candidate twice: the pool (`total`) and its own family's
bucket (`static` / `provider` / `teamtailor`). The wave takes the bucket permit first, so a candidate
whose family is saturated waits while holding nothing another family needs. Taken the other way round
the queue consumes the wave's lanes -- measured on the audit's 3,038 candidates as 8.7 of 40 pool
permits held by blocked candidates while the provider family, four fifths of the population, probed at
21 of the 40 lanes its cap allows. The two tests here pin that order, and are shown to fail against a
pool-first twin of the shipped function by
`_out/perf-runs/discovery/verify_permit_order_tests_discriminate.py`.

The batch's fetch bounds and lane contracts live in `test_probe_runtime_bounded_fetch.py`; the shared
client's lifecycle in `test_probe_batch_client_lifecycle.py`.

Test style follows the repo convention (`asyncio.run` inside sync test functions; there are no
`async def test_` functions in this suite).
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.source_discovery import probe_runtime

# --- the permit-order contract: a blocked bucket must not hold a pool permit --------------------

# How long a parked probe waits for the other family to reach a permit before it calls the order
# wrong. The pass path never waits at all (the other family enters as soon as it has a permit), so
# this bound is only ever paid by the defect it detects, and it stays well inside the batch's own
# deadline (`timeout_s * rows/concurrency + timeout_s`, 5 s at these settings).
_PERMIT_ORDER_TIMEOUT_S = 1.5


def _run_at_limits(
    candidates: list[dict[str, Any]],
    async_probe: Any,
    *,
    timeout_s: int = 2,
) -> list[Any]:
    """Run the batch under whatever lane limits the caller patched into the environment."""

    async def _inner() -> list[Any]:
        return await asyncio.wait_for(
            probe_runtime.run_bounded_probe_batch_async(
                candidates,
                timeout_s=timeout_s,
                fetcher=probe_runtime.fetch_text,  # IS default_fetcher
                async_probe=async_probe,
                default_fetcher=probe_runtime.fetch_text,
            ),
            timeout=30,
        )

    return asyncio.run(_inner())


def _permit_order_case(
    *,
    static_rows: int,
    provider_rows: int,
    providers_that_must_fit: int,
) -> tuple[list[Any], dict[str, Any]]:
    """Park the family that is capped narrowest, and see what the other family can still start.

    `s0` is the first static task submitted, so it takes static's only bucket permit; every further
    static row queues behind that seam. The provider rows can only enter their probe bodies if the
    pool permits exist for them while `s0` is parked -- which is exactly what the old acquire order
    spent on the static queue instead.
    """
    candidates = [{"id": f"s{i}", "adapter": "static"} for i in range(static_rows)]
    candidates += [{"id": f"p{i}", "adapter": "greenhouse"} for i in range(provider_rows)]
    state: dict[str, Any] = {
        "static_inside": 0,
        "static_peak": 0,
        "provider_peak": 0,
        "providers_inside_with_static_parked": 0,
        "providers_that_must_fit": providers_that_must_fit,
        "parked_timeout": False,
    }
    providers_entered = asyncio.Event()

    async def probe(row: dict[str, Any], _timeout_s: int, **_kwargs: Any) -> tuple[bool, int, str]:
        if str(row.get("adapter")) == "static":
            state["static_inside"] += 1
            state["static_peak"] = max(state["static_peak"], state["static_inside"])
            try:
                if row.get("id") == "s0":
                    # `s0` is the parked one: it holds the bucket permit and must not need a pool
                    # permit to keep holding it, so the provider rows are free to take theirs.
                    try:
                        await asyncio.wait_for(
                            providers_entered.wait(), timeout=_PERMIT_ORDER_TIMEOUT_S
                        )
                    except TimeoutError:
                        state["parked_timeout"] = True
            finally:
                state["static_inside"] -= 1
            return True, 1, ""
        state["provider_peak"] = max(state["provider_peak"], state["provider_peak"] + 1)
        if state["provider_peak"] >= providers_that_must_fit:
            state["providers_inside_with_static_parked"] = state["static_inside"]
            providers_entered.set()
        # Hold the permit briefly so the peak is observed, not raced past. `sleep(0)` yields only to
        # the loop, which is enough: the parked static task is awaiting the same loop's event.
        await asyncio.sleep(0)
        state["provider_peak"] -= 1
        return True, 1, ""

    return _run_at_limits(candidates, probe), state


def test_a_bucket_blocked_candidate_cannot_hold_a_pool_permit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A row queued for its own family's permit must hold no pool permit while it waits.

    Pool 2, static 1: the shipped order used to spend the second pool permit on the *second* static
    row, so the provider row could not start until a static probe retired -- measured on the audit's
    population as **6.8 of 40 pool permits held by static's queue**, all of the blocked permit-time
    in the wave. With the bucket acquired first, the queued static row holds nothing, and the
    provider row starts at once.
    """
    monkeypatch.setenv("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TOTAL", "2")
    monkeypatch.setenv("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_STATIC", "1")
    monkeypatch.setenv("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_PROVIDER", "2")

    results, state = _permit_order_case(static_rows=2, provider_rows=1, providers_that_must_fit=1)

    assert not state["parked_timeout"], (
        "the parked static probe never saw the provider row start: a candidate waiting on a full "
        "bucket is holding a pool permit, so the pool is spent on the bucket queue"
    )
    assert state["providers_inside_with_static_parked"] == 1, (
        "the provider row must start while the static bucket's queue is parked"
    )
    assert len(results) == 3, "one result per candidate is the contract"
    by_id = {row["id"]: ok for row, ok, _jobs, _error, _ms in results}
    assert by_id == {"s0": True, "s1": True, "p0": True}, (
        "the permit order must not change any candidate's identity or outcome"
    )


def test_a_saturated_bucket_leaves_the_pool_to_the_other_families(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Bucket-first must not raise the family cap to reach the pool's lane count.

    Pool 4, static 1, four static rows and three provider rows: the pool is the binding resource, so
    all three provider rows probe *while* static's single lane is parked on a queue of three. The
    static peak stays at its cap of 1, so the swap moves which permit carries the wait and not what
    any family's politeness limit allows.
    """
    monkeypatch.setenv("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TOTAL", "4")
    monkeypatch.setenv("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_STATIC", "1")
    monkeypatch.setenv("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_PROVIDER", "4")

    results, state = _permit_order_case(static_rows=4, provider_rows=3, providers_that_must_fit=3)

    assert not state["parked_timeout"], (
        "three provider rows could not reach the pool while one static row held its bucket permit "
        "and three more static rows queued -- the queue is holding pool permits"
    )
    assert state["providers_inside_with_static_parked"] == 1, (
        "the provider family must probe at the pool's lane count while static's cap is saturated"
    )
    assert state["static_peak"] == 1, (
        f"the static bucket ran {state['static_peak']} probe bodies at once; the fix moves which "
        f"permit carries the wait and must leave every family cap where it was"
    )
    assert len(results) == 7, "one result per candidate is the contract"
    assert {row["id"] for row, _ok, _j, _e, _ms in results} == {f"s{i}" for i in range(4)} | {
        f"p{i}" for i in range(3)
    }
