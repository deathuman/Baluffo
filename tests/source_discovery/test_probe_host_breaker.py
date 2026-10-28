"""The per-wave probe host breaker.

The probe's tail on the 2026-10-28 profile is not slowness, it is whole hosts timing out in
bursts: Lever's 404s are half a second, but its timeouts land 50 s into the run and 18 of them
cluster in three waves. The breaker skips the remainder of a host's wave once it has timed out
`BALUFFO_DISCOVERY_PROBE_CIRCUIT_BREAKER_TIMEOUTS` times, which is the only intervention that
cannot hurt a healthy board: a host that answered before is left alone, and a host that timed
out is retried in the next batch.

The knob defaults off, so the shipped audit's probe is byte-identical until an arm sets it. These
tests pin the off default as hard as the on behaviour, because a breaker that fires without being
asked for would quietly drop candidate boards. The matching measurement
(`docs/snapshots/discovery-probe-tail-cuts-2026-10-28.md`) is why the default stays off: the breaker
loses no board on the audited population, but its saving tracks how many timeouts the session
happens to have and could not be shown live.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from src.source_discovery import probe_runtime


def _candidate(host: str, index: int) -> dict[str, Any]:
    url = f"https://{host}/careers"
    return {
        "name": f"Studio {index}",
        "studio": f"Studio {index}",
        "adapter": "static",
        "listing_url": url,
        "pages": [url],
        "discoveryMethod": "test",
        "evidenceScore": 40,
    }


async def _probe_once(
    row: dict[str, Any],
    timeout_s: int,
    *,
    fetcher: Any,
    **_kwargs: Any,
) -> tuple[bool, int, str]:
    await fetcher(str(row["listing_url"]), timeout_s)
    return True, 1, ""


class _Fetcher:
    """Times out for the hosts it is told to, and records every host it was actually asked for."""

    def __init__(self, timeout_hosts: set[str]) -> None:
        self.timeout_hosts = timeout_hosts
        self.calls: list[str] = []

    def __call__(self, url: str, timeout_s: int) -> str:
        host = url.split("/")[2]
        self.calls.append(host)
        if host in self.timeout_hosts:
            raise TimeoutError("socket read never returned")
        return "<html>ok</html>"


def _run(candidates: list[dict[str, Any]], fetcher: _Fetcher, *, timeout_s: int = 1) -> list[Any]:
    return asyncio.run(
        probe_runtime.run_bounded_probe_batch_async(
            candidates,
            timeout_s=timeout_s,
            fetcher=fetcher,
            async_probe=_probe_once,
            default_fetcher=lambda url, timeout_s: "<html>default</html>",
        )
    )


def _errors(results: list[Any]) -> list[str]:
    return [str(result[3]) for result in results]


@pytest.fixture
def serialized_lanes(monkeypatch: pytest.MonkeyPatch) -> None:
    """One lane, so wave order is candidate order and the breaker's effect is deterministic."""
    monkeypatch.setenv("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_TOTAL", "1")
    monkeypatch.setenv("BALUFFO_DISCOVERY_PROBE_CONCURRENCY_STATIC", "1")


def test_breaker_skips_the_rest_of_a_timed_out_hosts_wave(
    monkeypatch: pytest.MonkeyPatch, serialized_lanes: None
) -> None:
    monkeypatch.setenv("BALUFFO_DISCOVERY_PROBE_CIRCUIT_BREAKER_TIMEOUTS", "1")
    fetcher = _Fetcher(timeout_hosts={"jobs.lever.co"})
    candidates = [
        _candidate("jobs.lever.co", 0),
        _candidate("jobs.lever.co", 1),
        _candidate("jobs.lever.co", 2),
    ]

    results = _run(candidates, fetcher)

    assert len(results) == 3, "every candidate still gets a result row"
    assert fetcher.calls == ["jobs.lever.co"], (
        "after the breaker opens, the host's remaining candidates must not be fetched"
    )
    assert "timed out" in _errors(results)[0]
    assert all("circuit open" in error for error in _errors(results)[1:])


def test_breaker_leaves_other_hosts_alone(
    monkeypatch: pytest.MonkeyPatch, serialized_lanes: None
) -> None:
    monkeypatch.setenv("BALUFFO_DISCOVERY_PROBE_CIRCUIT_BREAKER_TIMEOUTS", "1")
    fetcher = _Fetcher(timeout_hosts={"jobs.lever.co"})
    candidates = [
        _candidate("jobs.lever.co", 0),
        _candidate("acme.example", 1),
        _candidate("jobs.lever.co", 2),
    ]

    results = _run(candidates, fetcher)

    assert fetcher.calls == ["jobs.lever.co", "acme.example"], (
        "opening one host must not cost another host its probe"
    )
    assert results[1][1] is True, "the other host's candidate is a success"
    assert "circuit open" in _errors(results)[2]


def test_breaker_is_off_unless_asked_for(
    monkeypatch: pytest.MonkeyPatch, serialized_lanes: None
) -> None:
    monkeypatch.delenv("BALUFFO_DISCOVERY_PROBE_CIRCUIT_BREAKER_TIMEOUTS", raising=False)
    fetcher = _Fetcher(timeout_hosts={"jobs.lever.co"})
    candidates = [_candidate("jobs.lever.co", index) for index in range(3)]

    results = _run(candidates, fetcher)

    assert fetcher.calls == ["jobs.lever.co"] * 3, (
        "with the breaker off every candidate is fetched, exactly as before the knob existed"
    )
    assert all("timed out" in error for error in _errors(results))
