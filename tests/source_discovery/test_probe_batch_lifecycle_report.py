"""The probe batch's own receipt: abandoned fetches, pool and client retirement, in the artifact.

The batch's contract is one result per candidate, and the artifact said nothing about the other
question a live run has to answer -- what the wave did with the pool and the shared client it
created. A sync fetch cannot be cancelled, so a per-call bound can abandon one; the abandoned fetch
is what keeps a pool thread, and (on the default path) the batch's `httpx.Client`, alive after the
batch has returned. Before this receipt, a batch that closed its client under a live fetch and one
that retired it cleanly produced the same artifact.

The receipt is deliberately *live*: the deferred close lands on the abandoned fetch's own thread
after the batch has returned, so a run reads the same object again when it writes its artifact.
`test_the_receipt_settles_when_the_abandoned_fetch_closes_the_client` is that property, and
`test_the_rollup_flags_a_client_that_never_closed` is the flag an operator reads. The audit artifact's
copy is pinned end to end in the last test.

The probe's other contracts live beside this module: the per-call and batch bounds, the default httpx
path and the lane count in `test_probe_runtime_bounded_fetch.py`, the client's close ordering in
`test_probe_batch_client_lifecycle.py`, and the permit order in `test_probe_batch_permits.py`.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from pathlib import Path
from typing import Any

import pytest

from src.source_discovery import gamedevmap_active_dry_run as dry_run
from src.source_discovery import gamedevmap_artifact_store as artifact_store
from src.source_discovery import probe_runtime
from src.source_discovery.gamedevmap_artifact_store import _probe_lifecycle_rollup

from ._helpers import sd, workspace_tmpdir
from .gamedevmap_test_helpers import (
    gamedevmap_config as _config,
)
from .gamedevmap_test_helpers import (
    gamedevmap_fetcher as _fetcher,
)
from .gamedevmap_test_helpers import (
    gamedevmap_payloads as _payloads,
)

_DEFAULT_FETCHER = probe_runtime.fetch_text


class _RecordingClient:
    """Stands in for the batch's `httpx.Client`: records that a close happened, and from where."""

    def __init__(self, **_kwargs: Any) -> None:
        self.closed = False
        self.closed_from: str | None = None

    def close(self) -> None:
        self.closed = True
        self.closed_from = threading.current_thread().name


async def _probe_calls_fetcher_once(
    row: dict[str, Any],
    timeout_s: int,
    *,
    fetcher: Any,
    **_kwargs: Any,
) -> tuple[bool, int, str]:
    await fetcher(f"https://example.invalid/{row.get('id')}", timeout_s)
    return True, 1, ""


def _run_batch(
    candidates: list[dict[str, Any]],
    *,
    timeout_s: int,
    lifecycle: probe_runtime.ProbeBatchLifecycle,
    async_probe: Any = _probe_calls_fetcher_once,
    outer_timeout: float = 30,
) -> tuple[list[Any], float]:
    async def _inner() -> list[Any]:
        return await asyncio.wait_for(
            probe_runtime.run_bounded_probe_batch_async(
                candidates,
                timeout_s=timeout_s,
                fetcher=_DEFAULT_FETCHER,  # IS default_fetcher -> the batch owns a client
                async_probe=async_probe,
                default_fetcher=_DEFAULT_FETCHER,
                lifecycle=lifecycle,
            ),
            timeout=outer_timeout,
        )

    started = time.perf_counter()
    results = asyncio.run(_inner())
    return results, time.perf_counter() - started


# --- a batch that abandons nothing ----------------------------------------------------------------


def test_a_clean_batch_reports_a_batch_thread_close_and_no_outstanding_fetches(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nothing is abandoned, so the batch retires its own client and says so.

    This is the row an operator should see on an ordinary run: every fetch it started finished
    before the batch returned, the client was closed by the batch's own thread (not deferred to a
    straggler), and no pool thread is left holding anything.
    """
    clients: list[_RecordingClient] = []

    def client_factory(**_kwargs: Any) -> _RecordingClient:
        client = _RecordingClient(**_kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr(probe_runtime.httpx, "Client", client_factory)
    monkeypatch.setattr(
        probe_runtime, "fetch_text_httpx", lambda _client, url, _timeout: f"<html>{url}</html>"
    )

    candidates = [{"id": f"c{i}", "adapter": "static"} for i in range(4)]
    lifecycle = probe_runtime.ProbeBatchLifecycle()
    results, _elapsed = _run_batch(candidates, timeout_s=5, lifecycle=lifecycle)

    assert len(results) == len(candidates)
    assert all(row[1] for row in results), "every candidate here succeeds"
    assert len(clients) == 1 and clients[0].closed

    receipt = lifecycle.snapshot()
    assert receipt["poolWorkers"] == 4, "the pool is sized to the batch's lanes"
    assert receipt["fetchesStarted"] == 4
    assert receipt["fetchesThroughClient"] == 4, "the default path carries every fetch through it"
    assert receipt["fetchesBoundedOut"] == 0, "nothing here outlives its per-call bound"
    assert receipt["fetchesOutstandingAtExit"] == 0
    assert receipt["clientFetchesOutstandingAtExit"] == 0
    assert receipt["clientClosed"] is True
    assert receipt["clientRetiredBy"] == "batch"
    assert receipt["clientClosedFrom"] and not receipt["clientClosedFrom"].startswith(
        "probe-fetch"
    ), (
        "the batch's own thread closes an idle client; a probe-fetch close would mean it was deferred"
    )
    assert receipt["clientCloseDeferredMs"] < 1000, "an idle close is not deferred"
    assert receipt["deadlineExit"] is False


# --- a batch that abandons a fetch ----------------------------------------------------------------


def test_an_abandoned_fetch_is_reported_and_the_receipt_settles_when_it_closes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fetch past its per-call bound is visible twice: at the batch's exit, and once it is out.

    The first read is what a run sees immediately -- the batch returned with the fetch still inside
    the client, so the client is not closed and the count says by how much. The second read is why
    the receipt is a live object: the abandoned fetch closes the client itself when it finally
    returns, and its own thread writes that into the same receipt the run already holds.
    """
    clients: list[_RecordingClient] = []

    def client_factory(**_kwargs: Any) -> _RecordingClient:
        client = _RecordingClient(**_kwargs)
        clients.append(client)
        return client

    monkeypatch.setattr(probe_runtime.httpx, "Client", client_factory)

    release = threading.Event()
    fetch_exited = threading.Event()

    def dripping_fetch(_client: _RecordingClient, url: str, _timeout: int) -> str:
        try:
            release.wait(timeout=6)
            return f"<html>{url}</html>"
        finally:
            fetch_exited.set()

    monkeypatch.setattr(probe_runtime, "fetch_text_httpx", dripping_fetch)

    lifecycle = probe_runtime.ProbeBatchLifecycle()
    results, elapsed = _run_batch(
        [{"id": "c0", "adapter": "static"}], timeout_s=1, lifecycle=lifecycle
    )

    _row, ok, _jobs, error, _ms = results[0]
    assert not ok and "timed out" in str(error), "the abandoned fetch is a per-candidate timeout"

    at_exit = lifecycle.snapshot()
    assert elapsed < 1.5, f"the batch waited {elapsed:.2f}s for the fetch it abandoned"
    assert at_exit["fetchesStarted"] == 1
    assert at_exit["fetchesBoundedOut"] == 1, "the per-call bound fired on this fetch"
    assert at_exit["fetchesOutstandingAtExit"] == 1, "its thread is still running"
    assert at_exit["clientFetchesOutstandingAtExit"] == 1, "and it is still inside the client"
    assert at_exit["clientClosed"] is False, (
        "reporting a close here would be the defect this receipt exists to expose"
    )
    assert at_exit["clientRetiredBy"] == "unclosed"
    assert at_exit["deadlineExit"] is False, "the batch returned on its own, not on its deadline"

    # The deferral is measured against the batch's exit and reported in whole milliseconds, so hold
    # the fetch open briefly: released at once, the close lands inside the exit's own millisecond and
    # reads as 0. A real straggler holds it for the rest of its call, which is what the field sizes.
    time.sleep(0.05)
    release.set()
    assert fetch_exited.wait(timeout=5), "the abandoned fetch never finished"

    deadline = time.perf_counter() + 2.0
    settled = lifecycle.snapshot()
    while not settled["clientClosed"] and time.perf_counter() < deadline:
        time.sleep(0.01)
        settled = lifecycle.snapshot()

    assert settled["clientClosed"] is True, "the last fetch out must still retire the client"
    assert settled["clientRetiredBy"] == "last-fetch", (
        "a deferred close is what keeps the batch's clock off the straggler"
    )
    assert str(settled["clientClosedFrom"] or "").startswith("probe-fetch"), (
        f"closed from {settled['clientClosedFrom']!r}: the fetch itself closes it, not a waiter"
    )
    assert settled["clientCloseDeferredMs"] >= 20, (
        "the close landed after the batch's exit, by its own measured deferral"
    )


def test_a_batch_that_left_on_its_deadline_reports_that_exit(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The deadline exit is the one that abandons the most work, and it is named as such.

    `_cancel_pending` cancels the tasks, never the threads behind them, so the receipt has to say
    which exit abandoned them -- a run reading `fetchesOutstandingAtExit` on a deadline arm is
    looking at fetches the wave walked away from on purpose.
    """
    monkeypatch.setattr(
        probe_runtime.httpx,
        "Client",
        lambda **kwargs: _RecordingClient(**kwargs),
    )
    release = threading.Event()
    fetch_exited = threading.Event()

    def parked_fetch(_client: _RecordingClient, url: str, _timeout: int) -> str:
        try:
            release.wait(timeout=10)
            return f"<html>{url}</html>"
        finally:
            fetch_exited.set()

    # The fetch parks, so when the wave's deadline fires each candidate has a fetch still inside the
    # pool: that is the work a deadline arm abandons, and what the receipt has to count.
    monkeypatch.setattr(probe_runtime, "fetch_text_httpx", parked_fetch)

    async def probe_that_outlives_the_deadline(
        row: dict[str, Any], timeout_s: int, *, fetcher: Any, **_kwargs: Any
    ) -> tuple[bool, int, str]:
        try:
            await fetcher(str(row.get("id")), timeout_s)
        except TimeoutError:
            pass
        await asyncio.sleep(10)
        return True, 1, ""

    # Two candidates at a 1 s per-call budget: the wave's own deadline is 1 * (2/2) + 1 = 2 s.
    lifecycle = probe_runtime.ProbeBatchLifecycle()
    results, elapsed = _run_batch(
        [{"id": "c0", "adapter": "static"}, {"id": "c1", "adapter": "static"}],
        timeout_s=1,
        lifecycle=lifecycle,
        async_probe=probe_that_outlives_the_deadline,
        outer_timeout=20,
    )

    assert elapsed < 6, f"the deadline arm took {elapsed:.1f}s to return"
    assert all(not row[1] for row in results), "a deadline leaves a failure row per candidate"
    receipt = lifecycle.snapshot()
    assert receipt["deadlineExit"] is True
    assert receipt["fetchesOutstandingAtExit"] >= 1, (
        "the cancelled tasks' threads are still running"
    )

    release.set()
    assert fetch_exited.wait(timeout=5), "a parked fetch was left behind by the test"


# --- the rollup an operator reads -----------------------------------------------------------------


def test_the_rollup_flags_a_client_that_never_closed() -> None:
    """`clientsUnclosed` is the leak flag: it counts waves whose close never happened.

    This is the instrument's own negative control. The receipts are driven directly rather than
    through a batch, because the property under test is the *report*: a receipt that has been through
    `note_batch_exit` but never through `note_client_closed` must show up as unclosed, and one that
    was closed by the last fetch out must show up as retired-by-last-fetch with its deferral time.
    """
    never_closed = probe_runtime.ProbeBatchLifecycle()
    never_closed.note_pool_created(workers=40)
    never_closed.note_fetch_started(through_client=True)
    never_closed.note_fetch_bounded_out()
    never_closed.note_batch_exit(outstanding=1, through_client=1, pool_threads_alive=1)

    retired_late = probe_runtime.ProbeBatchLifecycle()
    retired_late.note_pool_created(workers=40)
    retired_late.note_fetch_started(through_client=True)
    retired_late.note_fetch_bounded_out()
    retired_late.note_batch_exit(outstanding=1, through_client=1, pool_threads_alive=1)
    retired_late.note_client_closed(deferred=True)

    unclosed_rollup = _probe_lifecycle_rollup([never_closed])
    assert unclosed_rollup["batches"] == 1
    assert unclosed_rollup["clientsUnclosed"] == 1
    assert unclosed_rollup["clientsRetiredByLastFetch"] == 0
    assert unclosed_rollup["fetchesOutstandingAtExit"] == 1
    assert unclosed_rollup["maxFetchesOutstandingAtExit"] == 1
    assert unclosed_rollup["maxClientCloseDeferredMs"] == 0

    settled_rollup = _probe_lifecycle_rollup([retired_late])
    assert settled_rollup["clientsUnclosed"] == 0
    assert settled_rollup["clientsRetiredByLastFetch"] == 1
    assert settled_rollup["clientsRetiredByBatch"] == 0

    no_receipts = _probe_lifecycle_rollup([])
    assert no_receipts == {}, "a run with no probe wave writes no section at all"


# --- the artifact ---------------------------------------------------------------------------------


def test_the_audit_artifact_carries_the_probe_lifecycle() -> None:
    """A live run's artifact answers the question without a bench or a log: per batch, and rolled up.

    Driven through the audit's own entrypoint, which is what a deployed run writes. The fetcher here
    is a stub, so the client counts are zero by design -- `fetchesThroughClient` is what tells a
    reader that, and the pool counts are real either way.
    """
    with workspace_tmpdir("gamedevmap-probe-lifecycle") as root:
        output_path: Path = root / "dry-run.json"

        output = sd.run_gamedevmap_active_source_dry_run(
            timeout_s=5,
            config=_config(),
            fetcher=_fetcher(_payloads()),
            output_path=output_path,
            batch_size=2,
            max_batches=1,
            reset=True,
        )

        saved = json.loads(output_path.read_text(encoding="utf-8"))
        assert output == saved, "the receipt must survive the artifact round trip"

        batches = saved["timings"]["batches"]
        assert len(batches) == 1
        receipt = batches[0]["probeLifecycle"]
        for key in (
            "poolWorkers",
            "fetchesStarted",
            "fetchesThroughClient",
            "fetchesBoundedOut",
            "fetchesOutstandingAtExit",
            "clientFetchesOutstandingAtExit",
            "clientClosed",
            "clientRetiredBy",
            "clientCloseDeferredMs",
            "deadlineExit",
        ):
            assert key in receipt, f"the batch's receipt is missing {key}"
        assert receipt["poolWorkers"] >= 1, "the wave sizes its pool"
        assert receipt["fetchesStarted"] >= 1, "the stubbed fetcher still runs fetches on it"
        assert receipt["clientClosed"] is True

        rollup = saved["summary"]["probeLifecycle"]
        assert rollup["batches"] == 1
        assert rollup["clientsUnclosed"] == 0, "a clean run reports no unclosed client"
        assert rollup["deadlineExits"] == 0
        assert rollup["fetchesStarted"] == receipt["fetchesStarted"]
        # The per-batch timing rollup is unchanged: `probeLifecycle` is not an `Ms` key.
        assert "probeLifecycleMs" not in saved["timings"]["totalsMs"]
        assert dry_run.DRY_RUN_SCHEMA_VERSION == saved["schemaVersion"]

        # The Ops view reads the receipt file, not the artifact: a real walk's artifact is tens of MB,
        # and an operator watching `kpis.probeLifecycle` cannot pay for that parse on a poll.
        receipt_path = artifact_store.probe_lifecycle_receipt_path(output_path)
        assert receipt_path.exists(), "the run publishes its receipt beside the artifact"
        assert receipt_path.parent == output_path.parent
        published = json.loads(receipt_path.read_text(encoding="utf-8"))
        assert published["probeLifecycle"] == rollup, (
            "the published receipt is the artifact's rollup"
        )
        assert published["sourceArtifact"] == output_path.name
        assert published["schemaVersion"] == artifact_store.PROBE_LIFECYCLE_RECEIPT_SCHEMA_VERSION
        assert published["runId"] == saved["runId"]

        # Browser recovery rewrites this artifact after the probe wave, and it passes no receipts of
        # its own: the run's published answer has to survive that re-summarize, or the artifact and
        # the sidecar beside it would disagree.
        artifact_store._summarize_artifact(
            saved,
            parsed_rows=[],
            representative_rows=[],
            completed_urls=set(),
        )
        assert saved["summary"]["probeLifecycle"] == rollup


def test_a_run_with_no_probe_wave_publishes_no_receipt() -> None:
    """Nothing to report writes nothing: the reader's "absent" and "clean" states stay distinct."""
    assert (
        artifact_store.write_probe_lifecycle_receipt(
            artifact_path=Path("unused-dry-run.json"),
            artifact={"runId": "", "startedAt": "", "finishedAt": ""},
            lifecycles=[],
        )
        == {}
    )
