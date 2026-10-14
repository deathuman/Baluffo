from __future__ import annotations

from tests.helpers.concurrency import BlockingActiveCounter

from ._helpers import sd, threading


def _wrapped_directory_fetch_lane_peak(lanes: int, per_host: int) -> tuple[int, set[str]]:
    """The peak in-flight count and worker thread names for one wrapped directory batch.

    The fetcher is an indirection over the page fetch -- not `fetch_text` by identity -- which is the
    shape the audit bench uses to record per-phase timings, and the shape that takes the sync path.
    """
    jobs = [
        {
            "url": f"https://lane{i}.example/",
            "payload": {"id": f"lane-{i}"},
            "name": f"lane-{i}",
            "adapter": "gamedevmap",
            "failureStage": "homepage_fetch",
        }
        for i in range(lanes)
    ]
    fetches = BlockingActiveCounter()
    released = threading.Event()
    thread_names: set[str] = set()
    lock = threading.Lock()

    def recorded_fetch(url: str, _timeout_s: int) -> str:
        with lock:
            thread_names.add(threading.current_thread().name)
        fetches.enter()
        try:
            # Block only long enough for every lane to come alive at once, so a batch that cannot
            # reach its lane count reports the count it did reach instead of hanging here.
            if fetches.peak >= lanes:
                released.set()
            released.wait(timeout=1.0)
            return f"<html>{url}</html>"
        finally:
            fetches.exit()

    def wrapped_fetcher(url: str, timeout_s: int) -> str:
        return recorded_fetch(url, timeout_s)

    assert wrapped_fetcher is not sd.fetch_text
    results = sd.fetch_directory_pages(
        5,
        jobs,
        fetcher=wrapped_fetcher,
        total_concurrency=lanes,
        per_host_concurrency=per_host,
        progress_label="Lane guard",
        progress_every=0,
        emit_progress_log=False,
    )
    assert all(bool(row.get("ok")) for row in results), lanes
    return fetches.peak, thread_names


def test_fetch_directory_pages_wrapped_fetcher_runs_every_configured_lane() -> None:
    """A wrapping fetcher must still get the lane count its caller configured.

    `fetch_directory_pages` picks the async httpx branch only when its fetcher is `fetch_text` by
    identity, so anything that wraps that fetcher -- the audit bench records per-phase timings that
    way -- takes the sync path. That path used the event loop's shared default executor
    (`min(32, cpu_count + 4)`), so the homepage phase peaked at 20 in-flight of 32 configured with no
    error and no log line, and the 72-lane recovery wave ran at roughly a quarter of its budget.

    This guard sweeps the lane counts the bundled config actually configures, so a lane count
    diverging from its batch's worker count fails here rather than as a phase that quietly cost a
    third more wall clock. The top of that sweep is above the shared executor's ceiling on every
    machine, and the thread-name check catches a cap below it, which together keep the guard able to
    see the regression whatever the box's core count.
    """
    cfg = sd.DEFAULT_DISCOVERY_CONFIG["gamedevmap"]
    lane_cases = [
        ("fetchConcurrency", int(cfg.get("fetchConcurrency") or 0)),
        (
            "activeAuditHomepageFetchConcurrency",
            int(cfg.get("activeAuditHomepageFetchConcurrency") or 0),
        ),
        (
            "activeAuditRecoveryFetchConcurrency",
            int(cfg.get("activeAuditRecoveryFetchConcurrency") or 0),
        ),
    ]
    per_host = int(cfg.get("perHostConcurrency") or 0)
    assert all(lanes > 0 for _key, lanes in lane_cases)
    diverged: list[str] = []

    for key, lanes in lane_cases:
        peak, thread_names = _wrapped_directory_fetch_lane_peak(lanes, per_host or lanes)
        if peak != lanes:
            diverged.append(f"{key}: peak {peak} in-flight of {lanes} configured lanes")
        if not thread_names or any(not name.startswith("page-fetch") for name in thread_names):
            diverged.append(f"{key}: fetches ran on {sorted(thread_names)}")

    assert not diverged, (
        "a wrapping fetcher must not cap a batch below the lane count its caller configured: "
        + "; ".join(diverged)
    )
