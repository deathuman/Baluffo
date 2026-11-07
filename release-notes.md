## [0.3.039] - 2026-11-07

### Added

- The Admin Action Center now reports a failed browser-fallback probe, naming the stage, error type, sources needing browser fallback, and when the failure last happened. The item can be dismissed or copied into diagnostics.
- Ops fetcher diagnostics load only when their disclosure is opened, and reuse the same metrics request as the Dedup tab. An opened panel shows browser-fallback health and static deadline cuts.
- A completed Jobs pipeline now reports fetch-source failures and oversized sync snapshots as warnings instead of a clean run. The Jobs page says how many sources failed without treating the run as an error.

### Fixed

- Static fetches share one absolute source deadline across retries, redirect hops, and page batches, and each HTTP request has a hard total timeout, so a slow or trickling server cannot outlive its budget. A source that kept jobs stays a healthy partial result when a deadline cuts it; a source that kept none is reported as failed.
- Static deadline rows now preserve which clock fired (`request cap`, `source budget`, or `page-batch deadline`) in the fetch report and Ops view instead of collapsing to one generic timeout.
- The Jobs cache repairs a missing database index without discarding seen-job history, and a blocked database upgrade reports an error instead of hanging or silently clearing the cache.
- The browser fallback pool records the original launch or navigation error before the circuit breaker normalizes it, probes the browser after an environment-level failure, and closes a failed pool under the same lock before restarting.
- The Jobs page no longer labels a fetch-only warning as a source-sync problem; error, sync, and fetch outcomes keep their own messages.
- A run that cannot acquire the feed reconciliation lock now records the lock path, holder process, and wait limit in its failed report, so the timeout remains diagnosable after the live process is gone.
- A cached Ops summary refreshes when its browser-health, probe, or page-fetch sidecar changes, instead of serving the stale receipt until the cache expires.
- The Source Policy Review panel no longer times out or flips the bridge status to Reconnecting while its recommendations load on a large registry.
- Closing the desktop window now finishes cleanup within the five-second packaged target. The app waits two seconds for the bridge to settle, then stops even if the browser wrapper is still exiting, instead of waiting up to 20 seconds for a startup handoff after the window has closed.

### Notes

- Distribution compatibility is unchanged: the same-origin Linux container,
  GHCR multi-arch image publishing, wildcard browser CORS allow headers, and
  desktop localhost bridge compatibility continue to work as before.
