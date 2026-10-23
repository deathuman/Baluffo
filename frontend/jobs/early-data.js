/**
 * Hoists the jobs page's first-render data requests ahead of the module graph.
 *
 * `jobs.html` loads this as a classic script in front of the module entry, so
 * these GETs are issued while the browser is still fetching the boot modules
 * (115 files in a cold packaged page load). Issued from the graph instead they
 * queue behind that burst: measured cold starts reached first render only after
 * a ~1 s wait on a file the site answers in a few milliseconds.
 *
 * This file is a hand-off, not a data source. `data-source.js` takes each
 * response with `take(url)` and falls back to a normal fetch when nothing was
 * hoisted, when the caller already consumed it, or when the hoist itself failed.
 */
(function startJobsEarlyData(win) {
  if (win.__baluffoJobsEarlyData) return;

  const HOIST_TIMEOUT_MS = 15000;

  function isDesktopPage() {
    try {
      if (win.__baluffoDesktopMode) return true;
      return new URL(String(win.location?.href || "")).searchParams.get("desktop") === "1";
    } catch {
      return false;
    }
  }

  function request(url) {
    try {
      const init = {
        cache: "no-store",
        credentials: "omit",
        headers: { Accept: "application/json" },
        mode: "cors"
      };
      const timeout = win.AbortSignal?.timeout?.(HOIST_TIMEOUT_MS);
      if (timeout) init.signal = timeout;
      // A failed hoist resolves to null instead of rejecting: by the time the
      // graph asks for the payload nothing may be observing this promise, and an
      // unhandled rejection would be noise rather than a finding.
      return Promise.resolve(win.fetch(url, init)).then(response => response || null, () => null);
    } catch {
      return Promise.resolve(null);
    }
  }

  if (typeof win.fetch !== "function") return;
  const dataset = win.document?.currentScript?.dataset || {};
  const snapshotUrl = String(dataset.startupSnapshotUrl || "").trim();
  // The report probe only runs in the desktop app; hoisting it anywhere else
  // would just add a request the page never reads.
  const reportUrl = isDesktopPage() ? String(dataset.fetchReportUrl || "").trim() : "";

  const pending = new Map();
  for (const url of [snapshotUrl, reportUrl].filter(Boolean)) {
    pending.set(url, request(url));
  }
  if (pending.size === 0) return;

  win.__baluffoJobsEarlyData = {
    // Which URLs were actually hoisted, for support traces and tests.
    urls: Object.freeze([...pending.keys()]),
    take(url) {
      const key = String(url || "").trim();
      const response = pending.get(key);
      if (!response) return null;
      // One shot: a second caller for the same URL fetches it normally rather
      // than receiving a body another consumer already read.
      pending.delete(key);
      return response;
    }
  };
})(globalThis);
