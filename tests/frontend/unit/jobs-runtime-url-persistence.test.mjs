import test from "node:test";
import assert from "node:assert/strict";

import { createJobsUrlPersistence } from "../../../frontend/jobs/app/runtime/url-persistence.js";
import {
  buildJobsPageUrl,
  isDesktopRuntimeMode,
  parseJobsPageUrlState
} from "../../../frontend/jobs/app/startup.js";
import { normalizeLifecycleStatus } from "../../../frontend/jobs/app/filters.js";

function createStorageMock(seed = {}) {
  const map = new Map(Object.entries(seed).map(([key, value]) => [String(key), String(value)]));
  return {
    getItem(key) {
      return map.has(key) ? map.get(key) : null;
    },
    setItem(key, value) {
      map.set(String(key), String(value));
    },
    removeItem(key) {
      map.delete(String(key));
    }
  };
}

function createHarness({ desktop = true, ready = false, probe = false } = {}) {
  const calls = {
    metrics: [],
    remember: [],
    history: [],
    builds: [],
    timers: []
  };
  let desktopUrlStateReady = Boolean(ready);
  let desktopPendingRememberJobsUrl = false;
  let desktopPendingJobsUrl = "";
  const windowObject = {
    location: {
      pathname: "/jobs",
      search: "?q=engineer"
    },
    history: {
      replaceState: (...args) => {
        calls.history.push(args);
        const url = String(args[2] || "");
        const parsed = new URL(url || "/jobs", "https://example.invalid");
        windowObject.location.pathname = parsed.pathname;
        windowObject.location.search = parsed.search;
      }
    },
    setTimeout: fn => {
      calls.timers.push(fn);
      return calls.timers.length;
    }
  };

  return {
    calls,
    setReady: value => {
      desktopUrlStateReady = Boolean(value);
    },
    getState: () => ({
      desktopUrlStateReady,
      desktopPendingRememberJobsUrl,
      desktopPendingJobsUrl
    }),
    persistence: createJobsUrlPersistence({
      windowObject,
      buildJobsPageUrl: (pathname, state) => {
        calls.builds.push({ pathname, state });
        return `${pathname}?page=${state.currentPage}`;
      },
      resolveStartupProbeEnabled: () => probe,
      isDesktopRuntimeMode: () => desktop,
      rememberJobsUrl: (key, url) => {
        calls.remember.push({ key, url });
      },
      emitMetric: (event, payload = {}) => {
        calls.metrics.push({ event, payload });
      },
      getDesktopUrlStateReady: () => desktopUrlStateReady,
      setDesktopUrlStateReady: value => {
        desktopUrlStateReady = Boolean(value);
      },
      getDesktopPendingRememberJobsUrl: () => desktopPendingRememberJobsUrl,
      setDesktopPendingRememberJobsUrl: value => {
        desktopPendingRememberJobsUrl = Boolean(value);
      },
      getDesktopPendingJobsUrl: () => desktopPendingJobsUrl,
      setDesktopPendingJobsUrl: value => {
        desktopPendingJobsUrl = String(value || "");
      },
      lastUrlKey: "last-url"
    })
  };
}

test("jobs URL persistence defers desktop writes until the runtime is ready", () => {
  const harness = createHarness({ desktop: true, ready: false });

  harness.persistence.writeStateToUrl({
    currentPage: 3,
    filters: {
      countries: ["NL"],
      city: "",
      sector: "",
      profession: "",
      workType: "",
      lifecycleStatus: "active",
      newOnly: false,
      excludeInternship: false,
      search: "",
      sort: "relevance"
    }
  });

  assert.equal(harness.calls.builds.length, 1);
  assert.deepEqual(harness.getState(), {
    desktopUrlStateReady: false,
    desktopPendingRememberJobsUrl: true,
    desktopPendingJobsUrl: "/jobs?page=3"
  });
  assert.deepEqual(harness.calls.history, []);
  assert.deepEqual(harness.calls.remember, []);

  harness.setReady(true);
  assert.equal(harness.persistence.flushDesktopPendingJobsUrlState(), true);
  assert.equal(harness.calls.timers.length, 1);
  harness.calls.timers[0]();

  assert.deepEqual(harness.calls.remember, [
    { key: "last-url", url: "/jobs?page=3" }
  ]);
  assert.deepEqual(harness.getState(), {
    desktopUrlStateReady: true,
    desktopPendingRememberJobsUrl: false,
    desktopPendingJobsUrl: ""
  });
});

test("jobs URL persistence keeps the non-desktop remember and replace flow canonical", () => {
  const harness = createHarness({ desktop: false });

  harness.persistence.rememberCurrentJobsUrl();
  assert.deepEqual(harness.calls.remember, [
    { key: "last-url", url: "/jobs?q=engineer" }
  ]);

  harness.persistence.writeStateToUrl({
    currentPage: 2,
    filters: {
      countries: [],
      city: "",
      sector: "",
      profession: "",
      workType: "",
      lifecycleStatus: "active",
      newOnly: false,
      excludeInternship: false,
      search: "",
      sort: "relevance"
    }
  });

  assert.equal(harness.calls.history.length, 1);
  assert.deepEqual(harness.calls.remember.at(-1), {
    key: "last-url",
    url: "/jobs?page=2"
  });
});

test("jobs URL persistence skips writes in startup probe mode", () => {
  const harness = createHarness({ desktop: false, probe: true });

  harness.persistence.writeStateToUrl({
    currentPage: 4,
    filters: {
      countries: [],
      city: "",
      sector: "",
      profession: "",
      workType: "",
      lifecycleStatus: "active",
      newOnly: false,
      excludeInternship: false,
      search: "",
      sort: "relevance"
    }
  });

  assert.equal(harness.calls.history.length, 0);
  assert.equal(harness.calls.remember.length, 0);
  assert.equal(
    harness.calls.metrics.some(entry => entry.event === "jobs_write_state_probe_skip"),
    true
  );
});

test("jobs startup URL state preserves read-only lifecycle filter values", () => {
  const defaultFilters = {
    countries: [],
    lifecycleStatus: "active",
    workType: "",
    city: "",
    sector: "",
    profession: "",
    newOnly: false,
    excludeInternship: false,
    search: "",
    sort: "relevance"
  };
  const state = parseJobsPageUrlState("?lifecycleStatus=reappeared", {
    defaultFilters,
    normalizeLifecycleStatus
  });

  assert.equal(state.filters.lifecycleStatus, "reappeared");
  assert.equal(buildJobsPageUrl("/jobs.html", state), "/jobs.html?lifecycleStatus=reappeared");
});

test("jobs desktop mode stays enabled from sticky session storage without a desktop query", () => {
  const originalWindow = global.window;
  global.window = {
    location: {
      href: "http://127.0.0.1:4173/jobs.html?page=2"
    },
    sessionStorage: createStorageMock({
      baluffo_runtime_mode: "desktop"
    })
  };

  try {
    assert.equal(isDesktopRuntimeMode(), true);
    assert.equal(isDesktopRuntimeMode("http://127.0.0.1:4173/jobs.html?page=3"), true);
  } finally {
    global.window = originalWindow;
  }
});
