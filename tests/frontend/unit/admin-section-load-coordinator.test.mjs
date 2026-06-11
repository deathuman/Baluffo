import test from "node:test";
import assert from "node:assert/strict";

import { createAdminSectionLoadCoordinator } from "../../../frontend/admin/app/runtime/section-loader.js";

function createDeferred() {
  let resolve;
  const promise = new Promise(resolvePromise => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function createElement() {
  const listeners = new Map();
  return {
    innerHTML: "",
    open: false,
    addEventListener(eventName, handler) {
      const key = String(eventName || "");
      listeners.set(key, [...(listeners.get(key) || []), handler]);
    },
    dispatch(eventName, event) {
      (listeners.get(String(eventName || "")) || []).forEach(handler => handler(event));
    },
    matches(selector) {
      return selector === "[data-ops-load-older-history]";
    },
    querySelector() {
      return createElement();
    }
  };
}

function createLink(href) {
  const listeners = new Map();
  return {
    addEventListener(eventName, handler) {
      const key = String(eventName || "");
      listeners.set(key, [...(listeners.get(key) || []), handler]);
    },
    getAttribute(name) {
      return name === "href" ? href : "";
    },
    click() {
      (listeners.get("click") || []).forEach(handler => handler({ preventDefault() {} }));
    }
  };
}

async function flushMicrotasks(count = 6) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
}

test("admin section loader queues visible section loads with concurrency one", async () => {
  const fetcherSummaryLoad = createDeferred();
  const discoveryLoad = createDeferred();
  const calls = [];
  const coordinator = createAdminSectionLoadCoordinator({
    state: { adminSectionLoadState: {} },
    refs: {},
    fetcherController: {
      setFetcherLogPlaceholder(message) {
        calls.push(["fetcherPlaceholder", message]);
      },
      loadLatestFetcherSummary(options) {
        calls.push(["fetcherSummary", options]);
        return fetcherSummaryLoad.promise;
      },
      loadFetcherLogChunk(options) {
        calls.push(["fetcherLog", options]);
        return Promise.resolve({});
      }
    },
    discoveryController: {
      setDiscoveryLogPlaceholder(message) {
        calls.push(["discoveryPlaceholder", message]);
      },
      loadDiscoveryLogChunk(options) {
        calls.push(["discoveryLog", options]);
        return Promise.resolve({});
      }
    },
    registryController: {
      loadDiscoveryData(options) {
        calls.push(["discoveryData", options]);
        return discoveryLoad.promise;
      }
    }
  });

  coordinator.enqueueSection("fetcher");
  coordinator.enqueueSection("discovery");
  await Promise.resolve();

  assert.deepEqual(calls.slice(0, 2), [
    ["fetcherPlaceholder", "Loading latest fetcher output..."],
    ["fetcherLog", { reset: true, showEmptyState: true, view: "tail", limitChars: 8192 }]
  ]);
  assert.equal(calls.some(([name]) => name === "discoveryData"), false);

  fetcherSummaryLoad.resolve({});
  await flushMicrotasks();

  assert.deepEqual(calls.slice(2), [
    ["fetcherSummary", { silent: false }],
    ["discoveryPlaceholder", "Loading discovery output..."],
    ["discoveryData", { sourceTablesOnly: true, skipIfFreshMs: 10000 }]
  ]);

  discoveryLoad.resolve({});
  await flushMicrotasks();
  assert.deepEqual(calls.at(-1), ["discoveryLog", { reset: true, guarded: false, view: "tail", limitChars: 8192 }]);
});

test("admin section loader loads recent history on ops focus and older history on disclosure", async () => {
  const historyEl = createElement();
  const calls = [];
  const coordinator = createAdminSectionLoadCoordinator({
    state: { adminSectionLoadState: {}, opsHistoryLoaded: false, opsHistoryFullLoaded: false },
    refs: { adminOpsHistoryEl: historyEl },
    opsController: {
      loadOpsHistoryData(options) {
        calls.push(options);
        return Promise.resolve({});
      }
    }
  });

  coordinator.enqueueSection("ops");
  await Promise.resolve();
  await Promise.resolve();

  assert.equal(historyEl.innerHTML.includes("Loading recent run history"), true);
  assert.deepEqual(calls, [{ limit: 2, silent: true }]);

  coordinator.handleOlderHistoryToggle({ target: { ...historyEl, open: true } });
  await Promise.resolve();

  assert.deepEqual(calls, [
    { limit: 2, silent: true },
    { limit: 80, silent: true }
  ]);
});

test("admin section loader does not auto-observe Admin sections on boot", () => {
  const observedIds = [];
  const documentObject = {
    getElementById(id) {
      return { id };
    },
    querySelectorAll() {
      return [];
    }
  };
  const windowObject = {
    location: { hash: "" },
    addEventListener() {},
    IntersectionObserver: class {
      constructor() {}
      observe(element) {
        observedIds.push(element.id);
      }
    }
  };
  const coordinator = createAdminSectionLoadCoordinator({
    state: { adminSectionLoadState: {} },
    refs: {},
    documentObject,
    windowObject
  });

  coordinator.start();

  assert.deepEqual(observedIds, []);
});

test("admin section loader loads deferred Fetcher section only from explicit navigation", async () => {
  const link = createLink("#admin-fetcher-section");
  const calls = [];
  const documentObject = {
    getElementById() {
      return null;
    },
    querySelectorAll() {
      return [link];
    }
  };
  const windowObject = {
    location: { hash: "" },
    addEventListener() {},
    setTimeout(handler) {
      handler();
    }
  };
  const coordinator = createAdminSectionLoadCoordinator({
    state: { adminSectionLoadState: {} },
    refs: {},
    documentObject,
    windowObject,
    fetcherController: {
      setFetcherLogPlaceholder(message) {
        calls.push(["fetcherPlaceholder", message]);
      },
      loadLatestFetcherSummary(options) {
        calls.push(["fetcherSummary", options]);
        return Promise.resolve({});
      },
      loadFetcherLogChunk(options) {
        calls.push(["fetcherLog", options]);
        return Promise.resolve({});
      }
    },
    registryController: {
      loadDiscoveryData(options) {
        calls.push(["discoveryData", options]);
        return Promise.resolve({});
      }
    }
  });

  coordinator.start();
  assert.deepEqual(calls, []);

  link.click();
  await flushMicrotasks();

  assert.deepEqual(calls, [
    ["fetcherPlaceholder", "Loading latest fetcher output..."],
    ["fetcherLog", { reset: true, showEmptyState: true, view: "tail", limitChars: 8192 }],
    ["fetcherSummary", { silent: false }]
  ]);
});
