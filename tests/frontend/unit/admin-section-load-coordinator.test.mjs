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

async function flushMicrotasks(count = 6) {
  for (let index = 0; index < count; index += 1) {
    await Promise.resolve();
  }
}

test("admin section loader queues visible section loads with concurrency one", async () => {
  const fetcherLoad = createDeferred();
  const discoveryLoad = createDeferred();
  const calls = [];
  const coordinator = createAdminSectionLoadCoordinator({
    state: { adminSectionLoadState: {} },
    refs: {},
    fetcherController: {
      setFetcherLogPlaceholder(message) {
        calls.push(["fetcherPlaceholder", message]);
      },
      loadLatestFetcherReport(options) {
        calls.push(["fetcherReport", options]);
        return fetcherLoad.promise;
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
    ["fetcherReport", { silent: true }]
  ]);
  assert.equal(calls.some(([name]) => name === "discoveryData"), false);

  fetcherLoad.resolve({});
  await flushMicrotasks();

  assert.deepEqual(calls.slice(2), [
    ["fetcherLog", { reset: true, showEmptyState: true }],
    ["discoveryPlaceholder", "Loading discovery output..."],
    ["discoveryData", { sourceTablesOnly: true, skipIfFreshMs: 10000 }]
  ]);

  discoveryLoad.resolve({});
  await flushMicrotasks();
  assert.deepEqual(calls.at(-1), ["discoveryLog", { reset: true, guarded: false }]);
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
