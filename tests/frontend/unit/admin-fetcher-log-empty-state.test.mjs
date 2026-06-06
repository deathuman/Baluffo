import test from "node:test";
import assert from "node:assert/strict";
import { createAdminFetcherLogController } from "../../../frontend/admin/app/fetcher/logs.js";

function createLogElement() {
  return {
    innerHTML: "",
    textContent: ""
  };
}

function createController({ getBridge, logEl = createLogElement() } = {}) {
  const state = {
    fetcherLogRemoteOffset: 0,
    fetcherLiveProgressState: null
  };
  const controller = createAdminFetcherLogController({
    state,
    refs: { adminFetcherLogEl: logEl },
    getBridge,
    createLogEvent: (_scope, message, level) => ({ message, level }),
    appendLogRow: (el, event) => {
      const line = String(event?.message || "");
      el.textContent = `${el.textContent || ""}${el.textContent ? "\n" : ""}${line}`;
      el.innerHTML = el.textContent;
    },
    setFetcherProgress() {}
  });
  return { controller, state, logEl };
}

test("empty fetcher log response renders a settled empty state after reset", async () => {
  const calls = [];
  const { controller, logEl, state } = createController({
    getBridge: async path => {
      calls.push(path);
      return { text: "", offset: 0, nextOffset: 0, hasMore: false };
    }
  });

  await controller.loadFetcherLogChunk({ reset: true, showEmptyState: true });

  assert.deepEqual(calls, ["/fetcher/log?offset=0"]);
  assert.equal(state.fetcherLogRemoteOffset, 0);
  assert.match(logEl.textContent, /No fetch log entries yet\./);
});

test("empty fetcher log response does not overwrite existing log text", async () => {
  const logEl = createLogElement();
  logEl.textContent = "Existing fetch event";
  logEl.innerHTML = "Existing fetch event";
  const { controller } = createController({
    logEl,
    getBridge: async () => ({ text: "", offset: 0, nextOffset: 0, hasMore: false })
  });

  await controller.loadFetcherLogChunk({ reset: true, showEmptyState: true });

  assert.equal(logEl.textContent, "Existing fetch event");
});
