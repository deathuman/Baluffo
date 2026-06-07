import test from "node:test";
import assert from "node:assert/strict";

import { loadLiveTaskLogChunk } from "../../../frontend/admin/app/live-task.js";

test("loadLiveTaskLogChunk requests bounded tail view and stores returned offset", async () => {
  const calls = [];
  const state = { fetcherOffset: 123 };
  const payload = await loadLiveTaskLogChunk({
    getBridge: async path => {
      calls.push(path);
      return { text: "recent log", offset: 4000, nextOffset: 8123, hasMore: true };
    },
    path: "/fetcher/log",
    state,
    offsetKey: "fetcherOffset",
    reset: true,
    view: "tail",
    limitChars: 65536
  });

  assert.deepEqual(calls, ["/fetcher/log?view=tail&limitChars=65536"]);
  assert.equal(payload.text, "recent log");
  assert.equal(state.fetcherOffset, 8123);
});
