import test from "node:test";
import assert from "node:assert/strict";
import { loadTaskLivePayload } from "../../../frontend/admin/app/live-task.js";

test("loadTaskLivePayload appends summary view when requested", async () => {
  const calls = [];
  const payload = await loadTaskLivePayload({
    taskType: "fetch",
    view: "summary",
    getBridge: async (path, options) => {
      calls.push({ path, options });
      return { taskType: "fetch", active: true };
    },
    requestOptions: { timeoutMs: 1234 }
  });

  assert.deepEqual(calls, [
    {
      path: "/ops/task-live/fetch?view=summary",
      options: { timeoutMs: 1234 }
    }
  ]);
  assert.equal(payload.active, true);
});
