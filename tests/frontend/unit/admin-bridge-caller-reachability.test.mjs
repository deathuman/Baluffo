/**
 * Reachability attribution for bridge calls.
 *
 * `callBridge` used to report every rejected request as "the bridge is offline".
 * The Admin Action Center polls /ops/storage-health with a 5 s timeout, so one
 * slow diagnostic read flipped the whole page into the reconnect state while
 * every data route in the same batch answered. Reachability is now an explicit
 * claim a caller has to ask for.
 */

import test from "node:test";
import assert from "node:assert/strict";

import { createBridgeCaller } from "../../../frontend/admin/app/runtime/actions.js";

function createRecorder() {
  const events = [];
  const callBridge = createBridgeCaller({
    setBridgeOnline: () => events.push("online"),
    setBridgeOffline: () => events.push("offline")
  });
  return { callBridge, events };
}

test("a successful call reports the bridge online", async () => {
  const { callBridge, events } = createRecorder();

  const data = await callBridge(async () => ({ ok: true }));

  assert.deepEqual(data, { ok: true });
  assert.deepEqual(events, ["online"]);
});

test("a failed call reports the bridge offline by default", async () => {
  const { callBridge, events } = createRecorder();

  await assert.rejects(
    callBridge(async () => {
      throw new Error("Failed to fetch");
    })
  );

  assert.deepEqual(events, ["offline"]);
});

test("a diagnostic-only timeout leaves reachability untouched", async () => {
  const { callBridge, events } = createRecorder();

  await assert.rejects(
    callBridge(
      async () => {
        const error = new Error("The operation was aborted");
        error.name = "AbortError";
        throw error;
      },
      { affectsReachability: false }
    )
  );

  assert.deepEqual(events, []);
});

test("a diagnostic-only success still reports the bridge online", async () => {
  const { callBridge, events } = createRecorder();

  await callBridge(async () => ({ ok: true }), { affectsReachability: false });

  // A success is never a wrong claim about reachability.
  assert.deepEqual(events, ["online"]);
});
