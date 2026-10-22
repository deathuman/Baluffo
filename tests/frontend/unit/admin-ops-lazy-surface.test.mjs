import assert from "node:assert/strict";
import test from "node:test";
import { createLazyAdminOpsController } from "../../../frontend/admin/app/ops/lazy.js";

function deferred() {
  let resolve;
  const promise = new Promise(done => {
    resolve = done;
  });
  return { promise, resolve };
}

function sleep(ms = 0) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

function createRecordingController(calls) {
  const record = name => (...args) => {
    calls.push([name, args]);
    return Promise.resolve(`${name}-done`);
  };
  return {
    setBridgeStatusBadge: record("setBridgeStatusBadge"),
    setOpsReadinessShell: record("setOpsReadinessShell"),
    setOpsPlaceholders: record("setOpsPlaceholders"),
    applyBootstrapPayload: record("applyBootstrapPayload"),
    loadOpsHealthData: record("loadOpsHealthData"),
    loadPipelineStatusFallbackData: record("loadPipelineStatusFallbackData"),
    stopOpsHealthPolling: record("stopOpsHealthPolling")
  };
}

test("shell-time calls are buffered until the ops chunk is applied", async () => {
  const calls = [];
  const gate = deferred();
  const facade = createLazyAdminOpsController({
    createController: () => gate.promise.then(() => createRecordingController(calls))
  });

  const badge = facade.setBridgeStatusBadge("checking", "Bridge Checking");
  const shell = facade.setOpsReadinessShell();

  // Both are inert while the chunk is pending: a facade that returned a promise
  // here would make the shell's synchronous paint depend on the network.
  assert.equal(badge, undefined);
  assert.equal(shell, undefined);
  assert.deepEqual(calls, []);
  assert.equal(facade.isLoaded(), false);

  gate.resolve(await Promise.resolve());
  const controller = await facade.ready();

  assert.equal(facade.isLoaded(), true);
  assert.deepEqual(calls, [
    ["setBridgeStatusBadge", ["checking", "Bridge Checking"]],
    ["setOpsReadinessShell", []]
  ]);
  assert.equal(typeof controller.loadOpsHealthData, "function");
});

test("the latest buffered call per method wins and keeps its call position", async () => {
  const calls = [];
  const facade = createLazyAdminOpsController({
    createController: async () => createRecordingController(calls)
  });

  facade.setBridgeStatusBadge("checking", "Bridge Checking");
  facade.setOpsReadinessShell();
  // Every bridge call reports reachability, so the badge is re-queued many times
  // during boot; replaying each of them would repaint stale states in order.
  facade.setBridgeStatusBadge("online", "Bridge Online");

  await facade.ready();

  assert.deepEqual(calls, [
    ["setBridgeStatusBadge", ["online", "Bridge Online"]],
    ["setOpsReadinessShell", []]
  ]);
});

test("delegated methods wait for the chunk and keep their promise contract", async () => {
  const calls = [];
  const facade = createLazyAdminOpsController({
    createController: async () => createRecordingController(calls)
  });

  const result = facade.loadOpsHealthData({ silent: true });

  assert.equal(typeof result.then, "function");
  assert.deepEqual(calls, []);
  facade.start();
  assert.equal(await result, "loadOpsHealthData-done");
  assert.deepEqual(calls, [["loadOpsHealthData", [{ silent: true }]]]);
});

test("neither construction nor a buffered call fetches the chunk; start() does", async () => {
  let created = 0;
  const facade = createLazyAdminOpsController({
    createController: async () => {
      created += 1;
      return createRecordingController([]);
    }
  });

  await sleep(5);
  facade.setOpsReadinessShell();
  await sleep(20);
  // The shell owns the first paint; fetching the Ops chunk because it asked for
  // a readiness shell is exactly the regression this gate exists to prevent.
  assert.equal(created, 0);

  await facade.start();
  assert.equal(created, 1);
  assert.equal(facade.isLoaded(), true);
});

test("delegated calls wait for start() instead of pulling the chunk in early", async () => {
  let created = 0;
  const calls = [];
  const facade = createLazyAdminOpsController({
    createController: async () => {
      created += 1;
      return createRecordingController(calls);
    }
  });

  const pending = facade.loadPipelineStatusFallbackData();
  await sleep(20);
  assert.equal(created, 0);

  facade.start();
  assert.equal(await pending, "loadPipelineStatusFallbackData-done");
  assert.equal(created, 1);
});

test("a shell that never settles still loads ops after the fallback timeout", async () => {
  let created = 0;
  const facade = createLazyAdminOpsController({
    createController: async () => {
      created += 1;
      return createRecordingController([]);
    },
    fallbackTimeoutMs: 10
  });

  facade.setOpsReadinessShell();
  await sleep(40);
  assert.equal(created, 1);
  assert.equal(facade.isLoaded(), true);
});

test("a chunk that fails to load leaves a promise-shaped inert controller", async () => {
  const facade = createLazyAdminOpsController({
    createController: async () => {
      throw new Error("chunk 404");
    }
  });

  const result = facade.stopOpsHealthPolling();
  facade.ready();

  assert.equal(await result, undefined);
  // A second call must not retry the failed chunk or reject.
  assert.equal(await facade.loadOpsHealthData(), undefined);
  assert.equal(facade.isLoaded(), true);
});

test("the facade exposes every method the shell wires into other controllers", () => {
  const facade = createLazyAdminOpsController({ createController: async () => ({}) });
  const wired = [
    "applyBootstrapPayload",
    "loadActiveOpsSummaryData",
    "loadIdleOpsHeavyHydration",
    "loadOpsHealthData",
    "loadOpsHistoryData",
    "loadOpsOverviewDetailData",
    "loadPipelineScheduleData",
    "loadPipelineStatusFallbackData",
    "loadRegistrySyncDiagnosticsData",
    "pollBridgeStatus",
    "scheduleOpsHealthPolling",
    "setBridgeStatusBadge",
    "setOpsPlaceholders",
    "setOpsReadinessShell",
    "startBridgeStatusWatch",
    "stopBridgeStatusWatch",
    "stopOpsHealthPolling"
  ];
  for (const name of wired) {
    assert.equal(typeof facade[name], "function", `missing ops facade method ${name}`);
  }
});
