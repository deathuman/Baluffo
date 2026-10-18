import test from "node:test";

import assert from "node:assert/strict";

import { createAdminDiscoveryController } from "../../../frontend/admin/app/discovery.js";

import { deriveAdminActiveWorkContext } from "../../../frontend/admin/app/active-work-policy.js";

import { createDiscoveryControllerFixture, createElement } from "./helpers/admin-controller-test-helpers.mjs";

function createDeferredPromise() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

test("admin discovery controller loads and saves auto-approve config", async () => {
  const calls = [];
  const toasts = [];
  const state = {
    latestDiscoveryConfigCache: null,
    discoveryConfigDirty: false,
    discoveryLogRemoteOffset: 0,
    discoveryLaunchAtMs: 0,
    discoveryCompletionPollTimer: null,
    discoveryLiveProgressState: null,
    discoveryOptimisticRun: null,
    adminBusyState: {
      discoveryRun: false,
      discoveryWatch: false,
      discoveryLoad: false,
      discoveryWrite: false,
      manualAdd: false,
      manualCheck: false,
      liveDiscoveryRunning: false
    }
  };
  const refs = {
    adminDiscoveryLogEl: createElement(),
    adminDiscoveryAutoApproveToggleEl: createElement({ checked: true })
  };

  const controller = createAdminDiscoveryController({
    state,
    refs,
    getBridge: async path => {
      calls.push(path);
      if (path === "/discovery/config") {
        return {
          ok: true,
          savedConfig: {
            autoApproveHealthyPendingOnComplete: false
          }
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    postBridge: async (path, payload) => {
      calls.push(`${path}:${JSON.stringify(payload)}`);
      if (path === "/discovery/config") {
        return {
          ok: true,
          savedConfig: {
            autoApproveHealthyPendingOnComplete: Boolean(payload?.autoApproveHealthyPendingOnComplete)
          }
        };
      }
      throw new Error(`unexpected path ${path}`);
    },
    setBusyFlag(key, value) {
      state.adminBusyState[key] = value;
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    logAdminError() {},
    showToast(message, level) {
      toasts.push({ message, level });
    },
    createLogEvent(scope, message, level) {
      return { scope, message, level, timestamp: "2026-03-08T10:01:00.000Z" };
    },
    appendLogRow() {},
    loadOpsHealthData: async () => {},
    scheduleOpsHealthPolling() {},
    loadDiscoveryData: async () => {}
  });

  const loaded = await controller.loadDiscoveryConfig({ forceForm: true });
  assert.equal(loaded.savedConfig.autoApproveHealthyPendingOnComplete, false);
  assert.equal(refs.adminDiscoveryAutoApproveToggleEl.checked, false);

  refs.adminDiscoveryAutoApproveToggleEl.checked = true;
  state.discoveryConfigDirty = true;
  await controller.saveDiscoveryConfig();

  assert.ok(calls.includes("/discovery/config"));
  assert.ok(calls.includes('/discovery/config:{"autoApproveHealthyPendingOnComplete":true}'));
  assert.equal(state.discoveryConfigDirty, false);
  assert.equal(state.latestDiscoveryConfigCache.savedConfig.autoApproveHealthyPendingOnComplete, true);
  assert.ok(
    toasts.some(
      item =>
        item.message === "Discovery auto-approve preference updated." &&
        item.level === "success"
    )
  );
});

test("admin discovery config save is single-flight, preserves queued edits, and owns its busy flag", async () => {
  const fixture = createDiscoveryControllerFixture({
    refs: { adminDiscoveryAutoApproveToggleEl: createElement({ checked: true }) }
  });
  const postDeferred = createDeferredPromise();
  const submitted = [];
  fixture.options.postBridge = (path, payload) => {
    submitted.push({ path, payload });
    return postDeferred.promise.then(() => ({
      ok: true,
      savedConfig: {
        autoApproveHealthyPendingOnComplete: Boolean(payload?.autoApproveHealthyPendingOnComplete)
      }
    }));
  };
  const controller = createAdminDiscoveryController(fixture.options);
  const firstSave = controller.saveDiscoveryConfig();

  assert.deepEqual(submitted, [{
    path: "/discovery/config",
    payload: { autoApproveHealthyPendingOnComplete: true }
  }]);
  assert.equal(fixture.state.adminBusyState.discoveryConfigSave, true);
  assert.equal(fixture.state.adminBusyState.discoveryWrite, false);
  assert.equal(fixture.refs.adminDiscoveryAutoApproveToggleEl.attributes["aria-busy"], "true");
  assert.equal(deriveAdminActiveWorkContext({ state: fixture.state }).sourceMutationsAllowed, false);

  fixture.refs.adminDiscoveryAutoApproveToggleEl.checked = false;
  fixture.state.discoveryConfigDirty = true;
  controller.saveDiscoveryConfig();
  assert.equal(submitted.length, 1);

  postDeferred.resolve({
    ok: true,
    savedConfig: { autoApproveHealthyPendingOnComplete: true }
  });
  await firstSave;

  assert.deepEqual(submitted.map(item => item.payload), [
    { autoApproveHealthyPendingOnComplete: true },
    { autoApproveHealthyPendingOnComplete: false }
  ]);
  assert.equal(fixture.refs.adminDiscoveryAutoApproveToggleEl.checked, false);
  assert.equal(fixture.state.discoveryConfigDirty, false);
  assert.equal(fixture.state.latestDiscoveryConfigCache.savedConfig.autoApproveHealthyPendingOnComplete, false);
  assert.equal(fixture.state.adminBusyState.discoveryConfigSave, false);
  assert.equal(fixture.state.adminBusyState.discoveryWrite, false);
  assert.equal(fixture.refs.adminDiscoveryAutoApproveToggleEl.attributes["aria-busy"], "false");
  assert.equal(fixture.toasts.filter(item => item.level === "success").length, 1);
});

test("admin discovery config save rejection retains dirty state and can be retried", async () => {
  const fixture = createDiscoveryControllerFixture({
    refs: { adminDiscoveryAutoApproveToggleEl: createElement({ checked: true }) }
  });
  const failedSave = createDeferredPromise();
  let postCount = 0;
  fixture.options.postBridge = () => {
    postCount += 1;
    if (postCount === 1) return failedSave.promise;
    return Promise.resolve({ savedConfig: { autoApproveHealthyPendingOnComplete: true } });
  };
  const controller = createAdminDiscoveryController(fixture.options);
  const save = controller.saveDiscoveryConfig();
  fixture.state.discoveryConfigDirty = true;
  failedSave.reject(new Error("temporary failure"));

  await assert.rejects(save, /temporary failure/);
  assert.equal(fixture.state.discoveryConfigDirty, true);
  assert.equal(fixture.state.adminBusyState.discoveryConfigSave, false);
  assert.equal(fixture.refs.adminDiscoveryAutoApproveToggleEl.attributes["aria-busy"], "false");
  assert.ok(fixture.toasts.some(item => item.level === "error" && /temporary failure/.test(item.message)));

  await controller.saveDiscoveryConfig();
  assert.equal(postCount, 2);
  assert.equal(fixture.state.discoveryConfigDirty, false);
  assert.equal(fixture.state.latestDiscoveryConfigCache.savedConfig.autoApproveHealthyPendingOnComplete, true);
});

test("admin discovery config retries latest queued value after an earlier save rejects", async () => {
  const fixture = createDiscoveryControllerFixture({
    refs: { adminDiscoveryAutoApproveToggleEl: createElement({ checked: true }) }
  });
  const firstSave = createDeferredPromise();
  const submitted = [];
  fixture.options.postBridge = (path, payload) => {
    submitted.push(payload);
    if (submitted.length === 1) return firstSave.promise;
    return Promise.resolve({
      savedConfig: { autoApproveHealthyPendingOnComplete: payload.autoApproveHealthyPendingOnComplete }
    });
  };
  const controller = createAdminDiscoveryController(fixture.options);
  const save = controller.saveDiscoveryConfig();

  fixture.refs.adminDiscoveryAutoApproveToggleEl.checked = false;
  fixture.state.discoveryConfigDirty = true;
  controller.saveDiscoveryConfig();
  firstSave.reject(new Error("old value failed"));
  await save;

  assert.deepEqual(submitted, [
    { autoApproveHealthyPendingOnComplete: true },
    { autoApproveHealthyPendingOnComplete: false }
  ]);
  assert.equal(fixture.state.discoveryConfigDirty, false);
  assert.equal(fixture.state.latestDiscoveryConfigCache.savedConfig.autoApproveHealthyPendingOnComplete, false);
  assert.equal(fixture.state.adminBusyState.discoveryConfigSave, false);
  assert.equal(fixture.toasts.filter(item => item.level === "error").length, 0);
  assert.equal(fixture.toasts.filter(item => item.level === "success").length, 1);
});

test("admin discovery config save admits only when registry writes are idle", async () => {
  const fixture = createDiscoveryControllerFixture({
    refs: { adminDiscoveryAutoApproveToggleEl: createElement({ checked: true }) },
    state: { adminBusyState: { discoveryWrite: true } }
  });
  let postCount = 0;
  fixture.options.postBridge = async () => {
    postCount += 1;
    return { ok: true };
  };
  const controller = createAdminDiscoveryController(fixture.options);

  const result = await controller.saveDiscoveryConfig();

  assert.equal(result, null);
  assert.equal(postCount, 0);
  assert.equal(fixture.state.discoveryConfigDirty, true);
  assert.equal(fixture.state.adminBusyState.discoveryWrite, true);
  assert.equal(fixture.state.adminBusyState.discoveryConfigSave, undefined);
  assert.ok(fixture.toasts.some(item => /source registry operation is in progress/.test(item.message)));
});

test("admin discovery config load started during save cannot overwrite the draft or cache", async () => {
  const fixture = createDiscoveryControllerFixture({
    refs: { adminDiscoveryAutoApproveToggleEl: createElement({ checked: true }) },
    state: { latestDiscoveryConfigCache: { marker: "old" }, discoveryConfigDirty: true }
  });
  const getDeferred = createDeferredPromise();
  const postDeferred = createDeferredPromise();
  fixture.options.getBridge = async path => {
    fixture.calls.push(path);
    if (path === "/discovery/config") return getDeferred.promise;
    return {};
  };
  fixture.options.postBridge = async () => postDeferred.promise;
  const controller = createAdminDiscoveryController(fixture.options);
  const save = controller.saveDiscoveryConfig();
  const load = controller.loadDiscoveryConfig({ forceForm: true });

  getDeferred.resolve({ savedConfig: { autoApproveHealthyPendingOnComplete: false }, marker: "stale" });
  await load;
  assert.equal(fixture.refs.adminDiscoveryAutoApproveToggleEl.checked, true);
  assert.deepEqual(fixture.state.latestDiscoveryConfigCache, { marker: "old" });

  postDeferred.resolve({ savedConfig: { autoApproveHealthyPendingOnComplete: true }, marker: "saved" });
  await save;
  assert.equal(fixture.refs.adminDiscoveryAutoApproveToggleEl.checked, true);
  assert.deepEqual(fixture.state.latestDiscoveryConfigCache, {
    savedConfig: { autoApproveHealthyPendingOnComplete: true },
    marker: "saved"
  });
  assert.equal(fixture.state.discoveryConfigDirty, false);
});

test("admin discovery config save leaves discoveryWrite ownership with a concurrent registry mutation", async () => {
  const fixture = createDiscoveryControllerFixture({
    refs: { adminDiscoveryAutoApproveToggleEl: createElement({ checked: true }) }
  });
  const postDeferred = createDeferredPromise();
  fixture.options.postBridge = async () => postDeferred.promise;
  const controller = createAdminDiscoveryController(fixture.options);
  const save = controller.saveDiscoveryConfig();

  const registryDeferred = createDeferredPromise();
  fixture.state.adminBusyState.discoveryWrite = true;
  const registryMutation = registryDeferred.promise.finally(() => {
    fixture.state.adminBusyState.discoveryWrite = false;
  });

  postDeferred.resolve({ savedConfig: { autoApproveHealthyPendingOnComplete: true } });
  await save;

  assert.equal(fixture.state.adminBusyState.discoveryWrite, true);
  assert.equal(fixture.state.adminBusyState.discoveryConfigSave, false);
  assert.equal(deriveAdminActiveWorkContext({ state: fixture.state }).sourceMutationsAllowed, false);

  registryDeferred.resolve();
  await registryMutation;
  assert.equal(fixture.state.adminBusyState.discoveryWrite, false);
});

test("admin discovery config save does not alter idle registry state when registry work finishes first", async () => {
  const fixture = createDiscoveryControllerFixture({
    refs: { adminDiscoveryAutoApproveToggleEl: createElement({ checked: true }) }
  });
  const saveDeferred = createDeferredPromise();
  fixture.options.postBridge = async () => saveDeferred.promise;
  const controller = createAdminDiscoveryController(fixture.options);
  const save = controller.saveDiscoveryConfig();

  const registryDeferred = createDeferredPromise();
  fixture.state.adminBusyState.discoveryWrite = true;
  const registryMutation = registryDeferred.promise.finally(() => {
    fixture.state.adminBusyState.discoveryWrite = false;
  });
  registryDeferred.resolve();
  await registryMutation;

  assert.equal(fixture.state.adminBusyState.discoveryWrite, false);
  assert.equal(fixture.state.adminBusyState.discoveryConfigSave, true);

  saveDeferred.resolve({ savedConfig: { autoApproveHealthyPendingOnComplete: true } });
  await save;

  assert.equal(fixture.state.adminBusyState.discoveryWrite, false);
  assert.equal(fixture.state.adminBusyState.discoveryConfigSave, false);
});

test("admin discovery config save tolerates missing or malformed toggle refs", async t => {
  const toggleCases = [
    { name: "missing toggle", toggle: undefined, expectedValue: false },
    { name: "non-callable setAttribute", toggle: { checked: true, setAttribute: 1 }, expectedValue: true }
  ];

  for (const { name, toggle, expectedValue } of toggleCases) {
    await t.test(name, async () => {
      const fixture = createDiscoveryControllerFixture({
        refs: { adminDiscoveryAutoApproveToggleEl: toggle }
      });
      const posted = [];
      fixture.options.postBridge = async (path, payload) => {
        posted.push({ path, payload });
        return { savedConfig: { autoApproveHealthyPendingOnComplete: payload.autoApproveHealthyPendingOnComplete } };
      };
      const controller = createAdminDiscoveryController(fixture.options);

      await controller.saveDiscoveryConfig();

      assert.ok(posted.some(({ path, payload }) => (
        path === "/discovery/config"
        && payload.autoApproveHealthyPendingOnComplete === expectedValue
      )));
      assert.equal(fixture.state.adminBusyState.discoveryConfigSave, false);
    });
  }
});
