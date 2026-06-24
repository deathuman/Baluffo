import test from "node:test";
import assert from "node:assert/strict";

import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import {
  createRegistryControllerFixture,
  stubScheduledTimers
} from "./helpers/admin-controller-test-helpers.mjs";

async function flushAsyncWork() {
  await Promise.resolve();
  await Promise.resolve();
  await new Promise(resolve => setImmediate(resolve));
  await Promise.resolve();
}

test("admin registry delays source tables during pipeline discovery", async () => {
  const calls = [];
  const fixture = createRegistryControllerFixture({
    state: {
      adminBusyState: {
        discoveryLoad: false,
        livePipelineRunning: true,
        liveDiscoveryRunning: true,
        liveFetchRunning: false
      }
    },
    options: {
      getBridge: async path => {
        calls.push(String(path));
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: true,
            stage: "discovery",
            activeChildren: [{ taskType: "discovery", type: "discovery", active: true }]
          };
        }
        throw new Error(`unexpected path ${path}`);
      }
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData();
  fixture.renderScheduler.flush();

  assert.equal(result?.skipped, true);
  assert.equal(result?.sourceTablesDelayed, true);
  assert.ok(calls.includes("/tasks/run-jobs-pipeline-status"));
  assert.ok(!calls.some(path => path.startsWith("/registry/sources")));
  assert.ok(!calls.includes("/discovery/report"));
  assert.ok(!calls.includes("/discovery/candidates"));
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Source tables delayed while job update is running/);
});

test("admin registry renders delayed placeholders without loading source tables during active discovery", async () => {
  const logs = [];
  const calls = [];
  const fixture = createRegistryControllerFixture({
    state: {
      adminBusyState: {
        discoveryLoad: false,
        livePipelineRunning: true,
        liveDiscoveryRunning: true,
        liveFetchRunning: false
      }
    },
    options: {
      getBridge: async path => {
        calls.push(String(path));
        if (path === "/tasks/run-jobs-pipeline-status") {
          return { active: true, stage: "discovery" };
        }
        throw new Error(`unexpected path ${path}`);
      },
      appendDiscoveryLog(message) {
        logs.push(String(message));
      }
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData();
  fixture.renderScheduler.flush();

  assert.equal(result?.skipped, true);
  assert.equal(result?.sourceTablesDelayed, true);
  assert.ok(calls.includes("/tasks/run-jobs-pipeline-status"));
  assert.ok(!calls.some(path => path.startsWith("/registry/sources")));
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.match(fixture.refs.adminRejectedSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.match(logs.join("\n"), /Source tables delayed while job update is running/);
  assert.doesNotMatch(logs.join("\n"), /Could not load Admin registry source tables/);
});

test("admin registry delayed fetch source tables recover when retry observes idle", async () => {
  const timers = stubScheduledTimers();
  try {
    const calls = [];
    const fixture = createRegistryControllerFixture({
      state: {
        adminBusyState: {
          discoveryLoad: false,
          livePipelineRunning: true,
          liveFetchRunning: true
        }
      },
      options: {
        getBridge: async path => {
          calls.push(String(path));
          if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
          if (path === "/registry/summary") return { ok: true, summary: { pendingCount: 1, activeCount: 1 } };
          if (String(path).startsWith("/registry/sources")) {
            return {
              ok: true,
              sources: {
                pending: [{ name: "Recovered Pending Studio", sourceId: "p1", url: "https://pending.example" }],
                active: [{ name: "Recovered Active Studio", sourceId: "a1", url: "https://active.example" }],
                rejected: []
              },
              summary: { pendingCount: 1, activeCount: 1, rejectedCount: 0 }
            };
          }
          throw new Error(`unexpected path ${path}`);
        }
      }
    });
    const controller = createAdminRegistryController(fixture.options);

    const firstResult = await controller.loadDiscoveryData({ background: true });
    assert.equal(firstResult?.skipped, true);
    assert.equal(firstResult?.sourceTablesDelayed, true);
    assert.ok(!calls.some(path => path.startsWith("/registry/sources?view=table")));
    assert.ok(!calls.includes("/discovery/report"));
    assert.ok(!calls.includes("/discovery/candidates"));
    assert.equal(timers.scheduled.length, 1);
    assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Source tables delayed while job update is running/);

    timers.scheduled.shift()();
    await flushAsyncWork();
    await fixture.state.discoveryLoadPromise;
    fixture.renderScheduler.flush();

    assert.ok(calls.includes("/tasks/run-jobs-pipeline-status"));
    assert.ok(calls.some(path => path.startsWith("/registry/sources?view=table")));
    assert.equal(fixture.state.adminBusyState.livePipelineRunning, false);
    assert.equal(fixture.state.adminBusyState.liveFetchRunning, false);
    assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Recovered Pending Studio/);
    assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Recovered Active Studio/);
  } finally {
    timers.restore();
  }
});
