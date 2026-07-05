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

test("admin registry active discovery preserves delayed placeholders with bounded retry", async () => {
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

test("admin registry active fetch source tables remain delayed without registry fan-out", async () => {
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
          if (path === "/tasks/run-jobs-pipeline-status") return { active: true, stage: "fetch" };
          throw new Error(`unexpected path ${path}`);
        }
      }
    });
    const controller = createAdminRegistryController(fixture.options);

    const firstResult = await controller.loadDiscoveryData({ background: true });
    fixture.renderScheduler.flush();

    assert.equal(firstResult?.skipped, true);
    assert.equal(firstResult?.sourceTablesDelayed, true);
    assert.ok(!calls.some(path => path.startsWith("/registry/sources")));
    assert.ok(!calls.includes("/discovery/report"));
    assert.ok(!calls.includes("/discovery/candidates"));
    assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Source tables delayed while job update is running/);
    assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  } finally {
    timers.restore();
  }
});
