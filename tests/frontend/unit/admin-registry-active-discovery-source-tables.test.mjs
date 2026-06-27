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

test("admin registry uses compact source tables during pipeline discovery", async () => {
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
        if (String(path).startsWith("/registry/sources")) {
          return {
            ok: true,
            activeCompact: true,
            sources: {
              pending: [{ name: "Pending Discovery Studio" }],
              active: [{ name: "Active Discovery Studio" }],
              rejected: []
            },
            summary: {}
          };
        }
        throw new Error(`unexpected path ${path}`);
      }
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData();
  fixture.renderScheduler.flush();

  assert.notEqual(result?.skipped, true);
  assert.notEqual(result?.sourceTablesDelayed, true);
  assert.ok(calls.includes("/tasks/run-jobs-pipeline-status"));
  assert.ok(calls.some(path => path.startsWith("/registry/sources") && path.includes("activeCompact=1")));
  assert.ok(!calls.includes("/discovery/report"));
  assert.ok(!calls.includes("/discovery/candidates"));
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Pending Discovery Studio/);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Active Discovery Studio/);
});

test("admin registry compact active discovery failure preserves delayed placeholders with bounded retry", async () => {
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
        if (String(path).startsWith("/registry/sources")) {
          throw new Error("Bridge error (HTTP 504)");
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

  assert.equal(result?.partialLoadFailed, true);
  assert.ok(calls.includes("/tasks/run-jobs-pipeline-status"));
  assert.ok(calls.some(path => path.startsWith("/registry/sources") && path.includes("activeCompact=1")));
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.match(fixture.refs.adminRejectedSourcesEl.innerHTML, /Source tables delayed while job update is running/);
  assert.match(logs.join("\n"), /Source tables delayed while job update is running/);
  assert.doesNotMatch(logs.join("\n"), /Could not load Admin registry source tables/);
});

test("admin registry active fetch source tables load through compact route", async () => {
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
          if (String(path).startsWith("/registry/sources")) {
            return {
              ok: true,
              activeCompact: true,
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
    fixture.renderScheduler.flush();

    assert.notEqual(firstResult?.skipped, true);
    assert.notEqual(firstResult?.sourceTablesDelayed, true);
    assert.ok(calls.some(path => path.startsWith("/registry/sources?view=table") && path.includes("activeCompact=1")));
    assert.ok(!calls.includes("/discovery/report"));
    assert.ok(!calls.includes("/discovery/candidates"));
    assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Recovered Pending Studio/);
    assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Recovered Active Studio/);
  } finally {
    timers.restore();
  }
});
