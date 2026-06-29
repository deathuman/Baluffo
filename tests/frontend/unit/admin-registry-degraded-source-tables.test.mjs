import test from "node:test";
import assert from "node:assert/strict";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { createRegistryControllerFixture } from "./helpers/admin-controller-test-helpers.mjs";

test("admin registry controller treats degraded empty compact source payload as refreshing counts", async () => {
  const calls = [];
  const fixture = createRegistryControllerFixture({
    state: {
      latestDiscoveryReportCache: { runId: "discovery_live_1", summary: {} },
      adminBusyState: { discoveryLoad: false, discoveryWatch: true, liveDiscoveryRunning: true }
    },
    options: {
      getBridge: async path => {
        calls.push(String(path));
        if (path === "/tasks/run-jobs-pipeline-status") {
          return { active: true, stage: "discovery" };
        }
        if (String(path).startsWith("/registry/sources")) {
          return {
            ok: true,
            activeCompact: true,
            degraded: true,
            source: "registry-json-compact-fallback",
            sources: { pending: [], active: [], rejected: [] },
            summary: { pendingCount: 813, activeCount: 2312, rejectedCount: 0 }
          };
        }
        throw new Error(`unexpected path ${path}`);
      }
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData();
  fixture.renderScheduler.flush();

  assert.equal(result?.partialLoadFailed, true);
  assert.ok(calls.some(path => String(path).startsWith("/registry/sources") && String(path).includes("activeCompact=1")));
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Source tables refreshing/i);
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /pending 813/i);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /active 2[,.]312/i);
});
