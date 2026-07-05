import test from "node:test";
import assert from "node:assert/strict";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { createRegistryControllerFixture } from "./helpers/admin-controller-test-helpers.mjs";

test("admin registry controller delays source tables during active discovery before compact source payloads", async () => {
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
        throw new Error(`unexpected path ${path}`);
      }
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.loadDiscoveryData();
  fixture.renderScheduler.flush();

  assert.equal(result?.skipped, true);
  assert.equal(result?.sourceTablesDelayed, true);
  assert.ok(!calls.some(path => String(path).startsWith("/registry/sources")));
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /Source tables delayed while job update is running/i);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /Source tables delayed while job update is running/i);
});
