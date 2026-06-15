import test from "node:test";
import assert from "node:assert/strict";

import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import { createRegistryControllerFixture } from "./helpers/admin-controller-test-helpers.mjs";

test("admin source tables recover from delayed load state after active run becomes idle", async () => {
  const calls = [];
  const fixture = createRegistryControllerFixture({
    state: {
      sourceTablesLoadState: "delayed-active",
      sourceTablesDelayedDuringActiveRun: false,
      discoveryTablesRendered: true,
      adminBusyState: { discoveryLoad: false, livePipelineRunning: false, liveFetchRunning: false }
    },
    options: {
      getBridge: async path => {
        calls.push(String(path));
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/registry/summary") return { ok: true, summary: { pendingCount: 1 } };
        if (String(path).startsWith("/registry/sources")) {
          return {
            ok: true,
            sources: {
              pending: [{ name: "State Recovered Pending", sourceId: "p1", url: "https://pending.example" }],
              active: [{ name: "State Recovered Active", sourceId: "a1", url: "https://active.example" }],
              rejected: []
            },
            summary: { pendingCount: 1, activeCount: 1, rejectedCount: 0 }
          };
        }
        throw new Error(`unexpected path ${path}`);
      },
      fetchJobsFetchReportJson: async () => ({ sources: [] })
    }
  });
  const controller = createAdminRegistryController(fixture.options);

  const result = await controller.refreshSourceTablesAfterActiveRunIdle();
  fixture.renderScheduler.flush();

  assert.equal(result?.partialLoadFailed, false);
  assert.equal(fixture.state.sourceTablesLoadState, "loaded");
  assert.ok(calls.some(path => path.startsWith("/registry/sources")));
  assert.match(fixture.refs.adminPendingSourcesEl.innerHTML, /State Recovered Pending/);
  assert.match(fixture.refs.adminActiveSourcesEl.innerHTML, /State Recovered Active/);
});
