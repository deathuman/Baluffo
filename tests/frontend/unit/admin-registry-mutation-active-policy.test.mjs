import test from "node:test";
import assert from "node:assert/strict";

import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import {
  FakeInputElement,
  createRegistryControllerFixture,
  withDom
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin registry controller blocks source mutations during active fetch", async () => {
  await withDom(
    new Map([
      [".pending-source-checkbox", [new FakeInputElement({ checked: true, sourceId: "pending_1" })]]
    ]),
    async () => {
      const fixture = createRegistryControllerFixture({
        state: {
          adminBusyState: {
            discoveryLoad: false,
            liveFetchRunning: true,
            livePipelineRunning: false,
            liveDiscoveryRunning: false,
            liveSyncRunning: false
          }
        }
      });
      fixture.refs.adminPendingSourcesEl.querySelectorAll = selector => global.document.querySelectorAll(selector);
      const controller = createAdminRegistryController(fixture.options);

      await controller.approveSelectedSources();

      assert.equal(fixture.bridgePosts.length, 0);
      assert.ok(fixture.toasts.some(item => /Source registry actions are paused while Admin work is running/i.test(item.message)));
    }
  );
});
