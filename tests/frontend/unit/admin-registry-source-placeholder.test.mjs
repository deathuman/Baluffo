import test from "node:test";
import assert from "node:assert/strict";
import { createAdminRegistryController } from "../../../frontend/admin/app/registry.js";
import {
  createRegistryControllerFixture
} from "./helpers/admin-controller-test-helpers.mjs";

test("admin registry controller renders delayed source placeholders into empty containers", async () => {
  const fixture = createRegistryControllerFixture();
  const controller = createAdminRegistryController(fixture.options);

  controller.renderSourceTablesDelayed({ onlyIfPlaceholder: true });

  assert.match(
    fixture.refs.adminPendingSourcesEl.innerHTML,
    /Source tables delayed while job update is running/
  );
  assert.match(
    fixture.refs.adminActiveSourcesEl.innerHTML,
    /Source tables delayed while job update is running/
  );
  assert.match(
    fixture.refs.adminRejectedSourcesEl.innerHTML,
    /Source tables delayed while job update is running/
  );

  fixture.refs.adminPendingSourcesEl.innerHTML = "Existing pending rows";
  controller.renderSourceTablesDelayed({ onlyIfPlaceholder: true });
  assert.equal(fixture.refs.adminPendingSourcesEl.innerHTML, "Existing pending rows");

  controller.renderSourceTablesDelayed({ onlyIfPlaceholder: false });
  assert.match(
    fixture.refs.adminPendingSourcesEl.innerHTML,
    /Source tables delayed while job update is running/
  );
});
