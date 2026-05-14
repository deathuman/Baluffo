import test from "node:test";
import assert from "node:assert/strict";

import { createAdminOverviewController } from "../../../frontend/admin/app/runtime/overview.js";
import { createElement } from "./helpers/admin-controller-test-helpers.mjs";

function createOverviewControllerFixture({ getAdminOverview, overviewTimeoutMs = 25 } = {}) {
  const statuses = [];
  const toasts = [];
  const refs = {
    adminTotalsEl: createElement(),
    adminUsersListEl: createElement({ querySelectorAll: () => [] })
  };
  const controller = createAdminOverviewController({
    refs,
    adminService: {
      getAdminOverview: getAdminOverview || (async () => ({
        ok: true,
        data: { users: [], totals: {} }
      })),
      wipeAccountAdmin: async () => ({ ok: true })
    },
    requestConfirmationDialog: async () => true,
    showToast(message, level) {
      toasts.push({ message, level });
    },
    getErrorMessage: err => String(err?.message || err || "unknown"),
    setSourceStatus(message) {
      statuses.push(message);
    },
    adminDispatch: { dispatch() {} },
    adminActions: { OVERVIEW_REFRESHED: "overview/refreshed" },
    formatBytes: value => String(value),
    renderTotalsHtml: () => "",
    renderUsersTableHtml: rows => rows.map(row => row.uid).join("|"),
    renderUsersEmptyHtml: message => message,
    overviewTimeoutMs
  });
  return { controller, refs, statuses, toasts };
}

test("admin overview forwards timeout to local-data requests", async () => {
  const receivedOptions = [];
  const fixture = createOverviewControllerFixture({
    overviewTimeoutMs: 1234,
    getAdminOverview: async options => {
      receivedOptions.push(options);
      return {
        ok: true,
        data: { users: [{ uid: "local-user" }], totals: {} }
      };
    }
  });

  await fixture.controller.refreshOverview();

  assert.deepEqual(receivedOptions, [{ timeoutMs: 1234 }]);
  assert.equal(fixture.refs.adminUsersListEl.innerHTML, "local-user");
  assert.equal(fixture.statuses.at(-1), "Loaded 1 user account(s).");
});

test("admin overview timeout produces scoped unavailable state", async () => {
  const fixture = createOverviewControllerFixture({
    overviewTimeoutMs: 1,
    getAdminOverview: async () => new Promise(() => {})
  });

  await fixture.controller.refreshOverview();

  assert.equal(fixture.refs.adminUsersListEl.innerHTML, "Could not load admin overview.");
  assert.match(fixture.statuses.at(-1), /Admin overview unavailable: Admin overview request timed out\./);
  assert.deepEqual(fixture.toasts.at(-1), {
    message: "Could not load overview: Admin overview request timed out.",
    level: "error"
  });
});
