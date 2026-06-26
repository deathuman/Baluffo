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

  assert.deepEqual(receivedOptions, [{ timeoutMs: 1234, detail: "full" }]);
  assert.equal(fixture.refs.adminUsersListEl.innerHTML, "local-user");
  assert.equal(fixture.statuses.at(-1), "Loaded 1 user account(s).");
});

test("admin overview summary schedules a full background refresh", async () => {
  const receivedOptions = [];
  const fixture = createOverviewControllerFixture({
    overviewTimeoutMs: 1234,
    getAdminOverview: async options => {
      receivedOptions.push(options);
      const detail = options?.detail || "full";
      return {
        ok: true,
        data: { users: [{ uid: `${detail}-user` }], totals: {}, detailLevel: detail }
      };
    }
  });

  await fixture.controller.refreshOverview({ detail: "summary", scheduleFullRefresh: true });

  assert.deepEqual(receivedOptions, [{ timeoutMs: 1234, detail: "summary" }]);
  assert.equal(fixture.refs.adminUsersListEl.innerHTML, "summary-user");

  await new Promise(resolve => setTimeout(resolve, 180));

  assert.deepEqual(receivedOptions, [
    { timeoutMs: 1234, detail: "summary" },
    { timeoutMs: 1234, detail: "full" }
  ]);
  assert.equal(fixture.refs.adminUsersListEl.innerHTML, "full-user");
});

test("admin overview background full failure preserves summary UI", async () => {
  const receivedOptions = [];
  const fixture = createOverviewControllerFixture({
    overviewTimeoutMs: 1234,
    getAdminOverview: async options => {
      receivedOptions.push(options);
      if (options?.detail === "full") {
        return { ok: false, error: "full overview down", data: null };
      }
      return {
        ok: true,
        data: { users: [{ uid: "summary-user" }], totals: {}, detailLevel: "summary" }
      };
    }
  });

  await fixture.controller.refreshOverview({ detail: "summary", scheduleFullRefresh: true });
  await new Promise(resolve => setTimeout(resolve, 180));

  assert.deepEqual(receivedOptions, [
    { timeoutMs: 1234, detail: "summary" },
    { timeoutMs: 1234, detail: "full" }
  ]);
  assert.equal(fixture.refs.adminUsersListEl.innerHTML, "summary-user");
  assert.match(fixture.statuses.at(-1), /Admin overview exact refresh delayed: full overview down/);
  assert.deepEqual(fixture.toasts, []);
});

test("admin overview empty refresh preserves known signed-in user shell", async () => {
  const fixture = createOverviewControllerFixture({
    getAdminOverview: async () => ({
      ok: true,
      data: { users: [], totals: {}, detailLevel: "full" }
    })
  });

  fixture.controller.renderOverview({
    users: [{ uid: "local_andrea", name: "Andrea" }],
    totals: { usersCount: 1, savedJobsCount: 3 },
    detailLevel: "summary"
  });
  await fixture.controller.refreshOverview();

  assert.equal(fixture.refs.adminUsersListEl.innerHTML, "local_andrea");
  assert.equal(fixture.statuses.at(-1), "Loaded 1 user account(s).");
});

test("admin overview degraded bootstrap renders delayed state instead of false empty", () => {
  const fixture = createOverviewControllerFixture();

  fixture.controller.renderOverview({}, { degraded: true });

  assert.equal(fixture.refs.adminUsersListEl.innerHTML, "Stored profile overview delayed; retrying.");
  assert.equal(fixture.statuses.at(-1), "Stored profile overview delayed; retrying.");
});

test("admin overview explicit empty local-data response remains authoritative empty", () => {
  const fixture = createOverviewControllerFixture();

  fixture.controller.renderOverview({ users: [], totals: {}, detailLevel: "summary" });

  assert.equal(fixture.refs.adminUsersListEl.innerHTML, "No local users found.");
  assert.equal(fixture.statuses.at(-1), "Loaded 0 user account(s).");
});

test("admin overview delayed bootstrap is replaced by summary refresh user data", async () => {
  const fixture = createOverviewControllerFixture({
    getAdminOverview: async () => ({
      ok: true,
      data: {
        users: [{ uid: "local_andrea", name: "Andrea" }],
        totals: { usersCount: 1, savedJobsCount: 3 },
        detailLevel: "summary"
      }
    })
  });

  fixture.controller.renderOverview({}, { degraded: true });
  await fixture.controller.refreshOverview({ detail: "summary", scheduleFullRefresh: false });

  assert.equal(fixture.refs.adminUsersListEl.innerHTML, "local_andrea");
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
