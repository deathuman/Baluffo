import test from "node:test";
import assert from "node:assert/strict";

function setMockApi(api) {
  global.window = { JobAppLocalData: api };
}

test("authService returns success result", async () => {
  setMockApi({
    isReady: () => true,
    getCurrentUser: () => ({ uid: "u1" }),
    onAuthStateChanged: () => () => {},
    signIn: async () => {},
    signOut: async () => {}
  });
  const { authService } = await import("../../../frontend/local-data/services.js");
  const result = await authService.signIn();
  assert.equal(result.ok, true);
  assert.equal(result.error, "");
});

test("savedJobsService normalizes key list result", async () => {
  setMockApi({
    getSavedJobKeys: async () => ["job_a", "job_b"]
  });
  const { savedJobsService } = await import("../../../frontend/local-data/services.js");
  const result = await savedJobsService.getSavedJobKeys("u1");
  assert.equal(result.ok, true);
  assert.deepEqual(result.data, ["job_a", "job_b"]);
});

test("adminService overview forwards without a PIN argument", async () => {
  const calls = [];
  setMockApi({
    getAdminOverview: async (...args) => {
      calls.push(args);
      return { users: [{ uid: "u1" }], totals: { users: 1 } };
    }
  });
  const { adminService } = await import("../../../frontend/local-data/services.js");
  const result = await adminService.getAdminOverview();
  assert.equal(result.ok, true);
  assert.deepEqual(calls, [[]]);
  assert.equal(result.data.users.length, 1);
});

test("adminService overview forwards detail options", async () => {
  const calls = [];
  setMockApi({
    getAdminOverview: async (...args) => {
      calls.push(args);
      return { users: [{ uid: "u1" }], totals: { users: 1 }, detailLevel: "summary" };
    }
  });
  const { adminService } = await import("../../../frontend/local-data/services.js");
  const result = await adminService.getAdminOverview({ detail: "summary", timeoutMs: 250 });
  assert.equal(result.ok, true);
  assert.deepEqual(calls, [[{ detail: "summary", timeoutMs: 250 }]]);
  assert.equal(result.data.detailLevel, "summary");
});

test("historyService returns error contract on failure", async () => {
  setMockApi({
    listActivityForUser: async () => {
      throw new Error("history boom");
    }
  });
  const { historyService } = await import("../../../frontend/local-data/services.js");
  const result = await historyService.listActivityForUser("u1", 20);
  assert.equal(result.ok, false);
  assert.match(result.error, /history boom/i);
});

test("backupService returns error contract on failure", async () => {
  setMockApi({
    exportProfileData: async () => {
      throw new Error("export failed");
    }
  });
  const { backupService } = await import("../../../frontend/local-data/services.js");
  const result = await backupService.exportProfileData("u1", {});
  assert.equal(result.ok, false);
  assert.match(result.error, /export failed/i);
});

test("savedJobsService remove returns error contract on failure", async () => {
  setMockApi({
    removeSavedJobForUser: async () => {
      throw new Error("remove failed");
    }
  });
  const { savedJobsService } = await import("../../../frontend/local-data/services.js");
  const result = await savedJobsService.removeSavedJobForUser("u1", "job_1");
  assert.equal(result.ok, false);
  assert.match(result.error, /remove failed/i);
});

test("attachmentsService list returns normalized list", async () => {
  setMockApi({
    listAttachmentsForJob: async () => [{ id: "a1" }]
  });
  const { attachmentsService } = await import("../../../frontend/local-data/services.js");
  const result = await attachmentsService.listAttachmentsForJob("u1", "job_1");
  assert.equal(result.ok, true);
  assert.deepEqual(result.data, [{ id: "a1" }]);
});

test("attachmentsService list preserves hydrated image blobs for previews", async () => {
  const blob = new Blob(["img"], { type: "image/png" });
  setMockApi({
    listAttachmentsForJob: async () => [{ id: "a1", name: "shot.png", type: "image/png", blob }]
  });
  const { attachmentsService } = await import("../../../frontend/local-data/services.js");
  const result = await attachmentsService.listAttachmentsForJob("u1", "job_1");
  assert.equal(result.ok, true);
  assert.equal(result.data.length, 1);
  assert.equal(result.data[0].blob, blob);
});

test("adminService overview returns fallback data on error", async () => {
  setMockApi({
    getAdminOverview: async () => {
      throw new Error("overview down");
    }
  });
  const { adminService } = await import("../../../frontend/local-data/services.js");
  const result = await adminService.getAdminOverview();
  assert.equal(result.ok, false);
  assert.deepEqual(result.data, { users: [], totals: {} });
  assert.match(result.error, /overview down/i);
});

test("authService.isReady mirrors runtime readiness even with a minimal contract", async () => {
  setMockApi({
    isReady: () => true
  });
  const { authService } = await import("../../../frontend/local-data/services.js");
  assert.equal(authService.isReady(), true);
});
