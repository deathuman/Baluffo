import test from "node:test";
import assert from "node:assert/strict";

import { createAdminDomain } from "../../../frontend/local-data/admin-service.js";
import {
  ensureAdminUserRow,
  getAttachmentByteSize,
  utf8ByteLength
} from "../../../frontend/local-data/admin-overview.js";

function createAdminDomainFixture() {
  return createAdminDomain({
    readProfiles: () => [{ id: "u1", name: "Andrea", email: "" }],
    writeProfiles() {},
    listAllSavedJobs: async () => [{ profileId: "u1", notes: "hello" }],
    listAllAttachments: async () => [{ profileId: "u1", size: 12 }],
    withStore: async () => {},
    ensureAdminUserRow,
    utf8ByteLength,
    getAttachmentByteSize,
    sessionKey: "session",
    getCurrentUser: () => null,
    setCurrentUser() {},
    notifyAuthChanged() {},
    notifySavedJobsChanged() {}
  });
}

test("browser admin overview accepts summary detail with metadata size basis", async () => {
  const domain = createAdminDomainFixture();

  const overview = await domain.getAdminOverview({ detail: "summary" });

  assert.equal(overview.detailLevel, "summary");
  assert.equal(overview.attachmentSizeBasis, "metadata");
  assert.equal(overview.totals.usersCount, 1);
  assert.equal(overview.totals.attachmentsBytes, 12);
});

test("browser admin overview rejects unknown detail", async () => {
  const domain = createAdminDomainFixture();

  await assert.rejects(
    () => domain.getAdminOverview({ detail: "deep" }),
    /Invalid admin overview detail/
  );
});
