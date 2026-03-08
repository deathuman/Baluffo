import test from "node:test";
import assert from "node:assert/strict";
import { createBackupDomain } from "../../../frontend/local-data/backup-service.js";

test("backup domain importProfileData reports created/updated/skipped and attachment hydration counters", async () => {
  const writes = {
    saved_jobs: [],
    attachments: [],
    activity_log: []
  };
  const metadataCalls = [];
  const updateAttachmentCalls = [];
  let notifyCount = 0;

  const domain = createBackupDomain({
    withStore: async (storeName, _mode, fn) => {
      const store = {
        put(row) {
          writes[storeName].push(row);
        }
      };
      await new Promise((resolve, reject) => {
        fn(store, resolve, reject);
      });
    },
    ensureCurrentUser: () => ({ uid: "u1" }),
    readProfiles: () => [{ id: "u1", name: "User One" }],
    listSavedJobs: async () => [
      { jobKey: "job_existing", title: "Old Title", company: "Studio", profileId: "u1" }
    ],
    listAttachmentMetadata: async () => {
      metadataCalls.push("called");
      return [
        {
          id: "att_same",
          jobKey: "job_existing",
          profileId: "u1",
          name: "cv.pdf",
          type: "application/pdf",
          size: 10,
          createdAt: "2026-03-08T10:00:00.000Z",
          blob: null
        }
      ];
    },
    listAllActivityForProfile: async () => [
      {
        id: "log_1",
        profileId: "u1",
        type: "event",
        jobKey: "job_existing",
        title: "Old Title",
        company: "Studio",
        createdAt: "2026-03-08T09:00:00.000Z",
        details: {}
      }
    ],
    notifySavedJobsChanged: async () => {
      notifyCount += 1;
    },
    updateAttachmentMetadata: async (uid, jobKey, count) => {
      updateAttachmentCalls.push({ uid, jobKey, count });
    },
    parseBackupPayload: payload => payload,
    buildProfileBackupPayload: () => {
      throw new Error("not used in import test");
    },
    normalizeIsoOrNow: (value, fallback = "") => String(value || fallback),
    toPlainObject: value => (value && typeof value === "object" && !Array.isArray(value) ? value : {}),
    areSavedRowsEquivalent: (a, b) => String(a?.title || "") === String(b?.title || ""),
    serializeAttachmentWithBlob: async row => row,
    stripAttachmentBlob: row => row,
    stripAttachmentPk: row => row,
    deserializeAttachmentBlob: row => row.blob || null,
    nowIso: () => "2026-03-08T12:00:00.000Z",
    toActivityId: (_uid, _type, _jobKey) => "generated_activity_id",
    toAttachmentId: () => "generated_attachment_id",
    normalizeSavedJobRecord: (_uid, row) => ({
      pk: `u1::${row.jobKey}`,
      profileId: "u1",
      jobKey: row.jobKey,
      title: String(row.title || ""),
      company: String(row.company || "")
    }),
    mergeSavedJobRows: (_uid, existing, imported) => ({
      ...existing,
      title: String(imported.title || existing.title),
      company: String(imported.company || existing.company)
    }),
    normalizeImportedAttachmentRow: (_uid, row) => ({
      pk: `u1::${row.jobKey}::${row.id}`,
      id: row.id,
      profileId: "u1",
      jobKey: row.jobKey,
      name: row.name,
      type: row.type,
      size: row.size,
      createdAt: row.createdAt,
      blob: row.blob
    }),
    toAttachmentFingerprint: row =>
      [row.profileId, row.jobKey, row.id, row.name, row.type, row.size, row.createdAt].join("|")
  });

  const result = await domain.importProfileData("u1", {
    schemaVersion: 2,
    savedJobs: [
      { jobKey: "job_invalid", title: "Invalid Missing Company", company: "" },
      { jobKey: "job_new", title: "New Role", company: "New Studio" },
      { jobKey: "job_existing", title: "Updated Role", company: "Studio" }
    ],
    attachments: [
      {
        id: "att_same",
        jobKey: "job_existing",
        name: "cv.pdf",
        type: "application/pdf",
        size: 10,
        createdAt: "2026-03-08T10:00:00.000Z",
        blob: new Blob(["hydrated"])
      },
      {
        id: "att_new",
        jobKey: "job_new",
        name: "portfolio.pdf",
        type: "application/pdf",
        size: 20,
        createdAt: "2026-03-08T10:10:00.000Z",
        blob: new Blob(["new"])
      }
    ],
    activityLog: [
      {
        id: "log_2",
        type: "phase_changed",
        jobKey: "job_new",
        title: "New Role",
        company: "New Studio",
        createdAt: "2026-03-08T11:00:00.000Z",
        details: { previousStatus: "bookmark", nextStatus: "applied" }
      }
    ],
    warnings: ["payload-warning"]
  });

  assert.equal(result.schemaVersion, 2);
  assert.equal(result.created, 1);
  assert.equal(result.updated, 1);
  assert.equal(result.skippedInvalid, 1);
  assert.equal(result.attachmentsAdded, 1);
  assert.equal(result.attachmentsHydrated, 1);
  assert.equal(result.historyAdded, 1);
  assert.match(result.warnings.join(" | "), /payload-warning/);
  assert.match(result.warnings.join(" | "), /missing title\/company/i);

  assert.equal(writes.saved_jobs.length, 2);
  assert.equal(writes.attachments.length, 2);
  assert.equal(writes.activity_log.length, 1);
  assert.equal(metadataCalls.length >= 2, true);
  assert.equal(updateAttachmentCalls.length >= 2, true);
  assert.equal(notifyCount, 1);
});
