import test from "node:test";
import assert from "node:assert/strict";
import { buildProfileBackupPayload } from "../../../frontend/local-data/backup-import-export.js";

test("backup import-export: buildProfileBackupPayload emits deterministic ordering", () => {
  const payload = buildProfileBackupPayload("u1", {
    includeFiles: true,
    nowIso: () => "2026-03-09T10:00:00.000Z",
    backupSchemaVersion: 2,
    normalizeSavedJobRecord: (_uid, row) => ({ ...row }),
    profile: { id: "u1", name: "User One" },
    savedJobs: [
      { jobKey: "job_b", title: "B", savedAt: "2026-03-09T09:00:00.000Z", updatedAt: "2026-03-09T09:10:00.000Z" },
      { jobKey: "job_a", title: "A", savedAt: "2026-03-09T09:30:00.000Z", updatedAt: "2026-03-09T09:35:00.000Z" }
    ],
    attachments: [
      { id: "att_2", jobKey: "job_b", createdAt: "2026-03-09T09:40:00.000Z", name: "cv-b.pdf" },
      { id: "att_1", jobKey: "job_a", createdAt: "2026-03-09T09:20:00.000Z", name: "cv-a.pdf" }
    ],
    activityLog: [
      { id: "ev_2", type: "phase_changed", jobKey: "job_b", createdAt: "2026-03-09T09:45:00.000Z" },
      { id: "ev_1", type: "saved", jobKey: "job_a", createdAt: "2026-03-09T09:15:00.000Z" }
    ]
  });

  assert.deepEqual(
    payload.savedJobs.map(row => row.jobKey),
    ["job_a", "job_b"]
  );
  assert.deepEqual(
    payload.attachments.map(row => row.id),
    ["att_1", "att_2"]
  );
  assert.deepEqual(
    payload.activityLog.map(row => row.id),
    ["ev_1", "ev_2"]
  );
  assert.equal(payload.schemaVersion, 2);
  assert.equal(payload.version, 2);
  assert.equal(payload.includesFiles, true);
  assert.ok(Array.isArray(payload.savedJobs));
  assert.ok(Array.isArray(payload.attachments));
  assert.ok(Array.isArray(payload.activityLog));
  assert.ok(payload.counts && typeof payload.counts === "object");
  assert.equal(typeof payload.counts.savedJobs, "number");
  assert.equal(typeof payload.counts.historyEvents, "number");
  assert.equal(typeof payload.counts.attachments, "number");
});

test("backup import-export: includesFiles false keeps payload contract intact", () => {
  const payload = buildProfileBackupPayload("u1", {
    includeFiles: false,
    nowIso: () => "2026-03-09T10:00:00.000Z",
    backupSchemaVersion: 2,
    normalizeSavedJobRecord: (_uid, row) => ({ ...row }),
    profile: { id: "u1", name: "User One" },
    savedJobs: [{ jobKey: "job_1", title: "A", company: "Studio" }],
    attachments: [{ id: "att_1", jobKey: "job_1", name: "a.txt", size: 1, type: "text/plain" }],
    activityLog: []
  });
  assert.equal(payload.includesFiles, false);
  assert.equal(Array.isArray(payload.savedJobs), true);
  assert.equal(Array.isArray(payload.attachments), true);
  assert.equal(Array.isArray(payload.activityLog), true);
  assert.equal(payload.savedJobs.length, 1);
  assert.equal(payload.attachments.length, 1);
  assert.equal(payload.activityLog.length, 0);
  assert.equal(payload.attachments[0].blobDataUrl, undefined);
});

test("backup import-export: includesFiles true keeps attachment payload references", () => {
  const payload = buildProfileBackupPayload("u1", {
    includeFiles: true,
    nowIso: () => "2026-03-09T10:00:00.000Z",
    backupSchemaVersion: 2,
    normalizeSavedJobRecord: (_uid, row) => ({ ...row }),
    profile: { id: "u1", name: "User One" },
    savedJobs: [{ jobKey: "job_1", title: "A", company: "Studio" }],
    attachments: [
      {
        id: "att_1",
        jobKey: "job_1",
        name: "a.txt",
        size: 1,
        type: "text/plain",
        blobDataUrl: "data:text/plain;base64,QQ=="
      }
    ],
    activityLog: []
  });
  assert.equal(payload.includesFiles, true);
  assert.equal(payload.attachments[0].blobDataUrl, "data:text/plain;base64,QQ==");
  assert.equal(payload.schemaVersion, 2);
  assert.equal(typeof payload.counts.savedJobs, "number");
  assert.equal(typeof payload.counts.historyEvents, "number");
  assert.equal(typeof payload.counts.attachments, "number");
});
