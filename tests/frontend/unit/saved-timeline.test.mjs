import test from "node:test";
import assert from "node:assert/strict";
import {
  normalizeTimelineScope,
  timelineTypeForEntry,
  filterActivityEntriesForScope,
  countRecentActivityEntries,
  buildTimelinePrefsKey
} from "../../../frontend/saved/app.js";

test("saved timeline helpers normalize scope and build preference key", () => {
  assert.equal(normalizeTimelineScope("phase"), "phase");
  assert.equal(normalizeTimelineScope("invalid"), "all");
  assert.equal(buildTimelinePrefsKey("u1"), "baluffo_saved_timeline_prefs:u1");
});

test("saved timeline helpers classify activity entry types", () => {
  assert.equal(timelineTypeForEntry({ type: "phase_changed" }), "phase");
  assert.equal(timelineTypeForEntry({ type: "notes_saved" }), "notes");
  assert.equal(timelineTypeForEntry({ type: "attachment_deleted" }), "attachments");
  assert.equal(timelineTypeForEntry({ type: "saved_job_added" }), "all");
});

test("saved timeline helpers filter entries by scope", () => {
  const entries = [
    { type: "phase_changed", jobKey: "job_1", createdAt: "2026-03-08T10:00:00.000Z" },
    { type: "notes_saved", jobKey: "job_2", createdAt: "2026-03-08T11:00:00.000Z" },
    { type: "attachment_added", jobKey: "job_1", createdAt: "2026-03-08T12:00:00.000Z" }
  ];

  assert.equal(filterActivityEntriesForScope(entries, "all", "").length, 3);
  assert.equal(filterActivityEntriesForScope(entries, "selected", "job_1").length, 2);
  assert.equal(filterActivityEntriesForScope(entries, "phase", "").length, 1);
  assert.equal(filterActivityEntriesForScope(entries, "notes", "").length, 1);
  assert.equal(filterActivityEntriesForScope(entries, "attachments", "").length, 1);
});

test("saved timeline helpers count recent activity within 24h window", () => {
  const now = Date.now();
  const recent = new Date(now - 2 * 60 * 60 * 1000).toISOString();
  const old = new Date(now - 30 * 60 * 60 * 1000).toISOString();
  assert.equal(countRecentActivityEntries([{ createdAt: recent }, { createdAt: old }], 24), 1);
});
