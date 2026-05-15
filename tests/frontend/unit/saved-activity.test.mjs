import test from "node:test";
import assert from "node:assert/strict";
import {
  renderActivityEntries,
  renderSelectedJobHint
} from "../../../frontend/saved/app/activity.js";
import {
  buildTimelinePrefsKey,
  countRecentActivityEntries,
  filterActivityEntriesForScope,
  needsInterviewTimestamp,
  normalizeTimelineScope,
  parseScheduledTimestampInput,
  timelineTypeForEntry,
  toPromptLocalDateTime
} from "../../../frontend/saved/app.js";

test("saved phase time: interview phases require timestamp", () => {
  assert.equal(needsInterviewTimestamp("interview_1"), true);
  assert.equal(needsInterviewTimestamp("interview_2"), true);
  assert.equal(needsInterviewTimestamp("applied"), false);
});

test("saved phase time: parser accepts supported datetime formats", () => {
  const parsedSpaced = parseScheduledTimestampInput("2026-03-09 14:30");
  const parsedIsoLocal = parseScheduledTimestampInput("2026-03-09T14:30");
  assert.match(parsedSpaced, /^2026-03-09T/);
  assert.match(parsedIsoLocal, /^2026-03-09T/);
});

test("saved phase time: parser rejects invalid input and prompt formatter is stable", () => {
  assert.equal(parseScheduledTimestampInput("not a date"), "");
  assert.equal(parseScheduledTimestampInput(""), "");
  assert.match(toPromptLocalDateTime("2026-03-09T14:30:00.000Z"), /^2026-03-09 \d{2}:\d{2}$/);
});

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

test("saved activity copy replaces selected-none and empty timeline text by default", () => {
  const selectedHint = { textContent: "" };
  renderSelectedJobHint(selectedHint, "", new Map());
  assert.equal(selectedHint.textContent, "Showing all saved-job activity.");

  renderSelectedJobHint(selectedHint, "job_1", new Map());
  assert.equal(selectedHint.textContent, "Showing activity for this job.");

  const activityPanelBodyEl = { innerHTML: "" };
  const deps = {
    activityPanelBodyEl,
    lastActivityPulse: null,
    renderActivityEntry: () => "",
    renderTimeline: () => {},
    clearExpiredPulseState: () => {},
    activityHighlightMs: 20
  };
  renderActivityEntries([], deps);
  assert.equal(
    activityPanelBodyEl.innerHTML,
    '<div class="muted">No activity yet. Changes to phases, notes, and files will appear here.</div>'
  );
});
