import test from "node:test";
import assert from "node:assert/strict";
import {
  parseIsoDate,
  getReminderMeta,
  formatRelativeTime,
  getJobHistoryEntries,
  renderPhaseBar,
  formatPhaseTimestamp,
  renderDetailsSummary
} from "../../../frontend/saved/render.js";

test("saved render: date/reminder helpers parse and classify near reminders", () => {
  assert.equal(parseIsoDate("not-a-date"), null);
  assert.ok(parseIsoDate("2026-03-08T10:00:00.000Z") instanceof Date);

  const soon = new Date(Date.now() + 60 * 60 * 1000).toISOString();
  const far = new Date(Date.now() + 120 * 60 * 60 * 1000).toISOString();
  const soonMeta = getReminderMeta(soon, { reminderSoonHours: 72 });
  const farMeta = getReminderMeta(far, { reminderSoonHours: 72 });
  assert.equal(soonMeta.isSoon, true);
  assert.equal(farMeta.isSoon, false);
  assert.ok(soonMeta.label.length > 0);
});

test("saved render: relative time/details summary formatting", () => {
  assert.equal(formatRelativeTime(new Date().toISOString()), "just now");
  assert.equal(formatPhaseTimestamp("invalid"), "");
  assert.ok(formatPhaseTimestamp("2026-03-08T10:00:00.000Z").length > 0);

  const emptySummary = renderDetailsSummary({ notes: "", attachmentsCount: 0 });
  const withSummary = renderDetailsSummary({ notes: "x", attachmentsCount: 2 });
  assert.equal(emptySummary, "");
  assert.match(withSummary, /details-has-content/);
  assert.match(withSummary, /\(2\)/);
});

test("saved render: phase bar and history rows render expected markup", () => {
  const phaseHtml = renderPhaseBar(
    "job-1",
    "applied",
    { applied: "2026-03-08T10:00:00.000Z" },
    "2026-03-08T09:00:00.000Z",
    {
      phaseOptions: ["bookmark", "applied", "rejected"],
      phaseLabels: { bookmark: "Saved", applied: "Applied", rejected: "Rejected" },
      canTransition: () => false,
      currentUser: { uid: "u1" },
      phaseOverrideArmedGlobal: true
    }
  );
  assert.match(phaseHtml, /phase-bar/);
  assert.match(phaseHtml, /override-enabled/);
  assert.match(phaseHtml, /Set phase to Applied/);
  assert.match(phaseHtml, /data-job-key="job-1"/);

  const historyHtml = getJobHistoryEntries("job-1", {
    cachedActivityEntries: [
      { jobKey: "job-1", type: "phase_changed", createdAt: "2026-03-08T10:00:00.000Z", detail: "Applied" }
    ],
    activityTypeLabel: () => "Phase Updated",
    formatPhaseTimestamp,
    formatActivityDetail: () => "Applied"
  });
  assert.match(historyHtml, /job-history-item/);
  assert.match(historyHtml, /Phase Updated/);
  assert.match(historyHtml, /Applied/);
});
