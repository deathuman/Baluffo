import test from "node:test";
import assert from "node:assert/strict";
import {
  parseIsoDate,
  getReminderMeta,
  formatRelativeTime,
  getJobHistoryEntries,
  renderSavedJobBlockHtml,
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
  assert.match(phaseHtml, /data-tooltip="Set phase to Saved\."/);
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

test("saved render shows lifecycle overlay badges read-only", () => {
  const html = renderSavedJobBlockHtml({
    jobKey: "job_1",
    title: "Gameplay Engineer",
    company: "Studio",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time",
    jobLink: "https://example.com/jobs/1",
    applicationStatus: "bookmark",
    phaseTimestamps: {},
    savedAt: "2026-03-08T09:00:00.000Z",
    notes: ""
  }, {
    isCustomJob: () => false,
    customSourceLabel: "Custom",
    normalizeSavedSector: () => "Game",
    fullCountryName: value => value,
    sanitizeUrl: value => value,
    toContractClass: () => "full-time",
    normalizePhase: value => value || "bookmark",
    expandedJobKey: "",
    selectedJobKey: "",
    getJobDetailsTab: () => "notes",
    renderDetailsSummary: () => "",
    getReminderMeta: () => ({ isSoon: false, label: "" }),
    renderMissingInfoChips: () => "",
    renderUpdatedHint: () => "",
    getJobHistoryEntries: () => "",
    renderWebIcon: () => "",
    renderPhaseBar: () => "",
    lifecycleOverlay: {
      status: "likely_removed",
      removedAt: "2026-03-07T00:00:00.000Z",
      lifecycleEvent: "",
      lifecycleReason: ""
    },
    currentUser: { uid: "u1" },
    maxAttachmentsPerJob: 10,
    maxAttachmentBytes: 1024
  });

  assert.match(html, /job-lifecycle-badge likely-removed/);
  assert.match(html, /Recently removed/);
  assert.match(html, /data-tooltip="Gameplay Engineer"/);
  assert.match(html, /data-tooltip="Studio"/);
  assert.match(html, /data-tooltip="Recently removed since Mar 7, 2026"/);
  assert.match(html, /remove-saved-btn[\s\S]*data-tooltip="Remove this job from Saved Jobs\."/);
  assert.match(html, /details-toggle-btn[\s\S]*data-tooltip="Show notes, files, and history for this job\."/);
  assert.match(html, /attach-upload-btn[\s\S]*data-tooltip="Attach files to this saved job\."/);
  assert.match(html, /job-history-refresh-btn[\s\S]*data-tooltip="Reload activity history for this job\."/);
  assert.doesNotMatch(html, /\stitle="/);
  assert.doesNotMatch(html, /save-job-btn/);
});

test("saved render exposes custom job action tooltips", () => {
  const html = renderSavedJobBlockHtml({
    jobKey: "custom_1",
    title: "Personal lead",
    company: "Studio",
    city: "Paris",
    country: "France",
    applicationStatus: "bookmark",
    phaseTimestamps: {},
    savedAt: "2026-03-08T09:00:00.000Z",
    notes: ""
  }, {
    isCustomJob: () => true,
    customSourceLabel: "Custom",
    normalizeSavedSector: () => "Custom",
    fullCountryName: value => value,
    sanitizeUrl: value => value,
    toContractClass: () => "unknown",
    normalizePhase: value => value || "bookmark",
    expandedJobKey: "",
    selectedJobKey: "",
    getJobDetailsTab: () => "notes",
    renderDetailsSummary: () => "",
    getReminderMeta: () => ({ isSoon: false, label: "" }),
    renderMissingInfoChips: () => "",
    renderUpdatedHint: () => "",
    getJobHistoryEntries: () => "",
    renderWebIcon: () => "",
    renderPhaseBar: () => "",
    lifecycleOverlay: null,
    currentUser: { uid: "u1" },
    maxAttachmentsPerJob: 10,
    maxAttachmentBytes: 1024
  });

  assert.match(html, /personal-edit-btn[\s\S]*data-tooltip="Edit this custom saved job\."/);
  assert.match(html, /personal-duplicate-btn[\s\S]*data-tooltip="Duplicate this custom job as a new entry\."/);
  assert.doesNotMatch(html, /\stitle="/);
});

test("saved render uses compact location display without repeated unknowns", () => {
  const html = renderSavedJobBlockHtml({
    jobKey: "job_2",
    title: "Analytics Manager",
    company: "Atari",
    city: "Remote",
    country: "Remote",
    locationSummary: "Remote, Remote",
    workType: "Remote",
    contractType: "Unknown",
    applicationStatus: "bookmark",
    phaseTimestamps: {},
    savedAt: "2026-03-08T09:00:00.000Z",
    notes: ""
  }, {
    isCustomJob: () => false,
    customSourceLabel: "Custom",
    normalizeSavedSector: () => "Game",
    fullCountryName: value => value,
    sanitizeUrl: value => value,
    toContractClass: () => "unknown",
    normalizePhase: value => value || "bookmark",
    expandedJobKey: "",
    selectedJobKey: "",
    getJobDetailsTab: () => "notes",
    renderDetailsSummary: () => "",
    getReminderMeta: () => ({ isSoon: false, label: "" }),
    renderMissingInfoChips: () => "",
    renderUpdatedHint: () => "",
    getJobHistoryEntries: () => "",
    renderWebIcon: () => "",
    renderPhaseBar: () => "",
    lifecycleOverlay: null,
    currentUser: { uid: "u1" },
    maxAttachmentsPerJob: 10,
    maxAttachmentBytes: 1024
  });

  assert.doesNotMatch(html, /Remote, Remote/);
  assert.doesNotMatch(html, /Unknown Unknown/);
});
