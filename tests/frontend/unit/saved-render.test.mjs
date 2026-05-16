import test from "node:test";
import assert from "node:assert/strict";
import {
  parseIsoDate,
  getReminderMeta,
  formatRelativeTime,
  getJobHistoryEntries,
  renderSavedJobBlockHtml,
  renderRemoveSavedIcon,
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
      phaseOptions: ["bookmark", "applied", "offer"],
      phaseLabels: { bookmark: "Saved", applied: "Applied", offer: "Offer" },
      canTransition: () => false,
      currentUser: { uid: "u1" }
    }
  );
  assert.match(phaseHtml, /phase-bar/);
  assert.match(phaseHtml, /phase-timeline-step/);
  assert.match(phaseHtml, /phase-step-node/);
  assert.match(phaseHtml, /phase-step-check/);
  assert.match(phaseHtml, /data-phase-status="completed"/);
  assert.match(phaseHtml, /data-phase-status="current"/);
  assert.match(phaseHtml, /aria-current="step"/);
  assert.match(phaseHtml, /aria-label="Applied, current phase, entered/);
  assert.match(phaseHtml, /data-phase-time=/);
  assert.doesNotMatch(phaseHtml, /phase-step-time/);
  assert.doesNotMatch(phaseHtml, /data-tooltip="Set phase to Saved\."/);
  assert.doesNotMatch(phaseHtml, /data-tooltip="Set phase to Applied\."/);
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
  assert.doesNotMatch(html, /data-tooltip="Gameplay Engineer"/);
  assert.match(html, /data-tooltip-if-clipped="Gameplay Engineer"/);
  assert.doesNotMatch(html, /data-tooltip="Studio"/);
  assert.doesNotMatch(html, /data-tooltip="Italy, Rome"/);
  assert.doesNotMatch(html, /data-tooltip="Rome"/);
  assert.match(html, /<div class="saved-title-line">\s*<span class="saved-title-main" data-tooltip-if-clipped="Gameplay Engineer">Gameplay Engineer<\/span>\s*<\/div>/);
  assert.match(html, /<div class="job-sector-line"[^>]*>Game<\/div>/);
  assert.match(html, /<div class="col-company job-cell" data-label="Company">\s*<span class="job-company-compact">Studio<\/span>/);
  assert.match(html, /<div class="col-location job-cell" data-label="Location">\s*<div class="job-location-stack">/);
  assert.match(html, /<span class="job-country-main">Italy<\/span>/);
  assert.match(html, /<span class="job-city-sub">Rome<\/span>/);
  assert.match(html, /data-tooltip="Recently removed since Mar 7, 2026"/);
  assert.match(html, /remove-saved-btn[\s\S]*data-tooltip="Remove saved job"/);
  assert.doesNotMatch(html, /details-toggle-btn[\s\S]*data-tooltip="Show notes, files, and history for this job\."/);
  assert.match(html, /details-toggle-icon[\s\S]*<svg viewBox="0 0 24 24"/);
  assert.match(html, /details-toggle-text[\s\S]*Notes, Files &amp; History|details-toggle-text[\s\S]*Notes, Files & History/);
  assert.match(html, /<span class="details-toggle-arrow\s*" aria-hidden="true"><\/span>/);
  assert.doesNotMatch(html, /details-toggle-arrow[\s\S]*<svg viewBox="0 0 24 24"/);
  assert.doesNotMatch(html, /details-toggle-arrow[^>]*>[v>]</);
  assert.doesNotMatch(html, /attach-upload-btn[\s\S]*data-tooltip="Attach files to this saved job\."/);
  assert.doesNotMatch(html, /job-history-refresh-btn[\s\S]*data-tooltip="Reload activity history for this job\."/);
  assert.doesNotMatch(html, /class="col-sector job-cell"/);
  assert.doesNotMatch(html, /class="col-city job-cell"/);
  assert.doesNotMatch(html, /class="col-country job-cell"/);
  assert.doesNotMatch(html, /\stitle="/);
  assert.doesNotMatch(html, /save-job-btn/);
});

test("saved render uses remove icon and contextual phase override by default", () => {
  const baseJob = {
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
  };
  const baseOptions = {
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
    lifecycleOverlay: null,
    currentUser: { uid: "u1" },
    maxAttachmentsPerJob: 10,
    maxAttachmentBytes: 1024
  };

  const html = renderSavedJobBlockHtml(baseJob, baseOptions);
  assert.match(html, /remove-saved-btn[\s\S]*data-tooltip="Remove saved job"/);
  assert.match(html, /<svg viewBox="0 0 24 24"/);
  assert.doesNotMatch(html, />X<\/button>/);
  assert.match(renderRemoveSavedIcon(), /currentColor/);

  const selectedHtml = renderSavedJobBlockHtml(baseJob, { ...baseOptions, selectedJobKey: "job_1" });
  assert.doesNotMatch(selectedHtml, /saved-job-block[^"]*\bselected\b/);
  assert.match(selectedHtml, /data-selected="true"/);

  const phaseHtml = renderPhaseBar("job_1", "bookmark", {}, "", {
    phaseOptions: ["bookmark", "applied", "offer"],
    phaseLabels: { bookmark: "Saved", applied: "Applied", offer: "Final Round" },
    canTransition: (_current, next) => next === "applied",
    currentUser: { uid: "u1" },
    phaseOverrideContext: { jobKey: "job_1", phase: "offer" }
  });
  assert.match(phaseHtml, /phase-override-context/);
  assert.match(phaseHtml, /This phase change is normally locked because it skips or rewinds an application step\./);
  assert.match(phaseHtml, /Override phase/);
  assert.match(phaseHtml, /data-ui="tracking-override-cancel-btn"/);
});

test("saved render omits redundant custom job action tooltips", () => {
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

  assert.doesNotMatch(html, /personal-edit-btn[\s\S]*data-tooltip="Edit this custom saved job\."/);
  assert.doesNotMatch(html, /personal-duplicate-btn[\s\S]*data-tooltip="Duplicate this custom job as a new entry\."/);
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
  assert.match(html, /<div class="col-location job-cell" data-label="Location"[^>]*>/);
  assert.match(html, /<span class="job-country-main"><\/span>/);
  assert.match(html, /<span class="job-city-sub"><\/span>/);
});
