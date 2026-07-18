import test from "node:test";
import assert from "node:assert/strict";

import { renderSavedJobBlockHtml } from "../../../frontend/saved/render.js";

function render(canManageAvailability, overrides = {}) {
  return renderSavedJobBlockHtml({
    jobKey: "job_1",
    title: "Gameplay Engineer",
    company: "Studio",
    city: "Rome",
    country: "Italy",
    workType: "Remote",
    contractType: "Full-time",
    jobLink: "https://example.com/jobs/1",
    availabilityId: "availability_1",
    applicationStatus: "bookmark",
    phaseTimestamps: {},
    savedAt: "2026-03-08T09:00:00.000Z",
    notes: "",
    ...overrides
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
    lifecycleOverlay: { availabilityStatus: "available" },
    canManageAvailability,
    currentUser: { uid: "u1" },
    maxAttachmentsPerJob: 10,
    maxAttachmentBytes: 1024
  });
}

test("Saved hides availability mutations in static mode and shows them with bridge capability", () => {
  const staticHtml = render(false);
  assert.doesNotMatch(staticHtml, /data-ui="saved-check-availability-btn"/);
  assert.doesNotMatch(staticHtml, /data-ui="saved-report-unavailable-btn"/);

  const managedHtml = render(true);
  assert.match(managedHtml, /data-ui="saved-check-availability-btn"/);
  assert.match(managedHtml, /data-ui="saved-report-unavailable-btn"/);
  assert.match(managedHtml, /class="saved-link-actions"/);
  assert.match(managedHtml, /saved-check-availability-btn[\s\S]*class="availability-action-icon"[\s\S]*<svg/);
  assert.match(managedHtml, /saved-report-unavailable-btn[\s\S]*aria-label="Report unavailable"/);
  assert.match(managedHtml, /saved-check-availability-btn[^>]*data-tooltip="Check availability now"/);
  assert.match(managedHtml, /saved-report-unavailable-btn[^>]*data-tooltip="Report unavailable"/);
  assert.doesNotMatch(managedHtml, /saved-(?:check-availability|report-unavailable)-btn[^>]*\stitle=/);
  assert.doesNotMatch(managedHtml, />Check now<\/button>/);
});

test("Saved keeps reported rows visible with a state badge and Clear action", () => {
  const html = render(true, {
    availabilityAttention: {
      localReport: { reportedAt: "2026-07-18T10:00:00Z" },
      hiddenByReport: true,
      events: []
    }
  });

  assert.match(html, />Reported unavailable<\/span>/);
  assert.match(html, /data-action="clear"/);
  assert.match(html, /aria-label="Clear unavailable report"/);
  assert.match(html, /data-tooltip="Clear unavailable report"/);
});
