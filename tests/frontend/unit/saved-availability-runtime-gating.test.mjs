import test from "node:test";
import assert from "node:assert/strict";

import { renderSavedJobBlockHtml } from "../../../frontend/saved/render.js";

function render(canManageAvailability) {
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
});
