import test from "node:test";
import assert from "node:assert/strict";

import { createSavedRenderController } from "../../../frontend/saved/app/runtime/render-controller.js";

// `renderMissingInfoChips` is not exported directly; it is handed to
// `renderSavedJobBlockHtml` as an option. These tests drive the real controller
// and capture what it passes, so the assertions exercise the shipped function
// rather than a copy of it.
async function captureChips(job) {
  const originalDocument = globalThis.document;
  const originalWindow = globalThis.window;
  const originalHTMLElement = globalThis.HTMLElement;
  const originalTextarea = globalThis.HTMLTextAreaElement;

  globalThis.HTMLElement = class {};
  globalThis.HTMLTextAreaElement = class {};
  globalThis.document = { activeElement: null };
  globalThis.window = { scrollX: 0, scrollY: 0, scrollTo() {}, scrollBy() {} };

  try {
    const savedJobsListEl = {
      scrollTop: 0,
      innerHTML: "",
      querySelector: () => null,
      querySelectorAll: () => []
    };
    const viewState = {
      currentUser: { uid: "u1" },
      expandedJobKey: "",
      selectedJobKey: "",
      jobDetailTabByKey: new Map(),
      savedLifecycleOverlayByJobKey: new Map(),
      activeSavedFilter: "all",
      activeSavedSort: "updated",
      activeSavedGroup: "stage",
      phaseOverrideContext: null,
      trackingOverrideContext: null,
      cachedActivityEntries: [],
      timelineScope: "all"
    };

    let captured = null;
    const controller = createSavedRenderController({
      dom: { savedJobsListEl },
      viewState,
      timelineScopeAll: "all",
      timelineScopeSelected: "selected",
      phaseOptions: ["bookmark"],
      phaseLabels: { bookmark: "Saved" },
      outcomeOptions: ["active"],
      outcomeLabels: { active: "Active" },
      customSourceLabel: "Custom",
      reminderSoonHours: 24,
      maxAttachmentsPerJob: 3,
      maxAttachmentBytes: 1024,
      computeAnchorScrollDelta: () => 0,
      cssEscape: value => String(value || ""),
      renderTimeline() {},
      setActivityPanelOpen() {},
      renderWorkspaceStats() {},
      renderSelectedJobHint() {},
      updateTimelineScopeButtons() {},
      setSavedFilterBarVisible() {},
      setSavedSortBarVisible() {},
      setSavedGroupBarVisible() {},
      renderSavedFilterMeta() {},
      renderReminderCounter() {},
      hydrateAttachmentLists: async () => {},
      hydrateAttachmentListForJob: async () => {},
      bindAttachmentActionButtons() {},
      renderSavedJobBlockHtml: (job, options) => {
        captured = options.renderMissingInfoChips(job);
        return `<article class="saved-job-block" data-job-key="${job.jobKey}"></article>`;
      },
      parseIsoDate: value => (value ? new Date(value) : null),
      getReminderMeta: () => ({ hasReminder: false }),
      formatRelativeTime: () => "",
      getJobHistoryEntries: () => [],
      renderPhaseBar: () => "",
      renderWebIcon: () => "",
      formatPhaseTimestamp: () => "",
      renderDetailsSummary: () => "",
      activityTypeLabel: () => "Activity",
      formatActivityDetail: () => ""
    });

    controller.renderSavedJobs([{
      jobKey: "job_1",
      title: "Gameplay Engineer",
      company: "Studio",
      savedAt: "2026-05-16T20:00:00.000Z",
      updatedAt: "2026-05-16T20:00:00.000Z",
      pipelinePhase: "bookmark",
      outcomeStatus: "active",
      ...job
    }]);

    return captured;
  } finally {
    globalThis.document = originalDocument;
    globalThis.window = originalWindow;
    globalThis.HTMLElement = originalHTMLElement;
    globalThis.HTMLTextAreaElement = originalTextarea;
  }
}

test("saved missing-info chips do not repeat what the metadata cells already show", async () => {
  // A custom job with no link and no contract type used to emit "No link" and
  // "No contract" here while the LINK cell rendered "No link" and the CONTRACT
  // cell rendered "Unknown" — the same two facts stated twice in one card.
  const chips = await captureChips({
    isCustom: true,
    customSourceLabel: "Custom",
    jobLink: "",
    city: "",
    contractType: ""
  });

  assert.doesNotMatch(chips, /No link/);
  assert.doesNotMatch(chips, /No contract/);

  // "No city" survives: the LOCATION cell simply omits the line, so the chip is
  // the only place that says a city is missing.
  assert.match(chips, /No city/);
});

test("saved missing-info chips stay silent when nothing is missing", async () => {
  const chips = await captureChips({
    isCustom: true,
    customSourceLabel: "Custom",
    jobLink: "https://example.com/jobs/1",
    city: "Amsterdam",
    contractType: "Full-time"
  });
  assert.equal(chips, "");
});

test("saved missing-info chips never render for a non-custom job", async () => {
  const chips = await captureChips({
    jobLink: "",
    city: "",
    contractType: ""
  });
  assert.equal(chips, "");
});
