import test from "node:test";
import assert from "node:assert/strict";

import { createSavedRenderController } from "../../../frontend/saved/app/runtime/render-controller.js";

async function withRenderGlobals(fn) {
  const originalDocument = globalThis.document;
  const originalWindow = globalThis.window;
  const originalHTMLElement = globalThis.HTMLElement;
  const originalTextarea = globalThis.HTMLTextAreaElement;

  globalThis.HTMLElement = class {};
  globalThis.HTMLTextAreaElement = class {};
  globalThis.document = { activeElement: null };
  globalThis.window = {
    scrollX: 0,
    scrollY: 0,
    scrollTo() {},
    scrollBy() {}
  };

  try {
    return await fn();
  } finally {
    globalThis.document = originalDocument;
    globalThis.window = originalWindow;
    globalThis.HTMLElement = originalHTMLElement;
    globalThis.HTMLTextAreaElement = originalTextarea;
  }
}

function createGroupingRenderController({ filter = "all", sort = "updated" } = {}) {
  const savedJobsListEl = {
    scrollTop: 0,
    innerHTML: "",
    querySelector() {
      return null;
    },
    querySelectorAll() {
      return [];
    }
  };
  const viewState = {
    currentUser: { uid: "u1" },
    expandedJobKey: "job_selected",
    selectedJobKey: "job_selected",
    jobDetailTabByKey: new Map(),
    savedLifecycleOverlayByJobKey: new Map(),
    activeSavedFilter: filter,
    activeSavedSort: sort,
    activeSavedGroup: "stage",
    phaseOverrideContext: null,
    trackingOverrideContext: null,
    cachedActivityEntries: [],
    timelineScope: "all"
  };
  const controller = createSavedRenderController({
    dom: { savedJobsListEl },
    viewState,
    timelineScopeAll: "all",
    timelineScopeSelected: "selected",
    phaseOptions: ["bookmark", "applied"],
    phaseLabels: { bookmark: "Saved", applied: "Applied" },
    outcomeOptions: ["active", "rejected", "accepted"],
    outcomeLabels: { active: "Active", rejected: "Rejected", accepted: "Accepted" },
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
    renderSavedJobBlockHtml: (job, options) => `
      <article
        class="saved-job-block${options.expandedJobKey === job.jobKey ? " expanded" : ""}"
        data-job-key="${job.jobKey}"
        data-selected="${options.selectedJobKey === job.jobKey ? "true" : "false"}"
      >${job.title || job.jobKey}</article>
    `,
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
  return { controller, savedJobsListEl };
}

const savedJob = {
  jobKey: "job_1",
  title: "Gameplay Engineer",
  company: "Studio",
  savedAt: "2026-05-16T20:00:00.000Z",
  updatedAt: "2026-05-16T20:00:00.000Z",
  pipelinePhase: "bookmark",
  outcomeStatus: "active"
};

test("saved grouped render shows group headers and keeps selected expanded row state", async () => {
  await withRenderGlobals(async () => {
    const { controller, savedJobsListEl } = createGroupingRenderController();

    controller.renderSavedJobs([
      {
        ...savedJob,
        jobKey: "job_selected",
        title: "Selected job",
        pipelinePhase: "interview_1"
      },
      {
        ...savedJob,
        jobKey: "job_rejected",
        title: "Rejected job",
        outcomeStatus: "rejected"
      }
    ]);

    assert.match(savedJobsListEl.innerHTML, /data-saved-group-key="stage_interviewing"/);
    assert.match(savedJobsListEl.innerHTML, />Interviewing<\/span>\s*<span class="saved-group-count">1<\/span>/);
    assert.match(savedJobsListEl.innerHTML, />Rejected<\/span>\s*<span class="saved-group-count">1<\/span>/);
    assert.match(savedJobsListEl.innerHTML, /class="saved-job-block expanded"[\s\S]*data-selected="true"/);
  });
});

test("saved grouped render applies filtering before grouping", async () => {
  await withRenderGlobals(async () => {
    const { controller, savedJobsListEl } = createGroupingRenderController({ filter: "closed" });

    controller.renderSavedJobs([
      {
        ...savedJob,
        jobKey: "job_applied",
        title: "Applied job",
        pipelinePhase: "applied"
      },
      {
        ...savedJob,
        jobKey: "job_accepted",
        title: "Accepted job",
        pipelinePhase: "offer",
        outcomeStatus: "accepted"
      }
    ]);

    assert.doesNotMatch(savedJobsListEl.innerHTML, />Applied<\/span>/);
    assert.match(savedJobsListEl.innerHTML, />Accepted<\/span>/);
    assert.match(savedJobsListEl.innerHTML, /Accepted job/);
    assert.doesNotMatch(savedJobsListEl.innerHTML, /Applied job/);
  });
});

test("saved grouped render preserves sorted order inside each group", async () => {
  await withRenderGlobals(async () => {
    const { controller, savedJobsListEl } = createGroupingRenderController({ sort: "activity" });

    controller.renderSavedJobs([
      {
        ...savedJob,
        jobKey: "job_old",
        title: "Older applied job",
        pipelinePhase: "screening",
        lastActivityAt: "2026-05-14T10:00:00.000Z"
      },
      {
        ...savedJob,
        jobKey: "job_new",
        title: "Newer applied job",
        pipelinePhase: "assignment",
        lastActivityAt: "2026-05-16T10:00:00.000Z"
      }
    ]);

    assert.ok(
      savedJobsListEl.innerHTML.indexOf("Newer applied job") < savedJobsListEl.innerHTML.indexOf("Older applied job")
    );
  });
});
