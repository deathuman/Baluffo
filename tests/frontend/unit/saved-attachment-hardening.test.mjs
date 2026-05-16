import test from "node:test";
import assert from "node:assert/strict";

import { createSavedAttachmentsController } from "../../../frontend/saved/app/runtime/attachments-controller.js";
import { createSavedMutations } from "../../../frontend/saved/app/runtime/mutations.js";
import { createSavedRenderController } from "../../../frontend/saved/app/runtime/render-controller.js";
import {
  createElement
} from "./helpers/saved-runtime-helpers.mjs";

async function flushHydration() {
  await Promise.resolve();
  await Promise.resolve();
  await Promise.resolve();
}

async function withRenderGlobals(fn) {
  const originalDocument = globalThis.document;
  const originalWindow = globalThis.window;
  const originalHTMLElement = globalThis.HTMLElement;
  const originalTextarea = globalThis.HTMLTextAreaElement;

  class FakeHTMLElement {}
  globalThis.HTMLElement = FakeHTMLElement;
  globalThis.HTMLTextAreaElement = class {};
  globalThis.document = { activeElement: null };
  globalThis.window = {
    scrollX: 0,
    scrollY: 0,
    scrollTo() {},
    scrollBy() {}
  };

  try {
    return await fn(FakeHTMLElement);
  } finally {
    globalThis.document = originalDocument;
    globalThis.window = originalWindow;
    globalThis.HTMLElement = originalHTMLElement;
    globalThis.HTMLTextAreaElement = originalTextarea;
  }
}

async function withToastGlobals(fn) {
  const originalDocument = globalThis.document;
  const originalRequestAnimationFrame = globalThis.requestAnimationFrame;
  const originalSetTimeout = globalThis.setTimeout;
  const toasts = [];

  function createToastElement() {
    return {
      className: "",
      textContent: "",
      children: [],
      classList: {
        add() {},
        remove() {}
      },
      appendChild(child) {
        this.children.push(child);
      },
      addEventListener() {},
      remove() {}
    };
  }

  globalThis.document = {
    createElement: () => createToastElement(),
    body: {
      appendChild(el) {
        toasts.push(el);
      }
    }
  };
  globalThis.requestAnimationFrame = callback => {
    callback();
    return 1;
  };
  globalThis.setTimeout = callback => {
    callback();
    return 1;
  };

  try {
    return await fn(toasts);
  } finally {
    globalThis.document = originalDocument;
    globalThis.requestAnimationFrame = originalRequestAnimationFrame;
    globalThis.setTimeout = originalSetTimeout;
  }
}

function createSavedListElement(block) {
  return {
    scrollTop: 0,
    innerHTML: "",
    querySelector(selector) {
      return String(selector || "").includes(".saved-job-block") ? block : null;
    },
    querySelectorAll(selector) {
      return String(selector || "") === ".saved-job-block" ? [block] : [];
    }
  };
}

function createFakeBlock(FakeHTMLElement) {
  return Object.assign(new FakeHTMLElement(), createElement({
    dataset: { jobKey: "job_1" },
    getBoundingClientRect() {
      return { top: 0 };
    },
    querySelector() {
      return null;
    },
    querySelectorAll() {
      return [];
    }
  }));
}

function createAttachmentControllerHarness({ savedJobsListEl, listResponses = [] } = {}) {
  const listCalls = [];
  const addCalls = [];
  const deleteCalls = [];
  const dispatchCalls = [];
  const pulseCalls = [];
  const viewState = {
    currentUser: { uid: "u1" },
    loadedAttachmentJobKeys: new Set(),
    loadingAttachmentJobKeys: new Set()
  };
  const deleteButton = createElement({
    dataset: { jobKey: "job_1", attachmentId: "att_1" }
  });
  const listElement = savedJobsListEl || {
    querySelector() {
      return null;
    },
    querySelectorAll(selector) {
      return selector === ".att-delete-btn" ? [deleteButton] : [];
    }
  };

  const controller = createSavedAttachmentsController({
    dom: { savedJobsListEl: listElement },
    viewState,
    savedPageService: {
      async listAttachmentsForJob(...args) {
        listCalls.push(args);
        return listResponses.shift() || { ok: true, data: [] };
      },
      async addAttachmentForJob(...args) {
        addCalls.push(args);
        return { ok: true, data: { id: "att_added" } };
      },
      async deleteAttachmentForJob(...args) {
        deleteCalls.push(args);
        return { ok: true };
      }
    },
    savedDispatch: {
      dispatch(action) {
        dispatchCalls.push(action);
      }
    },
    savedActions: { ATTACHMENT_MUTATED: "saved/attachmentMutated" },
    maxAttachmentsPerJob: 3,
    maxAttachmentBytes: 1024,
    attachmentPreviewUrls: new Map(),
    cssEscape: value => String(value || ""),
    setSelectedJobKey() {},
    queueActivityPulse(...args) {
      pulseCalls.push(args);
    },
    timelineScopeAttachments: "attachments"
  });

  return {
    controller,
    viewState,
    listCalls,
    addCalls,
    deleteCalls,
    dispatchCalls,
    pulseCalls,
    deleteButton,
  };
}

function createRenderControllerHarness(FakeHTMLElement) {
  const block = createFakeBlock(FakeHTMLElement);
  const savedJobsListEl = createSavedListElement(block);
  const attachmentHarness = createAttachmentControllerHarness({
    savedJobsListEl,
    listResponses: [
      { ok: true, data: [{ id: "att_1", name: "cv.pdf", size: 10 }] }
    ]
  });
  const viewState = {
    ...attachmentHarness.viewState,
    expandedJobKey: "job_1",
    selectedJobKey: "",
    jobDetailTabByKey: new Map([["job_1", "attachments"]]),
    savedLifecycleOverlayByJobKey: new Map(),
    activeSavedFilter: "all",
    activeSavedSort: "updated",
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
    outcomeOptions: ["active", "closed"],
    outcomeLabels: { active: "Active", closed: "Closed" },
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
    renderSavedFilterMeta() {},
    renderReminderCounter() {},
    hydrateAttachmentLists: async () => {},
    hydrateAttachmentListForJob: (...args) => attachmentHarness.controller.hydrateAttachmentListForJob(...args),
    bindAttachmentActionButtons: () => attachmentHarness.controller.bindAttachmentActionButtons(),
    renderSavedJobBlockHtml: job => `<div class="saved-job-block" data-job-key="${job.jobKey}"></div>`,
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

  return {
    controller,
    viewState,
    listCalls: attachmentHarness.listCalls
  };
}

const savedJob = {
  jobKey: "job_1",
  title: "Gameplay Engineer",
  company: "Studio",
  jobLink: "https://example.test/job",
  savedAt: "2026-05-16T20:00:00.000Z",
  updatedAt: "2026-05-16T20:00:00.000Z",
  pipelinePhase: "bookmark",
  outcomeStatus: "active"
};

test("saved render reuses loaded attachment lists on passive rerender", async () => {
  await withRenderGlobals(async FakeHTMLElement => {
    const { controller, listCalls, viewState } = createRenderControllerHarness(FakeHTMLElement);

    controller.renderSavedJobs([savedJob]);
    await flushHydration();

    assert.equal(listCalls.length, 1);
    assert.equal(viewState.loadedAttachmentJobKeys.has("job_1"), true);

    controller.renderSavedJobs([savedJob]);
    await flushHydration();

    assert.equal(listCalls.length, 1);
  });
});

test("saved attachments tab switch does not force reload for an already-loaded job", async () => {
  await withRenderGlobals(async FakeHTMLElement => {
    const { controller, listCalls } = createRenderControllerHarness(FakeHTMLElement);

    controller.renderSavedJobs([savedJob]);
    await flushHydration();
    controller.setJobDetailsTab("job_1", "notes");
    controller.applyJobDetailsTab("job_1", "notes");
    controller.setJobDetailsTab("job_1", "attachments");
    controller.applyJobDetailsTab("job_1", "attachments");
    await flushHydration();

    assert.equal(listCalls.length, 1);
  });
});

test("saved attachment mutation paths refresh the affected job list", async () => {
  await withToastGlobals(async () => {
    const harness = createAttachmentControllerHarness({
      listResponses: [
        { ok: true, data: [] },
        { ok: true, data: [{ id: "att_added", name: "notes.txt", size: 5 }] },
        { ok: true, data: [] }
      ]
    });

    await harness.controller.uploadAttachments("job_1", [
      { name: "notes.txt", type: "text/plain", size: 5 }
    ]);
    harness.controller.bindAttachmentActionButtons();
    await harness.deleteButton.onclick();

    assert.deepEqual(harness.listCalls.map(args => args.slice(0, 2)), [
      ["u1", "job_1"],
      ["u1", "job_1"],
      ["u1", "job_1"]
    ]);
    assert.equal(harness.addCalls.length, 1);
    assert.deepEqual(harness.deleteCalls, [["u1", "job_1", "att_1"]]);
    assert.equal(harness.viewState.loadedAttachmentJobKeys.has("job_1"), true);
    assert.deepEqual(harness.dispatchCalls.map(action => action.type), [
      "saved/attachmentMutated",
      "saved/attachmentMutated"
    ]);
    assert.deepEqual(harness.pulseCalls, [
      ["job_1", "attachments"],
      ["job_1", "attachments"]
    ]);
  });
});

test("saved remove undo restores the row while preserved attachments remain keyed to the job", async () => {
  const savedRows = new Map([["job_1", { ...savedJob, attachmentsCount: 1 }]]);
  const attachmentsByKey = new Map([
    ["u1::job_1", [{ id: "att_1", jobKey: "job_1", name: "cv.pdf" }]]
  ]);
  const toastCalls = [];
  const service = {
    async removeSavedJobForUser(uid, jobKey) {
      assert.equal(uid, "u1");
      savedRows.delete(jobKey);
      return { ok: true };
    },
    async saveJobForUser(uid, row) {
      assert.equal(uid, "u1");
      savedRows.set(row.jobKey, row);
      return { ok: true, data: row };
    },
    async listAttachmentsForJob(uid, jobKey) {
      return { ok: true, data: attachmentsByKey.get(`${uid}::${jobKey}`) || [] };
    }
  };
  const mutations = createSavedMutations({
    viewState: {
      currentUser: { uid: "u1" },
      phaseOverrideContext: null,
      trackingOverrideContext: null,
      lastSavedJobsByKey: new Map(savedRows)
    },
    savedPageService: service,
    requestConfirmationDialog: async () => true,
    showToast(message, type, options = {}) {
      toastCalls.push({ message, type, options });
    }
  });

  await mutations.removeSavedJob("job_1");

  assert.equal(savedRows.has("job_1"), false);
  assert.equal((await service.listAttachmentsForJob("u1", "job_1")).data.length, 1);

  await toastCalls[0].options.onAction();

  assert.equal(savedRows.has("job_1"), true);
  assert.deepEqual((await service.listAttachmentsForJob("u1", "job_1")).data, [
    { id: "att_1", jobKey: "job_1", name: "cv.pdf" }
  ]);
});
