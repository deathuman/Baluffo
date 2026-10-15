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

function createDeferred() {
  let resolve;
  const promise = new Promise(res => {
    resolve = res;
  });
  return { promise, resolve };
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

function createAttachmentControllerHarness({ savedJobsListEl, listResponses = [], serviceHooks = {} } = {}) {
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
        if (serviceHooks.listAttachmentsForJob) {
          return serviceHooks.listAttachmentsForJob(...args);
        }
        return listResponses.shift() || { ok: true, data: [] };
      },
      async addAttachmentForJob(...args) {
        addCalls.push(args);
        if (serviceHooks.addAttachmentForJob) {
          return serviceHooks.addAttachmentForJob(...args);
        }
        return { ok: true, data: { id: "att_added" } };
      },
      async deleteAttachmentForJob(...args) {
        deleteCalls.push(args);
        if (serviceHooks.deleteAttachmentForJob) {
          return serviceHooks.deleteAttachmentForJob(...args);
        }
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
    activeSavedGroup: "none",
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

test("saved attachment upload and delete contend only for the same job", async () => {
  await withToastGlobals(async toasts => {
    const addStarted = createDeferred();
    const finishAdd = createDeferred();
    const harness = createAttachmentControllerHarness({
      serviceHooks: {
        async addAttachmentForJob(uid, jobKey) {
          if (jobKey === "job_1") {
            addStarted.resolve();
            await finishAdd.promise;
          }
          return { ok: true, data: { id: "att_added" } };
        }
      }
    });

    const uploadPromise = harness.controller.uploadAttachments("job_1", [
      { name: "notes.txt", type: "text/plain", size: 5 }
    ]);
    await addStarted.promise;

    harness.controller.bindAttachmentActionButtons();
    await harness.deleteButton.onclick();
    assert.deepEqual(harness.deleteCalls, [], "delete must be rejected while this job is uploading");

    // A mutation for another job must not be blocked by job_1's in-flight upload.
    harness.deleteButton.dataset.jobKey = "job_2";
    await harness.deleteButton.onclick();
    assert.deepEqual(harness.deleteCalls, [["u1", "job_2", "att_1"]]);
    assert.ok(toasts.some(toast => toast.children[0]?.textContent === "An attachment update is already in progress."));

    finishAdd.resolve();
    await uploadPromise;
  });
});

test("saved attachment upload suppresses completion toasts after an account switch", async () => {
  for (const addOutcome of [
    { name: "success", result: { ok: true, data: { id: "att_added" } } },
    { name: "failure", result: { ok: false, error: "upload failed" } }
  ]) {
    await withToastGlobals(async toasts => {
      const addStarted = createDeferred();
      const finishAdd = createDeferred();
      const harness = createAttachmentControllerHarness({
        serviceHooks: {
          async addAttachmentForJob() {
            addStarted.resolve();
            return finishAdd.promise;
          }
        }
      });

      const uploadPromise = harness.controller.uploadAttachments("job_1", [
        { name: "notes.txt", type: "text/plain", size: 5 }
      ]);
      await addStarted.promise;
      harness.viewState.currentUser = { uid: "u2" };
      finishAdd.resolve(addOutcome.result);
      await uploadPromise;

      assert.deepEqual(harness.addCalls[0].slice(0, 2), ["u1", "job_1"],
        "the in-flight request remains bound to the original account");
      const messages = toasts.map(toast => toast.children[0]?.textContent);
      assert.ok(!messages.some(message => /Attachment uploaded|attachments uploaded|Could not upload notes\.txt|Could not refresh attachments|Attachment saved, but/.test(message)),
        `no upload completion feedback should leak to the new account after ${addOutcome.name}`);
    });
  }
});

test("saved attachment delete blocks upload for its job through refresh", async () => {
  await withToastGlobals(async () => {
    const deleteStarted = createDeferred();
    const finishDelete = createDeferred();
    const harness = createAttachmentControllerHarness({
      serviceHooks: {
        async deleteAttachmentForJob() {
          deleteStarted.resolve();
          await finishDelete.promise;
          return { ok: true };
        }
      }
    });
    harness.controller.bindAttachmentActionButtons();

    const deletePromise = harness.deleteButton.onclick();
    await deleteStarted.promise;
    await harness.controller.uploadAttachments("job_1", [
      { name: "notes.txt", type: "text/plain", size: 5 }
    ]);

    assert.deepEqual(harness.addCalls, [], "upload must be rejected while delete is in flight");
    assert.deepEqual(harness.listCalls, [], "a rejected upload must not start its preflight");

    finishDelete.resolve();
    await deletePromise;
    await harness.controller.uploadAttachments("job_1", [
      { name: "notes.txt", type: "text/plain", size: 5 }
    ]);

    assert.equal(harness.addCalls.length, 1, "the job becomes writable after delete refresh settles");
    assert.equal(harness.listCalls.length, 3, "delete refresh plus upload preflight and refresh run");
  });
});

test("saved attachment upload stops when its initial attachment-list preflight fails", async () => {
  await withToastGlobals(async toasts => {
    const harness = createAttachmentControllerHarness({
      listResponses: [{ ok: false, error: "bridge offline" }]
    });

    await harness.controller.uploadAttachments("job_1", [
      { name: "notes.txt", type: "text/plain", size: 5 }
    ]);

    assert.equal(harness.listCalls.length, 1);
    assert.deepEqual(harness.addCalls, [], "no file may be uploaded without a successful preflight");
    assert.ok(toasts.some(toast => toast.children[0]?.textContent ===
      "Could not check current attachments. Nothing was uploaded."));
  });
});

test("saved attachment upload reports persistence when its post-upload refresh fails", async () => {
  await withToastGlobals(async toasts => {
    const harness = createAttachmentControllerHarness({
      listResponses: [
        { ok: true, data: [] },
        { ok: false, error: "bridge offline" }
      ]
    });

    await harness.controller.uploadAttachments("job_1", [
      { name: "notes.txt", type: "text/plain", size: 5 }
    ]);

    const messages = toasts.map(toast => toast.children[0]?.textContent);
    assert.equal(harness.addCalls.length, 1, "the attachment was persisted before refresh failed");
    assert.deepEqual(harness.dispatchCalls.map(action => action.type), ["saved/attachmentMutated"]);
    assert.deepEqual(harness.pulseCalls, [["job_1", "attachments"]]);
    assert.equal(harness.viewState.loadedAttachmentJobKeys.has("job_1"), false,
      "the stale list must remain eligible for manual retry");
    assert.ok(messages.includes("Attachment uploaded."), "persistence success must remain truthful");
    assert.ok(messages.includes("Attachment saved, but the list could not refresh. Use Refresh to try again."),
      "the failed refresh must offer accurate retry guidance");
    assert.ok(!messages.includes("Could not upload notes.txt"));
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
