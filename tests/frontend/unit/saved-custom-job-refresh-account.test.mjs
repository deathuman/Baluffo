import test from "node:test";
import assert from "node:assert/strict";

import { createSavedCustomJobController } from "../../../frontend/saved/app/runtime/custom-job-controller.js";
import { createButton, createElement } from "./helpers/saved-runtime-helpers.mjs";

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

function createHarness(refreshActivityLog) {
  const dom = {
    customJobFormEl: createElement({ reset() {} }),
    customJobTitleEl: createElement({ value: "Gameplay Programmer" }),
    customJobCompanyEl: createElement({ value: "Example Studio" }),
    customJobCityEl: createElement(),
    customJobCountryEl: createElement(),
    customJobWorkTypeEl: createElement(),
    customJobContractTypeEl: createElement(),
    customJobSectorEl: createElement(),
    customJobProfessionEl: createElement(),
    customJobLinkEl: createElement(),
    customJobNotesEl: createElement(),
    customJobReminderEl: createElement(),
    customJobLinkWarningEl: createElement(),
    customJobPanelEl: createElement(),
    addCustomJobBtnEl: createButton(),
    customJobPanelTitleEl: createElement(),
    customJobPanelHintEl: createElement(),
    customJobSaveBtnEl: createButton()
  };
  const viewState = {
    currentUser: { uid: "user-1" },
    customJobMode: "create",
    customJobTargetKey: "",
    customJobPanelOpen: true,
    lastSavedJobsByKey: new Map()
  };
  const toasts = [];
  const dispatched = [];
  const pulses = [];
  const controller = createSavedCustomJobController({
    dom,
    viewState,
    savedPageService: {
      isAvailable: () => true,
      saveJobForUser: async () => ({ ok: true, data: { jobKey: "job-1" } })
    },
    normalizeCustomJobInput: values => ({ ...values }),
    toDatetimeLocalValue: value => String(value || ""),
    savedDispatch: { dispatch: action => dispatched.push(action) },
    savedActions: { CUSTOM_JOB_MUTATED: "custom_job_mutated" },
    queueActivityPulse: (...args) => pulses.push(args),
    timelineScopeAll: "all",
    refreshActivityLog,
    showToast: (message, type) => toasts.push({ message, type })
  });
  return { controller, viewState, toasts, dispatched, pulses };
}

test("custom job suppresses a refresh rejection after the active account changes", async () => {
  const refreshStarted = deferred();
  const refresh = deferred();
  const refreshError = new Error("activity refresh failed");
  const harness = createHarness(() => {
    refreshStarted.resolve();
    return refresh.promise;
  });
  const loggedErrors = [];
  const originalError = console.error;
  console.error = (...args) => loggedErrors.push(args);

  try {
    const saving = harness.controller.createCustomJob();
    await refreshStarted.promise;
    harness.viewState.currentUser = { uid: "user-2" };
    refresh.reject(refreshError);
    await saving;
  } finally {
    console.error = originalError;
  }

  assert.deepEqual(harness.toasts, [{ message: "Custom job saved.", type: "success" }]);
  assert.equal(harness.dispatched.length, 1);
  assert.equal(harness.pulses.length, 1);
  assert.equal(loggedErrors.length, 0);
});

test("custom job logs a refresh rejection while the initiating account remains active", async () => {
  const refreshError = new Error("activity refresh failed");
  const harness = createHarness(async () => {
    throw refreshError;
  });
  const loggedErrors = [];
  const originalError = console.error;
  console.error = (...args) => loggedErrors.push(args);

  try {
    await harness.controller.createCustomJob();
  } finally {
    console.error = originalError;
  }

  assert.deepEqual(harness.toasts, [{ message: "Custom job saved.", type: "success" }]);
  assert.equal(harness.dispatched.length, 1);
  assert.equal(harness.pulses.length, 1);
  assert.equal(loggedErrors.length, 1);
  assert.equal(loggedErrors[0][1], refreshError);
});
