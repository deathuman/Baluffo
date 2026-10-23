import test from "node:test";
import assert from "node:assert/strict";

import { createJobsDesktopUpdateController } from "../../../frontend/jobs/app/desktop-update.js";
import {
  createDesktopUpdateClickDiagnostics,
  describeDiagnosticNode,
  describeEventPath
} from "../../../frontend/jobs/app/desktop-update-click-diagnostics.js";
import { createClassList } from "./helpers/dom-test-helpers.mjs";
import { buildDesktopUpdateRefs } from "./helpers/jobs-runtime-helpers.mjs";

const BOOT_ID = "boot-test";

function createToggleElement({ id = "desktop-update-toggle-btn", ariaExpanded = "false" } = {}) {
  const listeners = new Map();
  return {
    id,
    tagName: "BUTTON",
    textContent: "",
    disabled: false,
    href: "#",
    dataset: { ui: "desktop-update-toggle-btn" },
    attributes: { "aria-expanded": ariaExpanded },
    isConnected: true,
    classList: createClassList(["hidden"]),
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
    getAttribute(name) {
      return this.attributes[name] ?? null;
    },
    addEventListener(name, handler) {
      listeners.set(name, handler);
    },
    clickEvent(event = {}) {
      listeners.get("click")?.({ preventDefault() {}, ...event });
    }
  };
}

function createDocumentStub() {
  const listeners = new Map();
  return {
    addEventListener(type, handler, capture) {
      listeners.set(type, { handler, capture });
    },
    listenerCount() {
      return listeners.size;
    },
    dispatchClick(target) {
      listeners.get("click")?.handler({
        target,
        composedPath: () => [target, { tagName: "BODY" }, { tagName: "HTML" }, { tagName: "DOCUMENT" }],
        timeStamp: 12.5,
        detail: 1,
        isTrusted: true,
        defaultPrevented: false
      });
    }
  };
}

function createRecorder() {
  const events = [];
  return {
    events,
    emit: (event, payload) => events.push({ event, payload }),
    named(event) {
      return events.filter(entry => entry.event === event).map(entry => entry.payload);
    }
  };
}

function createScheduler() {
  const callbacks = [];
  return {
    schedule: callback => callbacks.push(callback),
    flush() {
      while (callbacks.length) callbacks.shift()();
    }
  };
}

function createDiagnostics({
  controllerState = { bound: true, mounted: true, panelOpen: false },
  refs = buildDesktopUpdateRefs(createToggleElement)
} = {}) {
  const recorder = createRecorder();
  const scheduler = createScheduler();
  const documentObject = createDocumentStub();
  const diagnostics = createDesktopUpdateClickDiagnostics({
    documentObject,
    refs,
    bootId: BOOT_ID,
    emit: recorder.emit,
    schedule: scheduler.schedule,
    readControllerState: () => controllerState
  });
  return { diagnostics, recorder, scheduler, documentObject, refs, controllerState };
}

test("describeDiagnosticNode and describeEventPath keep the click path readable", () => {
  const toggle = createToggleElement();
  const path = describeEventPath({
    composedPath: () => [toggle, { tagName: "DIV", classList: ["header-actions"] }, { tagName: "DOCUMENT" }]
  });
  assert.deepEqual(path, ["button#desktop-update-toggle-btn[data-ui=desktop-update-toggle-btn]", "div.header-actions", "document"]);
  assert.equal(describeDiagnosticNode(null), "");
});

test("install records the diagnostics boot and refuses a second install", () => {
  const { diagnostics, recorder, documentObject } = createDiagnostics();
  assert.equal(diagnostics.install(), true);
  assert.equal(diagnostics.install(), false);
  assert.equal(documentObject.listenerCount(), 1);
  const [installed] = recorder.named("jobs_desktop_update_click_diagnostics_installed");
  assert.equal(installed.bootId, BOOT_ID);
  assert.equal(installed.controller.bound, true);
  assert.equal(installed.panelHidden, true);
});

test("a click the controller never claims is reported as dropped with its path and bind state", () => {
  const { diagnostics, recorder, scheduler, documentObject, refs, controllerState } = createDiagnostics({
    controllerState: { bound: false, mounted: false, panelOpen: false }
  });
  diagnostics.install();
  documentObject.dispatchClick(refs.desktopUpdateToggleBtn);

  const [dispatched] = recorder.named("jobs_desktop_update_click_dispatched");
  assert.equal(recorder.named("jobs_desktop_update_click_dropped").length, 0, "drop is only reported once the claim window passes");
  assert.deepEqual(dispatched.path, [
    "button#desktop-update-toggle-btn[data-ui=desktop-update-toggle-btn]",
    "body",
    "html",
    "document"
  ]);
  assert.equal(dispatched.targetMatchesBoundToggle, true);
  assert.equal(dispatched.isTrusted, true);

  scheduler.flush();
  const [dropped] = recorder.named("jobs_desktop_update_click_dropped");
  assert.equal(dropped.clickId, dispatched.clickId);
  assert.equal(dropped.controller.bound, false);
  assert.equal(dropped.controller.mounted, false);
  assert.equal(dropped.controllerAtDispatch.bound, false, "the dispatch-time bind state is what proves the click came too early");
  assert.equal(dropped.panelOpenInController, false);
  assert.equal(dropped.panelHidden, true);
  assert.equal(dropped.toggleAriaExpanded, "false");
  assert.equal(controllerState.bound, false);
});

test("a click on a replaced toggle node is captured and flagged as not the bound node", () => {
  const { diagnostics, recorder, scheduler, documentObject, refs } = createDiagnostics();
  diagnostics.install();
  const replacement = createToggleElement();
  documentObject.dispatchClick(replacement);
  scheduler.flush();

  const [dropped] = recorder.named("jobs_desktop_update_click_dropped");
  assert.equal(dropped.targetMatchesBoundToggle, false, "the listener lives on a node that is no longer in the page");
  assert.notEqual(replacement, refs.desktopUpdateToggleBtn);
  assert.equal(dropped.toggleConnected, true);
});

test("a claimed click records the handler result and never reports a drop", () => {
  const { diagnostics, recorder, scheduler, documentObject, refs } = createDiagnostics();
  diagnostics.install();
  documentObject.dispatchClick(refs.desktopUpdateToggleBtn);
  const claim = diagnostics.claimClick();
  diagnostics.recordHandled(claim, { action: "open", panelVisible: true });
  scheduler.flush();

  assert.equal(recorder.named("jobs_desktop_update_click_dropped").length, 0);
  const [handled] = recorder.named("jobs_desktop_update_click_handled");
  assert.equal(handled.hadDispatchedRecord, true);
  assert.equal(handled.action, "open");
  assert.equal(handled.panelVisible, true);
  assert.equal(handled.clickId.startsWith(`${BOOT_ID}-`), true);
});

test("a handler invoked without a captured click says so instead of pretending it was measured", () => {
  const { diagnostics, recorder } = createDiagnostics();
  diagnostics.recordHandled(diagnostics.claimClick(), { action: "open", panelVisible: false });
  const [handled] = recorder.named("jobs_desktop_update_click_handled");
  assert.equal(handled.hadDispatchedRecord, false);
  assert.equal(handled.clickId, "");
});

test("panel divergence is reported once until the panel is exposed again", () => {
  const { diagnostics, recorder, controllerState } = createDiagnostics();
  controllerState.panelOpen = true;
  diagnostics.reportPanelDiverged("status_not_exposed");
  diagnostics.reportPanelDiverged("status_not_exposed");
  assert.equal(recorder.named("jobs_desktop_update_panel_state_diverged").length, 1);
  const [diverged] = recorder.named("jobs_desktop_update_panel_state_diverged");
  assert.equal(diverged.reason, "status_not_exposed");
  assert.equal(diverged.controller.panelOpen, true);
  assert.equal(diverged.panelHidden, true);

  diagnostics.clearPanelDiverged();
  diagnostics.reportPanelDiverged("status_not_exposed");
  assert.equal(recorder.named("jobs_desktop_update_panel_state_diverged").length, 2);
});

test("the mounted controller records the capture event and the handled result", async () => {
  const refs = buildDesktopUpdateRefs(createToggleElement);
  const recorder = createRecorder();
  const documentObject = createDocumentStub();
  const controller = createJobsDesktopUpdateController({
    refs,
    documentObject,
    emitDiagnostic: recorder.emit,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async () => ({
      currentVersion: "0.3.23",
      availability: "up_to_date",
      downloadState: "idle",
      installState: "idle",
      lastCheckedAt: "2026-10-19T12:00:00Z"
    }),
    postJson: async () => ({}),
    bindAsyncClick: (element, handler) => element?.addEventListener?.("click", () => handler()),
    showToast: () => {},
    isDesktopRuntimeMode: () => true,
    awaitDesktopBootstrap: async () => true
  });

  await controller.mount();
  assert.equal(recorder.named("jobs_desktop_update_click_diagnostics_installed").length, 1);

  documentObject.dispatchClick(refs.desktopUpdateToggleBtn);
  refs.desktopUpdateToggleBtn.clickEvent();
  await new Promise(resolve => setTimeout(resolve, 0));

  assert.equal(recorder.named("jobs_desktop_update_click_dropped").length, 0);
  const [handled] = recorder.named("jobs_desktop_update_click_handled");
  assert.equal(handled.hadDispatchedRecord, true);
  assert.equal(handled.action, "open");
  assert.equal(handled.panelVisible, true);
  assert.equal(handled.panelHidden, false);
  assert.equal(handled.toggleAriaExpanded, "true");
});
