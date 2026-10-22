import test from "node:test";
import assert from "node:assert/strict";

import {
  createJobsDesktopUpdateController,
  deriveDesktopUpdateView
} from "../../../frontend/jobs/app/desktop-update.js";
import { createClassList } from "./helpers/dom-test-helpers.mjs";
import { buildDesktopUpdateRefs } from "./helpers/jobs-runtime-helpers.mjs";

function createElement(text = "") {
  const listeners = new Map();
  return {
    textContent: text,
    disabled: false,
    href: "#",
    dataset: {},
    attributes: {},
    classList: createClassList(["hidden"]),
    setAttribute(name, value) {
      this.attributes[name] = String(value);
    },
    addEventListener(name, handler) {
      listeners.set(name, handler);
    },
    clickEvent(event = {}) {
      listeners.get("click")?.({ preventDefault() {}, ...event });
    }
  };
}

const buildRefs = () => buildDesktopUpdateRefs(createElement);

const unsupportedStatus = {
  currentVersion: "0.3.025",
  latestVersion: "0.3.026",
  targetVersion: "0.3.026",
  availability: "blocked",
  updateAvailable: false,
  downloadState: "idle",
  installState: "blocked",
  blockedReason: "platform_unsupported",
  lastCheckedAt: "2026-10-22T12:00:00Z"
};

test("a platform-unsupported status keeps the update control hidden after mount", async () => {
  const refs = buildRefs();
  const controller = createJobsDesktopUpdateController({
    refs,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async () => unsupportedStatus,
    postJson: async () => ({ status: unsupportedStatus }),
    bindAsyncClick: (element, handler) => {
      element._handler = handler;
    },
    showToast() {},
    requestConfirmationDialog: async () => true,
    isDesktopRuntimeMode: () => true,
    showReleaseNotesDialog: () => ({ close() {} }),
    openExternalUrl() {},
    setTimeoutFn: () => ({ unref() {} }),
    clearTimeoutFn() {}
  });

  await controller.mount();

  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), true);
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), true);
});

test("a manual check cannot force the control back into view on an unsupported build", async () => {
  const refs = buildRefs();
  let currentStatus = { ...unsupportedStatus, blockedReason: "" };
  const controller = createJobsDesktopUpdateController({
    refs,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async () => currentStatus,
    postJson: async () => {
      currentStatus = unsupportedStatus;
      return { status: unsupportedStatus };
    },
    bindAsyncClick: (element, handler) => {
      element._handler = handler;
    },
    showToast() {},
    requestConfirmationDialog: async () => true,
    isDesktopRuntimeMode: () => true,
    showReleaseNotesDialog: () => ({ close() {} }),
    openExternalUrl() {},
    setTimeoutFn: () => ({ unref() {} }),
    clearTimeoutFn() {}
  });

  await controller.mount();
  await controller.startAutoCheck();

  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), true);
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), true);
});

test("the blocked platform reason tells the user where to get the build", () => {
  const view = deriveDesktopUpdateView(unsupportedStatus, { panelOpen: true });

  assert.equal(view.buttonLabel, "Update blocked");
  assert.match(view.body, /release page/);
});
