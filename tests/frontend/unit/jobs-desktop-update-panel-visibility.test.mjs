import test from "node:test";
import assert from "node:assert/strict";

import { createJobsDesktopUpdateController } from "../../../frontend/jobs/app/desktop-update.js";
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

const UP_TO_DATE = {
  currentVersion: "0.3.23",
  availability: "up_to_date",
  downloadState: "idle",
  installState: "idle",
  lastCheckedAt: "2026-10-19T12:00:00Z"
};

function createController({ statuses }) {
  const refs = buildDesktopUpdateRefs(createElement);
  const queue = [...statuses];
  const controller = createJobsDesktopUpdateController({
    refs,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async () => (queue.length > 1 ? queue.shift() : queue[0]),
    postJson: async () => ({}),
    bindAsyncClick: (element, handler) => element?.addEventListener?.("click", () => handler()),
    showToast: () => {},
    isDesktopRuntimeMode: () => true,
    awaitDesktopBootstrap: async () => true
  });
  return { controller, refs };
}

test("a check in flight at mount does not leave the panel open-but-hidden", async () => {
  const { controller, refs } = createController({
    statuses: [
      { availability: "checking", downloadState: "idle", installState: "idle" },
      UP_TO_DATE
    ]
  });

  await controller.mount();
  assert.equal(controller._getState().panelOpen, false, "a check in flight must not open the panel");
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), true);

  await controller.refreshStatus();
  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), false);

  // The first click must open the panel, not close a flag nobody can see.
  await controller.handleToggleClick();
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), false);
  assert.equal(refs.desktopUpdateToggleBtn.attributes["aria-expanded"], "true");
});

test("a click closes a panel the user can see and opens it again", async () => {
  const { controller, refs } = createController({ statuses: [UP_TO_DATE] });
  await controller.mount();

  await controller.handleToggleClick();
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), false);
  await controller.handleToggleClick();
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), true);
  await controller.handleToggleClick();
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), false);
});

test("a panel hidden while the controller still calls it open is reopened, not closed", async () => {
  const { controller, refs } = createController({ statuses: [UP_TO_DATE] });
  await controller.mount();
  await controller.handleToggleClick();
  assert.equal(controller._getState().panelOpen, true);

  // Simulate any render path that hides the panel without clearing the flag.
  refs.desktopUpdatePanel.classList.add("hidden");
  await controller.handleToggleClick();
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), false);
  assert.equal(controller._getState().panelOpen, true);
});
