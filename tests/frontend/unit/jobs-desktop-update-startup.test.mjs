import test from "node:test";
import assert from "node:assert/strict";

import { createJobsDesktopUpdateController } from "../../../frontend/jobs/app/desktop-update.js";

function createElement(text = "") {
  const values = new Set(["hidden"]);
  return {
    textContent: text,
    disabled: false,
    dataset: {},
    classList: {
      toggle(token, force) {
        if (force) values.add(token);
        else values.delete(token);
      },
      contains(token) {
        return values.has(token);
      }
    },
    setAttribute() {},
    addEventListener() {}
  };
}

function buildRefs() {
  return {
    desktopUpdateToggleBtn: createElement("Check updates"),
    desktopUpdatePanel: createElement(),
    desktopUpdateTitle: createElement(),
    desktopUpdateBody: createElement(),
    desktopUpdateMeta: createElement(),
    desktopUpdateProgress: createElement(),
    desktopUpdatePrimaryBtn: createElement(),
    desktopUpdateSecondaryBtn: createElement(),
    desktopUpdateReleaseNotes: createElement(),
  };
}

function createController(overrides = {}) {
  const refs = overrides.refs || buildRefs();
  return createJobsDesktopUpdateController({
    refs,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async () => ({
      currentVersion: "0.0.15",
      availability: "unknown",
      downloadState: "idle",
      installState: "idle",
      lastCheckedAt: "",
    }),
    postJson: async () => ({ status: {} }),
    bindAsyncClick: () => {},
    showToast: () => {},
    requestConfirmationDialog: async () => true,
    isDesktopRuntimeMode: () => true,
    setTimeoutFn: handler => ({ unref() {}, handler }),
    clearTimeoutFn() {},
    ...overrides,
    refs
  });
}

test("desktop update startup and manual checks force fresh release lookup", async () => {
  const refs = buildRefs();
  const postCalls = [];
  let checkCount = 0;
  const controller = createController({
    refs,
    postJson: async (_baseUrl, path, payload) => {
      postCalls.push({ path, payload });
      checkCount += 1;
      return {
        status: {
          currentVersion: "0.0.15",
          availability: "up_to_date",
          updateAvailable: false,
          downloadState: "idle",
          installState: "idle",
          lastCheckedAt: `2026-04-16T10:0${checkCount}:00Z`,
        }
      };
    }
  });

  await controller.mount();
  await controller.startAutoCheck();
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), true);
  await controller.startAutoCheck();
  refs.desktopUpdatePrimaryBtn.dataset.action = "check";
  await controller.handlePrimaryAction();

  assert.deepEqual(postCalls, [
    { path: "/app/check-for-update", payload: { force: true } },
    { path: "/app/check-for-update", payload: { force: true } }
  ]);
});

test("desktop update startup check stays quiet outside desktop runtime", async () => {
  const fetchCalls = [];
  const postCalls = [];
  const controller = createController({
    isDesktopRuntimeMode: () => false,
    fetchJson: async (_baseUrl, path) => {
      fetchCalls.push(path);
      return {};
    },
    postJson: async (_baseUrl, path) => {
      postCalls.push(path);
      return {};
    }
  });

  await controller.mount();
  await controller.startAutoCheck();

  assert.deepEqual(fetchCalls, []);
  assert.deepEqual(postCalls, []);
});
