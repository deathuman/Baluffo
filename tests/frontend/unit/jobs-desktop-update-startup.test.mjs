import test from "node:test";
import assert from "node:assert/strict";

import { createJobsDesktopUpdateController } from "../../../frontend/jobs/app/desktop-update.js";
import { buildDesktopUpdateRefs } from "./helpers/jobs-runtime-helpers.mjs";
const buildRefs = () => buildDesktopUpdateRefs(createElement);

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

function createController(overrides = {}) {
  const refs = overrides.refs || buildRefs();
  return createJobsDesktopUpdateController({
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

test("desktop update startup keeps timestamped cached check errors recoverable", async () => {
  const refs = buildRefs();
  const controller = createController({
    refs,
    fetchJson: async () => ({
      currentVersion: "0.0.15",
      availability: "error",
      downloadState: "idle",
      installState: "idle",
      lastCheckedAt: "2026-04-16T10:00:00Z",
      lastError: "GitHub timeout",
    })
  });

  await controller.mount();

  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), false);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Update error");
});

function immediateTimers() {
  return handler => {
    handler();
    return { unref() {} };
  };
}

const CACHED_UP_TO_DATE = {
  currentVersion: "0.0.15",
  availability: "up_to_date",
  updateAvailable: false,
  downloadState: "idle",
  installState: "idle",
  lastCheckedAt: "2026-04-16T10:00:00Z",
};

function bridgeUnreachable() {
  const error = new Error("Network error: bridge unreachable");
  error.code = "bridge_unreachable";
  return error;
}

const AVAILABLE_STATUS = {
  currentVersion: "0.0.15",
  targetVersion: "0.0.16",
  latestVersion: "0.0.16",
  availability: "available",
  updateAvailable: true,
  downloadState: "idle",
  installState: "idle",
  lastCheckedAt: "2026-04-16T10:01:00Z",
};

test("desktop update startup retries a check that could not reach the starting bridge", async () => {
  const refs = buildRefs();
  let attempts = 0;
  const controller = createController({
    refs,
    fetchJson: async () => CACHED_UP_TO_DATE,
    postJson: async () => {
      attempts += 1;
      if (attempts < 3) throw bridgeUnreachable();
      return { status: AVAILABLE_STATUS };
    },
    setTimeoutFn: immediateTimers()
  });

  await controller.mount();
  await controller.startAutoCheck();

  assert.equal(attempts, 3, "the refused startup checks must be retried");
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Update 0.0.16");
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), false);
});

test("desktop update startup stops retrying after the bounded delay list", async () => {
  const refs = buildRefs();
  let attempts = 0;
  const controller = createController({
    refs,
    fetchJson: async () => CACHED_UP_TO_DATE,
    postJson: async () => {
      attempts += 1;
      throw bridgeUnreachable();
    },
    setTimeoutFn: immediateTimers()
  });

  await controller.mount();
  await controller.startAutoCheck({ retryDelaysMs: [1, 1] });

  assert.equal(attempts, 3);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Update error");
});

test("desktop update startup ignores a cached status read that lands after a fresh check", async () => {
  const refs = buildRefs();
  let resolveStatus;
  const controller = createController({
    refs,
    fetchJson: () => new Promise(resolve => { resolveStatus = resolve; }),
    postJson: async () => ({ status: AVAILABLE_STATUS }),
    setTimeoutFn: immediateTimers()
  });

  const mounting = controller.mount();
  await controller.startAutoCheck();
  resolveStatus(CACHED_UP_TO_DATE);
  await mounting;

  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Update 0.0.16");
});

test("desktop update startup shows the running check instead of the cached up-to-date label", async () => {
  const refs = buildRefs();
  let resolveCheck;
  const controller = createController({
    refs,
    fetchJson: async () => CACHED_UP_TO_DATE,
    postJson: () => new Promise(resolve => { resolveCheck = resolve; }),
    setTimeoutFn: immediateTimers()
  });

  await controller.mount();
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Up to date");

  const checking = controller.startAutoCheck();
  await Promise.resolve();
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Checking...");

  resolveCheck({ status: AVAILABLE_STATUS });
  await checking;
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Update 0.0.16");
});

test("desktop update startup keeps a failed auto-check visible when the panel opens", async () => {
  const refs = buildRefs();
  let fetchCalls = 0;
  let failChecks = true;
  const controller = createController({
    refs,
    fetchJson: async () => {
      fetchCalls += 1;
      return CACHED_UP_TO_DATE;
    },
    postJson: async () => {
      if (failChecks) throw bridgeUnreachable();
      return { status: AVAILABLE_STATUS };
    },
    setTimeoutFn: immediateTimers()
  });

  await controller.mount();
  await controller.startAutoCheck({ retryDelaysMs: [1] });
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Update error");

  await controller.handleToggleClick();
  assert.equal(fetchCalls, 1, "opening the panel must not mask the failed check");
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Update error");

  failChecks = false;
  refs.desktopUpdatePrimaryBtn.dataset.action = "check";
  await controller.handlePrimaryAction();
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Update 0.0.16");
});

test("desktop update startup keeps stale up-to-date status usable for manual checks", async () => {
  const refs = buildRefs();
  const controller = createController({
    refs,
    fetchJson: async () => ({
      currentVersion: "0.0.15",
      availability: "up_to_date",
      downloadState: "idle",
      installState: "idle",
      lastCheckedAt: "2026-04-16T10:00:00Z",
    })
  });

  await controller.mount();

  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), false);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Up to date");
});
