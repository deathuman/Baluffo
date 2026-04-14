import test from "node:test";
import assert from "node:assert/strict";

import {
  createJobsDesktopUpdateController,
  deriveDesktopUpdateView,
  formatDesktopUpdateBytes,
  shouldPollDesktopUpdateStatus
} from "../../../frontend/jobs/app/desktop-update.js";

function createClassList(initial = []) {
  const values = new Set(initial);
  return {
    add(...tokens) {
      tokens.forEach(token => values.add(token));
    },
    remove(...tokens) {
      tokens.forEach(token => values.delete(token));
    },
    toggle(token, force) {
      if (force === true) {
        values.add(token);
        return true;
      }
      if (force === false) {
        values.delete(token);
        return false;
      }
      if (values.has(token)) {
        values.delete(token);
        return false;
      }
      values.add(token);
      return true;
    },
    contains(token) {
      return values.has(token);
    }
  };
}

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
      listeners.get("click")?.({
        preventDefault() {},
        ...event
      });
    }
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

test("formatDesktopUpdateBytes renders human-sized values", () => {
  assert.equal(formatDesktopUpdateBytes(900), "900 B");
  assert.equal(formatDesktopUpdateBytes(2048), "2.0 KB");
  assert.equal(formatDesktopUpdateBytes(5 * 1024 * 1024), "5.0 MB");
});

test("deriveDesktopUpdateView maps available and ready states to clear CTAs", () => {
  const available = deriveDesktopUpdateView({
    currentVersion: "0.0.15",
    latestVersion: "0.0.16",
    targetVersion: "0.0.16",
    availability: "available",
    totalBytes: 10 * 1024 * 1024,
    updateAvailable: true,
    releaseNotesUrl: "https://example.com/release"
  }, { panelOpen: true });
  assert.equal(available.buttonLabel, "Update 0.0.16");
  assert.equal(available.primaryAction, "download");
  assert.equal(available.secondaryLabel, "Later");
  assert.equal(available.releaseNotesUrl, "https://example.com/release");

  const ready = deriveDesktopUpdateView({
    currentVersion: "0.0.15",
    latestVersion: "0.0.16",
    targetVersion: "0.0.16",
    availability: "available",
    downloadState: "downloaded",
    installState: "ready",
    totalBytes: 10 * 1024 * 1024,
    updateAvailable: true
  }, { panelOpen: true });
  assert.equal(ready.buttonLabel, "Install 0.0.16");
  assert.equal(ready.primaryAction, "install");
  assert.equal(ready.primaryLabel, "Install and restart");
});

test("shouldPollDesktopUpdateStatus tracks checking, downloading, and install handoff", () => {
  assert.equal(shouldPollDesktopUpdateStatus({ availability: "checking" }), true);
  assert.equal(shouldPollDesktopUpdateStatus({ downloadState: "downloading" }), true);
  assert.equal(shouldPollDesktopUpdateStatus({ installState: "handoff_requested" }), true);
  assert.equal(shouldPollDesktopUpdateStatus({ availability: "up_to_date" }), false);
});

test("deriveDesktopUpdateView surfaces staged helper progress and failure retry state", () => {
  const installing = deriveDesktopUpdateView({
    installState: "installing",
    installStage: "replacing",
    installStageLabel: "Installing update"
  }, { panelOpen: true });
  assert.equal(installing.progress, "Installing update");

  const failed = deriveDesktopUpdateView({
    installState: "failed",
    downloadState: "downloaded",
    lastError: "desktop_install_failed"
  }, { panelOpen: true });
  assert.equal(failed.buttonLabel, "Update failed");
  assert.equal(failed.primaryAction, "install");
  assert.equal(failed.primaryLabel, "Try install again");
});

test("desktop update controller mounts, auto-checks, and starts a download from jobs UI", async () => {
  const refs = buildRefs();
  const fetchCalls = [];
  const postCalls = [];
  const toasts = [];
  const scheduled = [];
  let fetchStatus = {
    currentVersion: "0.0.15",
    availability: "unknown",
    downloadState: "idle",
    installState: "idle",
    lastCheckedAt: "",
  };

  const controller = createJobsDesktopUpdateController({
    refs,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async (_baseUrl, path) => {
      fetchCalls.push(path);
      return fetchStatus;
    },
    postJson: async (_baseUrl, path) => {
      postCalls.push(path);
      if (path === "/app/check-for-update") {
        fetchStatus = {
          currentVersion: "0.0.15",
          latestVersion: "0.0.16",
          targetVersion: "0.0.16",
          availability: "available",
          updateAvailable: true,
          downloadState: "idle",
          installState: "idle",
          totalBytes: 10 * 1024 * 1024,
          lastCheckedAt: "2026-04-14T12:00:00Z",
          releaseNotesUrl: "https://example.com/release"
        };
        return { status: fetchStatus };
      }
      if (path === "/app/download-update") {
        fetchStatus = {
          ...fetchStatus,
          downloadState: "downloading",
          downloadPercent: 0,
          downloadedBytes: 0
        };
        return { status: fetchStatus };
      }
      throw new Error(`Unexpected path ${path}`);
    },
    bindAsyncClick: (element, handler) => {
      element._handler = handler;
    },
    showToast: (message, level) => {
      toasts.push({ message, level });
    },
    requestConfirmationDialog: async () => true,
    isDesktopRuntimeMode: () => true,
    openExternalUrl() {},
    setTimeoutFn: handler => {
      scheduled.push(handler);
      return { unref() {} };
    },
    clearTimeoutFn() {}
  });

  await controller.mount();
  assert.deepEqual(fetchCalls, ["/app/update-status"]);
  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), false);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Check updates");

  await controller.startAutoCheck();
  assert.deepEqual(postCalls, ["/app/check-for-update"]);
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), false);
  assert.equal(refs.desktopUpdatePrimaryBtn.dataset.action, "download");
  assert.equal(refs.desktopUpdatePrimaryBtn.textContent, "Download");
  assert.equal(refs.desktopUpdateReleaseNotes.classList.contains("hidden"), false);

  await controller.handlePrimaryAction();
  assert.deepEqual(postCalls, ["/app/check-for-update", "/app/download-update"]);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Downloading update");
  assert.equal(refs.desktopUpdatePrimaryBtn.classList.contains("hidden"), true);
  assert.equal(scheduled.length >= 1, true);
  assert.ok(
    toasts.some(item => item.message === "Desktop update download started." && item.level === "info")
  );
});
