import test from "node:test";
import assert from "node:assert/strict";

import {
  createJobsDesktopUpdateController,
  deriveDesktopUpdateView,
  formatDesktopUpdateBytes,
  shouldExposeJobsDesktopUpdateStatus,
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
    releaseNotesUrl: "https://example.com/release",
    releaseNotesTitle: "Baluffo v0.0.16",
    releaseNotesBody: "### Fixed\n- Notes",
    releaseNotesPublishedAt: "2026-04-15T10:00:00Z"
  }, { panelOpen: true });
  assert.equal(available.buttonLabel, "Update 0.0.16");
  assert.equal(available.primaryAction, "download");
  assert.equal(available.secondaryLabel, "Later");
  assert.equal(available.releaseNotesUrl, "https://example.com/release");
  assert.equal(available.releaseNotesVisible, true);

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

test("jobs desktop update toggle stays hidden until a fresh non-critical status is known", () => {
  assert.equal(
    shouldExposeJobsDesktopUpdateStatus({ availability: "error", lastError: "socket timeout" }),
    false
  );
  assert.equal(
    shouldExposeJobsDesktopUpdateStatus({ availability: "unknown", downloadState: "idle" }),
    false
  );
  assert.equal(
    shouldExposeJobsDesktopUpdateStatus({ availability: "up_to_date" }, { hasFreshStatus: true }),
    true
  );
  assert.equal(
    shouldExposeJobsDesktopUpdateStatus({ downloadState: "downloading" }),
    true
  );
  assert.equal(
    shouldExposeJobsDesktopUpdateStatus({ installState: "installing" }),
    true
  );
  assert.equal(
    shouldExposeJobsDesktopUpdateStatus({ installState: "ready" }),
    true
  );
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

  const downloadFailed = deriveDesktopUpdateView({
    targetVersion: "0.0.16",
    downloadState: "failed",
    downloadedBytes: 5 * 1024 * 1024,
    totalBytes: 10 * 1024 * 1024,
    lastError: "manifest cache missing"
  }, { panelOpen: true });
  assert.equal(downloadFailed.buttonLabel, "Download failed");
  assert.equal(downloadFailed.primaryAction, "download");
  assert.equal(downloadFailed.primaryLabel, "Try download again");
  assert.equal(downloadFailed.body, "manifest cache missing");
});

test("desktop update controller mounts, auto-checks, and starts a download from jobs UI", async () => {
  const refs = buildRefs();
  const fetchCalls = [];
  const postCalls = [];
  const toasts = [];
  const scheduled = [];
  const dialogs = [];
  const externalUrls = [];
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
          releaseNotesUrl: "https://example.com/release",
          releaseNotesTitle: "Baluffo v0.0.16",
          releaseNotesBody: "### Fixed\n- Modal notes",
          releaseNotesPublishedAt: "2026-04-15T10:00:00Z"
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
    showReleaseNotesDialog: options => {
      dialogs.push(options);
      return { close() {} };
    },
    openExternalUrl: url => {
      externalUrls.push(url);
    },
    setTimeoutFn: handler => {
      scheduled.push(handler);
      return { unref() {} };
    },
    clearTimeoutFn() {}
  });

  await controller.mount();
  assert.deepEqual(fetchCalls, ["/app/update-status"]);
  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), true);

  await controller.startAutoCheck();
  assert.deepEqual(postCalls, ["/app/check-for-update"]);
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), false);
  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), false);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Update 0.0.16");
  assert.equal(refs.desktopUpdatePrimaryBtn.dataset.action, "download");
  assert.equal(refs.desktopUpdatePrimaryBtn.textContent, "Download");
  assert.equal(refs.desktopUpdateReleaseNotes.classList.contains("hidden"), false);

  refs.desktopUpdateReleaseNotes.clickEvent();
  assert.equal(dialogs.length, 1);
  assert.equal(dialogs[0].title, "Baluffo v0.0.16");
  assert.equal(dialogs[0].markdown, "### Fixed\n- Modal notes");
  assert.equal(dialogs[0].publishedAt, "2026-04-15T10:00:00Z");
  assert.equal(dialogs[0].releaseNotesUrl, "https://example.com/release");
  assert.deepEqual(externalUrls, []);

  await controller.handlePrimaryAction();
  assert.deepEqual(postCalls, ["/app/check-for-update", "/app/download-update"]);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Downloading update");
  assert.equal(refs.desktopUpdatePrimaryBtn.classList.contains("hidden"), true);
  assert.equal(scheduled.length >= 1, true);
  assert.ok(
    toasts.some(item => item.message === "Desktop update download started." && item.level === "info")
  );
});

test("desktop update controller keeps cached update errors hidden until a fresh check finishes", async () => {
  const refs = buildRefs();
  let fetchStatus = {
    currentVersion: "0.0.15",
    availability: "error",
    downloadState: "idle",
    installState: "idle",
    lastError: "socket timeout",
  };

  const controller = createJobsDesktopUpdateController({
    refs,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async () => fetchStatus,
    postJson: async () => {
      fetchStatus = {
        ...fetchStatus,
        availability: "up_to_date",
        lastCheckedAt: "2026-04-16T10:00:00Z",
        lastError: "",
      };
      return { status: fetchStatus };
    },
    bindAsyncClick: () => {},
    showToast: () => {},
    requestConfirmationDialog: async () => true,
    isDesktopRuntimeMode: () => true,
    setTimeoutFn: handler => ({ unref() {}, handler }),
    clearTimeoutFn() {}
  });

  await controller.mount();
  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), true);

  await controller.startAutoCheck();
  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), false);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Up to date");
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), true);
});

test("desktop update controller renders active cached update work immediately", async () => {
  const refs = buildRefs();

  const controller = createJobsDesktopUpdateController({
    refs,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async () => ({
      currentVersion: "0.0.15",
      targetVersion: "0.0.16",
      availability: "available",
      downloadState: "downloading",
      downloadPercent: 42,
      downloadedBytes: 42,
      totalBytes: 100,
      installState: "idle",
      lastCheckedAt: "2026-04-16T10:00:00Z",
    }),
    postJson: async () => {
      throw new Error("unexpected");
    },
    bindAsyncClick: () => {},
    showToast: () => {},
    requestConfirmationDialog: async () => true,
    isDesktopRuntimeMode: () => true,
    setTimeoutFn: handler => ({ unref() {}, handler }),
    clearTimeoutFn() {}
  });

  await controller.mount();
  assert.equal(refs.desktopUpdateToggleBtn.classList.contains("hidden"), false);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Downloading 42%");
  assert.equal(refs.desktopUpdatePanel.classList.contains("hidden"), false);
});

test("desktop update controller refreshes status after a transport failure during download", async () => {
  const refs = buildRefs();
  const fetchCalls = [];
  const postCalls = [];
  const toasts = [];
  let refreshAfterFailure = false;
  let fetchStatus = {
    currentVersion: "0.0.15",
    latestVersion: "0.0.16",
    targetVersion: "0.0.16",
    availability: "available",
    updateAvailable: true,
    downloadState: "idle",
    installState: "idle",
    downloadedBytes: 0,
    totalBytes: 137_800_000,
    downloadPercent: 0,
    lastCheckedAt: "2026-04-15T10:00:00Z"
  };

  const controller = createJobsDesktopUpdateController({
    refs,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async (_baseUrl, path) => {
      fetchCalls.push(path);
      if (refreshAfterFailure) {
        refreshAfterFailure = false;
        fetchStatus = {
          ...fetchStatus,
          downloadState: "downloading",
          downloadedBytes: 23_300_000,
          downloadPercent: 16
        };
      }
      return fetchStatus;
    },
    postJson: async (_baseUrl, path) => {
      postCalls.push(path);
      if (path === "/app/check-for-update") {
        return { status: fetchStatus };
      }
      if (path === "/app/download-update") {
        refreshAfterFailure = true;
        throw new Error("Bridge POST /app/download-update failed: Internal Server Error (HTTP 500)");
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
    showReleaseNotesDialog() {
      return { close() {} };
    },
    openExternalUrl() {},
    clearTimeoutFn() {}
  });

  await controller.mount();
  await controller.startAutoCheck();
  await controller.handlePrimaryAction();

  assert.deepEqual(postCalls, ["/app/check-for-update", "/app/download-update"]);
  assert.deepEqual(fetchCalls, ["/app/update-status", "/app/update-status"]);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Downloading 16%");
  assert.ok(
    toasts.some(item => item.message === "Desktop update download started." && item.level === "info")
  );
  assert.equal(
    toasts.some(item => item.message.startsWith("Could not download the update:") && item.level === "error"),
    false
  );
});

test("desktop update controller ignores repeated primary actions while a request is pending", async () => {
  const refs = buildRefs();
  const toasts = [];
  let downloadCalls = 0;
  let resolveDownload;
  let fetchStatus = {
    currentVersion: "0.0.15",
    latestVersion: "0.0.16",
    targetVersion: "0.0.16",
    availability: "available",
    updateAvailable: true,
    downloadState: "idle",
    installState: "idle",
    downloadedBytes: 0,
    totalBytes: 137_800_000,
    downloadPercent: 0,
    lastCheckedAt: "2026-04-15T10:00:00Z"
  };

  const controller = createJobsDesktopUpdateController({
    refs,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async () => fetchStatus,
    postJson: async (_baseUrl, path) => {
      if (path === "/app/check-for-update") {
        return { status: fetchStatus };
      }
      if (path !== "/app/download-update") {
        throw new Error(`Unexpected path ${path}`);
      }
      downloadCalls += 1;
      return await new Promise(resolve => {
        resolveDownload = () => {
          fetchStatus = {
            ...fetchStatus,
            downloadState: "downloading",
            downloadedBytes: 0,
            downloadPercent: 0
          };
          resolve({ started: true, status: fetchStatus });
        };
      });
    },
    bindAsyncClick: (element, handler) => {
      element._handler = handler;
    },
    showToast: (message, level) => {
      toasts.push({ message, level });
    },
    requestConfirmationDialog: async () => true,
    isDesktopRuntimeMode: () => true,
    showReleaseNotesDialog() {
      return { close() {} };
    },
    openExternalUrl() {},
    clearTimeoutFn() {}
  });

  await controller.mount();
  await controller.startAutoCheck();
  const first = controller.handlePrimaryAction();
  const second = controller.handlePrimaryAction();

  assert.equal(downloadCalls, 1);
  assert.equal(refs.desktopUpdatePrimaryBtn.disabled, true);

  resolveDownload();
  await first;
  await second;

  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Downloading update");
  assert.ok(
    toasts.some(item => item.message === "Desktop update download started." && item.level === "info")
  );
});

test("desktop update controller surfaces background download failure once and allows retry", async () => {
  const refs = buildRefs();
  const fetchCalls = [];
  const postCalls = [];
  const toasts = [];
  let fetchStatus = {
    currentVersion: "0.1.2",
    latestVersion: "0.1.22",
    targetVersion: "0.1.22",
    availability: "available",
    updateAvailable: true,
    downloadState: "downloading",
    installState: "idle",
    downloadedBytes: 12,
    totalBytes: 100,
    downloadPercent: 12,
    lastCheckedAt: "2026-04-16T09:00:00Z"
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
      if (path === "/app/download-update") {
        fetchStatus = {
          ...fetchStatus,
          downloadState: "downloading",
          downloadedBytes: 0,
          downloadPercent: 0,
          lastError: ""
        };
        return { started: true, status: fetchStatus };
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
    showReleaseNotesDialog() {
      return { close() {} };
    },
    openExternalUrl() {},
    clearTimeoutFn() {}
  });

  await controller.mount();
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Downloading 12%");

  fetchStatus = {
    ...fetchStatus,
    downloadState: "failed",
    installState: "idle",
    downloadedBytes: 0,
    downloadPercent: 0,
    lastError: "Downloaded portable ZIP checksum mismatch."
  };

  await controller.refreshStatus({ silent: true, openPanel: true });

  assert.deepEqual(fetchCalls, ["/app/update-status", "/app/update-status"]);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Download failed");
  assert.equal(refs.desktopUpdatePrimaryBtn.dataset.action, "download");
  assert.equal(refs.desktopUpdatePrimaryBtn.textContent, "Try download again");
  assert.equal(refs.desktopUpdateBody.textContent, "Downloaded portable ZIP checksum mismatch.");
  assert.equal(
    toasts.filter(
      item =>
        item.message === "Downloaded portable ZIP checksum mismatch." && item.level === "error"
    ).length,
    1
  );

  await controller.handlePrimaryAction();

  assert.deepEqual(postCalls, ["/app/download-update"]);
  assert.equal(refs.desktopUpdateToggleBtn.textContent, "Downloading update");
  assert.equal(
    toasts.filter(
      item =>
        item.message === "Downloaded portable ZIP checksum mismatch." && item.level === "error"
    ).length,
    1
  );
  assert.ok(
    toasts.some(item => item.message === "Desktop update download started." && item.level === "info")
  );
});
