import test from "node:test";
import assert from "node:assert/strict";

import {
  createJobsDesktopUpdateController,
  deriveDesktopUpdateView
} from "../../../frontend/jobs/app/desktop-update.js";

function classList() {
  const values = new Set(["hidden"]);
  return {
    add: token => values.add(token),
    remove: token => values.delete(token),
    contains: token => values.has(token),
    toggle(token, force) {
      force === false ? values.delete(token) : values.add(token);
    }
  };
}

function element() {
  const listeners = new Map();
  return {
    textContent: "",
    disabled: false,
    href: "#",
    dataset: {},
    classList: classList(),
    setAttribute() {},
    addEventListener(name, handler) {
      listeners.set(name, handler);
    },
    clickEvent() {
      listeners.get("click")?.({ preventDefault() {} });
    }
  };
}

function refs() {
  return {
    desktopUpdateToggleBtn: element(),
    desktopUpdatePanel: element(),
    desktopUpdateTitle: element(),
    desktopUpdateBody: element(),
    desktopUpdateMeta: element(),
    desktopUpdateProgress: element(),
    desktopUpdatePrimaryBtn: element(),
    desktopUpdateSecondaryBtn: element(),
    desktopUpdateReleaseNotes: element(),
  };
}

const releaseNotesHistory = [
  {
    releaseNotesUrl: "https://example.com/release",
    releaseNotesTitle: "Baluffo v0.0.16",
    releaseNotesBody: "### Fixed\n- Modal notes",
    releaseNotesPublishedAt: "2026-04-15T10:00:00Z",
    releaseTag: "v0.0.16",
    releaseVersion: "0.0.16"
  },
  {
    releaseNotesUrl: "https://example.com/previous",
    releaseNotesTitle: "Baluffo v0.0.15",
    releaseNotesBody: "### Added\n- Previous modal notes",
    releaseNotesPublishedAt: "2026-04-14T10:00:00Z",
    releaseTag: "v0.0.15",
    releaseVersion: "0.0.15"
  }
];

test("deriveDesktopUpdateView keeps release notes history visible", () => {
  const view = deriveDesktopUpdateView({
    currentVersion: "0.0.15",
    latestVersion: "0.0.16",
    targetVersion: "0.0.16",
    availability: "available",
    updateAvailable: true,
    releaseNotesHistory,
  }, { panelOpen: true });

  assert.equal(view.releaseNotesVisible, true);
  assert.equal(view.releaseNotesHistory[1].releaseVersion, "0.0.15");
});

test("jobs desktop update controller passes release notes history to the dialog", async () => {
  const testRefs = refs();
  const dialogs = [];
  const status = {
    currentVersion: "0.0.15",
    latestVersion: "0.0.16",
    targetVersion: "0.0.16",
    availability: "available",
    updateAvailable: true,
    downloadState: "idle",
    installState: "idle",
    releaseNotesUrl: "https://example.com/release",
    releaseNotesTitle: "Baluffo v0.0.16",
    releaseNotesBody: "### Fixed\n- Modal notes",
    releaseNotesPublishedAt: "2026-04-15T10:00:00Z",
    releaseNotesHistory,
  };
  const controller = createJobsDesktopUpdateController({
    refs: testRefs,
    fetchJson: async () => status,
    postJson: async () => ({ status }),
    bindAsyncClick: () => {},
    showReleaseNotesDialog: options => dialogs.push(options),
    isDesktopRuntimeMode: () => true,
    setTimeoutFn: () => ({ unref() {} }),
    clearTimeoutFn() {},
  });

  await controller.mount();
  await controller.refreshStatus({ openPanel: true, isFresh: true });
  testRefs.desktopUpdateReleaseNotes.clickEvent();

  assert.equal(dialogs[0].releaseNotesHistory[1].releaseNotesUrl, "https://example.com/previous");
});
