import test from "node:test";
import assert from "node:assert/strict";

import { createJobsDesktopUpdateController } from "../../../frontend/jobs/app/desktop-update.js";
import { createClassList } from "./helpers/dom-test-helpers.mjs";
import { buildDesktopUpdateRefs } from "./helpers/jobs-runtime-helpers.mjs";

const DISMISSED_REPAIR_KEY = "baluffo.desktopUpdate.dismissedRepair";

function createElement(text = "") {
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
    addEventListener() {}
  };
}

function createStorage() {
  const values = new Map();
  return {
    values,
    getItem(key) {
      return values.has(key) ? values.get(key) : null;
    },
    setItem(key, value) {
      values.set(key, String(value));
    },
    removeItem(key) {
      values.delete(key);
    }
  };
}

const REPAIRED_STATUS = {
  currentVersion: "0.3.064",
  availability: "up_to_date",
  downloadState: "idle",
  installState: "idle",
  repairState: "repaired",
  repairAction: "restore-snapshot",
  repairCompletedAt: "2026-11-17T12:05:00Z",
  lastRepair: { version: "0.3.064" },
  lastCheckedAt: "2026-11-17T12:00:00Z"
};

function createController({ storage, status = REPAIRED_STATUS, postJson }) {
  const refs = buildDesktopUpdateRefs(createElement);
  const controller = createJobsDesktopUpdateController({
    refs,
    baseUrl: "http://127.0.0.1:8877",
    fetchJson: async () => status,
    postJson: postJson || (async () => ({ status })),
    bindAsyncClick: () => {},
    showToast: () => {},
    isDesktopRuntimeMode: () => true,
    storage,
    setTimeoutFn: handler => ({ unref() {}, handler }),
    clearTimeoutFn() {}
  });
  return { controller, refs };
}

test("closing a completed repair keeps the panel closed on the next launch", async () => {
  const storage = createStorage();
  const first = createController({ storage });

  await first.controller.mount();
  assert.equal(first.controller._getState().panelOpen, true);
  assert.equal(first.refs.desktopUpdatePanel.classList.contains("hidden"), false);

  await first.controller.handleSecondaryAction();
  assert.equal(first.controller._getState().panelOpen, false);
  assert.equal(first.refs.desktopUpdatePanel.classList.contains("hidden"), true);
  assert.equal(
    storage.values.get(DISMISSED_REPAIR_KEY),
    "repaired:2026-11-17T12:05:00Z"
  );

  const relaunched = createController({ storage });
  await relaunched.controller.mount();

  assert.equal(relaunched.controller._getState().panelOpen, false);
  assert.equal(relaunched.refs.desktopUpdatePanel.classList.contains("hidden"), true);
  assert.equal(relaunched.refs.desktopUpdateToggleBtn.textContent, "Repair complete");
});

test("closing the repaired panel from the header toggle also dismisses it", async () => {
  const storage = createStorage();
  const first = createController({ storage });

  await first.controller.mount();
  assert.equal(first.refs.desktopUpdatePanel.classList.contains("hidden"), false);

  await first.controller.handleToggleClick();
  assert.equal(first.refs.desktopUpdatePanel.classList.contains("hidden"), true);
  assert.equal(
    storage.values.get(DISMISSED_REPAIR_KEY),
    "repaired:2026-11-17T12:05:00Z"
  );

  const relaunched = createController({ storage });
  await relaunched.controller.mount();
  assert.equal(relaunched.controller._getState().panelOpen, false);
  assert.equal(relaunched.refs.desktopUpdatePanel.classList.contains("hidden"), true);
});

test("a newer completed repair still opens the panel after an older dismissal", async () => {
  const storage = createStorage();
  storage.setItem(DISMISSED_REPAIR_KEY, "repaired:2026-11-17T12:05:00Z");

  const relaunched = createController({
    storage,
    status: { ...REPAIRED_STATUS, repairCompletedAt: "2026-11-18T09:00:00Z" }
  });
  await relaunched.controller.mount();

  assert.equal(relaunched.controller._getState().panelOpen, true);
  assert.equal(relaunched.refs.desktopUpdatePanel.classList.contains("hidden"), false);
});

test("an undismissed completed repair still skips the startup check", async () => {
  const postCalls = [];
  const first = createController({
    storage: createStorage(),
    postJson: async (_baseUrl, path) => {
      postCalls.push(path);
      return {
        status: {
          ...REPAIRED_STATUS,
          repairState: "idle",
          repairCompletedAt: "",
          lastCheckedAt: "2026-11-17T12:06:00Z"
        }
      };
    }
  });

  await first.controller.mount();
  assert.equal(first.controller._getState().panelOpen, true);
  await first.controller.startAutoCheck();

  assert.deepEqual(postCalls, []);
  assert.equal(first.controller._getState().status.repairState, "repaired");
});

test("a dismissed repair resumes the startup check and acknowledges the result", async () => {
  const storage = createStorage();
  const first = createController({ storage });
  await first.controller.mount();
  await first.controller.handleSecondaryAction();
  assert.equal(first.controller._getState().panelOpen, false);

  const postCalls = [];
  const relaunched = createController({
    storage,
    postJson: async (_baseUrl, path, payload) => {
      postCalls.push({ path, payload });
      return {
        status: {
          ...REPAIRED_STATUS,
          repairState: "idle",
          repairCompletedAt: "",
          lastCheckedAt: "2026-11-17T12:06:00Z"
        }
      };
    }
  });
  await relaunched.controller.mount();
  assert.equal(relaunched.controller._getState().panelOpen, false);
  assert.equal(relaunched.controller._getState().status.repairState, "repaired");

  await relaunched.controller.startAutoCheck();

  assert.deepEqual(
    postCalls.map(call => call.path),
    ["/app/check-for-update"]
  );
  assert.equal(postCalls[0].payload.force, true);
  assert.equal(relaunched.controller._getState().status.repairState, "idle");
  assert.equal(relaunched.controller._getState().panelOpen, false);
  assert.equal(relaunched.refs.desktopUpdatePanel.classList.contains("hidden"), true);
  assert.equal(
    storage.values.get(DISMISSED_REPAIR_KEY),
    "repaired:2026-11-17T12:05:00Z"
  );
});
