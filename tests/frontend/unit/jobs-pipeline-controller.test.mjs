import test from "node:test";
import assert from "node:assert/strict";

import { createJobsPipelineController } from "../../../frontend/jobs/app/runtime/pipeline-controller.js";
import { createJobsPipelineUiState } from "../../../frontend/jobs/app/runtime/state.js";

function createClassList() {
  const values = new Set();
  return {
    toggle(name, enabled) {
      if (enabled) values.add(name);
      else values.delete(name);
    },
    contains(name) {
      return values.has(name);
    }
  };
}

function createStyle() {
  const values = new Map();
  return {
    setProperty(name, value) {
      values.set(name, value);
      this[name] = value;
    },
    removeProperty(name) {
      values.delete(name);
      delete this[name];
    },
    getPropertyValue(name) {
      return values.get(name) || "";
    }
  };
}

function createElementMock(tagName) {
  return {
    tagName: String(tagName || "").toUpperCase(),
    dataset: {},
    style: createStyle(),
    className: "",
    textContent: "",
    setAttribute(name, value) {
      this[name] = value;
    }
  };
}

function createButtonMock(textContent = "Run Discovery + Fetch + Sync") {
  return {
    dataset: {},
    style: createStyle(),
    disabled: false,
    classList: createClassList(),
    _textContent: textContent,
    ownerDocument: {
      createElement: createElementMock
    },
    children: [],
    get textContent() {
      if (this.children.length > 0) {
        return this.children.map(child => String(child?.textContent || "")).join("");
      }
      return String(this._textContent || "");
    },
    set textContent(value) {
      this._textContent = String(value);
      this.children = [];
    },
    replaceChildren(...nodes) {
      this.children = nodes;
    },
    querySelector(selector) {
      const match = /^\[data-ui="([^"]+)"\]$/.exec(String(selector || ""));
      if (!match) return null;
      const wanted = match[1];
      return this.children.find(child => String(child?.dataset?.ui || "") === wanted) || null;
    },
    setAttribute(name, value) {
      this[name] = value;
    }
  };
}

function installFakeTimers() {
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  globalThis.setTimeout = callback => ({ callback });
  globalThis.clearTimeout = () => {};
  return () => {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
  };
}

test("pollJobsPipelineStatus keeps the Jobs button busy while fetch is still active", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    uiState.active = true;
    uiState.runId = "pipeline_1";
    uiState.startedAt = "2026-03-12T12:00:00.000Z";
    const toasts = [];
    let refreshNeedsAttention = false;

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: false,
            runId: "pipeline_1",
            stage: "error",
            error: "fetch report failed"
          };
        }
        if (path === "/ops/task-state") {
          return {
            tasks: [
              {
                taskType: "fetch",
                active: true,
                startedAt: "2026-03-12T12:00:10.000Z",
                taskProgress: { phaseLabel: "Executing sources" }
              }
            ]
          };
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, kind) => {
        toasts.push({ message, kind });
      },
      setRefreshJobsNeedsAttention: value => {
        refreshNeedsAttention = Boolean(value);
      },
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.pollJobsPipelineStatus();

    assert.equal(uiState.active, true);
    assert.equal(button.disabled, true);
    assert.match(String(button.textContent || ""), /^Fetch running\.\.\./);
    assert.equal(refreshNeedsAttention, false);
    assert.deepEqual(toasts, []);
  } finally {
    restoreTimers();
  }
});

test("pollJobsPipelineStatus announces completion only after blocking tasks clear", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    uiState.active = true;
    uiState.runId = "pipeline_1";
    uiState.startedAt = "2026-03-12T12:00:00.000Z";
    const toasts = [];
    let refreshNeedsAttention = false;

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") {
          return {
            active: false,
            runId: "pipeline_1",
            stage: "completed",
            updatesFound: true,
            refreshRecommended: true
          };
        }
        if (path === "/ops/task-state") {
          return { tasks: [] };
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: (message, kind) => {
        toasts.push({ message, kind });
      },
      setRefreshJobsNeedsAttention: value => {
        refreshNeedsAttention = Boolean(value);
      },
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.pollJobsPipelineStatus();

    assert.equal(uiState.active, false);
    assert.equal(button.disabled, false);
    assert.equal(refreshNeedsAttention, true);
    assert.deepEqual(toasts, [
      {
        message: "Pipeline completed. Refresh jobs to load updated listings.",
        kind: "success"
      }
    ]);
  } finally {
    restoreTimers();
  }
});
