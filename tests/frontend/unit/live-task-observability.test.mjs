import test from "node:test";
import assert from "node:assert/strict";

import { deriveAdminRunsModel } from "../../../frontend/admin/domain.js";
import { renderAdminOpsHistory } from "../../../frontend/admin/render.js";
import { createJobsPipelineController } from "../../../frontend/jobs/app/runtime/pipeline-controller.js";
import { createJobsPipelineUiState } from "../../../frontend/jobs/app/runtime/state.js";

function createClassList() {
  const values = new Set();
  return {
    add(...tokens) {
      tokens.forEach(token => values.add(token));
    },
    remove(...tokens) {
      tokens.forEach(token => values.delete(token));
    },
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

function createButtonMock(textContent = "Update jobs") {
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

function makeContainer() {
  return {
    innerHTML: "",
    textContent: "",
    dataset: {},
    classList: createClassList(),
    querySelectorAll: () => []
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

test("long-running fetch progress stays aligned across admin detailed view, ops summary, and jobs blocked state", async () => {
  const sharedCounts = {
    resolvedSources: 6,
    sourceCount: 12,
    runningTasks: 1,
    queuedTasks: 0,
    outputCount: 42,
    failedSources: 1,
    excludedSources: 0
  };
  const taskStatePayload = {
    tasks: [
      {
        taskType: "fetch",
        active: true,
        runId: "fetch_live_1",
        startedAt: "2026-03-12T12:00:10.000Z",
        status: "running",
        taskProgress: {
          active: true,
          phaseKey: "executing_sources",
          phaseLabel: "Executing sources",
          mode: "determinate",
          ratio: 0.5,
          counts: sharedCounts
        },
        summary: {
          outputCount: 42,
          failedSources: 1,
          sourceCount: 12
        }
      }
    ]
  };
  const historyEl = makeContainer();
  const runModel = deriveAdminRunsModel(
    {
      taskState: taskStatePayload,
      historyRuns: []
    },
    Date.parse("2026-03-12T12:07:10.000Z")
  );
  renderAdminOpsHistory(historyEl, runModel);
  assert.match(historyEl.innerHTML, /Executing sources \(50%\)/);
  assert.doesNotMatch(historyEl.innerHTML, /6\/12 sources resolved/);
  assert.doesNotMatch(historyEl.innerHTML, /running 1/);

  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    uiState.active = true;
    uiState.runId = "pipeline_1";
    uiState.startedAt = "2026-03-12T12:00:00.000Z";

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
          return taskStatePayload;
        }
        throw new Error(`Unexpected bridge path: ${path}`);
      },
      getAllJobs: () => [],
      showToast: () => {},
      setRefreshJobsNeedsAttention: () => {},
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    await controller.pollJobsPipelineStatus();

    assert.equal(uiState.active, true);
    assert.equal(button.disabled, false);
    assert.equal(button["aria-busy"], "true");
    assert.match(String(button.textContent || ""), /^Fetching job listings\.\.\./);
  } finally {
    restoreTimers();
  }
});
