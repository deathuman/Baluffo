import test from "node:test";
import assert from "node:assert/strict";

import { createJobsPipelineController } from "../../../frontend/jobs/app/runtime/pipeline-controller.js";
import { createJobsPipelineUiState } from "../../../frontend/jobs/app/runtime/state.js";
import { JOBS_UPDATE_COPY } from "../../../frontend/jobs/app/pipeline.js";

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
  return {
    setProperty(name, value) {
      this[name] = value;
    },
    removeProperty(name) {
      delete this[name];
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
    hidden: false,
    disabled: false,
    classList: createClassList(),
    attributes: new Map(),
    setAttribute(name, value) {
      this.attributes.set(name, String(value));
      if (name === "data-tooltip") this.dataset.tooltip = String(value);
      if (name === "data-abort-label") this.dataset.abortLabel = String(value);
    },
    getAttribute(name) {
      return this.attributes.get(name) || "";
    },
    removeAttribute(name) {
      this.attributes.delete(name);
      if (name === "data-abort-label") delete this.dataset.abortLabel;
    }
  };
}

function createButtonMock() {
  const children = [];
  const parent = {
    children,
    querySelector(selector) {
      if (selector === '[data-ui="jobs-pipeline-abort"]') {
        return children.find(child => child?.dataset?.ui === "jobs-pipeline-abort") || null;
      }
      return null;
    }
  };
  const button = createElementMock("button");
  button._textContent = "Update jobs";
  Object.defineProperty(button, "textContent", {
    get() {
      const buttonChildren = children.filter(child => child?.dataset?.ui !== "jobs-pipeline-abort");
      if (buttonChildren.length > 0) {
        return buttonChildren.map(child => String(child?.textContent || "")).join("");
      }
      return String(this._textContent || "");
    },
    set(value) {
      this._textContent = String(value);
      children.length = 0;
    }
  });
  button.parentElement = parent;
  button.ownerDocument = {
    createElement: createElementMock
  };
  button.querySelector = selector => {
    if (selector === '[data-ui="jobs-pipeline-fill"]') {
      return children.find(child => child?.dataset?.ui === "jobs-pipeline-fill") || null;
    }
    if (selector === '[data-ui="jobs-pipeline-label"]') {
      return children.find(child => child?.dataset?.ui === "jobs-pipeline-label") || null;
    }
    return null;
  };
  button.replaceChildren = (...nodes) => {
    children.length = 0;
    children.push(...nodes);
  };
  button.insertAdjacentElement = (_position, node) => {
    children.push(node);
  };
  return button;
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

function createBootstrapTask(runId) {
  return {
    taskType: "fetch",
    task: "jobs_bootstrap",
    runId,
    status: "running",
    startedAt: "2026-05-18T00:00:00.000Z",
    summary: { coverageScope: "bootstrap_sheets" },
    progress: {
      active: true,
      phaseLabel: "Executing sources"
    },
    taskProgress: {
      counts: { outputCount: 2634 }
    }
  };
}

test("pollJobsPipelineStatus periodically rechecks task-state and discovers external bootstrap fetch", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    uiState.taskStateSummaryChecked = true;
    uiState.lastTaskStateSummaryCheckedAt = Date.now() - 6000;
    const paths = [];
    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        paths.push(path);
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/ops/task-state?view=summary") {
          return { tasks: [createBootstrapTask("jobs_bootstrap_external")] };
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

    assert.deepEqual(paths, ["/tasks/run-jobs-pipeline-status", "/ops/task-state?view=summary"]);
    assert.equal(button.disabled, false);
    assert.match(String(button.textContent || ""), /^Fetching job listings\.\.\./);
    assert.equal(button.dataset.tooltip, JOBS_UPDATE_COPY.tooltipFirstRunBootstrap);
    assert.equal(uiState.updateTooltipFirstRunBootstrapActive, true);
  } finally {
    restoreTimers();
  }
});

test("idle Jobs status watch discovers external bootstrap fetch from scheduled poll", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const paths = [];
    let bootstrapActive = false;
    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        paths.push(path);
        if (path === "/tasks/run-jobs-pipeline-status") return { active: false, stage: "idle" };
        if (path === "/ops/task-state?view=summary") {
          return { tasks: bootstrapActive ? [createBootstrapTask("jobs_bootstrap_scheduled")] : [] };
        }
        if (path === "/ops/dashboard-health?view=summary") return { alerts: [] };
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
    assert.equal(typeof uiState.pollingTimer?.callback, "function");

    paths.length = 0;
    bootstrapActive = true;
    uiState.lastTaskStateSummaryCheckedAt = Date.now() - 6000;
    uiState.pollingTimer.callback();
    await new Promise(resolve => setImmediate(resolve));

    assert.deepEqual(paths, ["/tasks/run-jobs-pipeline-status", "/ops/task-state?view=summary"]);
    assert.equal(button.disabled, false);
    assert.equal(button.dataset.abortable, "true");
    assert.match(String(button.textContent || ""), /^Fetching job listings\.\.\./);
  } finally {
    restoreTimers();
  }
});
