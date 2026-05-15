import test from "node:test";
import assert from "node:assert/strict";
import {
  buildJobsPipelineButtonView,
  formatPipelineElapsed,
  getJobsUpdateTooltip,
  getPipelineRunningLabel,
  JOBS_UPDATE_COPY,
  updateJobsPipelineUi
} from "../../../frontend/jobs/app/pipeline.js";

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
    },
    removeAttribute(name) {
      delete this[name];
    }
  };
}

function createButtonMock(textContent = "Update jobs") {
  const button = {
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
    },
    removeAttribute(name) {
      delete this[name];
    }
  };
  return button;
}

test("jobs update tooltip copy covers default, warm, first-run, and unavailable states", () => {
  assert.equal(getJobsUpdateTooltip(), JOBS_UPDATE_COPY.tooltipDefault);
  assert.equal(
    getJobsUpdateTooltip({ firstRunKnown: true }),
    JOBS_UPDATE_COPY.tooltipWarm
  );
  assert.equal(
    getJobsUpdateTooltip({ firstRunKnown: true, firstRun: true }),
    JOBS_UPDATE_COPY.tooltipFirstRun
  );
  assert.equal(
    getJobsUpdateTooltip({ bridgeError: "Bridge request timed out" }),
    JOBS_UPDATE_COPY.tooltipBridgeTimedOut
  );
  assert.equal(
    getJobsUpdateTooltip({ bridgeError: "Network error: bridge unreachable" }),
    JOBS_UPDATE_COPY.tooltipBridgeUnavailable
  );
});

test("pipeline label formats running stage with elapsed seconds", () => {
  const now = Date.parse("2026-03-12T12:00:12.000Z");
  const label = getPipelineRunningLabel({
    progress: { label: "Running discovery..." },
    startedAt: "2026-03-12T12:00:00.000Z"
  }, now);
  assert.equal(label, "Checking sources... 12s");
});

test("pipeline label falls back to stage and minute formatting", () => {
  const now = Date.parse("2026-03-12T12:01:01.000Z");
  const label = getPipelineRunningLabel({
    stage: "sync_push",
    startedAt: "2026-03-12T12:00:00.000Z"
  }, now);
  assert.equal(label, "Updating local jobs... 1m 1s");
});

test("pipeline label works without startedAt", () => {
  const label = getPipelineRunningLabel({
    progress: { label: "Running fetch..." }
  }, Date.parse("2026-03-12T12:00:10.000Z"));
  assert.equal(label, "Fetching job listings...");
});

test("pipeline label maps starting pipeline copy to user-facing update copy", () => {
  const now = Date.parse("2026-03-12T12:00:08.000Z");
  const label = getPipelineRunningLabel({
    progress: { label: "Starting pipeline" },
    startedAt: "2026-03-12T12:00:00.000Z"
  }, now);
  assert.equal(label, "Updating jobs... 8s");
});

test("formatPipelineElapsed handles invalid and short durations", () => {
  const now = Date.parse("2026-03-12T12:00:08.000Z");
  assert.equal(formatPipelineElapsed("", now), "");
  assert.equal(formatPipelineElapsed("2026-03-12T12:00:00.000Z", now), "8s");
});

test("buildJobsPipelineButtonView keeps the starting state indeterminate", () => {
  const view = buildJobsPipelineButtonView(
    {
      active: true,
      startedAt: "2026-03-12T12:00:00.000Z",
      stage: "starting",
      progress: {
        active: true,
        currentStep: 0,
        totalSteps: 3,
        percent: 0,
        label: "Updating jobs..."
      }
    },
    {
      running: true,
      buttonLabel: "Updating jobs...",
      nowMs: Date.parse("2026-03-12T12:00:00.000Z")
    }
  );

  assert.equal(view.active, true);
  assert.equal(view.progressMode, "indeterminate");
  assert.equal(view.progressFill, 0);
  assert.equal(view.label, "Updating jobs...");
});

test("buildJobsPipelineButtonView derives determinate fill from pipeline steps", () => {
  const view = buildJobsPipelineButtonView(
    {
      active: true,
      startedAt: "2026-03-12T12:00:00.000Z",
      stage: "fetch",
      progress: {
        active: true,
        currentStep: 2,
        totalSteps: 3,
        percent: 67,
        label: "Running fetch..."
      }
    },
    {
      running: true,
      buttonLabel: "",
      nowMs: Date.parse("2026-03-12T12:07:27.000Z")
    }
  );

  assert.equal(view.progressMode, "determinate");
  assert.equal(view.progressFill, 0.67);
  assert.equal(view.label, "Fetching job listings... 7m 27s");
});

test("updateJobsPipelineUi updates button background progress", () => {
  const button = createButtonMock();

  updateJobsPipelineUi(
    { jobsPipelineRunBtn: button },
    {
      running: true,
      disabled: true,
      buttonLabel: "Fetching job listings... 7m 27s",
      buttonTooltip: JOBS_UPDATE_COPY.tooltipWarm,
      pipelinePayload: {
        active: true,
        startedAt: "2026-03-12T12:00:00.000Z",
        stage: "fetch",
        progress: {
          active: true,
          currentStep: 2,
          totalSteps: 3,
          percent: 67,
          label: "Running fetch..."
        }
      }
    }
  );

  assert.equal(button.textContent, "Fetching job listings... 7m 27s");
  assert.equal(button.dataset.tooltip, JOBS_UPDATE_COPY.tooltipWarm);
  assert.equal(button["data-tooltip"], JOBS_UPDATE_COPY.tooltipWarm);
  assert.equal(button.disabled, true);
  assert.equal(button["aria-disabled"], "true");
  assert.equal(button["aria-busy"], "true");
  assert.equal(button.classList.contains("running"), true);
  assert.equal(button.classList.contains("determinate"), true);
  assert.equal(button.dataset.progressMode, "determinate");
  assert.equal(button.dataset.progressFill, "67");
  assert.equal(button.style.getPropertyValue("--jobs-pipeline-fill"), "67%");
  assert.equal(button.children.length, 2);
  assert.equal(button.children[0].dataset.ui, "jobs-pipeline-fill");
  assert.equal(button.children[0].dataset.progressMode, "determinate");
  assert.equal(button.children[0].style.width, "67%");
  assert.equal(button.children[0].style.opacity, "1");
  assert.equal(button.children[1].dataset.ui, "jobs-pipeline-label");
  assert.equal(button.children[1].textContent, "Fetching job listings... 7m 27s");
});

test("updateJobsPipelineUi clears progress state when idle or errored", () => {
  const button = createButtonMock();
  button.disabled = true;
  button.title = "legacy tooltip";

  updateJobsPipelineUi(
    { jobsPipelineRunBtn: button },
    {
      running: false,
      disabled: false,
      buttonLabel: "",
      buttonTooltip: JOBS_UPDATE_COPY.tooltipDefault,
      isError: true
    }
  );

  assert.equal(button.textContent, "Update jobs");
  assert.equal(button.dataset.tooltip, JOBS_UPDATE_COPY.tooltipDefault);
  assert.equal(button.title, undefined);
  assert.equal(button.disabled, false);
  assert.equal(button["aria-disabled"], "false");
  assert.equal(button["aria-busy"], "false");
  assert.equal(button.classList.contains("running"), false);
  assert.equal(button.classList.contains("log-error"), true);
  assert.equal(button.dataset.progressMode, undefined);
  assert.equal(button.dataset.progressFill, undefined);
  assert.equal(button.style.getPropertyValue("--jobs-pipeline-fill"), "");
  assert.equal(button.children[0].dataset.progressMode, undefined);
  assert.equal(button.children[0].style.width, "0%");
  assert.equal(button.children[0].style.opacity, "0");
  assert.equal(button.children[1].textContent, "Update jobs");
});
