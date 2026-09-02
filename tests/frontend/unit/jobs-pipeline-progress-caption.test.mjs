import test from "node:test";
import assert from "node:assert/strict";
import {
  formatBlockingTaskProgressLabel,
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

function createElementMock() {
  return {
    dataset: {},
    style: createStyle(),
    className: "",
    textContent: "",
    hidden: false,
    classList: createClassList(),
    setAttribute(name, value) {
      this[name] = value;
    },
    removeAttribute(name) {
      delete this[name];
    }
  };
}

function createCaptionAwareButtonMock() {
  const button = {
    dataset: {},
    style: createStyle(),
    disabled: false,
    classList: createClassList(),
    _textContent: "Update jobs",
    ownerDocument: { createElement: createElementMock },
    children: [],
    get textContent() {
      return this.children.length
        ? this.children.map(child => String(child?.textContent || "")).join("")
        : String(this._textContent || "");
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
      return this.children.find(child => String(child?.dataset?.ui || "") === match[1]) || null;
    },
    setAttribute(name, value) {
      this[name] = value;
    },
    removeAttribute(name) {
      delete this[name];
    }
  };
  const inserted = [];
  const captionUi = "jobs-pipeline-progress-caption";
  button.insertAdjacentElement = (_position, el) => {
    // ponytail: mirror the real toolbar DOM — dynamically created siblings get
    // the button's parent as their parentElement so the status row can carry
    // the running class.
    el.parentElement = button.parentElement;
    inserted.push(el);
  };
  button.parentElement = {
    classList: createClassList(),
    querySelector(selector) {
      if (!String(selector || "").includes(captionUi)) return null;
      return inserted.find(el => String(el?.dataset?.ui || "") === captionUi) || null;
    }
  };
  return { button, inserted };
}

test("updateJobsPipelineUi routes sub-progress to the caption, not the in-button span", () => {
  const { button, inserted } = createCaptionAwareButtonMock();
  const update = (running, opts = {}) => updateJobsPipelineUi({ jobsPipelineRunBtn: button }, {
    running,
    disabled: running,
    buttonLabel: running ? "Fetching job listings... 7m 27s" : "",
    pipelinePayload: { active: running, stage: "fetch" },
    ...opts
  });

  update(true, { progressLabel: "Resolving sources 128/431 · ETA 4m" });

  const caption = inserted.find(el => String(el?.dataset?.ui || "") === "jobs-pipeline-progress-caption");
  assert.ok(caption, "caption element should be created as a sibling");
  assert.equal(caption.textContent, "Resolving sources 128/431 · ETA 4m");
  assert.equal(caption.hidden, false);
  assert.ok(caption.classList.contains("running"));
  assert.ok(button.classList.contains("running"));
  // ponytail: the in-button progress span stays hidden/empty — the button only
  // carries the compact stage + elapsed label and the fill groove.
  assert.equal(button.children[2].dataset.ui, "jobs-pipeline-progress");
  assert.equal(button.children[2].hidden, true);

  update(false);
  assert.equal(caption.textContent, "");
  assert.equal(caption.hidden, true);
  assert.equal(caption.classList.contains("running"), false);
});

test("formatBlockingTaskProgressLabel emits the high-signal caption without verbose tokens", () => {
  assert.equal(
    formatBlockingTaskProgressLabel({
      taskType: "fetch",
      taskProgress: {
        phaseLabel: "Executing sources",
        mode: "determinate",
        counts: {
          resolvedSources: 128,
          sourceCount: 431,
          runningTasks: 6,
          queuedTasks: 1097,
          outputCount: 48175,
          failedSources: 6,
          completedSourcesPerMinute: 12,
          estimatedRemainingMs: 240000,
          runningSourceNames: ["Studio A", "Studio B"],
          runningSourceNamesTruncated: true
        }
      }
    }),
    "Executing sources · 128/431 sources resolved · rate 12/min · ETA 4m"
  );
  // ponytail: verbose running/output/queued/failed/current-source detail is dropped.
  assert.doesNotMatch(formatBlockingTaskProgressLabel({
    taskType: "fetch",
    taskProgress: {
      phaseLabel: "Executing sources",
      mode: "determinate",
      counts: { resolvedSources: 1, sourceCount: 2, runningSourceNames: ["Studio A"] }
    }
  }), /runningSourceNames|Studio A|output|queued|failed/);
});

test("live child payload ticks resolved/rate/ETA across polls", () => {
  // Shape mirrors the pipeline status payload's active fetch child after the
  // backend projects the live task-state counts (jobs-fetch-tasks.json) into
  // taskProgress — the same surface the admin ops live page shows.
  const liveChild = {
    taskType: "fetch",
    type: "fetch",
    active: true,
    taskProgress: {
      active: true,
      phaseKey: "executing_sources",
      phaseLabel: "Executing sources",
      mode: "determinate",
      ratio: 0.24,
      counts: {
        sourceCount: 2135,
        totalTasks: 2135,
        queuedTasks: 1623,
        runningTasks: 40,
        completedTasks: 512,
        resolvedSources: 512,
        outputCount: 48200,
        failedSources: 6,
        excludedSources: 2,
        completedSourcesPerMinute: 12,
        etaBasis: "sources",
        estimatedRemainingMs: 600000
      }
    }
  };
  assert.equal(
    formatBlockingTaskProgressLabel(liveChild),
    "Executing sources · 512/2,135 sources resolved · rate 12/min · ETA 10m"
  );

  // Next poll: two more sources resolved, rate and ETA move with it.
  liveChild.taskProgress.ratio = 0.248;
  liveChild.taskProgress.counts.resolvedSources = 530;
  liveChild.taskProgress.counts.completedTasks = 530;
  liveChild.taskProgress.counts.completedSourcesPerMinute = 13;
  liveChild.taskProgress.counts.estimatedRemainingMs = 540000;
  assert.equal(
    formatBlockingTaskProgressLabel(liveChild),
    "Executing sources · 530/2,135 sources resolved · rate 13/min · ETA 9m"
  );

  // The caption element renders each poll's label as it arrives.
  const { button, inserted } = createCaptionAwareButtonMock();
  const update = (progressLabel) => updateJobsPipelineUi({ jobsPipelineRunBtn: button }, {
    running: true,
    disabled: true,
    buttonLabel: "Fetching job listings...",
    pipelinePayload: { active: true, stage: "fetch" },
    progressLabel
  });
  update(formatBlockingTaskProgressLabel(liveChild));
  let caption = inserted.find(el => String(el?.dataset?.ui || "") === "jobs-pipeline-progress-caption");
  assert.equal(caption.textContent, "Executing sources · 530/2,135 sources resolved · rate 13/min · ETA 9m");

  liveChild.taskProgress.counts.resolvedSources = 620;
  liveChild.taskProgress.counts.completedTasks = 620;
  liveChild.taskProgress.counts.completedSourcesPerMinute = 14;
  liveChild.taskProgress.counts.estimatedRemainingMs = 480000;
  update(formatBlockingTaskProgressLabel(liveChild));
  caption = inserted.find(el => String(el?.dataset?.ui || "") === "jobs-pipeline-progress-caption");
  assert.equal(caption.textContent, "Executing sources · 620/2,135 sources resolved · rate 14/min · ETA 8m");
  assert.equal(caption.classList.contains("running"), true);
});

test("discovery caption surfaces GameDevMap audit subtask ticks like the admin page", () => {
  // Shape mirrors the GameDevMap active-audit subtask: the jobs caption should
  // show batch/URL progress and fetch-phase detail, not just the phase name.
  const label = formatBlockingTaskProgressLabel({
    taskType: "discovery",
    taskProgress: {
      phaseKey: "scanning_sources",
      phaseLabel: "Scanning sources",
      mode: "indeterminate",
      counts: {
        subtaskKey: "gamedevmap_active_audit",
        subtaskLabel: "GameDevMap active audit",
        activeAuditBatch: 3,
        activeAuditCompletedUrls: 27,
        activeAuditTotalUrls: 120
      }
    }
  });
  assert.equal(label, "Scanning sources · GameDevMap active audit | batch 3 | 27/120 URLs");
});

test("discovery caption surfaces fetch-phase detail during GameDevMap page fetches", () => {
  const label = formatBlockingTaskProgressLabel({
    taskType: "discovery",
    taskProgress: {
      phaseKey: "scanning_sources",
      phaseLabel: "Scanning sources",
      mode: "indeterminate",
      counts: {
        subtaskKey: "gamedevmap_active_audit",
        subtaskLabel: "GameDevMap active audit",
        activeAuditPhase: "homepage_fetch",
        activeAuditPhaseCompleted: 8,
        activeAuditPhaseTotal: 42
      }
    }
  });
  assert.equal(label, "Scanning sources · GameDevMap active dry run | homepage fetch 8/42 pages");
});

test("discovery caption keeps stage/probed/counter detail for non-audit discovery runs", () => {
  const label = formatBlockingTaskProgressLabel({
    taskType: "discovery",
    taskProgress: {
      phaseKey: "probing_candidates",
      phaseLabel: "Probing candidates",
      mode: "determinate",
      targetLabel: "steam_curator_feeds",
      counts: {
        stageIndex: 2,
        stageTotal: 5,
        generatedCandidates: 312,
        foundEndpoints: 87,
        queuedCandidates: 41,
        probedCandidates: 138,
        probeTotal: 500
      }
    }
  });
  assert.equal(
    label,
    "Probing candidates · stage 2/5 · probing steam_curator_feeds · 138/500 candidates probed · generated 312 · endpoints 87 · queued 41"
  );
  // Silence when there is nothing beyond the phase: no dangling separators.
  assert.equal(
    formatBlockingTaskProgressLabel({
      taskType: "discovery",
      taskProgress: { phaseKey: "scanning_sources", phaseLabel: "Scanning sources" }
    }),
    "Scanning sources"
  );
});

test("formatBlockingTaskProgressLabel falls back to the phase for indeterminate phases", () => {
  // fetch prep phase: no determinate target, no per-source counts yet -> just the phase.
  assert.equal(
    formatBlockingTaskProgressLabel({
      taskType: "fetch",
      taskProgress: { phaseKey: "selecting_sources", phaseLabel: "Selecting sources" }
    }),
    "Selecting sources"
  );
  // discovery scanning has no probe counter -> phase only.
  assert.equal(
    formatBlockingTaskProgressLabel({
      taskType: "discovery",
      taskProgress: { phaseKey: "scanning_sources", phaseLabel: "Scanning sources" }
    }),
    "Scanning sources"
  );
  // pipeline keeps the step marker when present.
  assert.equal(
    formatBlockingTaskProgressLabel({
      taskType: "pipeline",
      taskProgress: {
        phaseLabel: "Fetching job listings",
        counts: { currentStep: 2, totalSteps: 3 }
      }
    }),
    "Fetching job listings · step 2/3"
  );
});

test("updateJobsPipelineUi marks the toolbar status row running for the whole active run", () => {
  const { button } = createCaptionAwareButtonMock();
  const statusRow = button.parentElement;

  updateJobsPipelineUi({ jobsPipelineRunBtn: button }, {
    running: true,
    disabled: true,
    buttonLabel: "Checking sources... 13s",
    pipelinePayload: { active: true, stage: "discovery" },
    progressLabel: "Scanning known careers pages"
  });
  assert.equal(statusRow.classList.contains("running"), true);
  // ponytail: the row stays marked running even in silent phases where the
  // caption is empty, so the Last-updated timestamp stays dimmed all run.
  updateJobsPipelineUi({ jobsPipelineRunBtn: button }, {
    running: true,
    disabled: true,
    buttonLabel: "Updating jobs...",
    pipelinePayload: { active: true, stage: "starting" },
    progressLabel: ""
  });
  assert.equal(statusRow.classList.contains("running"), true);

  updateJobsPipelineUi({ jobsPipelineRunBtn: button }, {
    running: false,
    disabled: false,
    buttonLabel: "",
    pipelinePayload: null
  });
  assert.equal(statusRow.classList.contains("running"), false);
});
