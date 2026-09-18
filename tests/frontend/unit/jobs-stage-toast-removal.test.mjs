import test from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";

import { createJobsPipelineController } from "../../../frontend/jobs/app/runtime/pipeline-controller.js";
import { createJobsPipelineUiState } from "../../../frontend/jobs/app/runtime/state.js";

const controllerSource = readFileSync(
  new URL("../../../frontend/jobs/app/runtime/pipeline-controller.js", import.meta.url),
  "utf8"
);
const stateSource = readFileSync(
  new URL("../../../frontend/jobs/app/runtime/state.js", import.meta.url),
  "utf8"
);
const componentsCss = readFileSync(
  new URL("../../../styles/components.css", import.meta.url),
  "utf8"
);

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

function createButtonMock() {
  const children = [];
  const button = {
    textContent: "Update jobs",
    dataset: {},
    style: { setProperty() {}, removeProperty() {} },
    classList: createClassList(),
    children,
    parentElement: null,
    setAttribute() {},
    removeAttribute() {},
    addEventListener() {},
    removeEventListener() {},
    appendChild(child) {
      children.push(child);
      return child;
    },
    querySelector: () => null,
    querySelectorAll: () => [],
    closest: () => null,
    ownerDocument: null
  };
  const parent = {
    children: [],
    classList: createClassList(),
    appendChild(child) {
      parent.children.push(child);
      return child;
    },
    querySelector: () => null,
    querySelectorAll: () => []
  };
  button.parentElement = parent;
  parent.parentElement = { classList: createClassList(), querySelector: () => null };
  return button;
}

function activePayload(transitions) {
  return {
    active: true,
    runId: "pipeline_stage_toast",
    stage: "fetch",
    startedAt: "2026-03-12T12:00:00.000Z",
    stageTransitions: transitions,
    activeChildren: [
      {
        runId: "child_1",
        taskType: "fetch",
        type: "fetch",
        active: true,
        startedAt: "2026-03-12T12:00:10.000Z",
        taskProgress: { active: true, phaseLabel: "Executing sources" }
      }
    ]
  };
}

test("stage transitions do not toast on the jobs page", async () => {
  const restoreTimers = installFakeTimers();
  try {
    const button = createButtonMock();
    const uiState = createJobsPipelineUiState();
    const toasts = [];
    let transitions = [{ from: "preflight", to: "discovery", at: "t1" }];

    const controller = createJobsPipelineController({
      refs: { jobsPipelineRunBtn: button },
      jobsPipelineUiState: uiState,
      callJobsBridge: async path => {
        if (path === "/tasks/run-jobs-pipeline-status") return activePayload(transitions);
        if (path === "/ops/task-state?view=summary") {
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
      showToast: (message, kind) => toasts.push({ message, kind }),
      isErrorStage: payload => Boolean(payload?.error),
      pollDelayMs: 25,
      idlePollDelayMs: 50
    });

    // Walk the same stage sequence the backend emits across a real run.
    for (const next of [
      [{ from: "preflight", to: "discovery", at: "t1" }],
      [
        { from: "preflight", to: "discovery", at: "t1" },
        { from: "discovery", to: "fetch", at: "t2" }
      ],
      [
        { from: "preflight", to: "discovery", at: "t1" },
        { from: "discovery", to: "fetch", at: "t2" },
        { from: "fetch", to: "sync_push", at: "t3" }
      ],
      [
        { from: "preflight", to: "discovery", at: "t1" },
        { from: "discovery", to: "fetch", at: "t2" },
        { from: "fetch", to: "sync_push", at: "t3" },
        { from: "sync_push", to: "completed", at: "t4" }
      ]
    ]) {
      transitions = next;
      await controller.pollJobsPipelineStatus();
    }

    const jargon = [
      "Source discovery",
      "Fetching job listings",
      "Syncing results",
      "Finishing up"
    ];
    for (const text of jargon) {
      assert.equal(
        toasts.some(toast => String(toast.message).trim() === text),
        false,
        `stage toast "${text}" must not be shown on the jobs page`
      );
    }
    assert.equal(toasts.length, 0, "an active run must not emit stage toasts");
  } finally {
    restoreTimers();
  }
});

test("stage-transition toast machinery is removed, not merely disabled", () => {
  // Guards against a silent reintroduction: the helper, its copy map and the
  // UI-state counter it relied on must all stay gone.
  assert.equal(
    controllerSource.includes("maybeToastStageTransitions"),
    false,
    "maybeToastStageTransitions must not be reintroduced"
  );
  assert.equal(
    controllerSource.includes("stageTransitions"),
    false,
    "the controller must not read stageTransitions again"
  );
  assert.equal(
    stateSource.includes("stageTransitionSeenCount"),
    false,
    "the stageTransitionSeenCount state field must stay removed"
  );
});

test("shared toast styling keeps a readable info toast and a real action button", () => {
  // A jobs density pass (5d860ccb) overwrote .toast.info into lowercase
  // micro-text and deleted .toast-action-btn, which broke every info toast and
  // the Undo/Revert buttons across Jobs, Saved and Admin.
  const infoRule = componentsCss.match(/\.toast\.info\s*\{[\s\S]*?\}/)?.[0] || "";
  assert.ok(infoRule, ".toast.info must exist");
  assert.equal(
    /text-transform:\s*lowercase/.test(infoRule),
    false,
    ".toast.info must not lowercase its message"
  );
  assert.equal(
    /font-size:\s*0\.65rem/.test(infoRule),
    false,
    ".toast.info must not shrink to micro-text"
  );
  assert.equal(
    /(^|\s)border:\s*0/.test(infoRule),
    false,
    ".toast.info must keep its toast border"
  );
  assert.equal(
    /background:\s*none/.test(infoRule),
    false,
    ".toast.info must keep its toast surface"
  );

  const actionRule = componentsCss.match(/\.toast-action-btn\s*\{[\s\S]*?\}/)?.[0] || "";
  assert.ok(actionRule, ".toast-action-btn must exist");
  assert.match(actionRule, /cursor:\s*pointer/);
  assert.match(actionRule, /border-radius:\s*999px/);
  assert.match(actionRule, /padding:\s*0\.2rem\s+0\.55rem/);
});

function installFakeTimers() {
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  // ponytail: the callback is deliberately NOT invoked. The controller schedules
  // its own re-poll through setTimeout, so a synchronous fake would recurse
  // forever. This mirrors jobs-pipeline-controller.test.mjs.
  globalThis.setTimeout = callback => ({ callback });
  globalThis.clearTimeout = () => {};
  return () => {
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
  };
}
