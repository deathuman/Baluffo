import test from "node:test";
import assert from "node:assert/strict";
import {
  formatPipelineElapsed,
  getPipelineRunningLabel,
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

test("pipeline label formats running stage with elapsed seconds", () => {
  const now = Date.parse("2026-03-12T12:00:12.000Z");
  const label = getPipelineRunningLabel({
    progress: { label: "Running discovery..." },
    startedAt: "2026-03-12T12:00:00.000Z"
  }, now);
  assert.equal(label, "Discovery running... 12s");
});

test("pipeline label falls back to stage and minute formatting", () => {
  const now = Date.parse("2026-03-12T12:01:01.000Z");
  const label = getPipelineRunningLabel({
    stage: "sync_push",
    startedAt: "2026-03-12T12:00:00.000Z"
  }, now);
  assert.equal(label, "Sync Push running... 1m 1s");
});

test("pipeline label works without startedAt", () => {
  const label = getPipelineRunningLabel({
    progress: { label: "Running fetch..." }
  }, Date.parse("2026-03-12T12:00:10.000Z"));
  assert.equal(label, "Fetch running...");
});

test("formatPipelineElapsed handles invalid and short durations", () => {
  const now = Date.parse("2026-03-12T12:00:08.000Z");
  assert.equal(formatPipelineElapsed("", now), "");
  assert.equal(formatPipelineElapsed("2026-03-12T12:00:00.000Z", now), "8s");
});

test("updateJobsPipelineUi updates button and ignores deprecated progress element", () => {
  const button = {
    textContent: "Run Discovery + Fetch + Sync",
    dataset: {},
    disabled: false,
    classList: createClassList(),
    setAttribute(name, value) {
      this[name] = value;
    }
  };
  const progress = {
    textContent: "legacy",
    classList: createClassList()
  };

  updateJobsPipelineUi(
    { jobsPipelineRunBtn: button, jobsPipelineProgressEl: progress },
    {
      running: true,
      disabled: true,
      buttonLabel: "Discovery running... 12s",
      progressLabel: "should not render"
    }
  );

  assert.equal(button.textContent, "Discovery running... 12s");
  assert.equal(button.disabled, true);
  assert.equal(button["aria-disabled"], "true");
  assert.equal(button.classList.contains("running"), true);
  assert.equal(progress.textContent, "legacy");
});
