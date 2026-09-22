// The drawer body replaces an inline panel that printed the same facts three
// times and padded itself out with "nothing recorded" placeholders. These tests
// pin the de-duplication and the hide-when-empty rules, because both are the kind
// of thing a later "just add the section back" edit silently undoes.

import test from "node:test";
import assert from "node:assert/strict";

import { renderRunDetailHtml } from "../../../frontend/admin/render/ops-run-detail.js";

function makeView(overrides = {}) {
  return {
    key: "fetch_1",
    typeText: "Fetch",
    runId: "fetch_1",
    statusText: "Completed",
    statusClass: "healthy",
    outputOrQueuedText: "output 175 | failed 0",
    isRunning: false,
    abortable: false,
    ...overrides
  };
}

test("empty evidence renders one muted line instead of placeholder sections", () => {
  const html = renderRunDetailHtml(makeView({ outputOrQueuedText: "" }), { analysis: {} });

  assert.match(html, /No supporting evidence was recorded for this run/);
  for (const heading of ["Evidence", "Timeline", "Diagnostic hints"]) {
    assert.doesNotMatch(html, new RegExp(heading), `${heading} must not render when empty`);
  }
  for (const placeholder of ["No timeline evidence", "No diagnostic hints", "No examples recorded", "No compact counts", "No timing data", "No warnings or failures"]) {
    assert.doesNotMatch(html, new RegExp(placeholder), placeholder);
  }
});

test("progress is stated once and the synthetic source-order echo is dropped", () => {
  const progressLabel = "Pipeline completed (100%) | step 3/3 | output 175 (baseline 120)";
  const html = renderRunDetailHtml(makeView({ outputOrQueuedText: progressLabel }), {
    analysis: {
      progressLabel,
      summaryCounts: { finalOutputCount: 175 },
      timelineEntries: [
        // The model synthesises this from the progress payload: no timestamp, and
        // its detail is the progress line verbatim.
        { source: "progress", timestamp: "", label: "Pipeline completed", detail: progressLabel, severity: "healthy" },
        { source: "event", timestamp: "2026-03-08T09:20:00.000Z", label: "Discovery finished", detail: "", severity: "healthy" }
      ]
    }
  });

  const occurrences = html.split("Pipeline completed (100%)").length - 1;
  assert.equal(occurrences, 1, "the progress string must appear exactly once");
  assert.doesNotMatch(html, /source order/i);
  assert.match(html, /Discovery finished/);
  assert.equal(html.split("Discovery finished").length - 1, 1);
});

test("a hint that only restates the progress line is suppressed", () => {
  const progressLabel = "Pipeline completed (100%) | step 3/3 | output 175 (baseline 120)";
  const html = renderRunDetailHtml(makeView({ outputOrQueuedText: progressLabel }), {
    analysis: {
      progressLabel,
      diagnosticHints: [
        "Pipeline stage 3/3 Pipeline completed",
        progressLabel,
        "Fetch failed 3 sources: example.com timed out"
      ]
    }
  });

  assert.doesNotMatch(html, /Pipeline stage 3\/3/);
  assert.match(html, /example\.com timed out/);
  assert.match(html, /Diagnostic hints/);
});

test("duration is shown once, never as both Duration and Elapsed", () => {
  const html = renderRunDetailHtml(makeView(), {
    analysis: {
      timing: {
        startedAt: "2026-03-08T09:00:00.000Z",
        finishedAt: "2026-03-08T10:00:00.000Z",
        durationLabel: "1h 0m",
        elapsedLabel: "1h 0m"
      }
    }
  });

  assert.match(html, /Started/);
  assert.match(html, /Finished/);
  assert.match(html, /Duration/);
  assert.doesNotMatch(html, /Elapsed/);
  assert.equal(html.split("1h 0m").length - 1, 1);
});

test("summary counts render as labelled stats, not raw camelCase keys", () => {
  const html = renderRunDetailHtml(makeView(), {
    analysis: {
      summaryCounts: { finalOutputCount: 175, failedProbeCount: 2, updatesFound: true }
    }
  });

  assert.match(html, /Final output/);
  assert.match(html, /175/);
  assert.match(html, /Failed probes/);
  assert.match(html, /Updates found/);
  assert.match(html, /yes/);
  assert.doesNotMatch(html, /finalOutputCount/);
  assert.doesNotMatch(html, /failedProbeCount/);
});

test("copy and abort controls carry the run key for the delegated handlers", () => {
  const html = renderRunDetailHtml(makeView({ abortable: true }), {
    analysis: {},
    canCopyRunDiagnostics: true
  });

  assert.match(html, /data-ops-run-diagnostics-copy="fetch_1"/);
  assert.match(html, /data-ops-run-abort="fetch_1"/);
});

test("no copy control is offered when the caller has no copy handler", () => {
  const html = renderRunDetailHtml(makeView(), { analysis: {}, canCopyRunDiagnostics: false });

  assert.doesNotMatch(html, /data-ops-run-diagnostics-copy/);
  assert.doesNotMatch(html, /admin-ops-run-detail-actions/);
});

test("a finished run never prints an empty Finished stamp", () => {
  const html = renderRunDetailHtml(makeView({ isRunning: true }), {
    analysis: { timing: { startedAt: "2026-03-08T09:00:00.000Z", finishedAt: "2026-03-08T10:00:00.000Z" } }
  });

  assert.match(html, /Started/);
  assert.doesNotMatch(html, /Finished/);
});

test("a missing view renders nothing", () => {
  assert.equal(renderRunDetailHtml(null), "");
  assert.equal(renderRunDetailHtml(undefined), "");
});
