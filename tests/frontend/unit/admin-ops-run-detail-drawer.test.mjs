// The run detail body lives in the inspector drawer, not in a panel inside the
// operations-history container. The renderer cannot reach the drawer's buttons
// with `querySelectorAll` — the inspector controller paints it asynchronously —
// so the integration under test is: a row click stages its detail body on the
// row, and the drawer host gets one delegated click listener that resolves
// Copy/Abort against the payload map from the most recent render.

import test from "node:test";
import assert from "node:assert/strict";

import { renderAdminOpsHistory } from "../../../frontend/admin/render.js";
import {
  clickRow,
  createDetailHostStub,
  makeDetailButton,
  makeHistoryEl,
  rowsFor
} from "./helpers/ops-history-test-helpers.mjs";

test("admin ops history: run diagnostics copy uses bounded payload without changing compact rows", () => {
  const runKey = "current||fetch_live_1|fetch|2026-03-08T10:00:00.000Z||0";
  const historyEl = makeHistoryEl();
  const detailHost = createDetailHostStub();
  const copied = [];

  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "fetch",
        runId: "fetch_live_1",
        active: true,
        isLive: true,
        startedAt: "2026-03-08T10:00:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: {
          outputCount: 12,
          failedSources: 0,
          recommendedApiPayload: { hidden: true }
        },
        workItems: Array.from({ length: 8 }, (_row, index) => ({
          id: `source_${index}`,
          name: `Source ${index}`,
          status: index === 0 ? "running" : "pending",
          rawLargeThing: { hidden: true }
        }))
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  }, {
    selectedRunKey: runKey,
    runDetailHost: detailHost,
    onCopyRunDiagnostics: payload => copied.push(payload)
  });

  assert.match(historyEl.innerHTML, /admin-ops-history-row/);
  assert.match(historyEl.innerHTML, /admin-ops-history-row-selected/);
  // The Copy button is not in the history markup any more: it belongs to the run
  // detail body, which the drawer renders from `__runDetailHtml`.
  assert.doesNotMatch(historyEl.innerHTML, /data-ops-run-diagnostics-copy=/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-card/);
  assert.doesNotMatch(historyEl.innerHTML, /role="progressbar"/i);

  const [rowEl] = rowsFor(historyEl);
  clickRow(rowEl);

  assert.match(rowEl.__runDetailHtml, /data-ops-run-diagnostics-copy="current\|\|fetch_live_1\|fetch\|2026-03-08T10:00:00\.000Z\|\|0"/);
  // The detail body is presentation only: the raw payload keys must not leak into
  // it, exactly as they must not leak into the copy payload.
  assert.doesNotMatch(rowEl.__runDetailHtml, /recommendedApiPayload|rawLargeThing/i);

  // The drawer is a long-lived element painted outside this renderer, so its
  // buttons are served by one delegated listener rather than per-button handlers.
  detailHost.click(makeDetailButton("data-ops-run-diagnostics-copy", runKey));

  assert.equal(copied.length, 1);
  assert.equal(copied[0].kind, "admin_run_diagnostics");
  assert.equal(copied[0].rowArea, "current");
  assert.equal(copied[0].runId, "fetch_live_1");
  assert.equal(copied[0].workItemExamples.length, 5);
  const serialized = JSON.stringify(copied[0]);
  assert.doesNotMatch(serialized, /recommendedApiPayload|rawLargeThing/i);
  assert.doesNotMatch(historyEl.innerHTML, /<button[^>]*>(?:Start|Stop|Retry|Clear|Cleanup|Lifecycle)/i);
});

test("admin ops history: selected run analysis renders bounded read-only evidence", () => {
  const historyEl = makeHistoryEl();
  const runKey = "current||fetch_selected_1|fetch|2026-03-08T10:00:00.000Z||0";

  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "fetch",
        runId: "fetch_selected_1",
        active: true,
        isLive: true,
        startedAt: "2026-03-08T10:00:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: {
          outputCount: 42,
          failedSources: 1,
          slowestSources: Array.from({ length: 8 }, (_row, index) => ({
            sourceId: `slow_${index}`,
            durationMs: 1000 + index
          }))
        },
        workItems: Array.from({ length: 8 }, (_row, index) => ({
          id: `source_${index}`,
          name: `Source ${index}`,
          status: index === 0 ? "running" : index === 1 ? "failed" : "pending",
          error: index === 1 ? "source failed after timeout" : "",
          updatedAt: index === 1 ? "2026-03-08T10:03:00.000Z" : ""
        })),
        recentEvents: Array.from({ length: 8 }, (_row, index) => ({
          at: `2026-03-08T10:0${Math.min(index, 5)}:00.000Z`,
          level: "info",
          message: `Event ${index}`
        }))
      }
    ],
    visibleCompletedRows: [
      {
        type: "sync",
        status: "ok",
        runId: "sync_done_1",
        finishedAt: "2026-03-08T09:30:00.000Z",
        durationMs: 1500,
        summary: { action: "push", activeCount: 7, pendingCount: 2, rejectedCount: 1 }
      }
    ],
    olderCompletedRows: []
  }, {
    selectedRunKey: runKey
  });

  // Selection is still reflected on the row, but the detail body is no longer in
  // the history markup at all — it is staged for the drawer on row click.
  assert.match(historyEl.innerHTML, /admin-ops-history-row-selected/);
  assert.match(historyEl.innerHTML, /fetch_selected_1/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-detail/);
  assert.doesNotMatch(historyEl.innerHTML, /Selected Run Analysis/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-card/);
  assert.doesNotMatch(historyEl.innerHTML, /role="progressbar"/i);
  assert.doesNotMatch(historyEl.innerHTML, /<button[^>]*>(?:Start|Stop|Retry|Clear|Cleanup|Lifecycle)/i);

  const [rowEl] = rowsFor(historyEl);
  clickRow(rowEl);
  const detail = rowEl.__runDetailHtml;

  assert.match(detail, /admin-ops-run-detail-head/);
  assert.match(detail, /fetch_selected_1/);
  assert.match(detail, /slow_4/);
  assert.doesNotMatch(detail, /slow_5/);
  assert.match(detail, /source_4/);
  assert.doesNotMatch(detail, /source_5/);
  // The timeline is one merged, sorted, capped list of distinct events — it is
  // the only place events appear. The old panel also printed a separate "Event
  // examples" list, which was the same events a second time.
  assert.match(detail, /Timeline/);
  assert.match(detail, /Event 3/);
  assert.doesNotMatch(detail, /Event 5/);
  assert.doesNotMatch(detail, /Event examples/);
  assert.doesNotMatch(detail, /admin-ops-run-card/);
  assert.doesNotMatch(detail, /role="progressbar"/i);
  assert.doesNotMatch(detail, /<button[^>]*>(?:Start|Stop|Retry|Clear|Cleanup|Lifecycle)/i);

  // A timestamped timeline entry must print a real stamp. The model also
  // synthesises an untimestamped `source: "progress"` entry; printing it produced
  // the literal text "source order" as if it were a time.
  assert.match(detail, /2026|Mar|08\/03/);
  assert.doesNotMatch(detail, /source order/i);
});

test("admin ops history: selected run analysis stays hidden without selection", () => {
  const historyEl = makeHistoryEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "fetch",
        runId: "fetch_live_1",
        active: true,
        isLive: true,
        startedAt: "2026-03-08T10:00:00.000Z",
        heartbeatAt: new Date().toISOString(),
        summary: { outputCount: 12, failedSources: 0 }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  });

  assert.doesNotMatch(historyEl.innerHTML, /Selected Run Analysis/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-detail/);
  assert.doesNotMatch(historyEl.innerHTML, /Select a run row to inspect bounded run evidence/);
  assert.doesNotMatch(historyEl.innerHTML, /Timeline/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-history-row-selected/);
  assert.doesNotMatch(historyEl.innerHTML, /admin-ops-run-card/);
  assert.doesNotMatch(historyEl.innerHTML, /role="progressbar"/i);
});

test("admin ops history: active rows do not render stale finished timestamps", () => {
  const historyEl = makeHistoryEl();
  const runKey = "current||discovery_live_1|discovery|2026-03-08T10:00:00.000Z||0";

  renderAdminOpsHistory(historyEl, {
    currentRows: [
      {
        type: "discovery",
        runId: "discovery_live_1",
        active: true,
        isLive: true,
        displayStatus: "running",
        startedAt: "2026-03-08T10:00:00.000Z",
        finishedAt: "2026-04-09T12:00:00.000Z",
        summary: { queuedCandidateCount: 4, failedProbeCount: 0 },
        taskProgress: {
          active: true,
          phaseKey: "probing",
          phaseLabel: "Probing candidates",
          counts: { queuedCandidates: 4 }
        }
      }
    ],
    visibleCompletedRows: [],
    olderCompletedRows: []
  }, {
    selectedRunKey: runKey
  });

  assert.match(historyEl.innerHTML, /running/i);
  assert.match(historyEl.innerHTML, /admin-ops-history-row-selected/);

  const [rowEl] = rowsFor(historyEl);
  clickRow(rowEl);

  assert.doesNotMatch(rowEl.__runDetailHtml, /<strong>Finished<\/strong>/);
  assert.doesNotMatch(rowEl.__runDetailHtml, /4\/9\/2026|Apr/i);
});

test("admin ops history: a run with no timeline evidence omits the section", () => {
  const historyEl = makeHistoryEl();
  const runKey = "completed||sync_no_timeline|sync||2026-03-08T09:30:00.000Z|0";

  renderAdminOpsHistory(historyEl, {
    currentRows: [],
    visibleCompletedRows: [
      {
        type: "sync",
        status: "ok",
        runId: "sync_no_timeline",
        finishedAt: "2026-03-08T09:30:00.000Z",
        durationMs: 1500,
        summary: { action: "push", activeCount: 7, pendingCount: 2, rejectedCount: 1 }
      }
    ],
    olderCompletedRows: []
  }, {
    selectedRunKey: runKey
  });

  assert.doesNotMatch(historyEl.innerHTML, /Selected Run Analysis/);
  assert.doesNotMatch(historyEl.innerHTML, /Timeline/);

  const [rowEl] = rowsFor(historyEl);
  clickRow(rowEl);
  const detail = rowEl.__runDetailHtml;

  assert.match(detail, /admin-ops-run-detail-head/);
  // Empty sections are hidden rather than padded with "nothing recorded" lines.
  assert.doesNotMatch(detail, /Timeline/);
  assert.doesNotMatch(detail, /No timeline evidence recorded for this run/);
  assert.doesNotMatch(detail, /Diagnostic hints/);
  // The summary counts are real evidence here, so the section does render.
  assert.match(detail, /Evidence/);
  assert.match(detail, /Review queue|Active|Pending|Rejected|Action/);
  assert.doesNotMatch(detail, /<button[^>]*>(?:Start|Stop|Retry|Clear|Cleanup|Lifecycle)/i);
});

test("admin ops history: selected completed pipeline analysis shows parent and child diagnostics", () => {
  const historyEl = makeHistoryEl();
  renderAdminOpsHistory(historyEl, {
    currentRows: [],
    visibleCompletedRows: [
      {
        type: "pipeline",
        status: "ok",
        startedAt: "2026-03-08T09:00:00.000Z",
        finishedAt: "2026-03-08T10:00:00.000Z",
        summary: {
          baselineOutputCount: 120,
          jobsPageLoadedCount: 150,
          finalOutputCount: 175,
          updatesFound: true
        },
        taskProgress: {
          active: false,
          phaseLabel: "Pipeline completed",
          mode: "determinate",
          ratio: 1,
          counts: {
            currentStep: 3,
            totalSteps: 3,
            baselineOutputCount: 120,
            jobsPageLoadedCount: 150,
            finalOutputCount: 175
          }
        },
        pipelineChildren: [
          {
            type: "discovery",
            status: "ok",
            finishedAt: "2026-03-08T09:20:00.000Z",
            summary: { queuedCandidateCount: 6, failedProbeCount: 1 },
            taskProgress: {
              counts: { generatedCandidates: 20, queuedCandidates: 6, failedProbes: 1 }
            }
          },
          {
            type: "fetch",
            status: "ok",
            finishedAt: "2026-03-08T09:50:00.000Z",
            summary: { outputCount: 175, failedSources: 0 },
            taskProgress: {
              counts: { resolvedSources: 12, sourceCount: 12, outputCount: 175, failedSources: 0 }
            }
          }
        ]
      }
    ],
    olderCompletedRows: []
  }, {
    selectedRunKey: "completed|||pipeline|2026-03-08T09:00:00.000Z|2026-03-08T10:00:00.000Z|0"
  });

  const [rowEl] = rowsFor(historyEl);
  clickRow(rowEl);
  const detail = rowEl.__runDetailHtml;

  assert.match(detail, /admin-ops-run-detail-head/);
  assert.match(detail, /Pipeline output 175 vs comparison base 150; updates found/);
  // The child-stage summaries survive as hints, but the generic phase echo
  // ("Pipeline stage 3/3 Pipeline completed") is dropped because the progress
  // line already states it.
  assert.match(detail, /Discovery completed/);
  assert.match(detail, /Fetch completed/);
  assert.doesNotMatch(detail, /Pipeline stage 3\/3/);
  assert.equal(detail.split("Pipeline completed (100%)").length - 1, 1);
  assert.doesNotMatch(detail, /<details class="admin-ops-run-detail"/i);
  assert.doesNotMatch(detail, /step 0/i);
  assert.doesNotMatch(detail, /output 0 \(baseline 0\)/i);
});
