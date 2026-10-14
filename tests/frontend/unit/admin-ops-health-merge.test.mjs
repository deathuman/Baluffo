/**
 * The probe-wave receipt across the two health payloads the Admin page polls.
 *
 * The page polls `/ops/dashboard-health` at two cadences: the full payload, and the much smaller
 * `?view=summary`. Both carry `kpis.probeLifecycle`, but they are not equally detailed, so the merge
 * has to treat the key like `registrySync` rather than as a plain assigned value: a summary poll
 * whose receipt read came back empty must not erase the reading a full poll already supplied, while a
 * summary poll carrying fresh counters -- including a clean `clientsUnclosed: 0` -- still lands.
 *
 * The rendering contract for those counters lives next door: the operator-facing sentences are pinned
 * in `admin-ops-diagnostics-render.test.mjs`.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { mergeOpsHealth } from "../../../frontend/admin/domain/ops-merge-model.js";
import { renderAdminOpsKpis } from "../../../frontend/admin/render.js";
import { createButtonMapEl as makeEl } from "./helpers/dom-test-helpers.mjs";

const FLAGGED_RECEIPT = {
  batches: 1,
  fetchesStarted: 17,
  fetchesBoundedOut: 2,
  fetchesOutstandingAtExit: 1,
  clientFetchesOutstandingAtExit: 1,
  clientsUnclosed: 1,
  deadlineExits: 0
};

function renderProbeLifecycle(kpis) {
  const el = makeEl();
  renderAdminOpsKpis(el, kpis, "warning", {});
  const html = String(el.innerHTML);
  const at = html.indexOf("Probe lifecycle");
  return { html, line: at < 0 ? "" : html.slice(at, at + 400) };
}

test("admin merge: a summary poll's empty receipt read keeps the detailed probe lifecycle", () => {
  const detail = { kpis: { registrySync: { activeCount: 3 }, probeLifecycle: FLAGGED_RECEIPT } };
  const summary = { summaryView: true, kpis: { registrySync: { pendingCount: 5 }, probeLifecycle: {} } };

  const merged = mergeOpsHealth(detail, summary, { summary: true });
  const { html, line } = renderProbeLifecycle(merged.kpis);

  assert.equal(merged.kpis.probeLifecycle.clientsUnclosed, 1);
  assert.equal(merged.kpis.registrySync.activeCount, 3, "the sibling nested KPI behaves the same way");
  assert.match(line, /2 abandoned by their own bound/i);
  assert.doesNotMatch(html, /no probe-wave receipt published/i);
});

test("admin merge: a summary poll's fresh counters replace the previous run's reading", () => {
  const detail = { kpis: { probeLifecycle: FLAGGED_RECEIPT } };
  const summary = {
    summaryView: true,
    kpis: {
      probeLifecycle: {
        batches: 1,
        fetchesStarted: 17,
        fetchesBoundedOut: 0,
        fetchesOutstandingAtExit: 0,
        clientFetchesOutstandingAtExit: 0,
        clientsUnclosed: 0,
        deadlineExits: 0
      }
    }
  };

  const merged = mergeOpsHealth(detail, summary, { summary: true });

  assert.equal(merged.kpis.probeLifecycle.clientsUnclosed, 0, "a clean wave clears the flag");
  assert.equal(merged.kpis.probeLifecycle.fetchesBoundedOut, 0);
});

/**
 * The page-fetch receipt across the same two payloads.
 *
 * It carries a per-source map, so it has one failure mode `probeLifecycle` does not: a summary poll
 * that cannot see one source must not erase the other sources a full poll already reported. The
 * per-source entries are assigned wholesale, so a changed entry replaces the previous reading while
 * a missing one is preserved.
 */
const PAGE_FETCH_DETAIL = {
  runId: "run-detail",
  coverage: "partial",
  truncated: true,
  omittedCount: 3,
  sources: {
    gameprog: {
      source: "gameprog",
      runId: "run-detail",
      state: "pending",
      coverage: "partial",
      truncated: true,
      omittedCount: 3,
      receiptCount: 2,
      rollup: { batchCount: 2, submitted: 9, finished: 6, failed: 1, outstandingNow: 2, pendingAsyncCleanup: 1, state: "pending" }
    },
    gamesmap: {
      source: "gamesmap",
      runId: "run-detail",
      state: "clean",
      coverage: "full",
      truncated: false,
      omittedCount: 0,
      receiptCount: 1,
      rollup: { batchCount: 1, submitted: 4, finished: 4, failed: 0, outstandingNow: 0, pendingAsyncCleanup: 0, state: "clean" }
    }
  }
};

function renderPageFetchLifecycle(kpis) {
  const el = makeEl();
  renderAdminOpsKpis(el, kpis, "warning", {});
  const html = String(el.innerHTML);
  const at = html.indexOf("Page fetch lifecycle");
  return { html, line: at < 0 ? "" : html.slice(at, at + 500) };
}

test("admin merge: a summary poll without a source keeps the detailed page-fetch reading", () => {
  const detail = { kpis: { pageFetchLifecycle: PAGE_FETCH_DETAIL } };
  const summary = { summaryView: true, kpis: { pageFetchLifecycle: {} } };

  const merged = mergeOpsHealth(detail, summary, { summary: true });
  const { html, line } = renderPageFetchLifecycle(merged.kpis);

  assert.equal(merged.kpis.pageFetchLifecycle.sources.gameprog.rollup.outstandingNow, 2);
  assert.equal(merged.kpis.pageFetchLifecycle.sources.gamesmap.state, "clean");
  assert.match(line, /gameprog/i);
  assert.match(line, /gamesmap/i);
  assert.doesNotMatch(html, /no page-fetch receipt published/i);
});

test("admin merge: a summary poll carrying one source keeps the other source's reading", () => {
  const detail = { kpis: { pageFetchLifecycle: PAGE_FETCH_DETAIL } };
  const summary = {
    summaryView: true,
    kpis: {
      pageFetchLifecycle: {
        runId: "run-summary",
        coverage: "full",
        truncated: false,
        sources: {
          gameprog: {
            source: "gameprog",
            runId: "run-summary",
            state: "clean",
            coverage: "full",
            truncated: false,
            omittedCount: 0,
            receiptCount: 2,
            rollup: { batchCount: 2, submitted: 9, finished: 9, failed: 0, outstandingNow: 0, pendingAsyncCleanup: 0, state: "clean" }
          }
        }
      }
    }
  };

  const merged = mergeOpsHealth(detail, summary, { summary: true });

  assert.equal(merged.kpis.pageFetchLifecycle.sources.gameprog.state, "clean", "fresh entry wins");
  assert.equal(merged.kpis.pageFetchLifecycle.sources.gameprog.rollup.outstandingNow, 0);
  assert.equal(
    merged.kpis.pageFetchLifecycle.sources.gamesmap.state,
    "clean",
    "a source absent from the summary poll is preserved, not erased"
  );
});
