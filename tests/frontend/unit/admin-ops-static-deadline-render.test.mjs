import test from "node:test";
import assert from "node:assert/strict";
import { deriveFetcherFailureSummary } from "../../../frontend/admin/domain.js";
import { renderAdminOpsFetcherMetrics } from "../../../frontend/admin/render.js";
import { createRenderEl as makeEl } from "./helpers/dom-test-helpers.mjs";

test("admin domain extracts per-source static deadline clocks", () => {
  const summary = deriveFetcherFailureSummary({
    sources: [
      {
        name: "static_sources",
        adapter: "static",
        status: "ok",
        details: [
          {
            name: "Slow Board",
            adapter: "static",
            status: "error",
            error: "time_budget_exceeded",
            deadlineKind: "request_cap"
          },
          {
            name: "Batch Board",
            status: "error",
            error: "time_budget_exceeded",
            deadlineKind: "batch_deadline"
          },
          {
            name: "Budget Board",
            adapter: "static",
            status: "error",
            error: "time budget exceeded (90s)",
            deadlineKind: "source_budget"
          },
          { name: "Healthy Board", adapter: "static", status: "ok", keptCount: 2 }
        ]
      },
      {
        name: "Legacy Static Board",
        adapter: "static",
        status: "error",
        error: "time_budget_exceeded",
        deadlineKind: "request_cap"
      },
      {
        name: "ashby_sources",
        adapter: "ashby",
        status: "error",
        error: "timeout",
        deadlineKind: "source_budget"
      }
    ]
  });

  assert.deepEqual(summary.deadlineRows, [
    { name: "Slow Board", kind: "request_cap", error: "time_budget_exceeded" },
    { name: "Batch Board", kind: "batch_deadline", error: "time_budget_exceeded" },
    { name: "Budget Board", kind: "source_budget", error: "time budget exceeded (90s)" },
    { name: "Legacy Static Board", kind: "request_cap", error: "time_budget_exceeded" }
  ]);
});

test("admin render: static deadline cuts name the clock and source", () => {
  const summary = deriveFetcherFailureSummary({
    sources: [
      {
        name: "static_sources",
        adapter: "static",
        status: "ok",
        details: [
          {
            name: "Slow Board",
            adapter: "static",
            status: "error",
            error: "time_budget_exceeded",
            deadlineKind: "request_cap"
          },
          {
            name: "Batch Board",
            adapter: "static",
            status: "error",
            error: "time_budget_exceeded",
            deadlineKind: "batch_deadline"
          },
          {
            name: "Budget Board",
            adapter: "static",
            status: "error",
            error: "time budget exceeded (90s)",
            deadlineKind: "source_budget"
          }
        ]
      },
      {
        name: "ashby_sources",
        adapter: "ashby",
        status: "error",
        error: "timeout",
        deadlineKind: "source_budget"
      }
    ]
  });
  const metricsEl = makeEl();

  renderAdminOpsFetcherMetrics(metricsEl, { latestRun: {}, history: {} }, summary);

  const html = metricsEl.innerHTML;
  assert.match(html, /Static deadline cuts/);
  assert.match(html, /3 static sources cut by/i);
  assert.match(html, /request cap 1/i);
  assert.match(html, /page-batch deadline 1/i);
  assert.match(html, /source budget 1/i);
  assert.match(html, /Slow Board/);
  assert.match(html, /Batch Board/);
  assert.match(html, /Budget Board/);
  assert.match(html, /admin-ops-static-deadline-table/);
  const deadlineTable = html.match(/<table class="admin-table admin-ops-static-deadline-table">([\s\S]*?)<\/table>/)?.[1] || "";
  assert.ok(deadlineTable, "expected the static deadline table to render");
  assert.doesNotMatch(deadlineTable, /ashby_sources/);
});
