/**
 * Admin's 7-day fetch success card when no fetch ran in the window.
 *
 * A rate of 0.0 means two different things — every run failed, or nothing ran at all —
 * and the payload's `fetchRunCount7d` is the only thing that separates them. These pin
 * the operator-facing reading of the empty case. The shared KPI renderer's other cards
 * and its patch-in-place behavior live in `admin-render.test.mjs`.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsKpis } from "../../../frontend/admin/render.js";
import { createRenderEl as makeEl } from "./helpers/dom-test-helpers.mjs";

test("admin render: an empty fetch window reads as not measured, not as total failure", () => {
  const kpisEl = makeEl();
  renderAdminOpsKpis(kpisEl, {
    sevenDayFetchSuccessRate: 0,
    fetchRunCount7d: 0,
    lastSuccessfulFetchAge: "Never"
  }, "healthy");

  assert.match(kpisEl.innerHTML, /Not measured/);
  assert.match(kpisEl.innerHTML, /no fetch finished in the last 7 days/);
  assert.doesNotMatch(kpisEl.innerHTML, /0\.0%/);
});

test("admin render: zero percent from finished runs stays a measured zero", () => {
  const kpisEl = makeEl();
  renderAdminOpsKpis(kpisEl, {
    sevenDayFetchSuccessRate: 0,
    fetchRunCount7d: 2,
    lastSuccessfulFetchAge: "2h ago"
  }, "warning");

  assert.match(kpisEl.innerHTML, /0\.0%/);
  assert.doesNotMatch(kpisEl.innerHTML, /Not measured/);
});

test("admin render: a changed run count repaints the not-measured card", () => {
  // The patch-in-place signature previously keyed off the rate alone, so a window
  // that went from no runs to all-failed runs would keep the stale "Not measured".
  const kpisEl = makeEl();
  kpisEl.dataset = {};
  const base = { sevenDayFetchSuccessRate: 0, lastSuccessfulFetchAge: "Never" };
  renderAdminOpsKpis(kpisEl, { ...base, fetchRunCount7d: 0 }, "healthy");
  assert.match(kpisEl.innerHTML, /Not measured/);

  renderAdminOpsKpis(kpisEl, { ...base, fetchRunCount7d: 3 }, "healthy");

  assert.match(kpisEl.innerHTML, /0\.0%/);
  assert.doesNotMatch(kpisEl.innerHTML, /Not measured/);
});
