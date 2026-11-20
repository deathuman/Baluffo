/**
 * Admin's 7-day fetch success card when no fetch ran in the window.
 *
 * A rate of 0.0 means two different things — every run failed, or nothing ran at all —
 * and the payload's `fetchRunCount7d` is the only thing that separates them. The lazy
 * Ops controller masks the rate for an empty window and sets the pending label, so the
 * shared shell renderer needs no unmeasured branch of its own.
 */

import test from "node:test";
import assert from "node:assert/strict";
import { renderAdminOpsKpis } from "../../../frontend/admin/render.js";
import {
  buildFetchKpiPendingLabels,
  maskUnmeasuredFetchSuccessRate
} from "../../../frontend/admin/domain/ops-fetch-kpi-model.js";
import { createRenderEl as makeEl } from "./helpers/dom-test-helpers.mjs";

function renderHealth(kpis, health, kpisEl = makeEl()) {
  const labels = buildFetchKpiPendingLabels({ ...health, kpis }, false);
  renderAdminOpsKpis(kpisEl, maskUnmeasuredFetchSuccessRate(kpis), health.status || "healthy", {
    fetchKpiPendingLabel: labels.default,
    fetchKpiPendingLabels: labels
  });
  return kpisEl;
}

test("admin kpi model: only a measured empty window drops the rate for render", () => {
  const unmeasured = maskUnmeasuredFetchSuccessRate({
    sevenDayFetchSuccessRate: 0,
    fetchRunCount7d: 0
  });
  assert.equal("sevenDayFetchSuccessRate" in unmeasured, false);
  assert.equal(unmeasured.fetchRunCount7d, 0);

  const measured = maskUnmeasuredFetchSuccessRate({
    sevenDayFetchSuccessRate: 0,
    fetchRunCount7d: 2
  });
  assert.equal(measured.sevenDayFetchSuccessRate, 0);

  const withoutCount = maskUnmeasuredFetchSuccessRate({ sevenDayFetchSuccessRate: 0 });
  assert.equal(withoutCount.sevenDayFetchSuccessRate, 0);
});

test("admin render: an empty fetch window reads as not measured, not as total failure", () => {
  const health = {
    fetchKpisLoaded: true,
    kpis: {
      sevenDayFetchSuccessRate: 0,
      fetchRunCount7d: 0,
      lastSuccessfulFetchAge: "Never"
    }
  };

  const kpisEl = renderHealth(health.kpis, health);

  assert.match(kpisEl.innerHTML, /Not measured \(no fetch in the last 7 days\)/);
  assert.doesNotMatch(kpisEl.innerHTML, /0\.0%/);
});

test("admin render: zero percent from finished runs stays a measured zero", () => {
  const health = {
    fetchKpisLoaded: true,
    kpis: {
      sevenDayFetchSuccessRate: 0,
      fetchRunCount7d: 2,
      lastSuccessfulFetchAge: "2h ago"
    }
  };

  const kpisEl = renderHealth(health.kpis, health);

  assert.match(kpisEl.innerHTML, /0\.0%/);
  assert.doesNotMatch(kpisEl.innerHTML, /Not measured/);
});

test("admin render: a changed run count repaints the not-measured card", () => {
  const kpisEl = makeEl();
  kpisEl.dataset = {};
  const base = { sevenDayFetchSuccessRate: 0, lastSuccessfulFetchAge: "Never" };
  renderHealth({ ...base, fetchRunCount7d: 0 }, { fetchKpisLoaded: true }, kpisEl);
  assert.match(kpisEl.innerHTML, /Not measured/);

  renderHealth({ ...base, fetchRunCount7d: 3 }, { fetchKpisLoaded: true }, kpisEl);

  assert.match(kpisEl.innerHTML, /0\.0%/);
  assert.doesNotMatch(kpisEl.innerHTML, /Not measured/);
});
