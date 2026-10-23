// Why this lane exists: the main packaged smoke now fails when the desktop
// update panel records `jobs_desktop_update_panel_state_diverged`, but a gate
// that is only ever observed passing can rot — an assertion narrowed, a helper
// short-circuited, or a metric event renamed would all keep the release lane
// green while the defect class walked straight through. This lane injects a
// real divergence metric over the same browser -> bridge startup-metric route
// the controller's diagnostics use, and then requires the gate to reject it. A
// green run of this lane is the evidence the gate still trips, because the
// rejection itself is asserted.

import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium, request as playwrightRequest } from "@playwright/test";
import {
  assertNoDesktopUpdatePanelDivergence,
  buildGotoDesktop,
  buildWriteReport,
  readDesktopUpdateClickDiagnostics,
  runScenario,
  BASE_URL,
  BRIDGE_BASE
} from "./helpers/packaged-smoke-shared.mjs";

const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/update-panel-gate-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/update-panel-gate-output");
const HEADED = process.env.PACKAGED_SMOKE_HEADED === "1";
const PAUSE_ON_FAILURE = process.env.PACKAGED_SMOKE_PAUSE_ON_FAILURE === "1";
const FAULT_REASON = "fault_injection";
const DIVERGED_EVENT = "jobs_desktop_update_panel_state_diverged";
const writeReport = buildWriteReport(REPORT_PATH);
const gotoDesktop = buildGotoDesktop();

async function waitForDesktopAdapter(page) {
  await page.waitForFunction(() => Boolean(window.JobAppLocalData), null, { timeout: 30_000 });
}

async function postFaultDivergenceMetric(page) {
  return page.evaluate(async ({ base, reason }) => {
    // Same single-row route `probes/startup-probe.js` posts diagnostics to, so
    // the injected row lands in the journal exactly where the controller's own
    // divergence record would.
    const response = await fetch(`${base}/desktop-local-data/startup-metric?t=${Date.now()}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        event: "jobs_desktop_update_panel_state_diverged",
        payload: {
          reason,
          availability: "checking",
          panelOpen: true,
          panelHidden: true,
          injectedBy: "packaged-desktop-smoke.update-panel-gate"
        }
      })
    });
    return { ok: response.ok, status: response.status, body: (await response.text()).slice(0, 400) };
  }, { base: BRIDGE_BASE, reason: FAULT_REASON });
}

async function main() {
  const scenarios = [];
  const errors = [];
  let browser;
  let context;
  let page;
  let apiRequest;
  try {
    await fs.mkdir(OUTPUT_DIR, { recursive: true });
    browser = await chromium.launch({ headless: !HEADED, ...(process.env.PACKAGED_SMOKE_SYSTEM_CHROMIUM === "1" ? { channel: "chromium" } : {}) });
    context = await browser.newContext({ baseURL: BASE_URL });
    page = await context.newPage();
    apiRequest = await playwrightRequest.newContext({ baseURL: BRIDGE_BASE });

    await runScenario("Injected update-panel divergence trips the release gate", async () => {
      await gotoDesktop(page, "jobs.html");
      await waitForDesktopAdapter(page);
      await page.locator("#jobs-list").waitFor({ state: "visible", timeout: 30_000 });

      // Control: the real page must be coherent before the injection, so a
      // rejection below can only be the injected row. This also proves the
      // recorder installed, which is what the gate requires as evidence.
      await assertNoDesktopUpdatePanelDivergence(page, { timeoutMs: 15_000 });
      const before = await readDesktopUpdateClickDiagnostics(page, 400);
      assert.equal(
        Array.isArray(before),
        true,
        "desktop update click diagnostics should be readable before injecting the fault"
      );
      assert.deepEqual(
        before.filter(row => row.event === DIVERGED_EVENT).map(row => row.event),
        [],
        "the page must not record a divergence before the fault is injected"
      );

      const injected = await postFaultDivergenceMetric(page);
      assert.equal(injected.ok, true, `startup metrics batch should accept the fault row: ${JSON.stringify(injected)}`);

      await assert.rejects(
        () => assertNoDesktopUpdatePanelDivergence(page, { timeoutMs: 5_000 }),
        error => {
          assert.match(error.message, /reported itself open while it rendered hidden/);
          assert.match(error.message, new RegExp(DIVERGED_EVENT));
          assert.match(error.message, new RegExp(`"reason":"${FAULT_REASON}"`));
          assert.match(error.message, /"panelHidden":true/);
          return true;
        },
        "the divergence gate accepted an injected divergence metric, so the release smoke can no longer catch this regression"
      );

      const after = await readDesktopUpdateClickDiagnostics(page, 400);
      assert.equal(
        Array.isArray(after) && after.some(row => row.event === DIVERGED_EVENT && row.reason === FAULT_REASON),
        true,
        "the injected divergence metric should be visible through the same reader the release gate uses"
      );

      return { gateTripped: true, injectedEvents: 1, faultReason: FAULT_REASON };
    }, scenarios);
  } catch (error) {
    errors.push(error instanceof Error ? error.message : String(error));
  } finally {
    if (PAUSE_ON_FAILURE && errors.length > 0 && page) {
      await page.pause();
    }
    await apiRequest?.dispose().catch(() => {});
    await context?.close().catch(() => {});
    await browser?.close().catch(() => {});
  }

  const report = {
    ok: errors.length === 0 && scenarios.every(scenario => scenario.status === "passed"),
    scenarios,
    errors
  };
  await writeReport(report);
  if (!report.ok) {
    console.error("Update-panel gate fault-injection lane failed:", report.errors);
    process.exitCode = 1;
  }
}

main().catch(async error => {
  await writeReport({
    ok: false,
    scenarios: [],
    errors: [error instanceof Error ? error.message : String(error)]
  });
  console.error(error);
  process.exitCode = 1;
});
