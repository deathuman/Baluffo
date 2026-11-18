// Why this lane exists: a completed pipeline row used to refuse the Jobs page's
// rebuild request even when the feed it produced was gone, so the page dead-ended
// on "Local jobs feed is missing or unreadable". The fixture seeds exactly that
// state, so a regression fails on `started: true` instead of passing because some
// feed was already servable.

import assert from "node:assert/strict";
import path from "node:path";
import { chromium, request as playwrightRequest } from "@playwright/test";
import {
  coverageScope,
  fetchBridgeJson as fetchBridgeJsonWithBase,
  waitUntil
} from "./helpers/packaged-first-run-smoke-helpers.mjs";
import {
  buildGotoDesktop,
  buildWriteReport,
  runScenario,
  BASE_URL,
  BRIDGE_BASE
} from "./helpers/packaged-smoke-shared.mjs";

const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/jobs-feed-recovery-report.json");
const HEADED = process.env.PACKAGED_SMOKE_HEADED === "1";
const SCENARIO_NAME = "Packaged Jobs feed recovery after a completed pipeline";
const PROMOTED_TITLE = "Packaged First-Run Technical Cinematic Animator";
const FEED_MISSING_TEXT = /Local jobs feed is missing or unreadable/i;
const RECOVERY_STATUS_TEXT = /Refreshing first-run sheet jobs/i;

const writeReport = buildWriteReport(REPORT_PATH);
const gotoDesktop = buildGotoDesktop();

async function fetchBridgeJson(apiRequest, relativePath, label) {
  return fetchBridgeJsonWithBase(apiRequest, BRIDGE_BASE, relativePath, label);
}

function isBootstrapRequest(request) {
  try {
    const url = new URL(request.url());
    return request.method() === "POST" && url.pathname === "/tasks/run-jobs-bootstrap";
  } catch {
    return false;
  }
}

// A feed the static site can already serve would let the page render rows and
// never ask for a rebuild, turning the regression into a green run.
async function assertFixtureHasNoServableFeed(apiRequest) {
  for (const name of ["jobs-unified-light.json", "jobs-unified.json"]) {
    const response = await apiRequest.get(`${BASE_URL}/data/${name}?feedRecovery=${Date.now()}`);
    if (!response.ok()) continue;
    const rows = JSON.parse((await response.text()) || "[]");
    assert.equal(
      Array.isArray(rows) ? rows.length : 0,
      0,
      `${name} already holds rows before the recovery bootstrap, so this lane proves nothing`
    );
  }
}

async function assertBootstrapStarted(request) {
  const body = JSON.parse(request.postData() || "{}");
  assert.deepEqual(
    body,
    { source: "jobs_first_run" },
    "Jobs UI recovery bootstrap must not force duplicate refreshes"
  );
  const response = await request.response();
  const payload = await response.json();
  // The admission regression answered alreadyCompleted /
  // full_pipeline_already_completed here with a 409, and the page then
  // dead-ended on the missing-feed error with no repair ever queued.
  assert.equal(
    payload?.started,
    true,
    "a missing feed must start a rebuild even after a completed pipeline row, but "
      + `the bridge answered ${response.status()} ${JSON.stringify(payload)}`
  );
  assert.equal(
    payload?.smokeMode,
    "controlled-heartbeat-success",
    "bootstrap route should expose the deterministic smoke mode"
  );
  assert.equal(payload?.coverageScope, "bootstrap_sheets");
  assert.match(String(payload?.runId || ""), /^jobs_bootstrap_[a-f0-9]{10}$/i);
  return payload;
}

async function assertNoDeadEndWhileRebuilding(page, durationMs = 3_000) {
  await waitUntil(
    "Jobs recovery status",
    async () => {
      const status = String((await page.locator("#source-status").textContent()) || "");
      return RECOVERY_STATUS_TEXT.test(status) ? status : null;
    },
    10_000
  );
  const deadline = Date.now() + durationMs;
  do {
    const bodyText = String((await page.locator("body").innerText({ timeout: 10_000 })) || "");
    assert.doesNotMatch(
      bodyText,
      FEED_MISSING_TEXT,
      "Jobs page must not dead-end on the missing-feed error while the rebuild runs"
    );
    await new Promise(resolve => setTimeout(resolve, 250));
  } while (Date.now() < deadline);
}

async function waitForPromotedFeed(page, apiRequest, runId) {
  const report = await waitUntil(
    "terminal recovery bootstrap report",
    async () => {
      const payload = await fetchBridgeJson(apiRequest, "/ops/fetch-report", "fetch report");
      if (
        String(payload?.runId || "") === runId
        && String(payload?.finishedAt || "").trim()
        && coverageScope(payload) === "bootstrap_sheets"
        && Number(payload?.summary?.outputCount || 0) > 0
      ) {
        return payload;
      }
      return null;
    },
    60_000,
    500
  );
  await page.waitForFunction(
    title => String(document.querySelector("#jobs-list")?.innerText || "").includes(title),
    PROMOTED_TITLE,
    { timeout: 30_000 }
  );
  const bodyText = String((await page.locator("body").innerText({ timeout: 10_000 })) || "");
  assert.doesNotMatch(
    bodyText,
    FEED_MISSING_TEXT,
    "the promoted recovery feed must replace the missing-feed error"
  );
  const response = await apiRequest.get(
    `${BASE_URL}/data/jobs-unified-light.json?feedRecovery=${encodeURIComponent(runId)}`
  );
  assert.equal(response.ok(), true, "the rebuilt feed should be served from static JSON");
  const rows = await response.json();
  assert.equal(rows?.length, 1, "the rebuilt feed should hold exactly the promoted bootstrap row");
  assert.equal(rows?.[0]?.title, PROMOTED_TITLE);
  return report;
}

async function main() {
  const scenarios = [];
  const errors = [];
  let browser;
  let context;
  let apiRequest;
  try {
    apiRequest = await playwrightRequest.newContext();
    await assertFixtureHasNoServableFeed(apiRequest);
    browser = await chromium.launch({
      headless: !HEADED,
      ...(process.env.PACKAGED_SMOKE_SYSTEM_CHROMIUM === "1" ? { channel: "chromium" } : {})
    });
    context = await browser.newContext({ viewport: { width: 1280, height: 860 } });
    const page = await context.newPage();
    const bootstrapRequests = [];
    page.on("request", request => {
      if (isBootstrapRequest(request)) bootstrapRequests.push(request);
    });
    await runScenario(
      SCENARIO_NAME,
      async () => {
        await gotoDesktop(page, "jobs.html");
        const request = await waitUntil(
          "Jobs UI recovery bootstrap request",
          async () => bootstrapRequests[0] || null,
          20_000
        );
        const startPayload = await assertBootstrapStarted(request);
        const runId = String(startPayload.runId || "");
        await assertNoDeadEndWhileRebuilding(page);
        const terminalReport = await waitForPromotedFeed(page, apiRequest, runId);
        return {
          runId,
          terminalOutputCount: Number(terminalReport?.summary?.outputCount || 0)
        };
      },
      scenarios
    );
  } catch (error) {
    errors.push(String(error?.stack || error?.message || error));
  } finally {
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
    process.exitCode = 1;
  }
}

await main();
