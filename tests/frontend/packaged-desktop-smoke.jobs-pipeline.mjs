import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium, request as playwrightRequest } from "@playwright/test";
import {
  dismissJobsFirstRunNotice,
  runJobsMainButtonAbortScenario,
  waitForBridgeTasksIdleWithBootstrapCleanup
} from "./packaged-desktop-smoke.jobs-pipeline-abort-helpers.mjs";
import {
  buildGotoDesktop,
  buildWriteReport,
  BASE_URL,
  BRIDGE_BASE,
  BRIDGE_PORT,
  BRIDGE_HOST
} from "./helpers/packaged-smoke-shared.mjs";
const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/jobs-pipeline-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/jobs-pipeline-output");
const BRIDGE_REQUEST_RETRY_TIMEOUT_MS = 30_000;
const BRIDGE_REQUEST_RETRY_INTERVAL_MS = 500;

const writeReport = buildWriteReport(REPORT_PATH);
const gotoDesktop = buildGotoDesktop();

async function waitForDesktopAdapter(page) {
  await page.waitForFunction(() => Boolean(window.JobAppLocalData), null, { timeout: 30_000 });
}
async function waitForJobsPageReady(page) {
  await page.waitForFunction(() => {
    const state = document.body?.getAttribute("data-jobs-startup-state") || "loading";
    return state === "interactive" || state === "error";
  }, null, { timeout: 30_000 });
  await page.locator("#jobs-list").waitFor({ state: "visible", timeout: 30_000 });
  await page.locator("#jobs-pipeline-run-btn").waitFor({ state: "visible", timeout: 30_000 });
  await page.waitForFunction(
    () => Boolean(document.querySelector("#jobs-pipeline-run-btn")) && !document.querySelector("#jobs-pipeline-run-btn").disabled,
    null,
    { timeout: 30_000 }
  );
  assert.match(
    String(await page.locator("#jobs-list").textContent() || ""),
    /\S/,
    "jobs list should contain rendered content"
  );
}
function isRetryableBridgeRequestError(error) {
  return /ECONNREFUSED|ECONNRESET|ECONNABORTED|ETIMEDOUT|socket hang up/i.test(
    String(error?.message || error || "")
  );
}
async function bridgeRequestWithRetry(apiRequest, method, url, options = {}) {
  const deadline = Date.now() + BRIDGE_REQUEST_RETRY_TIMEOUT_MS;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      return await apiRequest[method](url, options);
    } catch (error) {
      lastError = error;
      if (!isRetryableBridgeRequestError(error)) {
        throw error;
      }
      await new Promise(resolve => setTimeout(resolve, BRIDGE_REQUEST_RETRY_INTERVAL_MS));
    }
  }
  throw lastError || new Error(`Bridge ${method.toUpperCase()} request timed out: ${url}`);
}
async function fetchPipelineStatus(apiRequest) {
  const response = await bridgeRequestWithRetry(
    apiRequest,
    "get",
    `${BRIDGE_BASE}/tasks/run-jobs-pipeline-status`
  );
  assert.equal(response.ok(), true, "jobs pipeline status request should succeed");
  return response.json();
}
async function fetchBridgeJson(apiRequest, relativePath, label) {
  const response = await bridgeRequestWithRetry(apiRequest, "get", `${BRIDGE_BASE}${relativePath}`);
  assert.equal(response.ok(), true, `${label} request should succeed`);
  return response.json();
}
async function postBridgeJson(apiRequest, relativePath, data, label) {
  const response = await bridgeRequestWithRetry(
    apiRequest,
    "post",
    `${BRIDGE_BASE}${relativePath}`,
    { data }
  );
  assert.equal(response.ok(), true, `${label} request should succeed`);
  return response.json();
}
async function waitForPipelineRunStart(apiRequest, timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const payload = await fetchPipelineStatus(apiRequest);
    const runId = String(payload?.runId || "").trim();
    const stage = String(payload?.stage || "").trim().toLowerCase();
    if ((payload?.active && runId) || (runId && stage && stage !== "idle")) {
      return payload;
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  throw new Error("Jobs pipeline did not start within the allotted time.");
}

async function waitForPipelineRunTerminal(apiRequest, runId, timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const payload = await fetchPipelineStatus(apiRequest);
    const currentRunId = String(payload?.runId || "").trim();
    const stage = String(payload?.stage || "").trim().toLowerCase();
    const error = String(payload?.error || "").trim();
    if (currentRunId && currentRunId === runId) {
      if (error || stage === "error") {
        throw new Error(`Jobs pipeline entered error state: ${error || stage}`);
      }
      if (!payload?.active && stage && stage !== "starting") {
        return payload;
      }
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  throw new Error("Jobs pipeline did not reach a terminal non-error state within the allotted time.");
}

async function waitForPipelineButtonBusyState(pipelineButton, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const buttonState = await pipelineButton.evaluate(el => ({
      mode: String(el.dataset.progressMode || ""),
      fill: String(el.dataset.progressFill || ""),
      fillWidth: String(el.querySelector(".jobs-pipeline-btn-fill")?.style.width || ""),
      fillOpacity: String(el.querySelector(".jobs-pipeline-btn-fill")?.style.opacity || ""),
      fillMode: String(el.querySelector(".jobs-pipeline-btn-fill")?.dataset.progressMode || ""),
      label: String(el.textContent || ""),
      ariaBusy: String(el.getAttribute("aria-busy") || "")
    }));
    if (buttonState.ariaBusy === "true" && buttonState.mode && buttonState.fillOpacity !== "0") {
      return buttonState;
    }
    await new Promise(resolve => setTimeout(resolve, 250));
  }
  throw new Error("Jobs pipeline button never entered a visible busy/progress state.");
}

async function assertPackagedSourceRunsParity(apiRequest) {
  await waitForBridgeTasksIdleWithBootstrapCleanup(apiRequest);
  const startPayload = await postBridgeJson(
    apiRequest,
    "/tasks/run-fetcher",
    { preset: "default", quiet: true, socialEnabled: false },
    "packaged source-runs fetch"
  );
  assert.equal(Boolean(startPayload?.started), true, "packaged source-runs fetch should start");
  assert.equal(startPayload?.smokeMode, "source-runs", "packaged fetch should use source-runs smoke mode");
  const runId = String(startPayload?.runId || "").trim();
  assert.match(runId, /^fetch_[a-f0-9]{10}$/i, "source-runs fetch id should look like a fetch run");

  const storageHealth = await fetchBridgeJson(apiRequest, "/ops/storage-health", "storage health");
  assert.equal(
    storageHealth?.storage?.authorityModes?.sourceRuns,
    "sqlite",
    "packaged storage health should show sourceRuns=sqlite"
  );
  assert.equal(
    storageHealth?.storage?.authorityModes?.jobsFeed,
    "sqlite",
    "packaged storage health should show jobsFeed=sqlite"
  );
  const jobsDiagnostics = Array.isArray(storageHealth?.storage?.diagnostics)
    ? storageHealth.storage.diagnostics.filter(row => row?.surface === "jobsFeed")
    : [];
  assert.ok(
    jobsDiagnostics.some(row => row?.ok === true && row?.code === "jobs_feed_projection_match"),
    "storage diagnostics should include a passing jobs-feed parity diagnostic"
  );

  const fetchReport = await fetchBridgeJson(apiRequest, "/ops/fetch-report", "fetch report");
  assert.equal(fetchReport?.runId, runId, "fetch report should belong to the packaged source-runs fetch");
  assert.match(
    String(fetchReport?.sourceRuns?.sourceDetailsArchive?.path || ""),
    /source-details\.json\.gz$/,
    "compact report should reference archived source details"
  );
  assert.equal(fetchReport?.sources?.[0]?.name, "Packaged Smoke Source");
  assert.equal(
    fetchReport?.sources?.[0]?.details?.[0]?.name,
    "Packaged Smoke Job",
    "fetch report should hydrate normalized source details from SQLite"
  );

  const feedResponse = await apiRequest.get(
    `${BASE_URL}/data/jobs-unified-light.json?m5=${encodeURIComponent(runId)}`
  );
  assert.equal(feedResponse.ok(), true, "sqlite-exported jobs feed should be served as static JSON");
  const feedRows = await feedResponse.json();
  assert.equal(feedRows?.[0]?.title, "Packaged Smoke Job");
  assert.equal(feedRows?.[0]?.company, "Packaged Smoke Studio");
}

async function main() {
  const scenarios = [];
  const errors = [];
  let browser;
  let context;
  let page;
  let apiRequest;
  const pageErrors = [];
  try {
    browser = await chromium.launch({ headless: process.env.PACKAGED_SMOKE_HEADED !== "1", ...(process.env.PACKAGED_SMOKE_SYSTEM_CHROMIUM === "1" ? { channel: "chromium" } : {}) });
    context = await browser.newContext({ baseURL: BASE_URL, acceptDownloads: true });
    page = await context.newPage();
    page.on("pageerror", error => pageErrors.push(String(error?.message || error)));
    page.on("dialog", dialog => {
      const message = String(dialog.message?.() || "");
      const action = /Abort the current job update\?/i.test(message)
        ? dialog.accept()
        : dialog.dismiss();
      action.catch(() => {});
    });
    apiRequest = await playwrightRequest.newContext({ baseURL: BRIDGE_BASE });
    await fs.mkdir(OUTPUT_DIR, { recursive: true });

    const jobsStartup = {
      name: "Jobs startup and pipeline button progress",
      slug: "jobs-startup-and-pipeline-button-progress",
      status: "passed",
      durationMs: 0,
      error: ""
    };
    const pipelineRun = {
      name: "Jobs pipeline button fills while running",
      slug: "jobs-pipeline-button-fills-while-running",
      status: "passed",
      durationMs: 0,
      error: ""
    };
    const jobsButtonAbort = {
      name: "Jobs main pipeline button aborts active update",
      slug: "jobs-main-pipeline-button-abort-active-update",
      status: "passed",
      durationMs: 0,
      error: ""
    };
    const sourceRunsParity = {
      name: "Packaged fetch source-runs SQLite parity",
      slug: "packaged-fetch-source-runs-sqlite-parity",
      status: "passed",
      durationMs: 0,
      error: ""
    };

    const jobsStartedAt = Date.now();
    try {
      await gotoDesktop(page, "jobs.html");
      await waitForDesktopAdapter(page);
      await waitForJobsPageReady(page);
    } catch (error) {
      jobsStartup.status = "failed";
      jobsStartup.error = error instanceof Error ? error.message : String(error);
      throw error;
    } finally {
      jobsStartup.durationMs = Date.now() - jobsStartedAt;
      scenarios.push(jobsStartup);
    }

    const sourceRunsStartedAt = Date.now();
    try {
      await assertPackagedSourceRunsParity(apiRequest);
    } catch (error) {
      sourceRunsParity.status = "failed";
      sourceRunsParity.error = error instanceof Error ? error.message : String(error);
      throw error;
    } finally {
      sourceRunsParity.durationMs = Date.now() - sourceRunsStartedAt;
      scenarios.push(sourceRunsParity);
    }

    const jobsAbortStartedAt = Date.now();
    try {
      Object.assign(jobsButtonAbort, await runJobsMainButtonAbortScenario(apiRequest, page));
      assert.equal(pageErrors.length, 0, `unexpected jobs page errors: ${pageErrors.join("; ")}`);
    } catch (error) {
      jobsButtonAbort.status = "failed";
      jobsButtonAbort.error = error instanceof Error ? error.message : String(error);
      throw error;
    } finally {
      jobsButtonAbort.durationMs = Date.now() - jobsAbortStartedAt;
      scenarios.push(jobsButtonAbort);
    }

    const pipelineStartedAt = Date.now();
    try {
      const pipelineButton = page.locator("#jobs-pipeline-run-btn");
      await waitForBridgeTasksIdleWithBootstrapCleanup(apiRequest);
      await dismissJobsFirstRunNotice(page);
      await page.waitForFunction(
        () => {
          const button = document.querySelector("#jobs-pipeline-run-btn");
          const idleLabel = String(button?.dataset.idleLabel || "Run Discovery + Fetch + Sync");
          return Boolean(button)
            && !button.disabled
            && String(button.textContent || "").trim() === idleLabel
            && button.getAttribute("aria-busy") !== "true";
        },
        null,
        { timeout: 30_000 }
      );
      await pipelineButton.click();
      const startedPayload = await waitForPipelineRunStart(apiRequest);
      const runId = String(startedPayload?.runId || "");
      assert.match(runId, /^pipeline_[a-f0-9]{10}$/i, "jobs pipeline run id should look like a pipeline run");
      const buttonState = await waitForPipelineButtonBusyState(pipelineButton);
      assert.ok(buttonState.mode === "determinate" || buttonState.mode === "indeterminate", "pipeline button should show a progress mode");
      assert.match(buttonState.label, /(updating|checking|fetching)/i);
      assert.equal(buttonState.ariaBusy, "true");
      assert.ok(buttonState.fillMode === buttonState.mode || !buttonState.fillMode, "pipeline fill should track the button mode");
      assert.notEqual(buttonState.fillOpacity, "0", "pipeline fill should be visible");
      if (buttonState.mode === "determinate") {
        assert.match(buttonState.fill, /^\d+$/);
        assert.ok(Number(buttonState.fill) > 0, "determinate fill should be greater than zero");
        assert.match(buttonState.fillWidth, /%$/);
        assert.notEqual(buttonState.fillWidth, "0%", "determinate fill should be visible");
      } else {
        assert.match(buttonState.fillWidth, /%$/);
        assert.notEqual(buttonState.fillWidth, "0%", "indeterminate fill should be visible");
      }
      const terminalPayload = await waitForPipelineRunTerminal(apiRequest, runId);
      assert.equal(Boolean(terminalPayload?.active), false, "pipeline should finish in smoke mode");
      assert.notEqual(String(terminalPayload?.stage || "").trim().toLowerCase(), "error");
      assert.equal(String(terminalPayload?.error || "").trim(), "");
      assert.equal(pageErrors.length, 0, `unexpected jobs page errors: ${pageErrors.join("; ")}`);
      assert.match(
        String(page.url() || ""),
        /jobs\.html/i,
        "jobs pipeline should keep the browser on jobs.html"
      );
    } catch (error) {
      pipelineRun.status = "failed";
      pipelineRun.error = error instanceof Error ? error.message : String(error);
      throw error;
    } finally {
      pipelineRun.durationMs = Date.now() - pipelineStartedAt;
      scenarios.push(pipelineRun);
    }
  } catch (error) {
    errors.push(error instanceof Error ? error.message : String(error));
  } finally {
    await apiRequest?.dispose().catch(() => {});
    await context?.close().catch(() => {});
    await browser?.close().catch(() => {});
  }

  const report = {
    ok: errors.length === 0 && scenarios.every(scenario => scenario.status === "passed"),
    scenarios,
    errors,
    artifacts: {
      reportPath: REPORT_PATH,
      outputDir: OUTPUT_DIR
    }
  };
  await writeReport(report);
  if (!report.ok) {
    console.error("Smoke test failed:", report.errors);
  }
  process.exit(report.ok ? 0 : 1);
}

await main().catch(err => {
  console.error("Fatal error in main():", err);
  process.exit(1);
});
