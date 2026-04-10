import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium, request as playwrightRequest } from "@playwright/test";

const BASE_URL = process.env.PACKAGED_DESKTOP_BASE_URL || "http://127.0.0.1:8080";
const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";
const bridgeUrl = new URL(BRIDGE_BASE);
const BRIDGE_PORT = bridgeUrl.port || "8877";
const BRIDGE_HOST = bridgeUrl.hostname || "127.0.0.1";
const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".codex-tmp/packaged-desktop-smoke/jobs-pipeline-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".codex-tmp/packaged-desktop-smoke/jobs-pipeline-output");

async function writeReport(report) {
  await fs.mkdir(path.dirname(REPORT_PATH), { recursive: true });
  await fs.writeFile(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`, "utf8");
}

async function gotoDesktop(page, relativePath) {
  const separator = relativePath.includes("?") ? "&" : "?";
  await page.goto(
    `${BASE_URL}/${relativePath}${separator}desktop=1&bridgePort=${encodeURIComponent(BRIDGE_PORT)}&bridgeHost=${encodeURIComponent(BRIDGE_HOST)}`
  );
}

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

async function fetchPipelineStatus(apiRequest) {
  const response = await apiRequest.get(`${BRIDGE_BASE}/tasks/run-jobs-pipeline-status`);
  assert.equal(response.ok(), true, "jobs pipeline status request should succeed");
  return response.json();
}

async function waitForPipelineRunStart(apiRequest, timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const payload = await fetchPipelineStatus(apiRequest);
    if (payload?.active && String(payload?.runId || "").trim()) {
      return String(payload.runId);
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  throw new Error("Jobs pipeline did not start within the allotted time.");
}

async function main() {
  const scenarios = [];
  const errors = [];
  let browser;
  let context;
  let page;
  let apiRequest;
  let adminNavigationDetected = false;
  const pageErrors = [];
  try {
    browser = await chromium.launch({ headless: process.env.PACKAGED_SMOKE_HEADED !== "1" });
    context = await browser.newContext({ baseURL: BASE_URL, acceptDownloads: true });
    page = await context.newPage();
    page.on("pageerror", error => pageErrors.push(String(error?.message || error)));
    apiRequest = await playwrightRequest.newContext({ baseURL: BRIDGE_BASE });
    await fs.mkdir(OUTPUT_DIR, { recursive: true });
    page.on("framenavigated", frame => {
      if (frame === page.mainFrame() && /admin\.html/i.test(frame.url())) {
        adminNavigationDetected = true;
      }
    });

    const jobsStartup = {
      name: "Jobs startup without Admin navigation",
      slug: "jobs-startup-without-admin-navigation",
      status: "passed",
      durationMs: 0,
      error: ""
    };
    const pipelineRun = {
      name: "Jobs pipeline launches without Admin navigation",
      slug: "jobs-pipeline-launches-without-admin-navigation",
      status: "passed",
      durationMs: 0,
      error: ""
    };

    const jobsStartedAt = Date.now();
    try {
      await gotoDesktop(page, "jobs.html");
      await waitForDesktopAdapter(page);
      await waitForJobsPageReady(page);
      assert.equal(adminNavigationDetected, false, "jobs startup must not navigate to admin.html");
    } catch (error) {
      jobsStartup.status = "failed";
      jobsStartup.error = error instanceof Error ? error.message : String(error);
      throw error;
    } finally {
      jobsStartup.durationMs = Date.now() - jobsStartedAt;
      scenarios.push(jobsStartup);
    }

    const pipelineStartedAt = Date.now();
    try {
      const pipelineButton = page.locator("#jobs-pipeline-run-btn");
      await pipelineButton.click();
      await page.waitForFunction(
        () => /running/i.test(String(document.querySelector("#jobs-pipeline-run-btn")?.textContent || "")),
        null,
        { timeout: 30_000 }
      );
      const runId = await waitForPipelineRunStart(apiRequest);
      assert.match(runId, /^pipeline_[a-f0-9]{10}$/i, "jobs pipeline run id should look like a pipeline run");
      await page.waitForTimeout(15_000);
      assert.equal(pageErrors.length, 0, `unexpected jobs page errors: ${pageErrors.join("; ")}`);
      assert.equal(adminNavigationDetected, false, "jobs pipeline must not navigate to admin.html");
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
