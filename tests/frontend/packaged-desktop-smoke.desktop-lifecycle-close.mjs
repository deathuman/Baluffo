import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import fs from "node:fs/promises";
import path from "node:path";
import { promisify } from "node:util";
import { chromium } from "@playwright/test";
import { waitUntil } from "./helpers/packaged-first-run-smoke-helpers.mjs";
import { buildWriteReport, BASE_URL } from "./helpers/packaged-smoke-shared.mjs";

const CDP_PORT = Number(process.env.BALUFFO_PACKAGED_SMOKE_CDP_PORT || 0);
const BROWSER_PID = Number(process.env.BALUFFO_PACKAGED_SMOKE_BROWSER_PID || 0);
const execFileAsync = promisify(execFile);
const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/desktop-lifecycle-close-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/desktop-lifecycle-close-output");

const writeReport = buildWriteReport(REPORT_PATH);

function allPages(browser) {
  return browser.contexts().flatMap(context => context.pages());
}

function browserClosed(browser, page) {
  if (page.isClosed()) {
    return true;
  }
  if (typeof browser.isConnected === "function" && !browser.isConnected()) {
    return true;
  }
  try {
    return allPages(browser).every(candidate => candidate.isClosed());
  } catch {
    return true;
  }
}

async function managedPage(browser) {
  return waitUntil("managed Chromium page", () => {
    const pages = allPages(browser).filter(page => !page.isClosed());
    return pages.find(page => String(page.url() || "").startsWith(BASE_URL)) || pages[0] || null;
  }, 15_000);
}

async function connectManagedBrowser() {
  return waitUntil("managed Chromium CDP endpoint", async () => {
    try {
      return await chromium.connectOverCDP(`http://127.0.0.1:${CDP_PORT}`);
    } catch {
      return null;
    }
  }, 20_000, 500);
}

async function closeMainWindow(pid) {
  assert.ok(pid > 0, "desktop lifecycle close smoke requires BALUFFO_PACKAGED_SMOKE_BROWSER_PID");
  const script = [
    "$ErrorActionPreference = 'Stop'",
    `$process = [System.Diagnostics.Process]::GetProcessById(${Math.trunc(pid)})`,
    "if (-not $process.CloseMainWindow()) { throw 'CloseMainWindow returned false' }"
  ].join("; ");
  await execFileAsync("powershell.exe", [
    "-NoProfile",
    "-ExecutionPolicy",
    "Bypass",
    "-Command",
    script
  ], {
    timeout: 10_000,
    windowsHide: true
  });
}

async function main() {
  const report = {
    ok: false,
    scenarios: [],
    errors: [],
    startedAt: new Date().toISOString(),
    finishedAt: ""
  };
  let browser;
  try {
    await fs.mkdir(OUTPUT_DIR, { recursive: true });
    assert.ok(CDP_PORT > 0, "desktop lifecycle close smoke requires BALUFFO_PACKAGED_SMOKE_CDP_PORT");
    browser = await connectManagedBrowser();
    const page = await managedPage(browser);
    await page.waitForFunction(() => Boolean(window.JobAppLocalData), null, { timeout: 30_000 });
    await page.waitForFunction(
      () => Boolean(window.__baluffoDesktopLifecycleBound),
      null,
      { timeout: 30_000 }
    );
    await new Promise(resolve => setTimeout(resolve, 250));

    let dialogSeen = false;
    let dialogMessage = "";
    page.once("dialog", async dialog => {
      dialogSeen = true;
      dialogMessage = String(dialog.message() || "");
      await dialog.accept();
    });
    const lifecycleResult = await page.evaluate(() => {
      const beforeUnload = new Event("beforeunload", { cancelable: true });
      const beforeUnloadAllowed = window.dispatchEvent(beforeUnload);
      const pagehide = typeof PageTransitionEvent === "function"
        ? new PageTransitionEvent("pagehide", { persisted: false })
        : new Event("pagehide");
      window.dispatchEvent(pagehide);
      return {
        beforeUnloadAllowed,
        beforeUnloadDefaultPrevented: Boolean(beforeUnload.defaultPrevented),
        beforeUnloadReturnValue: String(beforeUnload.returnValue || "")
      };
    });
    assert.equal(
      lifecycleResult.beforeUnloadDefaultPrevented,
      false,
      "regular desktop lifecycle close should not be blocked as active work"
    );
    assert.equal(
      lifecycleResult.beforeUnloadAllowed,
      true,
      "regular desktop lifecycle close should allow unload"
    );
    await new Promise(resolve => setTimeout(resolve, 250));

    const closeMethod = "synthetic_lifecycle_windows_close_main_window";
    await closeMainWindow(BROWSER_PID);
    await waitUntil(
      "regular desktop browser close",
      () => browserClosed(browser, page),
      10_000,
      100
    );
    assert.equal(dialogSeen, false, "regular desktop close should not show active-work confirmation");
    report.scenarios.push({
      name: "Regular desktop close sends lifecycle shutdown intent",
      slug: "regular-desktop-close-lifecycle-intent",
      status: "passed",
      durationMs: 0,
      error: "",
      closeMethod,
      lifecycleResult,
      dialogSeen,
      dialogMessage
    });
    report.ok = true;
  } catch (error) {
    report.errors.push(String(error?.stack || error?.message || error));
    report.scenarios.push({
      name: "Regular desktop close sends lifecycle shutdown intent",
      slug: "regular-desktop-close-lifecycle-intent",
      status: "failed",
      durationMs: 0,
      error: String(error?.message || error)
    });
  } finally {
    report.finishedAt = new Date().toISOString();
    await writeReport(report);
    if (browser?.disconnect) {
      await browser.disconnect().catch(() => {});
    }
  }
  if (!report.ok) {
    process.exitCode = 1;
  }
}

await main();
