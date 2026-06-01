import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { chromium, request as playwrightRequest } from "@playwright/test";

const BASE_URL = process.env.PACKAGED_DESKTOP_BASE_URL || "http://127.0.0.1:8080";
const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";
const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/desktop-lifecycle-smoke-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/desktop-lifecycle-output");
const OWNER_IDLE_TIMEOUT_S = Math.max(
  3,
  Number(process.env.PACKAGED_DESKTOP_OWNER_IDLE_TIMEOUT_S || 10) || 10
);
const HEADED = process.env.PACKAGED_SMOKE_HEADED === "1";
const bridgeUrl = new URL(BRIDGE_BASE);
const BRIDGE_PORT = bridgeUrl.port || "8877";
const BRIDGE_HOST = bridgeUrl.hostname || "127.0.0.1";

function slugifyToken(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "-")
    .replace(/^-+|-+$/g, "") || "scenario";
}

function createScenario(name) {
  return {
    name,
    slug: slugifyToken(name),
    status: "passed",
    durationMs: 0,
    error: "",
    details: {}
  };
}

async function writeReport(report) {
  await fs.mkdir(path.dirname(REPORT_PATH), { recursive: true });
  await fs.writeFile(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`, "utf8");
}

async function runScenario(name, callback, scenarios) {
  const startedAt = Date.now();
  const scenario = createScenario(name);
  try {
    scenario.details = await callback() || {};
  } catch (error) {
    scenario.status = "failed";
    scenario.error = error instanceof Error ? error.message : String(error);
    throw error;
  } finally {
    scenario.durationMs = Date.now() - startedAt;
    scenarios.push(scenario);
  }
}

async function gotoDesktop(page, relativePath) {
  const separator = relativePath.includes("?") ? "&" : "?";
  await page.goto(
    `${BASE_URL}/${relativePath}${separator}desktop=1&bridgePort=${encodeURIComponent(BRIDGE_PORT)}&bridgeHost=${encodeURIComponent(BRIDGE_HOST)}`,
    { waitUntil: "domcontentloaded" }
  );
}

async function waitForControlledDesktopPageReady(page) {
  await page.waitForFunction(
    () => Boolean(window.JobAppLocalData),
    null,
    { timeout: 30_000 }
  );
  await page.locator("#saved-source-status").waitFor({ state: "visible", timeout: 20_000 });
}

async function waitForJobsDesktopPageReady(page) {
  await page.waitForFunction(
    () => Boolean(window.JobAppLocalData),
    null,
    { timeout: 30_000 }
  );
  await page.locator("#admin-page-btn").waitFor({ state: "visible", timeout: 30_000 });
}

async function waitForAdminDesktopPageReady(page) {
  await page.waitForFunction(
    () => Boolean(window.JobAppLocalData),
    null,
    { timeout: 30_000 }
  );
  await page.locator("#admin-content").waitFor({ state: "visible", timeout: 30_000 });
}

async function dismissJobsFirstRunNotice(page) {
  const notice = page.locator("[data-jobs-first-run-notice='true']");
  const dismissButton = page.locator(".jobs-first-run-notice .local-auth-dialog-submit");
  if (await notice.count() <= 0) {
    return false;
  }
  await dismissButton.click({ timeout: 10_000 });
  await notice.waitFor({ state: "detached", timeout: 10_000 });
  return true;
}

async function fetchHealth(apiRequest, options = {}) {
  const attempts = Math.max(1, Number(options.attempts || 8) || 8);
  const delayMs = Math.max(50, Number(options.delayMs || 250) || 250);
  let lastError;
  for (let attempt = 1; attempt <= attempts; attempt += 1) {
    try {
      const response = await apiRequest.get(`${BRIDGE_BASE}/ops/health?desktopLifecycleSmoke=${Date.now()}`);
      assert.equal(response.ok(), true, "ops health should remain reachable");
      const payload = await response.json();
      assert.equal(Boolean(payload?.desktopMode), true, "bridge should remain in desktop mode");
      return payload && typeof payload === "object" ? payload : {};
    } catch (error) {
      lastError = error;
      if (attempt >= attempts) {
        throw error;
      }
      await new Promise(resolve => setTimeout(resolve, delayMs));
    }
  }
  throw lastError;
}

function ownerLastActivityAt(healthPayload) {
  return String(healthPayload?.owner?.lastActivityAt || healthPayload?.ownerLastActivityAt || "").trim();
}

async function fetchStartupMetricRows(apiRequest, limit = 1000) {
  const response = await apiRequest.get(`${BRIDGE_BASE}/desktop-local-data/startup-metrics?limit=${Number(limit) || 1000}`);
  assert.equal(response.ok(), true, "startup metrics request should succeed");
  const payload = await response.json();
  return Array.isArray(payload?.rows) ? payload.rows : [];
}

function countStartupEvent(rows, eventName) {
  return rows.filter(row => String(row?.event || "") === eventName).length;
}

async function installLifecycleBlocker(context) {
  await context.addInitScript(() => {
    const lifecyclePath = "/app/desktop-session-lifecycle";
    const shouldBlock = value => {
      const raw = typeof value === "string" ? value : String(value?.url || value || "");
      return Boolean(globalThis.__baluffoBlockDesktopLifecycle) && raw.includes(lifecyclePath);
    };
    globalThis.__baluffoBlockDesktopLifecycle = false;
    globalThis.__baluffoLifecycleFetchBlocked = 0;
    globalThis.__baluffoLifecycleBeaconBlocked = 0;

    const originalFetch = globalThis.fetch.bind(globalThis);
    globalThis.fetch = (input, init) => {
      if (shouldBlock(input)) {
        globalThis.__baluffoLifecycleFetchBlocked += 1;
        return Promise.resolve(new Response(JSON.stringify({ ok: true, blockedBySmoke: true }), {
          status: 200,
          headers: { "Content-Type": "application/json" }
        }));
      }
      return originalFetch(input, init);
    };

    const originalBeacon = globalThis.navigator?.sendBeacon?.bind(globalThis.navigator);
    if (originalBeacon) {
      globalThis.navigator.sendBeacon = (url, data) => {
        if (shouldBlock(url)) {
          globalThis.__baluffoLifecycleBeaconBlocked += 1;
          return true;
        }
        return originalBeacon(url, data);
      };
    }
  });
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
    await installLifecycleBlocker(context);
    page = await context.newPage();
    apiRequest = await playwrightRequest.newContext({ baseURL: BRIDGE_BASE });

    await runScenario("Desktop owner ignores false idle while page traffic continues", async () => {
      await gotoDesktop(page, "saved.html");
      await waitForControlledDesktopPageReady(page);
      const beforeHealth = await fetchHealth(apiRequest);
      const beforeActivity = ownerLastActivityAt(beforeHealth);
      assert.ok(beforeActivity, "owner lastActivityAt should be present before lifecycle block");

      await page.evaluate(bridgeBase => {
        globalThis.__baluffoBlockDesktopLifecycle = true;
        globalThis.__baluffoTaskStatePollCount = 0;
        globalThis.__baluffoTaskStateLastStatus = 0;
        globalThis.__baluffoTaskStateLastError = "";
        const pollTaskState = async () => {
          try {
            const response = await fetch(
              `${bridgeBase}/ops/task-state?view=summary&desktopLifecycleSmoke=${Date.now()}`,
              { cache: "no-store" }
            );
            globalThis.__baluffoTaskStatePollCount += 1;
            globalThis.__baluffoTaskStateLastStatus = response.status;
          } catch (error) {
            globalThis.__baluffoTaskStateLastError = error instanceof Error ? error.message : String(error);
          }
        };
        pollTaskState();
        globalThis.__baluffoTaskStateTimer = setInterval(pollTaskState, 1000);
      }, BRIDGE_BASE);

      const deadline = Date.now() + OWNER_IDLE_TIMEOUT_S * 1000 + 4500;
      let latestHealth = beforeHealth;
      while (Date.now() < deadline) {
        await new Promise(resolve => setTimeout(resolve, 500));
        latestHealth = await fetchHealth(apiRequest);
      }

      const afterActivity = ownerLastActivityAt(latestHealth);
      assert.ok(afterActivity, "owner lastActivityAt should be present after lifecycle block");
      assert.ok(
        Date.parse(afterActivity) > Date.parse(beforeActivity),
        `owner activity should advance from non-health page traffic, before=${beforeActivity}, after=${afterActivity}`
      );
      const counters = await page.evaluate(() => ({
        taskStatePollCount: Number(globalThis.__baluffoTaskStatePollCount || 0),
        taskStateLastStatus: Number(globalThis.__baluffoTaskStateLastStatus || 0),
        taskStateLastError: String(globalThis.__baluffoTaskStateLastError || ""),
        lifecycleFetchBlocked: Number(globalThis.__baluffoLifecycleFetchBlocked || 0),
        lifecycleBeaconBlocked: Number(globalThis.__baluffoLifecycleBeaconBlocked || 0)
      }));
      assert.ok(counters.taskStatePollCount >= 3, "controlled page should continue task-state traffic");
      assert.equal(counters.taskStateLastStatus, 200, "task-state traffic should succeed");
      assert.equal(counters.taskStateLastError, "", "task-state traffic should not throw");
      assert.ok(
        counters.lifecycleFetchBlocked + counters.lifecycleBeaconBlocked > 0,
        "desktop lifecycle traffic should be blocked by the smoke harness"
      );
      return {
        ownerIdleTimeoutSeconds: OWNER_IDLE_TIMEOUT_S,
        beforeActivity,
        afterActivity,
        ...counters
      };
    }, scenarios);

    await runScenario("Approved desktop Jobs to Admin navigation keeps runtime alive", async () => {
      await page.evaluate(() => {
        globalThis.__baluffoBlockDesktopLifecycle = false;
        if (globalThis.__baluffoTaskStateTimer) {
          clearInterval(globalThis.__baluffoTaskStateTimer);
          globalThis.__baluffoTaskStateTimer = null;
        }
      });
      await page.locator("#jobs-page-btn").click();
      await page.waitForURL(
        url => {
          try {
            return new URL(String(url)).pathname === "/jobs.html";
          } catch {
            return false;
          }
        },
        { timeout: 30_000 }
      );
      await waitForJobsDesktopPageReady(page);
      const firstRunNoticeDismissed = await dismissJobsFirstRunNotice(page);
      const beforeRows = await fetchStartupMetricRows(apiRequest);
      const beforeRegularCloseCount = countStartupEvent(beforeRows, "desktop_regular_close_shutdown_requested");
      const beforeOwnerExitCount = countStartupEvent(beforeRows, "admin_bridge_owner_session_exit_requested");

      await page.locator("#admin-page-btn").click();
      await page.waitForURL(
        url => {
          try {
            return new URL(String(url)).pathname === "/admin.html";
          } catch {
            return false;
          }
        },
        { timeout: 30_000 }
      );
      await waitForAdminDesktopPageReady(page);
      await new Promise(resolve => setTimeout(resolve, 1500));

      const health = await fetchHealth(apiRequest, { attempts: 12, delayMs: 250 });
      const afterRows = await fetchStartupMetricRows(apiRequest);
      const afterRegularCloseCount = countStartupEvent(afterRows, "desktop_regular_close_shutdown_requested");
      const afterOwnerExitCount = countStartupEvent(afterRows, "admin_bridge_owner_session_exit_requested");
      assert.equal(
        afterRegularCloseCount,
        beforeRegularCloseCount,
        "approved in-app navigation should not request a regular desktop close"
      );
      assert.equal(
        afterOwnerExitCount,
        beforeOwnerExitCount,
        "approved in-app navigation should not request owner-session exit"
      );
      return {
        currentPath: new URL(page.url()).pathname,
        desktopMode: Boolean(health?.desktopMode),
        beforeRegularCloseCount,
        afterRegularCloseCount,
        beforeOwnerExitCount,
        afterOwnerExitCount,
        firstRunNoticeDismissed
      };
    }, scenarios);
  } catch (error) {
    errors.push(error instanceof Error ? error.message : String(error));
  } finally {
    await page?.evaluate(() => {
      if (globalThis.__baluffoTaskStateTimer) {
        clearInterval(globalThis.__baluffoTaskStateTimer);
        globalThis.__baluffoTaskStateTimer = null;
      }
    }).catch(() => {});
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
    console.error("Desktop lifecycle smoke failed:", report.errors);
  }
  process.exit(report.ok ? 0 : 1);
}

await main().catch(error => {
  console.error("Fatal error in desktop lifecycle smoke:", error);
  process.exit(1);
});
