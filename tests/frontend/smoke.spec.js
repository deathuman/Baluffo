import { readFileSync, writeFileSync } from "fs";
import path from "path";
import { test, expect } from "@playwright/test";

function resolveBridgeRuntimeBase() {
  try {
    const metaPath = path.resolve(".tmp", "playwright", "bridge-meta.json");
    const payload = JSON.parse(readFileSync(metaPath, "utf8"));
    const bridgeHost = String(payload?.bridgeHost || "127.0.0.1").trim() || "127.0.0.1";
    const bridgePort = Number(payload?.bridgePort || 0);
    if (bridgePort > 0) {
      return `http://${bridgeHost}:${bridgePort}`;
    }
  } catch {
    // Fall back to the historical local smoke port if the setup metadata is unavailable.
  }
  return "http://127.0.0.1:8877";
}

function resolveDesktopRuntimeQuery() {
  try {
    const metaPath = path.resolve(".tmp", "playwright", "bridge-meta.json");
    const payload = JSON.parse(readFileSync(metaPath, "utf8"));
    const bridgeHost = String(payload?.bridgeHost || "127.0.0.1").trim() || "127.0.0.1";
    const bridgePort = Number(payload?.bridgePort || 0);
    if (bridgePort > 0) {
      return `?desktop=1&bridgePort=${bridgePort}&bridgeHost=${bridgeHost}`;
    }
  } catch {
    // Fall back to the historical local smoke port if the setup metadata is unavailable.
  }
  return "?desktop=1&bridgePort=8877&bridgeHost=127.0.0.1";
}

const BRIDGE_RUNTIME_BASE = resolveBridgeRuntimeBase();
const DESKTOP_RUNTIME_QUERY = resolveDesktopRuntimeQuery();
const PLAYWRIGHT_BRIDGE_DATA_DIR = path.resolve(".tmp", "playwright", "admin-bridge-data");

function isIgnorableSmokeConsoleError(msg) {
  if (!msg || msg.type() !== "error") {
    return false;
  }
  const location = msg.location?.() || {};
  const url = String(location.url || "");
  const text = String(msg.text?.() || "");
  if (!/Failed to load resource: the server responded with a status of 404/i.test(text)) {
    return false;
  }
  return /\/(?:data\/)?jobs-unified-startup\.json(?:\?|$)/i.test(url);
}

async function seedBridgeRuntimeBase(page) {
  await page.addInitScript((runtimeBridgeBase) => {
    try {
      window.sessionStorage.setItem("baluffo_runtime_bridge_base", String(runtimeBridgeBase || ""));
    } catch {
      // Ignore storage errors in smoke setup.
    }
  }, BRIDGE_RUNTIME_BASE);
}

async function expectJobsPageReady(page, timeout = 90000) {
  await page.waitForFunction(() => {
    const state = document.body?.getAttribute("data-jobs-startup-state") || "loading";
    return state === "interactive" || state === "error";
  }, null, { timeout });
  await expect(page.locator("body")).not.toHaveAttribute("data-jobs-startup-state", "loading");
  await expect(page.locator("#refresh-jobs-btn")).toBeEnabled();
  await expect(page.locator("#auth-sign-in-btn")).toBeEnabled();
  await expect(page.locator("#jobs-list")).not.toContainText(/Loading jobs/i);
  await expect(page.locator("#source-status")).toContainText(/^Loaded \d[\d,]* jobs/i, { timeout });
}

async function expectDesktopUpdateToggleUsable(page, timeout = 15000) {
  const updateToggle = page.locator("#desktop-update-toggle-btn");
  await expect(updateToggle).toBeVisible({ timeout });
  await expect(updateToggle).toBeEnabled({ timeout });
  await updateToggle.click();
  await expect(page.locator("#desktop-update-panel")).toBeVisible({ timeout });
}

async function signInWithProfile(page, buttonSelector, profileName, expectedFocusSelector) {
  await expect(page.locator(buttonSelector)).toBeVisible();
  await page.click(buttonSelector);
  await page.waitForTimeout(1000);

  const profileInput = page.locator("#local-auth-name-input");
  await expect(profileInput).toBeVisible();
  await profileInput.fill(profileName);
  await profileInput.press("Enter");
  await expect(profileInput).toBeHidden();
  if (expectedFocusSelector) {
    await expect(page.locator(expectedFocusSelector)).toBeFocused();
  }
}

async function cancelSignIn(page) {
  const cancelBtn = page.locator("#local-auth-cancel-btn");
  await expect(cancelBtn).toBeVisible();
  await cancelBtn.click();
  await expect(page.locator("#local-auth-name-input")).toBeHidden();
}

function writePlaywrightBridgeJson(relativeName, payload) {
  writeFileSync(
    path.join(PLAYWRIGHT_BRIDGE_DATA_DIR, relativeName),
    `${JSON.stringify(payload, null, 2)}\n`,
    "utf8"
  );
}

test("index entry redirects to jobs", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto("/index.html");
  await page.waitForURL("**/jobs.html");
  await expect(page.locator("#jobs-list")).toBeVisible();
});

test("jobs smoke: filters + refresh + pagination + save/unsave + guest warning", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  test.setTimeout(120000);
  const pageErrors = [];
  page.on("pageerror", error => {
    const msg = String(error?.stack || error?.message || error);
    pageErrors.push(msg);
    console.error("[pageerror]", msg);
  });
  page.on("console", msg => {
    if (msg.type() === "error" && !isIgnorableSmokeConsoleError(msg)) {
      const location = msg.location();
      const where = location?.url
        ? `${location.url}:${location.lineNumber || 0}:${location.columnNumber || 0}`
        : "unknown";
      console.error(`[page:error] ${where} ${msg.text()}`);
    }
  });
  await page.addInitScript(() => {
    window.__baluffoSmokeErrors = [];
    window.onerror = (message, source, line, column, error) => {
      window.__baluffoSmokeErrors.push({
        message: String(message || ""),
        source: String(source || ""),
        line: Number(line || 0),
        column: Number(column || 0),
        stack: String(error?.stack || ""),
      });
    };
  });

  await page.goto("/jobs.html");

  // Diagnostic: poll startup state and log progress until resolved or timeout
  const startupState = await page.waitForFunction(() => {
    const state = document.body?.getAttribute("data-jobs-startup-state") || "loading";
    if (state !== "loading") return state;
    return false;
  }, null, { timeout: 90000 }).catch(async () => {
    const state = await page.evaluate(() => document.body?.getAttribute("data-jobs-startup-state") || "missing");
    const smokeErrors = await page.evaluate(() => window.__baluffoSmokeErrors || []);
    console.error(`[startup] state at timeout: ${state}`);
    throw new Error(
      `Jobs startup state never left 'loading'. Final state: ${state}. ` +
      `Page errors: ${JSON.stringify(pageErrors)}. ` +
      `Window errors: ${JSON.stringify(smokeErrors)}`
    );
  });
  console.log(`[startup] resolved to: ${startupState}`);

  await expectJobsPageReady(page);

  // Verify admin bridge is running by checking the admin button state
  const adminBtn = page.locator("#admin-page-btn");
  await expect(adminBtn).toHaveAttribute("data-bridge-state", "online", { timeout: 10000 });
  await expect(adminBtn).not.toBeDisabled();
  await expect(adminBtn).not.toContainText("Admin Checking...");

  await expect(pageErrors).toEqual([]);
  await page.selectOption("#work-type-filter", "Remote");
  await expect(page.locator(".save-job-btn").first()).toBeVisible({ timeout: 20000 });

  const pageButtons = page.locator("#pagination .page-btn");
  const count = await pageButtons.count();
  if (count > 1) {
    await pageButtons.nth(1).click();
  }

  await signInWithProfile(page, "#auth-sign-in-btn", "Smoke User", "#saved-jobs-btn");
  await expect(page.locator("#saved-jobs-btn")).toBeVisible();

  const saveBtn = page.locator(".save-job-btn").first();
  await expect(saveBtn).toBeVisible();
  await saveBtn.click();
  await expect(saveBtn).toHaveClass(/saved/);
  await saveBtn.click();
  await expect(saveBtn).not.toHaveClass(/saved/);

  await page.click("#auth-sign-out-btn");
  await saveBtn.click();
  await expect(page.locator(".toast").last()).toContainText("Sign in to save jobs");
  await cancelSignIn(page);

  await page.click("#refresh-jobs-btn");
  await expect(page.locator("#refresh-jobs-btn")).toBeDisabled({ timeout: 10000 });
  await expect(page.locator("#source-status")).toContainText(/Fetching/i, { timeout: 10000 });

  await page.locator(".jobs-sources summary").click();
  await expect(page.locator("#data-sources-list")).toContainText("Google Sheets");
});

test("jobs filter popups close on outside click and Escape", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto("/jobs.html");
  await expectJobsPageReady(page);

  const countryPickerBtn = page.locator("#country-picker-btn");
  const countryPickerPanel = page.locator("#country-picker-panel");
  const countryPickerSearch = page.locator("#country-picker-search");
  const pageHeading = page.locator("h1");
  const quickFiltersBtn = page.locator("#customize-quick-filters-btn");
  const quickFiltersPanel = page.locator("#quick-filters-panel");

  await countryPickerBtn.click();
  await expect(countryPickerPanel).toBeVisible();
  await pageHeading.click();
  await expect(countryPickerPanel).toBeHidden();

  await countryPickerBtn.click();
  await expect(countryPickerPanel).toBeVisible();
  await expect(countryPickerSearch).toBeFocused();
  await countryPickerSearch.press("Escape");
  await expect(countryPickerPanel).toBeHidden();

  await quickFiltersBtn.click();
  await expect(quickFiltersPanel).toBeVisible();
  await pageHeading.click();
  await expect(quickFiltersPanel).toBeHidden();

  await quickFiltersBtn.click();
  await expect(quickFiltersPanel).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(quickFiltersPanel).toBeHidden();
});

test("saved smoke: export stays available for signed-in browser users and guest state restores", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto("/saved.html");

  await signInWithProfile(page, "#saved-auth-sign-in-btn", "Smoke User", "#add-custom-job-btn");
  await expect(page.locator("#saved-auth-status")).not.toContainText(/Guest/i);
  await page.locator("#saved-utilities summary").click();
  await expect(page.locator("#export-backup-btn")).toBeEnabled();

  await expect(page.locator("#export-backup-btn")).toBeVisible();
  const downloadPromise = page.waitForEvent("download");
  await page.click("#export-backup-btn");
  const download = await downloadPromise;
  await expect(download.suggestedFilename()).toContain("baluffo-backup-");

  await page.click("#saved-auth-sign-out-btn");
  await expect(page.locator("#saved-source-status")).toContainText("Sign in to view your saved jobs");
});

test("jobs admin badge reaches online state after navigating back from saved", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto("/saved.html");
  await expect(page.locator("#jobs-page-btn")).toBeVisible();

  await page.click("#jobs-page-btn");
  await page.waitForURL("**/jobs.html");
  await expectJobsPageReady(page);

  const adminBtn = page.locator("#admin-page-btn");
  await expect(adminBtn).toHaveAttribute("data-bridge-state", "online", { timeout: 10000 });
  await expect(adminBtn).not.toBeDisabled();
  await expect(adminBtn).not.toContainText("Admin Checking...");
});

test("desktop jobs update toggle stays usable after Jobs to Saved to Jobs navigation", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto(`/jobs.html${DESKTOP_RUNTIME_QUERY}`);
  await expectJobsPageReady(page);
  await expectDesktopUpdateToggleUsable(page);

  await signInWithProfile(page, "#auth-sign-in-btn", "Desktop Smoke User", "#saved-jobs-btn");
  await page.click("#saved-jobs-btn");
  await page.waitForURL("**/saved.html**");
  await expect(page.locator("#jobs-page-btn")).toBeVisible();

  await page.click("#jobs-page-btn");
  await page.waitForURL("**/jobs.html**");
  await expectJobsPageReady(page);
  await expectDesktopUpdateToggleUsable(page);
});

test("desktop jobs update toggle stays usable after Jobs to Admin to Jobs navigation", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto(`/jobs.html${DESKTOP_RUNTIME_QUERY}`);
  await expectJobsPageReady(page);
  await expectDesktopUpdateToggleUsable(page);

  const adminBtn = page.locator("#admin-page-btn");
  await expect(adminBtn).toHaveAttribute("data-bridge-state", "online", { timeout: 10000 });
  await adminBtn.click();
  await page.waitForURL("**/admin.html**");
  await expect(page.locator("#admin-jobs-btn")).toBeVisible();

  await page.click("#admin-jobs-btn");
  await page.waitForURL("**/jobs.html**");
  await expectJobsPageReady(page);
  await expectDesktopUpdateToggleUsable(page);
});

test("admin smoke: direct admin load works without auth gating", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto("/admin.html");
  await expect(page.locator("#admin-content")).toBeVisible();
  await expect(page.locator("#admin-source-status")).toContainText(/Loading|Loaded|users|Could not|Admin overview/i);
});

test("admin smoke: direct admin load shows bucketed fetch failure summary", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto("/admin.html");
  await expect(page.locator("#admin-content")).toBeVisible();
  await expect(page.locator("h1")).toContainText(/Administration/i);

  // Load the fetch report - requires bridge to be running
  await page.click("#admin-refresh-report-btn");
  const metrics = page.locator("#admin-ops-fetcher-metrics");
  const overviewTab = page.getByRole("tab", { name: "Overview" });
  const discoveryTab = page.getByRole("tab", { name: "Discovery Review" });
  const sourcePolicyTab = page.getByRole("tab", { name: "Source Policy Review" });
  const registryConflictsTab = page.getByRole("tab", { name: "Registry Conflicts" });
  const dedupTab = page.getByRole("tab", { name: "Dedup Lists" });
  await expect(overviewTab).toBeVisible();
  await expect(discoveryTab).toBeVisible();
  await expect(sourcePolicyTab).toBeVisible();
  await expect(registryConflictsTab).toBeVisible();
  await expect(dedupTab).toBeVisible();
  await expect(overviewTab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator("#admin-ops-tab-overview-btn .admin-ops-tab-badge")).toBeVisible();
  await expect(page.locator("#admin-ops-tab-discovery-btn .admin-ops-tab-badge")).toBeVisible();
  await expect(page.locator("#admin-ops-tab-source-policy-btn .admin-ops-tab-badge")).toBeVisible();
  await expect(page.locator("#admin-ops-tab-registry-conflicts-btn .admin-ops-tab-badge")).toBeVisible();
  await expect(page.locator("#admin-ops-tab-dedup-btn .admin-ops-tab-badge")).toBeVisible();
  await expect(metrics).not.toContainText(/Loading/i, { timeout: 15000 });
  await expect(metrics.getByText("Task Status", { exact: true })).toBeVisible();
  await expect(metrics.getByRole("heading", { name: "Runtime" })).toBeVisible();
  await expect(metrics.getByRole("heading", { name: "Failures" })).toBeVisible();
  await expect(metrics.getByRole("heading", { name: "Source Health" })).toBeVisible();
  await expect(metrics.getByRole("heading", { name: "Source Policy Signals" })).toBeVisible();
  await expect(metrics.getByRole("button", { name: "Copy diagnostics" }).first()).toBeVisible();
  await expect(metrics.locator("details").first()).toBeVisible();

  await discoveryTab.click();
  await expect(discoveryTab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator("#admin-discovery-review")).toBeVisible();

  await sourcePolicyTab.click();
  await expect(sourcePolicyTab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator("#admin-source-policy-review")).toBeVisible();

  await registryConflictsTab.click();
  await expect(registryConflictsTab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator("#admin-registry-conflicts-review")).toBeVisible();

  await dedupTab.click();
  await expect(dedupTab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator("#admin-ops-dedup-lists")).toBeVisible();
  await expect(page.locator("#admin-ops-dedup-lists").getByRole("heading", { name: "Dedup Lists" })).toBeVisible();
});

test("admin smoke: run history trims fetch live detail and discovery omits the live table", async ({ page }) => {
  const nowIso = new Date().toISOString();
  const fetchRunId = "smoke_fetch_live_current_1";
  const discoveryRunId = "smoke_discovery_live_current_1";
  const fetchReport = {
    runId: fetchRunId,
    startedAt: nowIso,
    finishedAt: "",
    summary: {
      successfulSources: 10,
      failedSources: 0,
      excludedSources: 0,
      outputCount: 34081,
      sourceCount: 551
    },
    runtime: {
      selectedSourceCount: 551,
      heartbeatAt: nowIso
    },
    taskProgress: {
      active: true,
      phaseKey: "executing_sources",
      phaseLabel: "Executing sources",
      mode: "determinate",
      ratio: 10 / 551,
      counts: {
        resolvedSources: 10,
        sourceCount: 551,
        runningTasks: 541,
        queuedTasks: 0,
        outputCount: 34081,
        failedSources: 0,
        excludedSources: 0,
        completedTasks: 10
      }
    },
    sources: [
      {
        name: "Studio A",
        status: "running",
        adapter: "static",
        studio: "Studio A",
        keptCount: 17,
        durationMs: 26000
      }
    ]
  };
  const staleFetchTasks = {
    taskType: "fetch",
    runId: "smoke_fetch_live_stale_1",
    startedAt: "2026-04-18T11:00:00.000Z",
    status: "running",
    taskProgress: {
      active: true,
      phaseKey: "executing_sources",
      phaseLabel: "Executing sources",
      mode: "determinate",
      ratio: 9 / 551,
      counts: {
        resolvedSources: 9,
        sourceCount: 551,
        runningTasks: 542,
        queuedTasks: 0,
        outputCount: 29957,
        completedTasks: 9
      }
    }
  };
  const discoveryReport = {
    runId: discoveryRunId,
    startedAt: nowIso,
    finishedAt: "",
    summary: {
      foundEndpointCount: 12,
      probedCandidateCount: 5,
      queuedCandidateCount: 3,
      failedProbeCount: 0
    },
    taskProgress: {
      active: true,
      phaseKey: "probing_candidates",
      phaseLabel: "Probing candidates",
      mode: "determinate",
      ratio: 0.5,
      counts: {
        foundEndpoints: 12,
        probedCandidates: 5,
        queuedCandidates: 3
      }
    },
    runtime: {
      lifecycle: { heartbeatAt: nowIso },
      adapterTimings: [
        {
          adapter: "greenhouse",
          generatedCount: 12,
          failureCount: 0,
          probedCount: 5,
          healthyCount: 5,
          queuedCount: 3,
          durationMs: 12000
        }
      ]
    },
    failures: [],
    candidates: []
  };
  writePlaywrightBridgeJson("admin-task-state.json", {
    fetch: {
      runId: fetchRunId,
      taskType: "fetch",
      pid: process.pid,
      script: "jobs_fetcher.py",
      status: "running",
      startedAt: nowIso
    },
    discovery: {
      runId: discoveryRunId,
      taskType: "discovery",
      pid: process.pid,
      script: "source_discovery.py",
      status: "running",
      startedAt: nowIso
    }
  });
  writePlaywrightBridgeJson("admin-task-lifecycle.json", {
    schemaVersion: 1,
    updatedAt: nowIso,
    rows: [
      {
        schemaVersion: 1,
        runId: fetchRunId,
        taskType: "fetch",
        parentRunId: "",
        parentTaskType: "",
        status: "running",
        stage: "",
        startedAt: nowIso,
        heartbeatAt: nowIso,
        finishedAt: "",
        terminalReason: "",
        ownerKind: "process",
        ownerPid: process.pid,
        progress: fetchReport.taskProgress,
        summary: fetchReport.summary
      },
      {
        schemaVersion: 1,
        runId: discoveryRunId,
        taskType: "discovery",
        parentRunId: "",
        parentTaskType: "",
        status: "running",
        stage: "",
        startedAt: nowIso,
        heartbeatAt: nowIso,
        finishedAt: "",
        terminalReason: "",
        ownerKind: "process",
        ownerPid: process.pid,
        progress: discoveryReport.taskProgress,
        summary: discoveryReport.summary
      }
    ]
  });
  writePlaywrightBridgeJson("jobs-fetch-report.json", fetchReport);
  writePlaywrightBridgeJson("jobs-fetch-tasks.json", staleFetchTasks);
  writePlaywrightBridgeJson("source-discovery-report.json", discoveryReport);
  writePlaywrightBridgeJson("admin-run-history.json", []);

  await seedBridgeRuntimeBase(page);
  await page.route("**/data/jobs-fetch-report.json*", async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify(fetchReport)
    });
  });
  await page.goto(`/admin.html${DESKTOP_RUNTIME_QUERY}`);
  await expect(page.locator("#admin-content")).toBeVisible();
  await expect(page.locator(".admin-discovery-live-card")).toHaveCount(0);

  await expect(page.locator("#admin-fetcher-progress-label")).toContainText(/10\/551 sources resolved/i, { timeout: 15000 });
  await expect(page.locator("#admin-ops-history")).toContainText(/Executing sources \(2%\)/i, { timeout: 15000 });
  await expect(page.locator("[data-ui='admin-fetcher-live-items']")).toHaveCount(0);
});
