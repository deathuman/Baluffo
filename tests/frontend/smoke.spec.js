import { readFileSync, renameSync, rmSync, writeFileSync } from "fs";
import { execFileSync } from "child_process";
import path from "path";
import { test, expect } from "@playwright/test";

import { seedWarmState } from "../../scripts/measure_container_jobs_boot.mjs";

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
const ACTION_CENTER_TEST_POLL_MS = 1000;
const PLAYWRIGHT_BRIDGE_DATA_DIR = path.resolve(".tmp", "playwright", "admin-bridge-data");
const SMOKE_JOBS_FEED = Array.from({ length: 24 }, (_, idx) => ({
  id: `smoke-job-${idx + 1}`,
  title: idx === 0 ? "Smoke Gameplay Engineer" : `Smoke QA Analyst ${idx + 1}`,
  company: idx % 2 === 0 ? "Smoke Studio" : "Example Games",
  city: idx % 3 === 0 ? "Remote" : "Berlin",
  country: idx % 3 === 0 ? "US" : "DE",
  locationSummary: idx % 3 === 0 ? "Remote, US" : "Berlin, DE",
  locations: [{ city: idx % 3 === 0 ? "Remote" : "Berlin", country: idx % 3 === 0 ? "US" : "DE" }],
  workType: idx % 3 === 0 ? "Remote" : "Hybrid",
  contractType: "Full-time",
  sector: "Game",
  companyType: "Game",
  source: "Google Sheets",
  jobLink: `https://example.com/jobs/smoke-job-${idx + 1}`,
}));
const SMOKE_JOBS_CSV = [
  "id,title,company,city,country,workType,contractType,sector,source,jobLink",
  ...SMOKE_JOBS_FEED.map(job =>
    [
      job.id,
      job.title,
      job.company,
      job.city,
      job.country,
      job.workType,
      job.contractType,
      job.sector,
      job.source,
      job.jobLink,
    ].map(value => `"${String(value).replaceAll('"', '""')}"`).join(",")
  ),
].join("\n");

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
  await page.route(/\/(?:data\/)?jobs-unified(?:-startup|-light)?\.json(?:\?|$)/i, async route => {
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: `${JSON.stringify(SMOKE_JOBS_FEED)}\n`,
    });
  });
  await page.route(/\/(?:data\/)?jobs-unified\.csv(?:\?|$)/i, async route => {
    await route.fulfill({
      status: 200,
      contentType: "text/csv",
      body: `${SMOKE_JOBS_CSV}\n`,
    });
  });
  await page.addInitScript((runtimeBridgeBase) => {
    try {
      window.sessionStorage.setItem("baluffo_runtime_bridge_base", String(runtimeBridgeBase || ""));
    } catch {
      // Ignore storage errors in smoke setup.
    }
  }, BRIDGE_RUNTIME_BASE);
}

async function expectJobsPageReady(page, timeout = 90000, expectLoadedStatus = true) {
  await page.waitForFunction(() => {
    const state = document.body?.getAttribute("data-jobs-startup-state") || "loading";
    return state === "interactive" || state === "error";
  }, null, { timeout });
  await expect(page.locator("body")).not.toHaveAttribute("data-jobs-startup-state", "loading");
  await expect(page.locator("#jobs-pipeline-run-btn")).toBeEnabled();
  await expect(page.locator("#auth-sign-in-btn")).toBeEnabled();
  await expect(page.locator("#jobs-list")).not.toContainText(/Loading jobs/i);
  if (expectLoadedStatus) await expect(page.locator("#source-status")).toContainText(/^Loaded \d[\d,]* jobs/i, { timeout });
}

async function expectDesktopUpdateToggleUsable(page, timeout = 15000) {
  const updateToggle = page.locator("#desktop-update-toggle-btn");
  await expect(updateToggle).toBeVisible({ timeout });
  await expect(updateToggle).toBeEnabled({ timeout });
  await page.locator("[data-jobs-first-run-notice='true'] button").click({ timeout: 1000 }).catch(() => {});
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

async function expectTooltipText(page, target, expectedText) {
  await target.scrollIntoViewIfNeeded();
  const tooltipSource = String(await target.getAttribute("data-tooltip") || "");
  if (expectedText instanceof RegExp) {
    expect(tooltipSource).toMatch(expectedText);
  } else {
    expect(tooltipSource).toContain(expectedText);
  }
  await target.hover({ force: true });
  const tooltip = page.locator(".baluffo-tooltip-portal");
  try {
    await expect(tooltip).toContainText(expectedText, { timeout: 1500 });
  } catch {
    await target.evaluate(el => {
      const PointerCtor = window.PointerEvent || window.MouseEvent;
      el.dispatchEvent(new PointerCtor("pointerover", { bubbles: true, composed: true }));
      el.dispatchEvent(new FocusEvent("focusin", { bubbles: true, composed: true }));
    });
    await expect(tooltip).toContainText(expectedText);
  }
  await expect(tooltip).toHaveAttribute("aria-hidden", "false");
  await page.keyboard.press("Escape");
  await expect(tooltip).toHaveAttribute("aria-hidden", "true");
}

function writePlaywrightBridgeJson(relativeName, payload) {
  const targetPath = path.join(PLAYWRIGHT_BRIDGE_DATA_DIR, relativeName);
  const tempPath = `${targetPath}.${process.pid}.${Date.now()}.tmp`;
  writeFileSync(tempPath, `${JSON.stringify(payload, null, 2)}\n`, "utf8");
  // Rename so a bridge poll never parses a half-written sidecar.
  renameSync(tempPath, targetPath);
}

const BROWSER_FALLBACK_SIDECAR = "jobs-fetch-report-summary.json";

function writeBrowserFallbackSidecar(health, { sourceHealth = {} } = {}) {
  writePlaywrightBridgeJson(BROWSER_FALLBACK_SIDECAR, {
    ok: true,
    summaryView: true,
    runtime: { browserFallbackHealth: health },
    sourceHealth
  });
}

async function waitForBridgeBrowserFallback(request, lastFailureAt, timeout = 30000) {
  let payload = null;
  await expect.poll(
    async () => {
      const response = await request.get(`${BRIDGE_RUNTIME_BASE}/ops/fetch-kpis?view=summary`);
      payload = await response.json();
      return payload?.kpis?.browserFallbackHealth?.lastFailureAt || "";
    },
    { timeout }
  ).toBe(lastFailureAt);
  return payload;
}

function seedPlaywrightTaskRuntime(rows) {
  const python = process.env.PYTHON || "python";
  const script = `
import json
import sys
from pathlib import Path
from src.storage import BaluffoStore, TaskRuntimeStore

data_dir = Path(sys.argv[1])
rows = json.loads(sys.argv[2])
with BaluffoStore(data_dir) as store:
    store.set_authority_mode("taskRuns", "sqlite", reason="playwright-smoke")
    runtime = TaskRuntimeStore(store)
    for row in rows:
        runtime.upsert_task_run(row)
`;
  execFileSync(python, ["-c", script, PLAYWRIGHT_BRIDGE_DATA_DIR, JSON.stringify(rows)], {
    cwd: process.cwd(),
    stdio: "pipe"
  });
}

test("index entry redirects to jobs", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto("/index.html");
  await page.waitForURL("**/jobs.html");
  await expect(page.locator("#jobs-list")).toBeVisible();
});

test("jobs smoke: warm seeder creates the app's IndexedDB schema", async ({ page }) => {
  // A CSS document is enough: IndexedDB and dynamic imports are origin-scoped,
  // and no Jobs boot code runs to race the seeded cache.
  await page.goto("/styles/base.css");
  const rows = [
    { id: "warm-1", title: "Warm Cache Engineer", company: "Warm Studio" },
    { id: "warm-2", title: "Warm Cache Designer", company: "Warm Studio" }
  ];
  expect(await seedWarmState(page, {
    rows,
    rowFloor: rows.length,
    signalId: "warm-seed-schema-test"
  })).toBe(rows.length);

  const cacheSource = readFileSync(path.resolve("frontend/jobs/app/cache.js"), "utf8");
  const state = await page.evaluate(async ({ source }) => {
    const moduleUrl = URL.createObjectURL(new Blob([source], { type: "text/javascript" }));
    try {
      const cache = await import(moduleUrl);
      const db = await cache.openJobsCacheDb({
        indexedDb: indexedDB,
        dbName: "baluffo_jobs_cache",
        dbVersion: 2,
        cacheStore: "jobs_feed",
        seenStore: "jobs_seen"
      });
      const stores = Array.from(db.objectStoreNames);
      const seenIndexes = Array.from(
        db.transaction("jobs_seen", "readonly").objectStore("jobs_seen").indexNames
      );
      const cached = await cache.readJobsCache({
        openDb: async () => db,
        cacheStore: "jobs_feed",
        cacheKey: "latest"
      });
      await cache.markSeenJob("warm-profile", "warm-1", {
        openDb: async () => db,
        seenStore: "jobs_seen"
      });
      const seenKeys = Array.from(await cache.loadSeenJobKeys("warm-profile", {
        openDb: async () => db,
        seenStore: "jobs_seen"
      }));
      db.close();
      return {
        version: db.version,
        stores,
        seenIndexes,
        seenKeys,
        cachedCount: cached?.jobs?.length || 0,
        cachedSavedAt: cached?.savedAt || 0,
        signal: JSON.parse(localStorage.getItem("baluffo_jobs_auto_refresh_signal") || "null"),
        applied: localStorage.getItem("baluffo_jobs_auto_refresh_applied")
      };
    } finally {
      URL.revokeObjectURL(moduleUrl);
    }
  }, { source: cacheSource });

  expect(state.version).toBe(2);
  expect(state.stores).toEqual(expect.arrayContaining(["jobs_feed", "jobs_seen"]));
  expect(state.seenIndexes).toContain("profileId");
  expect(state.cachedCount).toBe(rows.length);
  expect(state.cachedSavedAt).toBeGreaterThan(0);
  expect(state.seenKeys).toEqual(["warm-1"]);
  expect(state.signal).toMatchObject({
    id: "warm-seed-schema-test",
    source: "admin_fetcher"
  });
  expect(state.signal.finishedAt).toEqual(expect.any(String));
  expect(state.applied).toBeNull();
});

test("jobs smoke: app repairs a v2 cache missing the profileId index", async ({ page }) => {
  await page.goto("/styles/base.css");
  const cacheSource = readFileSync(path.resolve("frontend/jobs/app/cache.js"), "utf8");
  const state = await page.evaluate(async ({ source }) => {
    const db = await new Promise((resolve, reject) => {
      const request = indexedDB.open("baluffo_jobs_cache", 2);
      request.onupgradeneeded = () => {
        request.result.createObjectStore("jobs_feed", { keyPath: "id" });
        request.result.createObjectStore("jobs_seen", { keyPath: "pk" });
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    // The repair delete must preserve seen-state, not just the schema.
    await new Promise((resolve, reject) => {
      const tx = db.transaction("jobs_seen", "readwrite");
      tx.objectStore("jobs_seen").put({
        pk: "warm-profile::warm-kept-1",
        profileId: "warm-profile",
        jobKey: "warm-kept-1",
        seenAt: 1720000000000
      });
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
    db.close();

    const moduleUrl = URL.createObjectURL(new Blob([source], { type: "text/javascript" }));
    try {
      const cache = await import(moduleUrl);
      const repaired = await cache.openJobsCacheDb({
        indexedDb: indexedDB,
        dbName: "baluffo_jobs_cache",
        dbVersion: 2,
        cacheStore: "jobs_feed",
        seenStore: "jobs_seen"
      });
      const seenRows = await new Promise((resolve, reject) => {
        const request = repaired
          .transaction("jobs_seen", "readonly")
          .objectStore("jobs_seen")
          .getAll();
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      const seenIndexes = await new Promise((resolve, reject) => {
        const store = repaired
          .transaction("jobs_seen", "readonly")
          .objectStore("jobs_seen");
        const result = Array.from(store.indexNames);
        const request = store.index("profileId").getAll("warm-profile");
        request.onsuccess = () => resolve({ result, keys: request.result.map(row => row.jobKey) });
        request.onerror = () => reject(request.error);
      });
      const result = {
        version: repaired.version,
        stores: Array.from(repaired.objectStoreNames),
        seenIndexes: seenIndexes.result,
        seenRows,
        seenKeys: seenIndexes.keys
      };
      repaired.close();
      return result;
    } finally {
      URL.revokeObjectURL(moduleUrl);
    }
  }, { source: cacheSource });

  expect(state.version).toBe(2);
  expect(state.stores).toEqual(expect.arrayContaining(["jobs_feed", "jobs_seen"]));
  expect(state.seenIndexes).toContain("profileId");
  expect(state.seenRows).toEqual([
    {
      pk: "warm-profile::warm-kept-1",
      profileId: "warm-profile",
      jobKey: "warm-kept-1",
      seenAt: 1720000000000
    }
  ]);
  expect(state.seenKeys).toEqual(["warm-kept-1"]);
});

test("jobs smoke: app reports a blocked repair instead of hanging or wiping", async ({ page }) => {
  await page.goto("/styles/base.css");
  const cacheSource = readFileSync(path.resolve("frontend/jobs/app/cache.js"), "utf8");
  const state = await page.evaluate(async ({ source }) => {
    const options = {
      indexedDb: indexedDB,
      dbName: "baluffo_jobs_cache",
      dbVersion: 2,
      cacheStore: "jobs_feed",
      seenStore: "jobs_seen"
    };
    const seed = await new Promise((resolve, reject) => {
      const request = indexedDB.open(options.dbName, options.dbVersion);
      request.onupgradeneeded = () => {
        request.result.createObjectStore("jobs_feed", { keyPath: "id" });
        request.result.createObjectStore("jobs_seen", { keyPath: "pk" });
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    await new Promise((resolve, reject) => {
      const tx = seed.transaction("jobs_seen", "readwrite");
      tx.objectStore("jobs_seen").put({
        pk: "blocked-profile::blocked-1",
        profileId: "blocked-profile",
        jobKey: "blocked-1",
        seenAt: 1720000000000
      });
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error);
    });
    // A peer tab that never handles versionchange keeps the database open, so
    // deleteDatabase must report `blocked` rather than wait forever.
    const peer = await new Promise((resolve, reject) => {
      const request = indexedDB.open(options.dbName, options.dbVersion);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    seed.close();

    const moduleUrl = URL.createObjectURL(new Blob([source], { type: "text/javascript" }));
    try {
      const cache = await import(moduleUrl);
      let blockedMessage = "";
      try {
        const db = await cache.openJobsCacheDb(options);
        db.close();
      } catch (error) {
        blockedMessage = String(error?.message || error);
      }
      const keptRows = await new Promise((resolve, reject) => {
        const request = peer
          .transaction("jobs_seen", "readonly")
          .objectStore("jobs_seen")
          .getAll();
        request.onsuccess = () => resolve(request.result);
        request.onerror = () => reject(request.error);
      });
      peer.close();

      // The blocked delete is still queued and drains once the peer leaves, so
      // the next open must repair the schema instead of staying wedged.
      const recovered = await cache.openJobsCacheDb(options);
      const result = {
        blockedMessage,
        keptRows,
        version: recovered.version,
        stores: Array.from(recovered.objectStoreNames),
        seenIndexes: Array.from(
          recovered.transaction("jobs_seen", "readonly").objectStore("jobs_seen").indexNames
        )
      };
      recovered.close();
      return result;
    } finally {
      URL.revokeObjectURL(moduleUrl);
    }
  }, { source: cacheSource });

  expect(state.blockedMessage).toContain("blocked by another connection");
  // The refused delete must leave the peer's data intact, not half-wiped.
  expect(state.keptRows).toHaveLength(1);
  expect(state.keptRows[0]).toMatchObject({
    pk: "blocked-profile::blocked-1",
    profileId: "blocked-profile",
    jobKey: "blocked-1",
    seenAt: 1720000000000
  });
  expect(state.version).toBe(2);
  expect(state.stores).toEqual(expect.arrayContaining(["jobs_feed", "jobs_seen"]));
  expect(state.seenIndexes).toContain("profileId");
});

test("jobs smoke: app reports a blocked cache open instead of hanging", async ({ page }) => {
  await page.goto("/styles/base.css");
  const cacheSource = readFileSync(path.resolve("frontend/jobs/app/cache.js"), "utf8");
  const state = await page.evaluate(async ({ source }) => {
    const options = {
      indexedDb: indexedDB,
      dbName: "baluffo_jobs_cache",
      dbVersion: 2,
      cacheStore: "jobs_feed",
      seenStore: "jobs_seen"
    };
    // A version-1 peer with no versionchange handler keeps the upgrade open,
    // so the app's version-2 open must report `blocked` rather than wait.
    const peer = await new Promise((resolve, reject) => {
      const request = indexedDB.open(options.dbName, 1);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    const peerVersion = peer.version;

    const moduleUrl = URL.createObjectURL(new Blob([source], { type: "text/javascript" }));
    try {
      const cache = await import(moduleUrl);
      const outcome = await Promise.race([
        cache.openJobsCacheDb(options).then(
          db => {
            db.close();
            return { status: "resolved" };
          },
          error => ({ status: "rejected", message: String(error?.message || error) })
        ),
        new Promise(resolve => setTimeout(() => resolve({ status: "pending" }), 1500))
      ]);

      // Closing the peer drains the queued upgrade; the rejected open's late
      // success must close its connection so this second open can proceed.
      peer.close();
      const recovered = await cache.openJobsCacheDb(options);
      const result = {
        outcome,
        peerVersion,
        version: recovered.version,
        stores: Array.from(recovered.objectStoreNames)
      };
      recovered.close();
      return result;
    } finally {
      peer.close();
      URL.revokeObjectURL(moduleUrl);
    }
  }, { source: cacheSource });

  expect(state.outcome.status).toBe("rejected");
  expect(state.outcome.message).toContain("blocked by another connection");
  expect(state.peerVersion).toBe(1);
  expect(state.version).toBe(2);
  expect(state.stores).toEqual(expect.arrayContaining(["jobs_feed", "jobs_seen"]));
});

test("jobs smoke: warm seeder repairs a v2 cache missing the profileId index", async ({ page }) => {
  await page.goto("/styles/base.css");
  const rows = [
    { id: "repair-1", title: "Repair Engineer", company: "Warm Studio" },
    { id: "repair-2", title: "Repair Designer", company: "Warm Studio" }
  ];

  const before = await page.evaluate(async () => {
    const db = await new Promise((resolve, reject) => {
      const request = indexedDB.open("baluffo_jobs_cache", 2);
      request.onupgradeneeded = () => {
        request.result.createObjectStore("jobs_feed", { keyPath: "id" });
        request.result.createObjectStore("jobs_seen", { keyPath: "pk" });
      };
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    const tx = db.transaction("jobs_seen", "readonly");
    const state = {
      version: db.version,
      stores: Array.from(db.objectStoreNames),
      seenIndexes: Array.from(tx.objectStore("jobs_seen").indexNames)
    };
    await new Promise(resolve => { tx.oncomplete = resolve; tx.onerror = resolve; tx.onabort = resolve; });
    db.close();
    return state;
  });
  expect(before.version).toBe(2);
  expect(before.stores).toEqual(expect.arrayContaining(["jobs_feed", "jobs_seen"]));
  expect(before.seenIndexes).not.toContain("profileId");

  expect(await seedWarmState(page, {
    rows,
    rowFloor: rows.length,
    signalId: "warm-seed-repair-test"
  })).toBe(rows.length);

  const after = await page.evaluate(async () => {
    const db = await new Promise((resolve, reject) => {
      const request = indexedDB.open("baluffo_jobs_cache", 2);
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    const seenTx = db.transaction("jobs_seen", "readonly");
    const seenIndexes = Array.from(seenTx.objectStore("jobs_seen").indexNames);
    const feedTx = db.transaction("jobs_feed", "readonly");
    const cached = await new Promise((resolve, reject) => {
      const request = feedTx.objectStore("jobs_feed").get("latest");
      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error);
    });
    const state = {
      version: db.version,
      stores: Array.from(db.objectStoreNames),
      seenIndexes,
      cachedCount: cached?.jobs?.length || 0
    };
    db.close();
    return state;
  });
  expect(after.version).toBe(2);
  expect(after.stores).toEqual(expect.arrayContaining(["jobs_feed", "jobs_seen"]));
  expect(after.seenIndexes).toContain("profileId");
  expect(after.cachedCount).toBe(rows.length);
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
  await expectTooltipText(
    page,
    page.locator("#jobs-pipeline-run-btn"),
    /(?:Find new openings|First update: find new openings) and rebuild the local job list\./i
  );
  await expectTooltipText(page, page.locator(".jobs-footer-admin[data-tooltip]"), /admin/i);

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
  await expectTooltipText(page, page.locator("#history-panel-toggle-btn"), "Show activity timeline");
  await expect(page.locator("#add-custom-job-btn")).not.toHaveAttribute("data-tooltip", /.+/);

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
  await expect(page.locator("#saved-source-status")).toContainText("Sign in to a local profile to view saved jobs.");
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
  await expectJobsPageReady(page, 90000, false);
  await expectDesktopUpdateToggleUsable(page);

  await signInWithProfile(page, "#auth-sign-in-btn", "Desktop Smoke User", "#saved-jobs-btn");
  await page.click("#saved-jobs-btn");
  await page.waitForURL("**/saved.html**");
  await expect(page.locator("#jobs-page-btn")).toBeVisible();

  await page.click("#jobs-page-btn");
  await page.waitForURL("**/jobs.html**");
  await expectJobsPageReady(page, 90000, false);
  await expectDesktopUpdateToggleUsable(page);
});

test("desktop jobs update toggle stays usable after Jobs to Admin to Jobs navigation", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto(`/jobs.html${DESKTOP_RUNTIME_QUERY}`);
  await expectJobsPageReady(page, 90000, false);
  await expectDesktopUpdateToggleUsable(page);

  const adminBtn = page.locator("#admin-page-btn");
  await expect(adminBtn).toHaveAttribute("data-bridge-state", "online", { timeout: 10000 });
  await adminBtn.click();
  await page.waitForURL("**/admin.html**");
  await expect(page.locator("#admin-jobs-btn")).toBeVisible();

  await page.click("#admin-jobs-btn");
  await page.waitForURL("**/jobs.html**");
  await expectJobsPageReady(page, 90000, false);
  await expectDesktopUpdateToggleUsable(page);
});

test("admin smoke: direct admin load works without auth gating", async ({ page }) => {
  await seedBridgeRuntimeBase(page);
  await page.goto("/admin.html");
  await expect(page.locator("#admin-content")).toBeVisible();
  await expect(page.locator("#admin-source-status")).toContainText(/Loading|Loaded|users|Could not|Admin overview/i);
});

test("admin smoke: Action Center shows a failed browser fallback probe", async ({ page, request }) => {
  test.setTimeout(90000);
  await seedBridgeRuntimeBase(page);
  const lastFailureAt = new Date(Date.now() - (3 * 60 * 60 * 1000) - (60 * 1000)).toISOString();
  const expectedFailureAt = `${lastFailureAt.slice(0, 19).replace("T", " ")} UTC`;
  const sourceHealth = {
    browserFallbackRecommendedSources: 206,
    browserFallbackAttemptedSources: 35
  };
  const failedHealth = {
    ok: false,
    stage: "launch",
    errorType: "executable_does_not_exist",
    error: "browserType.launch: Executable doesn't exist at /ms-playwright/chromium-1200\n  at openBrowser",
    durationMs: 281,
    lastFailureAt
  };

  try {
    writeBrowserFallbackSidecar(failedHealth, { sourceHealth });
    const seededKpis = await waitForBridgeBrowserFallback(request, lastFailureAt);
    expect(seededKpis.kpis.browserFallbackHealth).toMatchObject({
      stage: "launch",
      errorType: "executable_does_not_exist",
      lastFailureAt
    });
    expect(seededKpis.kpis.browserFallbackHealth.error).toContain("browserType.launch");
    expect(seededKpis.kpis.sourceOutcomeSummary).toMatchObject(sourceHealth);

    await page.goto("/admin.html");
    await expect(page.locator("#admin-content")).toBeVisible();

    const actionCenter = page.locator("#admin-action-center");
    const signal = actionCenter.locator('.action-center-signal[data-signal="browser_fallback_health"]');
    await expect(actionCenter).toBeVisible();
    await expect(signal).toBeVisible({ timeout: 20000 });
    await expect(signal).toContainText("Browser fallback health needs attention");
    await expect(signal).toContainText("Probe failed at launch (executable_does_not_exist): browserType.launch");
    await expect(signal).toContainText("206 sources need browser fallback");
    await expect(signal).toContainText(`last failure ${expectedFailureAt} (3h ago)`);
    await expect(signal).toContainText("Copy diagnostics");
  } finally {
    rmSync(path.join(PLAYWRIGHT_BRIDGE_DATA_DIR, BROWSER_FALLBACK_SIDECAR), { force: true });
  }
});

test("admin smoke: a passing browser fallback probe clears the Action Center signal", async ({ page, request }) => {
  test.setTimeout(120000);
  await seedBridgeRuntimeBase(page);
  const lastFailureAt = new Date(Date.now() - (2 * 60 * 60 * 1000)).toISOString();
  const sourceHealth = {
    browserFallbackRecommendedSources: 206,
    browserFallbackAttemptedSources: 35
  };
  const failedHealth = {
    ok: false,
    stage: "launch",
    errorType: "executable_does_not_exist",
    error: "browserType.launch: Executable doesn't exist at /ms-playwright/chromium-1200",
    durationMs: 281,
    lastFailureAt
  };
  const passingHealth = { ok: true, stage: "navigation", durationMs: 42 };

  try {
    // Seed the real sidecar the bridge projects into `/ops/fetch-kpis`, then wait
    // out its 15 s payload cache so a preceding test cannot mask the failure.
    writeBrowserFallbackSidecar(failedHealth, { sourceHealth });
    const seededKpis = await waitForBridgeBrowserFallback(request, lastFailureAt);
    expect(seededKpis.kpis.browserFallbackHealth).toMatchObject({
      stage: "launch",
      errorType: "executable_does_not_exist",
      lastFailureAt
    });
    expect(seededKpis.kpis.sourceOutcomeSummary).toMatchObject(sourceHealth);

    // Navigate while that failure payload is cached, so the initial Action
    // Center render is fed by the real bridge rather than an intercepted route.
    await page.goto(`/admin.html?actionCenterPollMs=${ACTION_CENTER_TEST_POLL_MS}`);
    await expect(page.locator("#admin-content")).toBeVisible();

    const actionCenter = page.locator("#admin-action-center");
    const actionCenterItems = page.locator("#admin-action-center-items");
    const signal = actionCenter.locator('.action-center-signal[data-signal="browser_fallback_health"]');
    await expect(actionCenter).toBeVisible();
    await expect(signal).toBeVisible({ timeout: 20000 });
    await expect(signal).toContainText("Browser fallback health needs attention");
    await expect(signal).toContainText("Probe failed at launch (executable_does_not_exist): browserType.launch");
    await expect(signal).toContainText("206 sources need browser fallback");
    const initialPollSequence = Number(await actionCenterItems.getAttribute("data-poll-sequence") || 0);

    // The probe recovers on disk. The next poll (sped up by the test-only
    // interval override) must replace the panel from the rewritten sidecar, so
    // the stale failure row disappears without a reload.
    const passingPoll = page.waitForResponse(
      async response => {
        if (
          response.status() !== 200 ||
          !/\/ops\/fetch-kpis\?view=summary(?:&|$)/.test(response.url())
        ) {
          return false;
        }
        const payload = await response.json().catch(() => null);
        return payload?.kpis?.browserFallbackHealth?.ok === true;
      },
      { timeout: 50000 }
    );
    writeBrowserFallbackSidecar(passingHealth, { sourceHealth });
    const passingResponse = await passingPoll;
    expect((await passingResponse.json()).kpis.browserFallbackHealth).toMatchObject(passingHealth);
    // Wait for the Action Center to complete its own poll, not for a wall-clock
    // duration. The timeout only guards a poll that never finishes.
    await page.waitForFunction(
      previous => Number(
        document.querySelector("#admin-action-center-items")?.dataset.pollSequence || 0
      ) > previous,
      initialPollSequence,
      { timeout: 45000, polling: 250 }
    );
    await expect(signal).toBeHidden();
    await expect(actionCenter).not.toContainText("Browser fallback health");
  } finally {
    rmSync(path.join(PLAYWRIGHT_BRIDGE_DATA_DIR, BROWSER_FALLBACK_SIDECAR), { force: true });
  }
});

test("admin smoke: direct admin load shows bucketed fetch failure summary", async ({ page }) => {
  test.setTimeout(60000);
  await seedBridgeRuntimeBase(page);
  const fetcherMetricsRoute = /\/ops\/fetcher-metrics\?windowRuns=80(?:&|$)/;
  let fetcherMetricsRequestCount = 0;
  await page.route(fetcherMetricsRoute, async route => {
    fetcherMetricsRequestCount += 1;
    await route.fulfill({
      status: 200,
      contentType: "application/json",
      body: JSON.stringify({
        latestRun: {
          browserFallbackHealth: {
            ok: false,
            stage: "launch",
            error: "BrowserType.launch: Executable doesn't exist at /ms-playwright/chromium-1200",
            errorType: "Error",
            durationMs: 281
          }
        },
        history: {}
      })
    });
  });
  await page.goto("/admin.html");
  await expect(page.locator("#admin-content")).toBeVisible();
  await expect(page.locator("h1")).toContainText(/Administration/i);
  await expectTooltipText(page, page.locator("#admin-sync-test-btn"), "Verify GitHub App access");
  await expect(page.locator("#admin-refresh-btn")).toHaveCount(0);

  // Diagnostics stay collapsed until requested, then load their own payload.
  const metrics = page.locator("#admin-ops-fetcher-metrics");
  const fetcherDiagnostics = page.locator("#admin-ops-fetcher-diagnostics");
  const fetcherDiagnosticsSummary = fetcherDiagnostics.locator("summary").first();
  await expect(fetcherDiagnostics).toBeVisible();
  await expect(fetcherDiagnostics).not.toHaveAttribute("open");
  await expect(metrics).not.toContainText("Browser fallback health");
  expect(fetcherMetricsRequestCount).toBe(0);
  await fetcherDiagnosticsSummary.click();
  await expect(fetcherDiagnostics).toHaveAttribute("open");
  await expect(metrics).toContainText("Browser fallback health", { timeout: 15000 });
  await expect(metrics).toContainText("Executable doesn't exist at /ms-playwright/chromium-1200");
  expect(fetcherMetricsRequestCount).toBeGreaterThan(0);
  const fetcherMetricsRequestsAfterOpen = fetcherMetricsRequestCount;

  // A normal idle health poll must leave the collapsed panel closed. Then reopen
  // from the cached payload; the on-demand loader must not issue a second
  // fetcher-metrics request for the same panel.
  await fetcherDiagnosticsSummary.click();
  await expect(fetcherDiagnostics).not.toHaveAttribute("open");
  await page.waitForResponse(
    response => response.url().includes("/ops/dashboard-health?view=summary"),
    { timeout: 20000 }
  );
  await page.waitForTimeout(300);
  await expect(fetcherDiagnostics).not.toHaveAttribute("open");
  await fetcherDiagnosticsSummary.click();
  await expect(fetcherDiagnostics).toHaveAttribute("open");
  await expect(metrics).toContainText("Browser fallback health");
  expect(fetcherMetricsRequestCount).toBe(fetcherMetricsRequestsAfterOpen);

  await page.unroute(fetcherMetricsRoute);
  await page.click("#admin-refresh-report-btn");
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
  await discoveryTab.click();
  await expect(discoveryTab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator("#admin-ops-tab-discovery")).toBeVisible();

  await sourcePolicyTab.click();
  await expect(sourcePolicyTab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator("#admin-ops-tab-source-policy")).toBeVisible();

  await registryConflictsTab.click();
  await expect(registryConflictsTab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator("#admin-ops-tab-registry-conflicts")).toBeVisible();

  await dedupTab.click();
  await expect(dedupTab).toHaveAttribute("aria-selected", "true");
  await expect(page.locator("#admin-ops-tab-dedup")).toBeVisible();
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
  seedPlaywrightTaskRuntime([
    {
      runId: fetchRunId,
      taskType: "fetch",
      status: "running",
      stage: "executing_sources",
      startedAt: nowIso,
      heartbeatAt: nowIso,
      ownerKind: "process",
      ownerPid: process.pid,
      taskProgress: fetchReport.taskProgress,
      summary: fetchReport.summary
    },
    {
      runId: discoveryRunId,
      taskType: "discovery",
      status: "running",
      stage: "probing_candidates",
      startedAt: nowIso,
      heartbeatAt: nowIso,
      ownerKind: "process",
      ownerPid: process.pid,
      taskProgress: discoveryReport.taskProgress,
      summary: discoveryReport.summary
    }
  ]);
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
