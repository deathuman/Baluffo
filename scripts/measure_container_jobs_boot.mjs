import { mkdirSync, writeFileSync } from "node:fs";
import path from "node:path";
import process from "node:process";
import { chromium } from "@playwright/test";

// ponytail: single-purpose measurement driver; promote into perf_complete only
// if container Jobs boot needs recurring benchmarking beyond acceptance runs.

const FORBIDDEN_INITIAL = [
  { label: "jobs-unified-light.json", re: /\/data\/jobs-unified-light\.json(?:\?|$)/ },
  { label: "jobs-fetch-report.json", re: /\/data\/jobs-fetch-report\.json(?:\?|$)/ },
  { label: "/ops/task-state", re: /\/ops\/task-state\?/ },
  { label: "/ops/dashboard-health", re: /\/ops\/dashboard-health\?/ }
];

const BOOT_GRACE_MS = 2500;
const BOOT_TIMEOUT_MS = 90_000;
const RELOAD_TIMEOUT_MS = 25_000;
const SEED_ROW_FLOOR = 5_000;

function parseArgs(argv) {
  const args = { baseUrl: "http://127.0.0.1:8877", mode: "cold", outDir: "_out/perf-traces" };
  for (let i = 0; i < argv.length; i += 1) {
    if (argv[i] === "--base-url") args.baseUrl = argv[++i];
    else if (argv[i] === "--mode") args.mode = argv[++i];
    else if (argv[i] === "--out-dir") args.outDir = argv[++i];
  }
  if (!["cold", "warm", "nav"].includes(args.mode)) {
    throw new Error(`Unknown mode: ${args.mode} (expected cold|warm|nav)`);
  }
  return args;
}

const PERF_INIT_SNIPPET = () => {
  window.__baluffoPerf = { longTasks: [], shifts: [], lcp: [] };
  try {
    new PerformanceObserver(list => {
      for (const entry of list.getEntries()) {
        window.__baluffoPerf.longTasks.push({
          startTime: Math.round(entry.startTime),
          duration: Math.round(entry.duration)
        });
      }
    }).observe({ type: "longtask", buffered: true });
    new PerformanceObserver(list => {
      for (const entry of list.getEntries()) {
        window.__baluffoPerf.shifts.push({
          startTime: Math.round(entry.startTime),
          value: Number(entry.value.toFixed(5)),
          hadRecentInput: Boolean(entry.hadRecentInput),
          sources: (entry.sources || []).map(source => {
            const node = source.node;
            const describe = el => el
              ? `${el.tagName?.toLowerCase() || "?"}${el.id ? `#${el.id}` : ""}${el.className && typeof el.className === "string" ? `.${el.className.split(" ")[0]}` : ""}`
              : "?";
            return {
              node: describe(node),
              previous: source.previousRect ? `${Math.round(source.previousRect.width)}x${Math.round(source.previousRect.height)}@y${Math.round(source.previousRect.y)}` : "",
              current: source.currentRect ? `${Math.round(source.currentRect.width)}x${Math.round(source.currentRect.height)}@y${Math.round(source.currentRect.y)}` : ""
            };
          })
        });
      }
    }).observe({ type: "layout-shift", buffered: true });
    new PerformanceObserver(list => {
      for (const entry of list.getEntries()) {
        window.__baluffoPerf.lcp.push({
          startTime: Math.round(entry.startTime),
          size: entry.size,
          element: entry.element ? `${entry.element.tagName.toLowerCase()}${entry.element.className ? `.${String(entry.element.className).split(" ")[0]}` : ""}` : ""
        });
      }
    }).observe({ type: "largest-contentful-paint", buffered: true });
  } catch {
    // Observers are best-effort evidence; assertions fall back to request logs.
  }
};

async function waitForJobsInteractive(page) {
  const startedAt = Date.now();
  await page.waitForFunction(() => {
    const state = document.body?.getAttribute("data-jobs-startup-state") || "loading";
    return state !== "loading";
  }, null, { timeout: BOOT_TIMEOUT_MS });
  return Date.now() - startedAt;
}

function createRequestTracker(page) {
  const requests = [];
  let legLabel = "boot";
  page.on("request", request => {
    requests.push({
      url: request.url(),
      leg: legLabel,
      atMs: Date.now()
    });
  });
  return {
    requests,
    setLeg(label) {
      legLabel = label;
    },
    inWindow(urls, windowStartMs, windowEndMs) {
      return urls.filter(item => item.atMs >= windowStartMs && item.atMs <= windowEndMs);
    }
  };
}

function findForbidden(entries) {
  return FORBIDDEN_INITIAL.flatMap(({ label, re }) => (
    entries.filter(item => re.test(item.url)).map(item => ({ what: label, url: item.url }))
  ));
}

async function collectMetrics(page) {
  return page.evaluate(() => {
    const perf = window.__baluffoPerf || { longTasks: [], shifts: [], lcp: [] };
    const badge = document.querySelector('[data-ui="refresh-jobs-needed-badge"]');
    const notice = document.querySelector('[data-ui="guest-signin-notice"]');
    const guestCopy = document.querySelector('[data-ui="guest-signin-notice-guest"]');
    const profileCopy = document.querySelector('[data-ui="guest-signin-notice-profile"]');
    return {
      startupState: document.body?.getAttribute("data-jobs-startup-state") || "",
      sourceStatus: String(document.querySelector('[data-ui="source-status"]')?.textContent || ""),
      badgeVisible: Boolean(badge && !badge.classList.contains("hidden")),
      noticeVisible: Boolean(notice && !notice.hidden),
      guestCopyVisible: Boolean(guestCopy && !guestCopy.hidden),
      profileCopyVisible: Boolean(profileCopy && !profileCopy.hidden),
      clsTotal: Number(perf.shifts
        .filter(shift => !shift.hadRecentInput)
        .reduce((total, shift) => total + shift.value, 0)
        .toFixed(4)),
      shifts: perf.shifts,
      longTasks: [...perf.longTasks].sort((a, b) => b.duration - a.duration),
      lcp: perf.lcp.at(-1) || null,
      rowSampleCount: document.querySelectorAll("#jobs-list > *").length,
      perfMarks: performance.getEntriesByType("mark").map(entry => entry.name),
      perfMeasures: performance.getEntriesByType("measure").map(entry => ({
        name: entry.name,
        durationMs: Math.round(entry.duration)
      }))
    };
  });
}

async function seedWarmState(page) {
  return page.evaluate(async ({ signalId, rowFloor }) => {
    localStorage.removeItem("baluffo_jobs_auto_refresh_applied");
    localStorage.setItem("baluffo_jobs_auto_refresh_signal", JSON.stringify({
      id: signalId,
      source: "admin_fetcher",
      finishedAt: new Date().toISOString()
    }));

    const response = await fetch("data/jobs-unified-light.json");
    const payload = await response.json();
    let rows = Array.isArray(payload) ? payload : Array.isArray(payload?.jobs) ? payload.jobs : [];
    if (rows.length < rowFloor) {
      const expanded = [];
      while (expanded.length < rowFloor) {
        for (const row of rows) {
          expanded.push({ ...row, id: `${row.id}-x${expanded.length}` });
        }
      }
      rows = expanded.slice(0, rowFloor);
    }

    await new Promise((resolve, reject) => {
      const openRequest = indexedDB.open("baluffo_jobs_cache", 2);
      openRequest.onupgradeneeded = event => {
        const db = event.target.result;
        if (!db.objectStoreNames.contains("jobs_feed")) db.createObjectStore("jobs_feed");
        if (!db.objectStoreNames.contains("jobs_seen")) db.createObjectStore("jobs_seen");
      };
      openRequest.onsuccess = event => {
        const db = event.target.result;
        try {
          const tx = db.transaction("jobs_feed", "readwrite");
          tx.objectStore("jobs_feed").put({ jobs: rows, savedAt: Date.now() }, "latest");
          tx.oncomplete = () => { db.close(); resolve(rows.length); };
          tx.onerror = () => { db.close(); reject(tx.error); };
        } catch (error) {
          db.close();
          reject(error);
        }
      };
      openRequest.onerror = () => reject(openRequest.error);
    });
    return rows.length;
  }, { signalId: "perf-warm-signal-1", rowFloor: SEED_ROW_FLOOR });
}

function reportChecks(checks) {
  let failed = 0;
  for (const check of checks) {
    const mark = check.ok ? "PASS" : "FAIL";
    console.log(`[${mark}] ${check.name}${check.detail ? ` -- ${check.detail}` : ""}`);
    if (!check.ok) failed += 1;
  }
  return failed;
}

async function runBootMode(browser, args) {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.addInitScript(PERF_INIT_SNIPPET);
  const consoleLog = [];
  page.on("console", message => {
    if (!["error", "warning"].includes(message.type())) return;
    consoleLog.push(`[console.${message.type()}] ${message.text()}`);
  });
  page.on("pageerror", error => consoleLog.push(`[pageerror] ${String(error?.stack || error?.message || error)}`));

  let seededRowCount = 0;
  if (args.mode === "warm") {
    await page.goto(`${args.baseUrl}/jobs.html`, { waitUntil: "domcontentloaded" });
    seededRowCount = await seedWarmState(page);
    console.log(`[seed] IndexedDB cache rows: ${seededRowCount}; unapplied signal written`);
  }

  const tracker = createRequestTracker(page);
  const navStart = Date.now();
  await page.goto(`${args.baseUrl}/jobs.html`);
  const interactiveMs = await waitForJobsInteractive(page);
  const interactiveAtMs = Date.now();
  const bootWindowEnd = interactiveAtMs + BOOT_GRACE_MS;

  await page.waitForTimeout(BOOT_GRACE_MS);
  const metrics = await collectMetrics(page);

  // Acceptance contract: nothing heavy BEFORE interactive (bounded boot).
  // The deferred full-feed sync intentionally fetches light JSON after
  // interactive, so pre-interactive is the forbidden zone.
  const preInteractiveRequests = tracker.requests.filter(item => item.atMs <= interactiveAtMs);
  const forbiddenInBoot = findForbidden(preInteractiveRequests);
  const forbiddenAfterBoot = findForbidden(tracker.requests.filter(item => item.atMs > interactiveAtMs));
  const startupSnapshotHit = tracker.requests.some(item => /\/data\/jobs-unified-startup\.json(?:\?|$)/.test(item.url));

  const checks = [
    { name: "no pre-interactive jobs-unified-light.json request", ok: !forbiddenInBoot.some(item => item.what === "jobs-unified-light.json") },
    { name: "no pre-interactive jobs-fetch-report.json request", ok: !forbiddenInBoot.some(item => item.what === "jobs-fetch-report.json") },
    { name: "no pre-interactive /ops/task-state request", ok: !forbiddenInBoot.some(item => item.what === "/ops/task-state") },
    { name: "no pre-interactive /ops/dashboard-health request", ok: !forbiddenInBoot.some(item => item.what === "/ops/dashboard-health") },
    { name: "startup snapshot requested", ok: args.mode === "warm" ? true : startupSnapshotHit, detail: startupSnapshotHit ? undefined : "not observed (may be cached)" },
    { name: "interactive under 5000ms", ok: interactiveMs < 5000, detail: `${interactiveMs}ms` },
    { name: "CLS below 0.1", ok: metrics.clsTotal < 0.1, detail: `cls=${metrics.clsTotal}` },
    { name: "no long task above 200ms during boot", ok: metrics.longTasks.every(task => task.duration <= 200), detail: `max=${metrics.longTasks[0]?.duration ?? 0}ms` },
    { name: "source status reflects bounded boot", ok: /startup snapshot|local cache|Use Reload|Syncing full feed|Unified JSON light/i.test(metrics.sourceStatus), detail: metrics.sourceStatus },
    { name: "guest notice stays present", ok: metrics.noticeVisible, detail: `guest=${metrics.guestCopyVisible} profile=${metrics.profileCopyVisible}` },
    { name: "exactly one notice copy visible", ok: metrics.guestCopyVisible !== metrics.profileCopyVisible }
  ];

  if (args.mode === "warm") {
    checks.push({ name: "boot did not render from cache", ok: !/local cache/i.test(metrics.sourceStatus), detail: metrics.sourceStatus });
    const lightAfterInteractive = tracker.requests.some(item =>
      /\/data\/jobs-unified-light\.json/.test(item.url) && item.atMs > interactiveAtMs);
    checks.push({
      name: "reload-needed badge visible or auto-hydration already fetched the full feed",
      ok: metrics.badgeVisible || lightAfterInteractive,
      detail: metrics.badgeVisible ? "badge" : "auto-hydrated"
    });

    const reloadStart = Date.now();
    const lightDuringReload = new Promise(resolve => {
      const handler = request => {
        if (/\/data\/jobs-unified-light\.json/.test(request.url())) {
          page.off("request", handler);
          resolve(true);
        }
      };
      page.on("request", handler);
      setTimeout(() => {
        page.off("request", handler);
        resolve(false);
      }, RELOAD_TIMEOUT_MS);
    });
    await page.click("#refresh-jobs-btn");
    const lightFetched = await lightDuringReload;
    await page.waitForTimeout(1500);
    const afterReload = await collectMetrics(page);
    checks.push(
      { name: "explicit Reload fetches full feed", ok: lightFetched, detail: lightFetched ? `${Math.round(Date.now() - reloadStart)}ms` : "timeout" },
      { name: "badge clears after Reload", ok: !afterReload.badgeVisible }
    );
  }

  const summary = {
    mode: args.mode,
    baseUrl: args.baseUrl,
    capturedAt: new Date().toISOString(),
    seededRowCount,
    interactiveMs,
    preInteractiveRequests: preInteractiveRequests.map(item => item.url.replace(args.baseUrl, "")),
    forbiddenInBoot,
    forbiddenAfterBootInformational: forbiddenAfterBoot,
    consoleLog,
    ...metrics
  };

  mkdirSync(args.outDir, { recursive: true });
  const summaryPath = path.join(args.outDir, `container-jobs-${args.mode}-${Date.now()}.json`);
  writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
  console.log(`[summary] ${summaryPath}`);
  console.log(`[metrics] interactiveMs=${interactiveMs} cls=${metrics.clsTotal} maxLongTask=${metrics.longTasks[0]?.duration ?? 0}ms rowsRendered=${metrics.rowSampleCount} status="${metrics.sourceStatus}"`);

  await context.close();
  return reportChecks(checks);
}

async function runNavMode(browser, args) {
  const context = await browser.newContext();
  const page = await context.newPage();
  await page.addInitScript(PERF_INIT_SNIPPET);
  const tracker = createRequestTracker(page);

  await page.goto(`${args.baseUrl}/admin.html`, { waitUntil: "domcontentloaded" });
  await page.waitForSelector("#admin-content", { timeout: 60_000 });

  const legs = [];
  let failed = 0;
  for (const [index, leg] of ["admin-to-jobs", "jobs-to-admin-and-back"].entries()) {
    tracker.setLeg(`leg${index + 1}-${leg}`);
    const legStart = Date.now();
    await page.goto(`${args.baseUrl}/jobs.html`);
    const interactiveMs = await waitForJobsInteractive(page);
    const legInteractiveAtMs = Date.now();
    const legBootWindowEnd = legInteractiveAtMs + BOOT_GRACE_MS;
    await page.waitForTimeout(BOOT_GRACE_MS);
    const metrics = await collectMetrics(page);
    const legRequests = tracker.requests.filter(item => item.leg === `leg${index + 1}-${leg}`
      && item.atMs >= legStart && item.atMs <= legInteractiveAtMs);
    const forbiddenInBoot = findForbidden(legRequests);
    const check = {
      leg,
      interactiveMs,
      forbiddenInBoot,
      clsTotal: metrics.clsTotal,
      maxLongTask: metrics.longTasks[0]?.duration ?? 0
    };
    legs.push(check);
    failed += reportChecks([
      { name: `${leg}: no forbidden initial requests`, ok: forbiddenInBoot.length === 0, detail: forbiddenInBoot.map(item => item.what).join(",") || "none" },
      { name: `${leg}: interactive under 5000ms`, ok: interactiveMs < 5000, detail: `${interactiveMs}ms` },
      { name: `${leg}: no long task above 200ms`, ok: check.maxLongTask <= 200, detail: `max=${check.maxLongTask}ms` }
    ]);
    if (index === 0) {
      await page.goto(`${args.baseUrl}/admin.html`, { waitUntil: "domcontentloaded" });
      await page.waitForSelector("#admin-content", { timeout: 60_000 });
    }
  }

  mkdirSync(args.outDir, { recursive: true });
  const summaryPath = path.join(args.outDir, `container-jobs-nav-${Date.now()}.json`);
  writeFileSync(summaryPath, `${JSON.stringify({ mode: "nav", baseUrl: args.baseUrl, capturedAt: new Date().toISOString(), legs }, null, 2)}\n`, "utf8");
  console.log(`[summary] ${summaryPath}`);

  await context.close();
  return failed;
}

const args = parseArgs(process.argv.slice(2));
console.log(`[measure] mode=${args.mode} base=${args.baseUrl}`);
const browser = await chromium.launch();
try {
  const failures = args.mode === "nav"
    ? await runNavMode(browser, args)
    : await runBootMode(browser, args);
  console.log(failures === 0 ? "[result] ALL CHECKS PASSED" : `[result] ${failures} CHECK(S) FAILED`);
  process.exitCode = failures === 0 ? 0 : 1;
} finally {
  await browser.close();
}
