import fs from "node:fs";
import path from "node:path";
import process from "node:process";
import { fileURLToPath } from "node:url";
import { chromium } from "@playwright/test";

// Manual UI review driver for the shared UI primitives work
// (docs/plans/shared-ui-primitives-plan.md). It is not a pass/fail test: it
// loads a RUNNING site, drives the served entry modules from inside the page,
// renders the states that are hard to reach by hand (a 10k-page pager, every
// Admin busy button at once) and records geometry plus screenshots so two runs
// can be compared. S1-S4 pass/fail lives in the node --test unit tests.
//
// Usage:
//   node scripts/ui_review_harness.mjs --site http://127.0.0.1:8080
//   node scripts/ui_review_harness.mjs --site http://127.0.0.1:8081 --full-page
//
// --site must serve jobs.html, admin.html, /styles, /frontend and /data, and it
// needs a loaded jobs feed; without one the Jobs steps wait out their timeout.
// --full-page screenshots the whole page instead of the viewport.
//
// The pager is re-rendered with getVisiblePages() imported from the served
// /frontend/jobs/app/pagination.js, and the Admin buttons go through the served
// syncAdminBusyUi(), so what is measured is the shipped code, not a copy.

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const DEFAULT_SITE = "http://127.0.0.1:8080";
const DEFAULT_OUT_DIR = "_out/ui-review";
const READY_TIMEOUT_MS = 90_000;
const THEMES = ["dark", "light"];
const PAGER_PAGE_CANDIDATES = [1, 4, 5, 6, 7, 300];
const PAGER_SHOT_PAGES = [1, 6, 300];

const ADMIN_BUSY_REFS = {
  adminRunFetcherBtnEl: '[data-ui="admin-run-fetcher-btn"]',
  adminRunFetcherIncrementalBtnEl: '[data-ui="admin-run-fetcher-incremental-btn"]',
  adminRunFetcherUncappedBtnEl: '[data-ui="admin-run-fetcher-uncapped-btn"]',
  adminRunFetcherForceBtnEl: '[data-ui="admin-run-fetcher-force-btn"]',
  adminRetryFailedBtnEl: '[data-ui="admin-retry-failed-btn"]',
  adminRefreshReportBtnEl: '[data-ui="admin-refresh-report-btn"]',
  adminRunDiscoveryBtnEl: '[data-ui="admin-run-discovery-btn"]',
  adminRunDiscoveryUncappedBtnEl: '[data-ui="admin-run-discovery-uncapped-btn"]',
  adminLoadDiscoveryBtnEl: '[data-ui="admin-load-discovery-btn"]',
  adminAddManualSourceBtnEl: '[data-ui="admin-add-manual-source-btn"]',
  adminApproveSourcesBtnEl: '[data-ui="admin-approve-sources-btn"]',
  adminRejectSourcesBtnEl: '[data-ui="admin-reject-sources-btn"]',
  adminSyncTestBtnEl: '[data-ui="admin-sync-test-btn"]',
  adminSyncPullBtnEl: '[data-ui="admin-sync-pull-btn"]',
  adminSyncPushBtnEl: '[data-ui="admin-sync-push-btn"]',
  adminSyncEnabledEl: '[data-ui="admin-sync-enabled"]',
  adminManualSourceUrlEl: '[data-ui="admin-manual-source-url"]',
  adminBulkBusyMessageEl: '[data-ui="admin-bulk-busy-message"]',
  adminSourceFilterBtnEls: ".admin-source-filter-btn",
  adminContentEl: '[data-ui="admin-content"]',
};

// ---- Everything below this line until the next marker runs INSIDE the page ----

// Swaps every module script for a blob shim that re-exports the same URL, which
// makes the module documents importable from an injected classic script.
function installModuleShim() {
  for (const element of document.querySelectorAll("script[type=module][src]")) {
    const absolute = new URL(element.getAttribute("src"), location.origin).href;
    const blob = new Blob([`export * from ${JSON.stringify(absolute)};\n`], {
      type: "text/javascript",
    });
    element.src = URL.createObjectURL(blob);
  }
  document.currentScript?.remove?.();
}

// Same markup as renderPagination() in frontend/jobs/app/runtime/list-view.js.
// The window arithmetic is never re-implemented here: it comes from
// window.__uiReviewPager, the served pagination module.
function renderPager(pageNo, totalPages) {
  const visible = window.__uiReviewPager.getVisiblePages(totalPages, pageNo);
  let html = "";
  if (pageNo > 1) {
    html += `<button class="page-btn" data-page="${pageNo - 1}" aria-label="Previous page">Prev</button>`;
  }
  for (const entry of visible) {
    if (entry === "...") html += '<span class="page-ellipsis">...</span>';
    else html += `<button class="page-btn ${entry === pageNo ? "active" : ""}" data-page="${entry}">${entry}</button>`;
  }
  if (pageNo < totalPages) {
    html += `<button class="page-btn" data-page="${pageNo + 1}" aria-label="Next page">Next</button>`;
  }
  const root = document.querySelector("#pagination");
  root.innerHTML = html;
  return root.children.length;
}

function measurePager() {
  const root = document.querySelector("#pagination");
  const cells = [...root.children];
  const prev = cells.find((cell) => cell.getAttribute("aria-label") === "Previous page");
  const next = cells.find((cell) => cell.getAttribute("aria-label") === "Next page");
  const numbers = cells.filter((cell) => cell.dataset.page && !cell.getAttribute("aria-label"));
  const active = cells.find((cell) => cell.classList.contains("active"));
  const x = (element) => (element ? Math.round(element.getBoundingClientRect().x) : null);
  return {
    cells: cells.length,
    active: active ? active.textContent : null,
    firstNumberX: numbers.length ? x(numbers[0]) : null,
    activeX: x(active),
    prevPresent: Boolean(prev),
    nextPresent: Boolean(next),
    ariaCurrentCount: cells.filter((cell) => cell.getAttribute("aria-current")).length,
    navLandmark: Boolean(root.closest("nav")) || root.tagName === "NAV",
  };
}

function measurePagerOverflow() {
  const root = document.querySelector("#pagination");
  const card = root.closest(".jobs-container") || document.body;
  const container = root.getBoundingClientRect();
  const bounds = card.getBoundingClientRect();
  const limit = Math.min(bounds.right, window.innerWidth);
  return {
    viewportWidth: window.innerWidth,
    pagerRight: Math.round(container.right),
    cardRight: Math.round(limit),
    overflowPx: Math.round(Math.max(0, container.right - limit)),
    cells: root.children.length,
  };
}

function measureJobsPage() {
  return {
    summary: document.querySelector("#results-summary")?.textContent || "",
    rows: document.querySelectorAll("#jobs-list .job-row").length,
    listHeight: Math.round(document.querySelector("#jobs-list").getBoundingClientRect().height),
    pagerSignature: document.querySelector("#pagination")?.dataset.contentSignature || "",
  };
}

function readPagerIdentity() {
  const root = document.querySelector("#pagination");
  const match = String(root?.dataset.contentSignature || "").match(/^p(\d+)c(\d+)$/);
  return {
    totalPages: match ? Number(match[1]) : 0,
    currentPage: match ? Number(match[2]) : 1,
  };
}

// Resolves the busy-state refs and calls the served syncAdminBusyUi().
function applyAdminBusy(call) {
  // busy-state.js iterates two multi-element refs; keep those arrays and let
  // every other selector resolve to a single element.
  const refs = {};
  for (const [name, selector] of Object.entries(call.labelMap)) {
    refs[name] = name.endsWith("Els") ? [...document.querySelectorAll(selector)] : document.querySelector(selector);
  }
  const singleRefs = Object.entries(refs).filter(([name]) => !name.endsWith("Els"));
  for (const [, element] of singleRefs) {
    if (element && element.nodeType === 1 && !element.dataset.idleLabel) {
      element.dataset.idleLabel = String(element.textContent || "").trim();
    }
  }
  const busyState = {
    fetcherRun: false,
    fetcherWatch: false,
    fetcherReportLoad: false,
    liveFetchRunning: false,
    discoveryRun: false,
    discoveryWatch: false,
    discoveryLoad: false,
    discoveryWrite: false,
    manualAdd: false,
    manualCheck: false,
    liveDiscoveryRunning: false,
    syncRun: false,
    liveSyncRunning: false,
    opsLoad: false,
    livePipelineRunning: false,
    ...call.overrides,
  };
  const snapshot = { missing: [], nodes: {} };
  try {
    window.__uiReviewBusy.syncAdminBusyUi({
      busyState,
      viewState: window.__uiReviewBusy.toAdminViewState(busyState, { isUnlocked: true }),
      fetcherPresetMeta: window.__uiReviewPresetMeta,
      refs,
      onSyncDiscoveryLogDisclosure() {},
    });
  } catch (error) {
    snapshot.error = String(error);
    return snapshot;
  }
  for (const [name, element] of singleRefs) {
    if (!name.endsWith("BtnEl")) continue;
    if (!element) {
      snapshot.missing.push(name);
      continue;
    }
    const rect = element.getBoundingClientRect();
    snapshot.nodes[name] = {
      text: String(element.textContent || "").trim(),
      disabled: Boolean(element.disabled),
      ariaDisabled: element.getAttribute("aria-disabled"),
      ariaBusy: element.getAttribute("aria-busy"),
      width: Math.round(rect.width),
      height: Math.round(rect.height),
    };
  }
  return snapshot;
}

// ---- Harness process ----

function parseArgs(argv) {
  const args = {
    site: DEFAULT_SITE,
    outDir: DEFAULT_OUT_DIR,
    fullPage: false,
    debug: Boolean(process.env.REVIEW_DEBUG),
  };
  for (let index = 0; index < argv.length; index += 1) {
    const flag = argv[index];
    if (flag === "--site") args.site = String(argv[++index] || "").trim() || DEFAULT_SITE;
    else if (flag === "--out-dir") args.outDir = String(argv[++index] || "").trim() || DEFAULT_OUT_DIR;
    else if (flag === "--full-page") args.fullPage = true;
    else if (flag === "--debug") args.debug = true;
    else if (flag === "--help" || flag === "-h") {
      console.log("usage: node scripts/ui_review_harness.mjs [--site <url>] [--out-dir <dir>] [--full-page] [--debug]");
      process.exit(0);
    } else throw new Error(`unknown argument: ${flag}`);
  }
  args.site = args.site.replace(/\/+$/, "");
  return args;
}

function busyWidthSummary(idle, running) {
  const rows = [];
  for (const [name, idleView] of Object.entries(idle?.nodes || {})) {
    const runningView = running?.nodes?.[name];
    if (!runningView) continue;
    rows.push({
      button: name,
      idleText: idleView.text,
      runText: runningView.text,
      idleWidth: idleView.width,
      runWidth: runningView.width,
      delta: runningView.width - idleView.width,
      disabled: runningView.disabled,
      ariaDisabled: runningView.ariaDisabled,
    });
  }
  return {
    rows,
    changedCount: rows.filter((row) => row.delta !== 0).length,
    maxAbsDelta: rows.reduce((max, row) => Math.max(max, Math.abs(row.delta)), 0),
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const outDir = path.resolve(REPO_ROOT, args.outDir);
  fs.mkdirSync(outDir, { recursive: true });
  const report = { site: args.site, generatedAt: new Date().toISOString(), screenshots: [] };

  const browser = await chromium.launch();
  try {
    const newPage = async ({ width, height, theme }) => {
      const context = await browser.newContext({
        viewport: { width, height },
        deviceScaleFactor: 1,
        colorScheme: theme,
      });
      await context.addInitScript(
        ([key, value]) => localStorage.setItem(key, value),
        ["baluffo_theme", theme]
      );
      await context.route("**/__ui-review/shim.js", (route) =>
        route.fulfill({ contentType: "text/javascript", body: `(${installModuleShim.toString()})();` })
      );
      const page = await context.newPage();
      page.setDefaultTimeout(READY_TIMEOUT_MS);
      if (args.debug) {
        page.on("console", (message) => console.log("[console]", message.type(), String(message.text()).slice(0, 300)));
        page.on("pageerror", (error) => console.log("[pageerror]", String(error).slice(0, 600)));
        page.on("response", (response) => {
          if (response.status() >= 400) console.log("[http]", response.status(), response.url().slice(0, 160));
        });
      }
      return page;
    };

    // Rows paint from the startup preview before the full feed lands; wait until
    // the pager signature stops describing only the preview.
    const waitForJobsRows = async (page) => {
      await page.waitForFunction(
        () => document.querySelectorAll("#jobs-list .job-row").length > 0
          || String(document.querySelector("#jobs-list")?.textContent || "").length > 40
      );
      await page
        .waitForFunction(
          () => Number(String(document.querySelector("#pagination")?.dataset.contentSignature || "").replace(/^p(\d+)c.*$/, "$1") || 0) > 9,
          undefined,
          { timeout: READY_TIMEOUT_MS }
        )
        .catch(() => {});
    };

    const screenshot = async (page, name, clipSelector) => {
      const file = path.join(outDir, `${name}.png`);
      const relative = path.relative(REPO_ROOT, file).replaceAll("\\", "/");
      try {
        const element = clipSelector ? await page.$(clipSelector) : null;
        if (element) {
          await element.scrollIntoViewIfNeeded();
          const box = await element.boundingBox();
          const viewport = page.viewportSize();
          if (box) {
            const clipX = Math.max(0, Math.min(box.x - 8, viewport.width - 40));
            const clipY = Math.max(0, Math.min(box.y - 8, viewport.height - 40));
            await page.screenshot({
              path: file,
              clip: {
                x: clipX,
                y: clipY,
                width: Math.min(box.width + 16, viewport.width - clipX),
                height: Math.min(box.height + 16, viewport.height - clipY),
              },
            });
            report.screenshots.push({ name, file: relative, clip: true });
            return;
          }
        }
        await page.screenshot({ path: file, fullPage: args.fullPage });
        report.screenshots.push({ name, file: relative, clip: false });
      } catch (error) {
        console.log("[shot-failed]", name, String(error?.message || error).slice(0, 200));
      }
    };

    const openJobs = async (page, theme) => {
      await page.goto(`${args.site}/jobs.html`, { waitUntil: "domcontentloaded" });
      await page.evaluate(([key, value]) => localStorage.setItem(key, value), ["baluffo_theme", theme]);
      await page.reload({ waitUntil: "domcontentloaded" });
      await page.addScriptTag({ url: "/__ui-review/shim.js" });
      await waitForJobsRows(page);
      await page.waitForTimeout(1500);
    };

    // ---- Jobs: default view in both themes; page 3 survives a reload ----
    for (const theme of THEMES) {
      const page = await newPage({ width: 1365, height: 900, theme });
      await openJobs(page, theme);
      report[`jobs_${theme}`] = await page.evaluate(
        (source) => (0, eval)(`(${source})`)(),
        measureJobsPage.toString()
      );
      if (theme === "dark") {
        await page.evaluate(() => document.querySelector('#pagination .page-btn[data-page="3"]')?.click());
        await page.waitForTimeout(800);
        report.jobs_before_reload = await page.evaluate(
          (source) => (0, eval)(`(${source})`)(),
          measureJobsPage.toString()
        );
        await page.reload({ waitUntil: "domcontentloaded" });
        await page.addScriptTag({ url: "/__ui-review/shim.js" });
        await waitForJobsRows(page);
        await page.waitForTimeout(1500);
        report.jobs_after_reload = await page.evaluate(
          (source) => (0, eval)(`(${source})`)(),
          measureJobsPage.toString()
        );
      }
      await screenshot(page, `jobs-${theme}-1365`);
      await page.close();
    }

    // ---- Pager: cell count and geometry at every window position ----
    {
      const page = await newPage({ width: 1365, height: 900, theme: "dark" });
      await openJobs(page, "dark");
      await page.addScriptTag({
        type: "module",
        content: `import * as pager from "/frontend/jobs/app/pagination.js"; window.__uiReviewPager = pager;`,
      });
      await page.waitForFunction(() => Boolean(window.__uiReviewPager?.getVisiblePages), null, { timeout: 20_000 });

      // The page count depends on how much of the feed has bootstrapped. The
      // startup preview paints a tiny pager first (its signature is the real
      // count), so wait until the signature is large instead of clicking: page
      // buttons move under the pointer once the real window replaces it.
      await page
        .waitForFunction(
          (source) => (0, eval)(`(${source})`)().totalPages > 9,
          readPagerIdentity.toString(),
          { timeout: READY_TIMEOUT_MS }
        )
        .catch(() => {});
      const identity = await page.evaluate(
        (source) => (0, eval)(`(${source})`)(),
        readPagerIdentity.toString()
      );
      const totalPages = Math.max(identity.totalPages, 2);
      report.pager = { totalPages, summary: report.jobs_dark?.summary || "", pages: [] };

      const pagesToWalk = [...PAGER_PAGE_CANDIDATES];
      if (totalPages > 7) {
        pagesToWalk.push(totalPages - 4, totalPages - 3, totalPages - 2, totalPages - 1, totalPages);
      }
      for (const pageNo of pagesToWalk) {
        if (pageNo < 1 || pageNo > totalPages) continue;
        const cellCount = await page.evaluate(
          ({ source, target, count }) => (0, eval)(`(${source})`)(target, count),
          { source: renderPager.toString(), target: pageNo, count: totalPages }
        );
        const geometry = await page.evaluate(
          (source) => (0, eval)(`(${source})`)(),
          measurePager.toString()
        );
        report.pager.pages.push({ page: pageNo, ...geometry });
        if (cellCount > 12) report.pager.warning = `renderPager produced ${cellCount} cells`;
        if (PAGER_SHOT_PAGES.includes(pageNo)) {
          await page.waitForTimeout(120);
          await screenshot(page, `pager-${pageNo}-dark`, "#pagination");
        }
      }
      const spreadOf = (key) => {
        const values = report.pager.pages.map((entry) => entry[key]).filter((value) => value !== null);
        return values.length ? Math.max(...values) - Math.min(...values) : null;
      };
      report.pager.spread = { firstNumberX: spreadOf("firstNumberX"), activeX: spreadOf("activeX") };

      // Narrow viewport: the pager must stay inside its card.
      await page.setViewportSize({ width: 430, height: 900 });
      await page.waitForTimeout(400);
      report.pager.narrow = await page.evaluate(
        (source) => (0, eval)(`(${source})`)(),
        measurePagerOverflow.toString()
      );
      await screenshot(page, "pager-430-dark", "#pagination");
      await page.close();
    }

    // ---- Admin: the served syncAdminBusyUi() against shipped CSS ----
    {
      const page = await newPage({ width: 1365, height: 900, theme: "dark" });
      await page.goto(`${args.site}/admin.html`, { waitUntil: "domcontentloaded" });
      await page.evaluate(() => localStorage.setItem("baluffo_theme", "dark"));
      await page.reload({ waitUntil: "domcontentloaded" });
      await page.addScriptTag({ url: "/__ui-review/shim.js" });
      await page.waitForSelector('[data-ui="admin-run-discovery-btn"]', { timeout: 30_000 });
      await page.waitForTimeout(3000);
      await page.evaluate(async () => {
        window.__uiReviewBusy = await import("/frontend/admin/app/busy-state.js");
        window.__uiReviewPresetMeta = (await import("/frontend/admin/app/fetcher.js")).FETCHER_PRESET_META;
      });
      await page.waitForFunction(() => Boolean(window.__uiReviewBusy?.syncAdminBusyUi), null, { timeout: 30_000 });
      // Admin can ship its task controls behind the unlock shell; reveal the
      // cards so their geometry can be measured without touching busy logic.
      await page.evaluate(() => {
        for (const selector of ["#admin-locked-shell", '[data-ui="admin-content"]', ".admin-card", "#admin-unlock-card"]) {
          for (const element of document.querySelectorAll(selector)) element.classList.remove("hidden");
        }
      });
      const applyState = (overrides) =>
        page.evaluate(
          ({ source, call }) => (0, eval)(`(${source})`)(call),
          { source: applyAdminBusy.toString(), call: { overrides, labelMap: ADMIN_BUSY_REFS } }
        );
      const idle = await applyState({});
      const running = await applyState({
        fetcherWatch: true,
        liveFetchRunning: true,
        discoveryWatch: true,
        liveDiscoveryRunning: true,
      });
      report.admin = {
        idleMissing: idle.missing,
        runningMissing: running.missing,
        errors: [idle.error, running.error].filter(Boolean),
        idle,
        running,
        widthSummary: busyWidthSummary(idle, running),
      };
      await screenshot(page, "admin-1365");
      await page.close();
    }

    // ---- Narrow Jobs layouts ----
    for (const width of [900, 430]) {
      const page = await newPage({ width, height: width === 430 ? 900 : 800, theme: "dark" });
      await openJobs(page, "dark");
      await screenshot(page, `jobs-${width}-dark`);
      await page.close();
    }
  } finally {
    await browser.close();
  }

  fs.writeFileSync(path.join(outDir, "report.json"), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        site: report.site,
        outDir: path.relative(REPO_ROOT, outDir).replaceAll("\\", "/"),
        screenshots: report.screenshots.map((entry) => entry.name),
        jobs: { dark: report.jobs_dark, light: report.jobs_light, beforeReload: report.jobs_before_reload, afterReload: report.jobs_after_reload },
        pager: report.pager
          ? {
              totalPages: report.pager.totalPages,
              cells: report.pager.pages.map((entry) => `${entry.page}:${entry.cells}`),
              spread: report.pager.spread,
              ariaCurrent: report.pager.pages.every((entry) => entry.ariaCurrentCount > 0),
              nav: report.pager.pages.every((entry) => entry.navLandmark),
              narrow: report.pager.narrow,
            }
          : null,
        admin: report.admin
          ? {
              missing: [...report.admin.idleMissing, ...report.admin.runningMissing],
              errors: report.admin.errors,
              changedCount: report.admin.widthSummary.changedCount,
              maxAbsDelta: report.admin.widthSummary.maxAbsDelta,
              deltas: report.admin.widthSummary.rows
                .filter((row) => row.delta !== 0)
                .map((row) => `${row.button} ${row.idleWidth}->${row.runWidth}`),
            }
          : null,
      },
      null,
      2
    )
  );
}

await main();
