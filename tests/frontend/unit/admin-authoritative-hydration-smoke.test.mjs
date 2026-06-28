import test from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import fs from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { chromium } from "@playwright/test";

const REPO_ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const CONTAINER_SMOKE_ROOT = path.join(REPO_ROOT, "_out", "container-hydration-smoke");
const execFileAsync = promisify(execFile);
let containerBundleBuild = null;

function jsonResponse(res, payload, status = 200) {
  const body = `${JSON.stringify(payload)}\n`;
  res.writeHead(status, {
    "content-type": "application/json; charset=utf-8",
    "cache-control": "no-store"
  });
  res.end(body);
}

function jsResponse(res, body) {
  res.writeHead(200, {
    "content-type": "application/javascript; charset=utf-8",
    "cache-control": "no-store"
  });
  res.end(body);
}

function contentTypeFor(filePath) {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".js") || filePath.endsWith(".mjs")) return "application/javascript; charset=utf-8";
  if (filePath.endsWith(".css")) return "text/css; charset=utf-8";
  if (filePath.endsWith(".svg")) return "image/svg+xml";
  if (filePath.endsWith(".ico")) return "image/x-icon";
  return "application/octet-stream";
}

async function buildContainerBundle() {
  if (!containerBundleBuild) {
    containerBundleBuild = execFileAsync(
      process.execPath,
      [path.join(REPO_ROOT, "scripts", "build_container_frontend.mjs"), "--out-dir", CONTAINER_SMOKE_ROOT],
      { cwd: REPO_ROOT }
    );
  }
  await containerBundleBuild;
}

async function serveFile(root, relativePath, res) {
  const filePath = path.resolve(root, relativePath);
  if (!filePath.startsWith(root)) {
    res.writeHead(403);
    res.end("forbidden");
    return true;
  }
  try {
    const body = await fs.readFile(filePath);
    res.writeHead(200, {
      "content-type": contentTypeFor(filePath),
      "cache-control": "no-store"
    });
    res.end(body);
    return true;
  } catch {
    return false;
  }
}

async function serveStatic(req, res, url) {
  const pathname = decodeURIComponent(url.pathname === "/" ? "/admin.html" : url.pathname);
  const safeRelative = pathname.replace(/^\/+/, "");
  if (safeRelative.startsWith("container-assets/")) {
    if (await serveFile(CONTAINER_SMOKE_ROOT, safeRelative.slice("container-assets/".length), res)) return;
  }
  if (safeRelative === "admin.html" && await serveFile(CONTAINER_SMOKE_ROOT, safeRelative, res)) {
    return;
  }
  if (await serveFile(REPO_ROOT, safeRelative, res)) return;
  res.writeHead(404);
  res.end("not found");
}

function createHydrationSmokeServer({ failAuthority = false } = {}) {
  const requests = [];
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url || "/", "http://127.0.0.1");
    requests.push(`${req.method || "GET"} ${url.pathname}${url.search}`);
    if (url.pathname === "/frontend-runtime-config.js") {
      jsResponse(res, "globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze({ bridge: { sameOrigin: true }, runtime: { mode: 'container', localDataMode: 'bridge' }, security: { github_app_enabled_default: true } });\n");
      return;
    }
    if (url.pathname === "/tasks/jobs-pipeline-schedule") {
      if (failAuthority) {
        jsonResponse(res, { ok: false, error: "schedule unavailable" }, 504);
        return;
      }
      jsonResponse(res, {
        ok: true,
        savedConfig: { schemaVersion: 1, enabled: true, intervalHours: 11 },
        status: {
          enabled: true,
          pending: false,
          due: false,
          nextRunAt: "2026-06-28T12:51:16Z",
          lastPipelineFinishedAt: "2026-06-28T01:51:16Z"
        }
      });
      return;
    }
    if (url.pathname === "/ops/history") {
      if (failAuthority) {
        jsonResponse(res, { ok: false, error: "history unavailable" }, 504);
        return;
      }
      jsonResponse(res, {
        runs: [{
          id: "pipeline_seeded",
          runId: "pipeline_seeded",
          type: "pipeline",
          taskType: "pipeline",
          status: "ok",
          lifecycleStatus: "succeeded",
          active: false,
          startedAt: "2026-06-28T01:00:00Z",
          finishedAt: "2026-06-28T01:30:00Z",
          durationMs: 1800000,
          summary: { outputCount: 46301, failedSources: 0 }
        }]
      });
      return;
    }
    if (url.pathname === "/admin/bootstrap") {
      jsonResponse(res, {
        ok: true,
        degraded: true,
        summaryView: true,
        source: "container-gateway-fallback",
        app: { ok: true, appVersion: "0.2.100", status: "healthy" },
        overview: { degraded: true, delayed: true },
        ops: {
          ok: true,
          status: "degraded",
          degraded: true,
          summaryView: true,
          alerts: [],
          kpis: {},
          schedule: {},
          scheduleDelayed: true
        },
        tasks: { current: [], recent: [], summary: true },
        sync: {},
        registrySummary: { summaryStatus: "unavailable", degraded: true, summary: {} },
        schedule: {}
      });
      return;
    }
    if (url.pathname === "/desktop-local-data/admin/overview") {
      jsonResponse(res, {
        ok: true,
        users: [{ userId: "local_andrea", name: "Andrea", savedJobs: 3 }],
        totals: { users: 1, savedJobs: 3, notesBytes: 0, attachmentCount: 0, attachmentBytes: 0, totalBytes: 0 }
      });
      return;
    }
    if (url.pathname === "/desktop-local-data/profiles") {
      jsonResponse(res, { ok: true, profiles: [{ userId: "local_andrea", name: "Andrea", current: true }] });
      return;
    }
    if (url.pathname === "/desktop-local-data/startup-metric" || url.pathname === "/desktop-local-data/startup-metrics/batch") {
      jsonResponse(res, { ok: true });
      return;
    }
    if (url.pathname === "/app/ready" || url.pathname === "/ops/health") {
      jsonResponse(res, { ok: true, status: "healthy", appVersion: "0.2.100", desktopMode: false, schedule: {} });
      return;
    }
    if (url.pathname === "/tasks/run-jobs-pipeline-status") {
      jsonResponse(res, { ok: true, active: false, stage: "completed", runId: "pipeline_seeded" });
      return;
    }
    if (url.pathname === "/ops/task-state") {
      jsonResponse(res, { tasks: [], count: 0, summary: true, pipeline: { active: false, stage: "completed" } });
      return;
    }
    if (url.pathname === "/ops/dashboard-health") {
      jsonResponse(res, {
        ok: true,
        status: "degraded",
        degraded: true,
        summaryView: true,
        alerts: [],
        kpis: {},
        kpisDelayed: true,
        schedule: {},
        scheduleDelayed: true
      });
      return;
    }
    if (url.pathname === "/sync/status") {
      jsonResponse(res, { ok: true, summaryView: true, configured: true, ready: true });
      return;
    }
    if (url.pathname === "/registry/conflicts") {
      jsonResponse(res, { ok: true, summaryView: true, summaryStatus: "unavailable", summary: {}, conflicts: [] });
      return;
    }
    if (url.pathname === "/ops/fetch-kpis") {
      jsonResponse(res, { ok: true, summaryView: true, kpis: { pendingSourcesCount: 812 } });
      return;
    }
    if (url.pathname === "/admin/ops-tab-counts") {
      jsonResponse(res, { ok: true, summaryView: true, badges: {} });
      return;
    }
    if (url.pathname === "/registry/sources") {
      jsonResponse(res, { ok: true, sources: { pending: [], active: [], rejected: [] }, summary: {} });
      return;
    }
    if (url.pathname === "/registry/summary") {
      jsonResponse(res, { ok: true, summary: {} });
      return;
    }
    if (url.pathname === "/discovery/report") {
      jsonResponse(res, { ok: true, summaryView: true, summary: {} });
      return;
    }
    await serveStatic(req, res, url);
  });
  return {
    requests,
    async start() {
      await new Promise(resolve => server.listen(0, "127.0.0.1", resolve));
      const address = server.address();
      return `http://127.0.0.1:${address.port}`;
    },
    async stop() {
      await new Promise(resolve => server.close(resolve));
    }
  };
}

async function withSmokePage(options, callback) {
  await buildContainerBundle();
  const server = createHydrationSmokeServer(options);
  const baseUrl = await server.start();
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(`${baseUrl}/admin.html?hydrationSmoke=1`);
    await callback({ page, requests: server.requests });
  } finally {
    await browser.close();
    await server.stop();
  }
}

test("admin hydration smoke renders authoritative schedule and activity despite degraded bootstrap", async () => {
  await withSmokePage({}, async ({ page, requests }) => {
    await page.locator("#admin-content").waitFor({ state: "visible", timeout: 15000 });
    await page.waitForFunction(() => /Pipeline:\s*every 11h, next/i.test(document.querySelector('[data-ui="admin-ops-schedule"]')?.textContent || ""), null, { timeout: 15000 });
    await page.waitForFunction(() => /Pipeline/i.test(document.querySelector('[data-ui="admin-ops-history"]')?.textContent || ""), null, { timeout: 15000 });

    const scheduleText = await page.locator('[data-ui="admin-ops-schedule"]').textContent();
    const historyText = await page.locator('[data-ui="admin-ops-history"]').textContent();
    const enabled = await page.locator('[data-ui="admin-pipeline-schedule-enabled"]').isChecked();
    const interval = await page.locator('[data-ui="admin-pipeline-schedule-interval"]').inputValue();
    const disabled = await page.locator('[data-ui="admin-pipeline-schedule-interval"]').isDisabled();

    assert.match(String(scheduleText || ""), /Pipeline:\s*every 11h, next/i);
    assert.equal(enabled, true);
    assert.equal(interval, "11");
    assert.equal(disabled, false);
    assert.doesNotMatch(String(scheduleText || ""), /loading schedule|due now/i);
    assert.doesNotMatch(String(scheduleText || ""), /Every\s*24\s*h/i);
    assert.doesNotMatch(String(historyText || ""), /No run history yet/i);
    assert.match(String(historyText || ""), /Pipeline/);
    assert.ok(requests.some(request => request.includes("GET /tasks/jobs-pipeline-schedule")));
    assert.ok(requests.some(request => request.includes("GET /ops/history?limit=2")));
    assert.deepEqual(
      requests.filter(request => /GET \/(ops\/fetch-kpis|admin\/ops-tab-counts|registry\/sources|registry\/summary)/.test(request)),
      []
    );
  });
});

test("admin hydration smoke keeps delayed states when authoritative schedule and history fail", async () => {
  await withSmokePage({ failAuthority: true }, async ({ page }) => {
    await page.locator("#admin-content").waitFor({ state: "visible", timeout: 15000 });
    await page.waitForFunction(() => /schedule delayed; retrying/i.test(document.querySelector('[data-ui="admin-ops-schedule"]')?.textContent || ""), null, { timeout: 15000 });
    await page.waitForFunction(() => /Activity delayed; retrying/i.test(document.querySelector('[data-ui="admin-ops-history"]')?.textContent || ""), null, { timeout: 15000 });

    const scheduleText = await page.locator('[data-ui="admin-ops-schedule"]').textContent();
    const historyText = await page.locator('[data-ui="admin-ops-history"]').textContent();
    const interval = await page.locator('[data-ui="admin-pipeline-schedule-interval"]').inputValue();
    const disabled = await page.locator('[data-ui="admin-pipeline-schedule-interval"]').isDisabled();

    assert.match(String(scheduleText || ""), /schedule delayed; retrying/i);
    assert.match(String(historyText || ""), /Activity delayed; retrying/i);
    assert.equal(interval, "");
    assert.equal(disabled, true);
    assert.doesNotMatch(String(scheduleText || ""), /due now|Every\s*24\s*h/i);
    assert.doesNotMatch(String(historyText || ""), /No run history yet/i);
  });
});
