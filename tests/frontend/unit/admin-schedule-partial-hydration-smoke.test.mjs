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
const OUT_ROOT = path.join(REPO_ROOT, "_out", "container-hydration-smoke");
const execFileAsync = promisify(execFile);
let bundleBuild = null;

async function buildBundle() {
  if (!bundleBuild) {
    bundleBuild = execFileAsync(
      process.execPath,
      [path.join(REPO_ROOT, "scripts", "build_container_frontend.mjs"), "--out-dir", OUT_ROOT],
      { cwd: REPO_ROOT }
    );
  }
  await bundleBuild;
}

function json(res, payload, status = 200) {
  res.writeHead(status, { "content-type": "application/json; charset=utf-8", "cache-control": "no-store" });
  res.end(`${JSON.stringify(payload)}\n`);
}

function contentType(filePath) {
  if (filePath.endsWith(".html")) return "text/html; charset=utf-8";
  if (filePath.endsWith(".js")) return "application/javascript; charset=utf-8";
  if (filePath.endsWith(".css")) return "text/css; charset=utf-8";
  return "application/octet-stream";
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
    res.writeHead(200, { "content-type": contentType(filePath), "cache-control": "no-store" });
    res.end(body);
    return true;
  } catch {
    return false;
  }
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const chunk of req) chunks.push(chunk);
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
  } catch {
    return {};
  }
}

function createServer() {
  const requests = [];
  const events = [];
  const metrics = [];
  let scheduleCalls = 0;
  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url || "/", "http://127.0.0.1");
    const started = Date.now();
    const event = { method: req.method || "GET", path: `${url.pathname}${url.search}`, status: 0, elapsedMs: 0 };
    events.push(event);
    requests.push(`${event.method} ${event.path}`);
    res.once("finish", () => {
      event.status = Number(res.statusCode || 0);
      event.elapsedMs = Date.now() - started;
    });
    if (url.pathname === "/frontend-runtime-config.js") {
      res.writeHead(200, { "content-type": "application/javascript; charset=utf-8", "cache-control": "no-store" });
      res.end("globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG = Object.freeze({ bridge: { sameOrigin: true }, runtime: { mode: 'container', localDataMode: 'bridge' } });\n");
      return;
    }
    if (url.pathname === "/tasks/jobs-pipeline-schedule") {
      scheduleCalls += 1;
      if (scheduleCalls === 1) {
        json(res, {
          ok: true,
          savedConfig: { schemaVersion: 1, enabled: true, intervalHours: 11 },
          status: { enabled: true, pending: false, due: false, scheduleDelayed: true, nextRunAt: "" }
        });
        return;
      }
      if (scheduleCalls >= 3) {
        json(res, {
          ok: true,
          summaryView: true,
          degraded: true,
          source: "container-gateway-fallback",
          savedConfig: { schemaVersion: 1, enabled: true, intervalHours: 11 },
          status: { enabled: true, pending: false, due: false, scheduleDelayed: true, scheduleAuthority: "degraded", nextRunAt: "" }
        });
        return;
      }
      await new Promise(resolve => setTimeout(resolve, 1200));
      json(res, {
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
    if (url.pathname === "/desktop-local-data/startup-metric" || url.pathname === "/desktop-local-data/startup-metrics/batch") {
      const body = await readJsonBody(req);
      if (url.pathname.endsWith("/batch")) metrics.push(...(Array.isArray(body?.metrics) ? body.metrics : []));
      else if (body?.event) metrics.push({ event: body.event, payload: body.payload || {} });
      json(res, { ok: true });
      return;
    }
    if (url.pathname === "/admin/bootstrap" || url.pathname === "/ops/dashboard-health") {
      json(res, { ok: true, degraded: true, summaryView: true, source: "container-gateway-fallback", ops: { schedule: {}, kpis: {} }, schedule: {}, kpis: {} });
      return;
    }
    if (url.pathname === "/ops/history") {
      json(res, { runs: [{ runId: "pipeline_seeded", taskType: "pipeline", lifecycleStatus: "succeeded", finishedAt: "2026-06-28T01:30:00Z", durationMs: 1800000 }] });
      return;
    }
    if (url.pathname === "/ops/fetch-kpis") {
      json(res, { ok: true, summaryView: true, kpis: { lastSuccessfulFetchAge: "6h", sevenDayFetchSuccessRate: 0.91, avgFetchDurationMs7d: 123456, failedSourceRatioLatest: 0.12, pendingSourcesCount: 812 } });
      return;
    }
    if (url.pathname === "/admin/ops-tab-counts") {
      json(res, { ok: true, summaryView: true, badges: { overview: { count: 0, loaded: true }, discovery: { count: 41, loaded: true }, "source-policy": { count: 2, loaded: true }, "registry-conflicts": { count: 0, loaded: true }, dedup: { count: 3, loaded: true } } });
      return;
    }
    if (["/app/ready", "/ops/health", "/sync/status", "/registry/conflicts", "/discovery/report", "/tasks/run-jobs-pipeline-status", "/ops/task-state"].includes(url.pathname)) {
      json(res, { ok: true, status: "healthy", active: false, tasks: [], summary: true });
      return;
    }
    if (url.pathname === "/desktop-local-data/admin/overview") {
      json(res, { ok: true, users: [{ userId: "local_andrea", name: "Andrea", savedJobs: 3 }] });
      return;
    }
    if (url.pathname === "/desktop-local-data/profiles") {
      json(res, { ok: true, profiles: [{ userId: "local_andrea", name: "Andrea", current: true }] });
      return;
    }
    if (url.pathname === "/registry/sources" || url.pathname === "/registry/summary") {
      json(res, {
        ok: true,
        activeCompact: true,
        degraded: true,
        source: "registry-json-compact-fallback",
        sources: { pending: [], active: [], rejected: [] },
        summary: { pendingCount: 813, activeCount: 2312, rejectedCount: 0 }
      });
      return;
    }
    const safe = decodeURIComponent(url.pathname === "/" ? "/admin.html" : url.pathname).replace(/^\/+/, "");
    if (safe.startsWith("container-assets/") && await serveFile(OUT_ROOT, safe.slice("container-assets/".length), res)) return;
    if (safe === "admin.html" && await serveFile(OUT_ROOT, safe, res)) return;
    if (await serveFile(REPO_ROOT, safe, res)) return;
    res.writeHead(404);
    res.end("not found");
  });
  return { server, requests, events, metrics };
}

function textState(page) {
  return page.evaluate(() => {
    const text = selector => document.querySelector(selector)?.textContent?.replace(/\s+/g, " ").trim() || "";
    const enabled = document.querySelector('[data-ui="admin-pipeline-schedule-enabled"]');
    const interval = document.querySelector('[data-ui="admin-pipeline-schedule-interval"]');
    return {
      schedule: text('[data-ui="admin-ops-schedule"]'),
      kpis: text('[data-ui="admin-ops-kpis"]'),
      history: text('[data-ui="admin-ops-history"]'),
      tabs: text(".admin-ops-tabs"),
      enabled: enabled ? { checked: enabled.checked, disabled: enabled.disabled } : null,
      interval: interval ? { value: interval.value, disabled: interval.disabled } : null
    };
  });
}

function evidence(events, metrics) {
  return [
    ...events.map(row => `${row.method} ${row.path} status=${row.status} elapsedMs=${row.elapsedMs}`),
    ...metrics.filter(row => String(row?.event || "").includes("admin_pipeline_schedule")).map(row => `${row.event} ${JSON.stringify(row.payload || {})}`)
  ].join("\n");
}

async function waitForScheduleCalls(events, count, timeoutMs = 20000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const scheduleCalls = events.filter(row => row.path.startsWith("/tasks/jobs-pipeline-schedule") && row.status >= 200 && row.status < 300).length;
    if (scheduleCalls >= count) return;
    await new Promise(resolve => setTimeout(resolve, 100));
  }
  assert.fail(`Timed out waiting for ${count} schedule calls.\n${evidence(events, [])}`);
}

test("admin schedule smoke renders saved config while next-run details refresh", async () => {
  await buildBundle();
  const harness = createServer();
  await new Promise(resolve => harness.server.listen(0, "127.0.0.1", resolve));
  const baseUrl = `http://127.0.0.1:${harness.server.address().port}`;
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  try {
    await page.goto(`${baseUrl}/admin.html?partialScheduleSmoke=1`);
    await page.locator("#admin-content").waitFor({ state: "visible", timeout: 15000 });
    await page.waitForFunction(() => /schedule details refreshing/i.test(document.querySelector('[data-ui="admin-ops-schedule"]')?.textContent || ""), null, { timeout: 10000 });
    let state = await textState(page);
    assert.equal(state.enabled.checked, true, evidence(harness.events, harness.metrics));
    assert.equal(state.enabled.disabled, false);
    assert.equal(state.interval.value, "11");
    assert.equal(state.interval.disabled, false);
    assert.doesNotMatch(state.schedule, /loading schedule|due now/i);

    await page.waitForFunction(() => /Pipeline:\s*every 11h, next/i.test(document.querySelector('[data-ui="admin-ops-schedule"]')?.textContent || ""), null, { timeout: 20000 });
    state = await textState(page);
    assert.match(state.schedule, /Pipeline:\s*every 11h, next/i);
    await waitForScheduleCalls(harness.events, 3);
    await page.waitForTimeout(250);
    state = await textState(page);
    assert.match(state.schedule, /Pipeline:\s*every 11h, next/i, evidence(harness.events, harness.metrics));
    assert.match(state.kpis, /Pending Sources\s*812/i);
    assert.doesNotMatch(state.kpis, /Loading latest fetch KPI/i);
    assert.doesNotMatch(state.tabs, /\.\.\./);
    assert.doesNotMatch(state.history, /No run history yet/i);
    assert.deepEqual(harness.requests.filter(request => /GET \/(registry\/sources|registry\/summary)/.test(request)), []);
    assert.deepEqual(harness.metrics.filter(row => row?.event === "admin_pipeline_schedule_model_fetch_done" && row?.payload?.ok === false), []);
    const loaded = harness.metrics.filter(row => row?.event === "admin_pipeline_schedule_model_loaded");
    let sawNext = false;
    for (const row of loaded) {
      const next = String(row?.payload?.nextRunAt || "");
      if (next) sawNext = true;
      assert.equal(sawNext && !next, false, `Schedule model regressed after loading nextRunAt.\n${evidence(harness.events, harness.metrics)}`);
    }
  } finally {
    await browser.close();
    await new Promise(resolve => harness.server.close(resolve));
  }
});
