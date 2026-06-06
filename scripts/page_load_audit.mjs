#!/usr/bin/env node
import { writeFile } from "node:fs/promises";
import { chromium } from "playwright";

function parseArgs(argv) {
  const options = {
    baseUrl: "http://127.0.0.1:8877",
    pages: ["admin.html", "jobs.html"],
    settleMs: 6500,
    slowRequestMs: 1000,
    output: ""
  };
  for (let index = 2; index < argv.length; index += 1) {
    const arg = argv[index];
    const next = argv[index + 1];
    if (arg === "--base-url" && next) {
      options.baseUrl = next;
      index += 1;
    } else if (arg === "--pages" && next) {
      options.pages = next.split(",").map(item => item.trim()).filter(Boolean);
      index += 1;
    } else if (arg === "--settle-ms" && next) {
      const value = Number(next);
      options.settleMs = Number.isFinite(value) ? Math.max(0, value) : options.settleMs;
      index += 1;
    } else if (arg === "--slow-request-ms" && next) {
      const value = Number(next);
      options.slowRequestMs = Number.isFinite(value) ? Math.max(0, value) : options.slowRequestMs;
      index += 1;
    } else if (arg === "--out" && next) {
      options.output = next;
      index += 1;
    } else if (arg === "--help" || arg === "-h") {
      options.help = true;
    } else {
      throw new Error(`Unknown or incomplete argument: ${arg}`);
    }
  }
  options.baseUrl = options.baseUrl.replace(/\/+$/, "");
  return options;
}

function usage() {
  return [
    "Usage: node scripts/page_load_audit.mjs [options]",
    "",
    "Options:",
    "  --base-url URL          App base URL, default http://127.0.0.1:8877",
    "  --pages LIST            Comma-separated page paths, default admin.html,jobs.html",
    "  --settle-ms N           Time to observe after DOMContentLoaded, default 6500",
    "  --slow-request-ms N     Slow request threshold, default 1000",
    "  --out PATH              Write JSON report to a file",
  ].join("\n");
}

function normalizePath(path) {
  const text = String(path || "").trim();
  if (!text) return "/";
  return text.startsWith("/") ? text : `/${text}`;
}

async function auditPage(browser, options, pagePath) {
  const context = await browser.newContext({ viewport: { width: 1366, height: 900 } });
  const page = await context.newPage();
  const requests = [];
  const failures = [];
  page.on("request", request => {
    requests.push({
      url: request.url(),
      method: request.method(),
      start: Date.now()
    });
  });
  page.on("requestfinished", async request => {
    const item = requests.find(row => row.url === request.url() && !row.done);
    if (!item) return;
    item.done = Date.now();
    item.ms = item.done - item.start;
    const response = await request.response();
    item.status = response?.status() ?? null;
  });
  page.on("requestfailed", request => {
    failures.push({
      url: request.url(),
      error: request.failure()?.errorText || "request failed"
    });
  });
  await page.addInitScript(() => {
    window.__baluffoLcpEntries = [];
    new PerformanceObserver(list => {
      window.__baluffoLcpEntries.push(
        ...list.getEntries().map(entry => ({
          startTime: entry.startTime,
          renderTime: entry.renderTime,
          loadTime: entry.loadTime,
          size: entry.size,
          id: entry.id,
          className: entry.element?.className || "",
          tagName: entry.element?.tagName || "",
          text: (entry.element?.innerText || entry.element?.textContent || "").slice(0, 160)
        }))
      );
    }).observe({ type: "largest-contentful-paint", buffered: true });
  });

  const targetUrl = `${options.baseUrl}${normalizePath(pagePath)}`;
  const startedAt = Date.now();
  await page.goto(targetUrl, { waitUntil: "domcontentloaded", timeout: 30000 });
  const domContentLoadedMs = Date.now() - startedAt;
  await page.waitForTimeout(options.settleMs);
  const browserEvidence = await page.evaluate(() => ({
    title: document.title,
    h1: document.querySelector("h1")?.innerText || "",
    paint: Object.fromEntries(
      performance.getEntriesByType("paint").map(entry => [entry.name, Math.round(entry.startTime)])
    ),
    lcp: window.__baluffoLcpEntries.at(-1) || null,
    marks: performance.getEntriesByType("mark").map(mark => ({
      name: mark.name,
      startTime: Math.round(mark.startTime)
    })),
    hasAdminOverviewError: /Could not load admin overview/i.test(document.body.innerText),
    hasSyncConfigMissing: /packaged_github_app_config|Missing packaged GitHub App config/i.test(document.body.innerText),
    jobCountText: (document.body.innerText.match(/Loaded [^\n]+ jobs[^\n]*/i) || [""])[0],
    showingText: (document.body.innerText.match(/Showing [^\n]+/i) || [""])[0]
  }));
  const slowRequests = requests
    .filter(row => Number(row.ms || 0) >= options.slowRequestMs)
    .map(row => ({
      url: row.url.replace(options.baseUrl, ""),
      method: row.method,
      ms: row.ms,
      status: row.status
    }))
    .sort((left, right) => right.ms - left.ms);
  await context.close();
  return {
    page: normalizePath(pagePath),
    url: targetUrl,
    domContentLoadedMs,
    ...browserEvidence,
    slowRequests,
    failures
  };
}

async function main() {
  const options = parseArgs(process.argv);
  if (options.help) {
    console.log(usage());
    return;
  }
  const browser = await chromium.launch({ headless: true });
  try {
    const pages = [];
    for (const pagePath of options.pages) {
      pages.push(await auditPage(browser, options, pagePath));
    }
    const report = {
      ok: true,
      generatedAt: new Date().toISOString(),
      baseUrl: options.baseUrl,
      settleMs: options.settleMs,
      slowRequestMs: options.slowRequestMs,
      pages
    };
    const json = JSON.stringify(report, null, 2);
    if (options.output) {
      await writeFile(options.output, `${json}\n`, "utf8");
    }
    console.log(json);
  } finally {
    await browser.close();
  }
}

main().catch(error => {
  console.error(JSON.stringify({ ok: false, error: String(error?.message || error) }, null, 2));
  process.exitCode = 1;
});
