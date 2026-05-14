import { existsSync, mkdirSync, readFileSync, statSync, writeFileSync } from "fs";
import path from "path";
import { test, expect } from "@playwright/test";

const PERF_TRACE_DIR = path.resolve(
  process.env.BALUFFO_PERF_TRACE_DIR || path.join("_out", "perf-traces")
);

function readBridgeRuntimeMeta() {
  try {
    const payload = JSON.parse(
      readFileSync(path.resolve(".tmp", "playwright", "bridge-meta.json"), "utf8")
    );
    return {
      bridgeHost: String(payload?.bridgeHost || "127.0.0.1").trim() || "127.0.0.1",
      bridgePort: Number(payload?.bridgePort || 8877) || 8877
    };
  } catch {
    return {
      bridgeHost: "127.0.0.1",
      bridgePort: 8877
    };
  }
}

function desktopStartupProbeQuery() {
  const { bridgeHost, bridgePort } = readBridgeRuntimeMeta();
  const params = new URLSearchParams({
    desktop: "1",
    bridgePort: String(bridgePort),
    bridgeHost,
    startupProbe: "1"
  });
  return `?${params.toString()}`;
}

async function collectPerformanceSummary(page) {
  return page.evaluate((serializeFallback) => {
    function serialize(entry) {
      if (!entry) return {};
      if (typeof entry.toJSON === "function") {
        return entry.toJSON();
      }
      return {
        name: String(entry.name || ""),
        entryType: String(entry.entryType || ""),
        startTime: Number(entry.startTime || 0),
        duration: Number(entry.duration || 0),
        fallback: serializeFallback
      };
    }
    return {
      url: window.location.href,
      navigation: performance.getEntriesByType("navigation").map(serialize),
      paint: performance.getEntriesByType("paint").map(serialize),
      mark: performance.getEntriesByType("mark").map(serialize),
      measure: performance.getEntriesByType("measure").map(serialize)
    };
  }, true);
}

async function waitForPageReady(page, pageName) {
  if (pageName === "jobs") {
    await page.waitForFunction(() => {
      const state = document.body?.getAttribute("data-jobs-startup-state") || "loading";
      return state !== "loading";
    }, null, { timeout: 90_000 });
    await expect(page.locator("#jobs-list")).toBeVisible();
    return;
  }
  if (pageName === "admin") {
    await expect(page.locator("#admin-content")).toBeVisible({ timeout: 60_000 });
    return;
  }
  if (pageName === "saved") {
    await expect(page.locator("#saved-auth-sign-in-btn")).toBeVisible({ timeout: 60_000 });
    await expect(page.locator("#saved-source-status")).toBeVisible({ timeout: 60_000 });
  }
}

async function captureBootTrace({ context, page, pageName, targetPath }) {
  mkdirSync(PERF_TRACE_DIR, { recursive: true });
  const tracePath = path.join(PERF_TRACE_DIR, `${pageName}-boot-trace.zip`);
  const summaryPath = path.join(PERF_TRACE_DIR, `${pageName}-boot-summary.json`);
  const startedAt = new Date().toISOString();

  await context.tracing.start({ screenshots: true, snapshots: true });
  try {
    await page.goto(`${targetPath}${desktopStartupProbeQuery()}`);
    await waitForPageReady(page, pageName);
    const performanceSummary = await collectPerformanceSummary(page);
    const summary = {
      page: pageName,
      startedAt,
      finishedAt: new Date().toISOString(),
      targetPath,
      performance: performanceSummary
    };
    writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`, "utf8");
    await context.tracing.stop({ path: tracePath });

    expect(performanceSummary.navigation.length).toBeGreaterThan(0);
    expect(existsSync(tracePath)).toBe(true);
    expect(statSync(tracePath).size).toBeGreaterThan(0);
    expect(existsSync(summaryPath)).toBe(true);
  } catch (error) {
    await context.tracing.stop({ path: tracePath }).catch(() => {});
    throw error;
  }
}

test("perf trace: jobs boot", async ({ context, page }) => {
  await captureBootTrace({
    context,
    page,
    pageName: "jobs",
    targetPath: "/jobs.html"
  });
});

test("perf trace: admin boot", async ({ context, page }) => {
  await captureBootTrace({
    context,
    page,
    pageName: "admin",
    targetPath: "/admin.html"
  });
});

test("perf trace: saved boot", async ({ context, page }) => {
  await captureBootTrace({
    context,
    page,
    pageName: "saved",
    targetPath: "/saved.html"
  });
});
