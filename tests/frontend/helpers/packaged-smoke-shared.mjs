// Shared scaffolding for the packaged desktop smoke scenarios.
// Covers the duplicated BASE_URL/bridge config, report writer, navigation,
// slug/scenario construction, and scenario runner used across the
// packaged-desktop-smoke*.mjs scripts.

import fs from "node:fs/promises";
import path from "node:path";

export const BASE_URL = process.env.PACKAGED_DESKTOP_BASE_URL || "http://127.0.0.1:8080";
export const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";

const _bridgeUrl = new URL(BRIDGE_BASE);
export const BRIDGE_PORT = _bridgeUrl.port || "8877";
export const BRIDGE_HOST = _bridgeUrl.hostname || "127.0.0.1";

export function buildGotoDesktop({ bridgePort, bridgeHost, waitUntil } = {}) {
  const port = bridgePort ?? BRIDGE_PORT;
  const host = bridgeHost ?? BRIDGE_HOST;
  const navigationOptions = waitUntil ? { waitUntil } : undefined;
  return async function gotoDesktop(page, relativePath) {
    const separator = relativePath.includes("?") ? "&" : "?";
    await page.goto(
      `${BASE_URL}/${relativePath}${separator}desktop=1&bridgePort=${encodeURIComponent(port)}&bridgeHost=${encodeURIComponent(host)}`,
      navigationOptions
    );
  };
}

export function buildDesktopUrl(relativePath) {
  const separator = relativePath.includes("?") ? "&" : "?";
  return `${BASE_URL}/${relativePath}${separator}desktop=1&bridgePort=${encodeURIComponent(BRIDGE_PORT)}&bridgeHost=${encodeURIComponent(BRIDGE_HOST)}`;
}

export function buildWriteReport(reportPath) {
  return async function writeReport(report) {
    await fs.mkdir(path.dirname(reportPath), { recursive: true });
    await fs.writeFile(reportPath, `${JSON.stringify(report, null, 2)}\n`, "utf8");
  };
}

export function slugifyToken(value) {
  return (
    String(value || "")
      .toLowerCase()
      .replace(/[^a-z0-9]+/g, "-")
      .replace(/^-+|-+$/g, "") || "scenario"
  );
}

export function createScenario(name, base = {}) {
  return {
    name,
    slug: slugifyToken(name),
    status: "passed",
    durationMs: 0,
    error: "",
    details: {},
    ...base
  };
}

export async function runScenario(nameOrSlug, callback, scenarios) {
  const startedAt = Date.now();
  const scenario = createScenario(nameOrSlug);
  try {
    const result = await callback();
    if (result && typeof result === "object") {
      scenario.details = result;
    }
  } catch (error) {
    scenario.status = "failed";
    scenario.error = error instanceof Error ? error.message : String(error);
    throw error;
  } finally {
    scenario.durationMs = Date.now() - startedAt;
    scenarios.push(scenario);
  }
}
