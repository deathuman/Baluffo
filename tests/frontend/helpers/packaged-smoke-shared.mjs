// Shared scaffolding for the packaged desktop smoke scenarios.
// Covers the duplicated BASE_URL/bridge config, report writer, navigation,
// slug/scenario construction, and scenario runner used across the
// packaged-desktop-smoke*.mjs scripts.

import assert from "node:assert/strict";
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

export async function readDesktopUpdatePanelState(page) {
  return page.evaluate(() => {
    const toggle = document.querySelector("#desktop-update-toggle-btn");
    const panel = document.querySelector("#desktop-update-panel");
    const title = document.querySelector("#desktop-update-title");
    return {
      togglePresent: Boolean(toggle),
      toggleHidden: toggle ? toggle.classList.contains("hidden") : null,
      toggleExpanded: String(toggle?.getAttribute("aria-expanded") || ""),
      toggleState: String(toggle?.dataset?.updateState || ""),
      panelPresent: Boolean(panel),
      panelHidden: panel ? panel.classList.contains("hidden") : null,
      panelState: String(panel?.dataset?.updateState || ""),
      panelTitle: String(title?.textContent || "").replace(/\s+/g, " ").trim(),
      panelBody: String(panel?.textContent || "").replace(/\s+/g, " ").trim().slice(0, 240)
    };
  });
}

// A click is occasionally delivered while the jobs header is still settling and
// is simply lost: Playwright reports a successful click, but the controller's
// handler never runs, so the panel stays hidden with the previous "Up to date"
// token. One missed click used to fail the whole packaged release. Retrying the
// toggle absorbs that race, but never past evidence of a real defect: a toggle
// that reports the panel open while the panel is hidden fails immediately with
// the DOM state attached instead of being retried into a green run.
// The page records every toggle click, the handler's verdict, and any
// panel-open/state divergence into the desktop startup-metric stream. Pulling
// those rows into a failure message turns the next dropped click into evidence
// (it survives the run directory being pruned).
async function readDesktopUpdateDiagnosticRows(page, limit = 60) {
  try {
    return await page.evaluate(async ({ base, max }) => {
      const response = await fetch(`${base}/desktop-local-data/startup-metrics?limit=${max}`);
      if (!response.ok) return null;
      const payload = await response.json();
      const rows = Array.isArray(payload?.rows) ? payload.rows : [];
      return rows
        .filter(row => String(row?.event || "").startsWith("jobs_desktop_update"))
        .map(row => ({ event: row.event, ...row.payload, ...(row.fields || {}) }));
    }, { base: BRIDGE_BASE, max: Math.max(1, Number(limit) || 60) });
  } catch {
    return null;
  }
}

export async function readDesktopUpdateClickDiagnostics(page, limit = 60) {
  const rows = await readDesktopUpdateDiagnosticRows(page, limit);
  return Array.isArray(rows) ? rows.slice(-10) : rows;
}

const DESKTOP_UPDATE_DIAGNOSTICS_INSTALLED_EVENT = "jobs_desktop_update_click_diagnostics_installed";
const DESKTOP_UPDATE_PANEL_DIVERGED_EVENT = "jobs_desktop_update_panel_state_diverged";

// The regression that swallowed the first toggle click recorded
// `jobs_desktop_update_panel_state_diverged` about 140 ms after boot and then
// rendered normally, so every later DOM snapshot looked healthy. Failing on the
// metric itself is what keeps that shape from ever passing a release smoke
// again. A run without the install record is a failure too: "no divergences"
// from a page that never installed the recorder is an absent measurement, not a
// passing one.
export async function assertNoDesktopUpdatePanelDivergence(page, { timeoutMs = 5_000, pollMs = 100 } = {}) {
  const deadline = Date.now() + Math.max(0, Number(timeoutMs) || 0);
  let rows;
  for (;;) {
    rows = await readDesktopUpdateDiagnosticRows(page, 400);
    if (Array.isArray(rows) && rows.some(row => row.event === DESKTOP_UPDATE_DIAGNOSTICS_INSTALLED_EVENT)) break;
    if (Date.now() >= deadline) break;
    await page.waitForTimeout(pollMs);
  }
  assert.notEqual(
    rows,
    null,
    "desktop update click diagnostics could not be read, so the smoke cannot prove the panel stayed coherent"
  );
  const diverged = rows.filter(row => row.event === DESKTOP_UPDATE_PANEL_DIVERGED_EVENT);
  assert.equal(
    diverged.length,
    0,
    `desktop update panel reported itself open while it rendered hidden, which turns the next toggle click into a close: ${JSON.stringify(diverged)}`
  );
  assert.equal(
    rows.some(row => row.event === DESKTOP_UPDATE_DIAGNOSTICS_INSTALLED_EVENT),
    true,
    "desktop update click diagnostics never installed, so a clean divergence count here would be an absence of data"
  );
  return { diagnosticsEvents: rows.length, divergences: 0 };
}

export async function openDesktopUpdatePanel(page, { attempts = 3, timeout = 5_000 } = {}) {
  const maxAttempts = Math.max(1, Number(attempts) || 1);
  const updatePanel = page.locator("#desktop-update-panel");
  // An update offered at launch legitimately auto-opens the panel. The defective
  // shape is the other one: a hidden panel whose toggle already claims it is
  // open, because the click then *closes* it and the panel looks dead.
  const startState = await readDesktopUpdatePanelState(page);
  assert.equal(
    startState.panelHidden === false || startState.toggleExpanded !== "true",
    true,
    `Desktop update panel started hidden while its toggle reported the panel open, so the first click would close it. State: ${JSON.stringify(startState)}`
  );
  const startedHidden = startState.panelHidden === true;
  let lastWaitError = null;
  let snapshot = null;
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    const updateToggle = page.locator("#desktop-update-toggle-btn");
    await updateToggle.waitFor({ state: "visible", timeout: 15_000 });
    assert.equal(await updateToggle.isEnabled(), true, "desktop update toggle should be enabled");
    // A panel that is already open must not be clicked again: another click
    // would close it. A late click from the previous attempt can land here too.
    let opened = await updatePanel.isVisible().catch(() => false);
    if (!opened) {
      await updateToggle.click();
      try {
        await updatePanel.waitFor({ state: "visible", timeout });
        opened = true;
      } catch (error) {
        lastWaitError = error;
      }
    }
    if (opened) {
      const state = await readDesktopUpdatePanelState(page);
      assert.equal(
        state.toggleExpanded,
        "true",
        `desktop update toggle should report the open panel. State: ${JSON.stringify(state)}`
      );
      return { attempts: attempt, state, startedHidden };
    }
    snapshot = await readDesktopUpdatePanelState(page).catch(error => ({
      evaluationError: error instanceof Error ? error.message : String(error)
    }));
    const detail = JSON.stringify(snapshot);
    if (snapshot?.toggleExpanded === "true") {
      throw new Error(
        `Desktop update panel stayed hidden while its toggle reported aria-expanded="true" (attempt ${attempt}/${maxAttempts}). State: ${detail}`
      );
    }
    if (attempt < maxAttempts) await page.waitForTimeout(250);
  }
  const waitDetail = lastWaitError instanceof Error ? ` Last wait error: ${lastWaitError.message}` : "";
  const diagnostics = await readDesktopUpdateClickDiagnostics(page);
  throw new Error(
    `Desktop update panel did not open after ${maxAttempts} toggle click(s). State: ${JSON.stringify(snapshot)}${waitDetail}`
    + (Array.isArray(diagnostics) && diagnostics.length ? ` Click diagnostics: ${JSON.stringify(diagnostics)}` : "")
  );
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
