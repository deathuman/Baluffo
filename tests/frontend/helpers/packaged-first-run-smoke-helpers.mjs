import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";

function isRetryableBridgeRequestError(error) {
  return /ECONNREFUSED|ECONNRESET|ECONNABORTED|ETIMEDOUT|socket hang up/i.test(
    String(error?.message || error || "")
  );
}

export async function bridgeRequestWithRetry(apiRequest, method, url, options = {}) {
  const deadline = Date.now() + 30_000;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      return await apiRequest[method](url, options);
    } catch (error) {
      lastError = error;
      if (!isRetryableBridgeRequestError(error)) throw error;
      await new Promise(resolve => setTimeout(resolve, 500));
    }
  }
  throw lastError || new Error(`Bridge ${method.toUpperCase()} request timed out: ${url}`);
}

export async function fetchBridgeJson(apiRequest, bridgeBase, relativePath, label) {
  const response = await bridgeRequestWithRetry(apiRequest, "get", `${bridgeBase}${relativePath}`);
  assert.equal(response.ok(), true, `${label} request should succeed`);
  return response.json();
}

export async function postBridgeJson(
  apiRequest,
  bridgeBase,
  relativePath,
  data,
  label,
  allowedStatuses = []
) {
  const response = await bridgeRequestWithRetry(apiRequest, "post", `${bridgeBase}${relativePath}`, { data });
  const status = response.status();
  assert.equal(
    response.ok() || allowedStatuses.includes(status),
    true,
    `${label} request should succeed`
  );
  return response.json();
}

export async function waitUntil(label, callback, timeoutMs = 30_000, intervalMs = 250) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      const result = await callback();
      if (result) return result;
    } catch (error) {
      lastError = error;
    }
    await new Promise(resolve => setTimeout(resolve, intervalMs));
  }
  throw new Error(`${label} timed out${lastError ? `: ${lastError.message || lastError}` : ""}`);
}

export function coverageScope(payload) {
  const runtime = payload?.runtime && typeof payload.runtime === "object" ? payload.runtime : {};
  const summary = payload?.summary && typeof payload.summary === "object" ? payload.summary : {};
  return String(summary.coverageScope || runtime.coverageScope || "").trim();
}

export function isActiveTaskRow(row) {
  const finishedAt = String(row?.finishedAt || "").trim();
  const status = String(row?.status || row?.lifecycleStatus || "").toLowerCase();
  return Boolean(row?.active) || (!finishedAt && ["running", "starting"].includes(status));
}

export async function setThemeAndViewport(page, theme, viewport) {
  await page.setViewportSize({ width: viewport.width, height: viewport.height });
  await page.evaluate(nextTheme => {
    document.documentElement.setAttribute("data-theme", nextTheme);
    document.body?.setAttribute("data-theme", nextTheme);
  }, theme);
  await page.waitForTimeout(250);
}

async function waitForPopupReady(page) {
  await page.waitForFunction(() => {
    const overlay = document.querySelector(".popup-overlay.popup-overlay-visible");
    const panel = document.querySelector(".popup.popup-visible");
    if (!overlay || !panel) return false;
    const overlayStyle = getComputedStyle(overlay);
    const panelStyle = getComputedStyle(panel);
    const overlayRect = overlay.getBoundingClientRect();
    const panelRect = panel.getBoundingClientRect();
    return overlayStyle.display !== "none"
      && panelStyle.display !== "none"
      && Number(overlayStyle.opacity || 0) >= 0.95
      && Number(panelStyle.opacity || 0) >= 0.95
      && overlayRect.width > 4
      && overlayRect.height > 4
      && panelRect.width > 4
      && panelRect.height > 4;
  }, { timeout: 5_000 });
}

async function collectPopupStyles(page, label) {
  return page.evaluate(snapshotLabel => {
    function parseColor(value) {
      const match = String(value || "").match(/rgba?\(([^)]+)\)/i);
      if (!match) return { r: 0, g: 0, b: 0, a: 1, raw: String(value || "") };
      const parts = match[1].split(",").map(part => Number.parseFloat(part.trim()));
      return {
        r: parts[0] || 0,
        g: parts[1] || 0,
        b: parts[2] || 0,
        a: Number.isFinite(parts[3]) ? parts[3] : 1,
        raw: String(value || "")
      };
    }
    function describe(selector) {
      const element = document.querySelector(selector);
      if (!element) return { selector, present: false, visible: false };
      const style = getComputedStyle(element);
      const rect = element.getBoundingClientRect();
      const visible = style.display !== "none"
        && style.visibility !== "hidden"
        && Number(style.opacity || 1) > 0.05
        && rect.width > 4
        && rect.height > 4;
      return {
        selector,
        present: true,
        visible,
        color: parseColor(style.color),
        backgroundColor: parseColor(style.backgroundColor),
        backgroundImage: String(style.backgroundImage || ""),
        opacity: Number(style.opacity || 1),
        rect: {
          x: rect.x,
          y: rect.y,
          width: rect.width,
          height: rect.height,
          right: rect.right,
          bottom: rect.bottom
        }
      };
    }
    return {
      label: snapshotLabel,
      theme: document.documentElement.getAttribute("data-theme") || "",
      viewport: { width: window.innerWidth, height: window.innerHeight },
      overlay: describe(".popup-overlay.popup-overlay-visible"),
      panel: describe(".popup.popup-visible"),
      primary: describe(".popup.popup-visible .popup-btn-primary"),
      secondary: describe(".popup.popup-visible .popup-btn-secondary"),
      tertiary: describe(".popup.popup-visible .popup-btn-tertiary"),
      input: describe(".popup.popup-visible .local-auth-dialog-input"),
      select: describe(".popup.popup-visible .local-auth-dialog-select")
    };
  }, label);
}

function luminance(color) {
  const channels = [color.r, color.g, color.b].map(value => {
    const normalized = Math.max(0, Math.min(255, Number(value || 0))) / 255;
    return normalized <= 0.03928
      ? normalized / 12.92
      : ((normalized + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * channels[0] + 0.7152 * channels[1] + 0.0722 * channels[2];
}

function contrastRatio(foreground, background) {
  const lighter = Math.max(luminance(foreground), luminance(background));
  const darker = Math.min(luminance(foreground), luminance(background));
  return (lighter + 0.05) / (darker + 0.05);
}

function assertElementVisible(snapshot, key) {
  assert.equal(snapshot[key]?.present, true, `${snapshot.label} ${key} should be present`);
  assert.equal(snapshot[key]?.visible, true, `${snapshot.label} ${key} should be visible`);
}

function assertPopupStyles(snapshot, options = {}) {
  const { requireSecondary, requireTertiary, requireInput, requireSelect } = options;
  assertElementVisible(snapshot, "overlay");
  assertElementVisible(snapshot, "panel");
  assertElementVisible(snapshot, "primary");
  if (requireSecondary) assertElementVisible(snapshot, "secondary");
  if (requireTertiary) assertElementVisible(snapshot, "tertiary");
  if (requireInput) assertElementVisible(snapshot, "input");
  if (requireSelect) assertElementVisible(snapshot, "select");
  assert.ok(snapshot.overlay.backgroundColor.a >= 0.2, `${snapshot.label} overlay should dim`);
  const rect = snapshot.panel.rect;
  assert.ok(rect.x >= -1 && rect.y >= -1, `${snapshot.label} panel origin should fit`);
  assert.ok(rect.right <= snapshot.viewport.width + 1, `${snapshot.label} panel width should fit`);
  assert.ok(rect.bottom <= snapshot.viewport.height + 1, `${snapshot.label} panel height should fit`);
  if (snapshot.theme === "light") {
    const bg = snapshot.panel.backgroundColor;
    assert.ok(bg.a >= 0.85 && bg.r >= 225 && bg.g >= 225 && bg.b >= 225, `${snapshot.label} light panel should be solid`);
  }
  assert.ok(
    contrastRatio(snapshot.panel.color, snapshot.panel.backgroundColor) >= 4.0,
    `${snapshot.label} panel text contrast should be readable`
  );
  for (const key of ["primary", requireSecondary && "secondary", requireTertiary && "tertiary"].filter(Boolean)) {
    assert.ok(
      snapshot[key].backgroundColor.a >= 0.05 || snapshot[key].backgroundImage !== "none",
      `${snapshot.label} ${key} should have a visible button surface`
    );
  }
  for (const key of [requireInput && "input", requireSelect && "select"].filter(Boolean)) {
    assert.ok(
      contrastRatio(snapshot[key].color, snapshot[key].backgroundColor) >= 2.5,
      `${snapshot.label} ${key} contrast should be readable`
    );
  }
}

export async function capturePopup(page, styles, outputDir, kind, theme, viewport, styleOptions = {}) {
  const label = `${kind}-${theme}-${viewport.name}`;
  await waitForPopupReady(page);
  const snapshot = await collectPopupStyles(page, label);
  const screenshotPath = path.join(outputDir, `${label}.png`);
  await fs.mkdir(outputDir, { recursive: true });
  await page.screenshot({ path: screenshotPath, fullPage: false });
  styles.push({ ...snapshot, screenshotPath });
  assertPopupStyles(snapshot, styleOptions);
  return screenshotPath;
}

export async function collectAndAssertPopupStyles(page, styles, label, styleOptions = {}) {
  await waitForPopupReady(page);
  const snapshot = await collectPopupStyles(page, label);
  styles.push(snapshot);
  assertPopupStyles(snapshot, styleOptions);
  return snapshot;
}

export async function dismissFirstRunNotice(page) {
  const button = page.locator(".jobs-first-run-notice .local-auth-dialog-submit");
  if (await button.isVisible({ timeout: 1000 }).catch(() => false)) {
    await button.click();
    await page.locator(".jobs-first-run-notice").waitFor({ state: "detached", timeout: 10_000 });
  }
}
