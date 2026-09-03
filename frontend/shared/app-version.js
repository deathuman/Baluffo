import { AdminConfig } from "./config/admin-config.js";
import { fetchJson } from "./api-client.js";
import { resolveRuntimeMode } from "./local-data/runtime-context.js";
import { UI_TOKENS, ui } from "./ui/selectors.js";

const VERSION_RETRY_ATTEMPTS = 12;
const VERSION_RETRY_DELAY_MS = 500;

function waitForDelay(ms) {
  return new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

async function fetchDesktopInstalledVersion({
  attempts = VERSION_RETRY_ATTEMPTS,
  retryDelayMs = VERSION_RETRY_DELAY_MS
} = {}) {
  const maxAttempts = Math.max(1, Number(attempts) || VERSION_RETRY_ATTEMPTS);
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const payload = await fetchJson(AdminConfig.ADMIN_BRIDGE_BASE, "/app/update-status", {
        timeoutMs: 2500
      });
      const version = String(payload?.currentVersion || "").trim();
      if (version) {
        return version;
      }
    } catch {
      // Keep the visible placeholder and retry while the bridge comes online.
    }
    if (attempt < maxAttempts) {
      await waitForDelay(retryDelayMs);
    }
  }
  return "";
}

async function fetchContainerInstalledVersion({
  attempts = VERSION_RETRY_ATTEMPTS,
  retryDelayMs = VERSION_RETRY_DELAY_MS
} = {}) {
  const maxAttempts = Math.max(1, Number(attempts) || VERSION_RETRY_ATTEMPTS);
  for (let attempt = 1; attempt <= maxAttempts; attempt += 1) {
    try {
      const payload = await fetchJson(AdminConfig.ADMIN_BRIDGE_BASE, "/app/ready", {
        timeoutMs: 2500
      });
      const version = String(payload?.appVersion || "").trim();
      if (version) {
        return version;
      }
    } catch {
      // Keep the visible placeholder and retry while the gateway comes online.
    }
    if (attempt < maxAttempts) {
      await waitForDelay(retryDelayMs);
    }
  }
  return "";
}

/**
 * Hydrate the footer version label wherever bridge local data runs (desktop and
 * container). Desktop keeps using `/app/update-status`; container mode reads the
 * same-origin `/app/ready` payload (container mode intentionally disables the
 * desktop updater routes).
 */
export async function hydrateAppVersionLabels(doc = document, options = {}) {
  if (!doc || typeof doc.querySelectorAll !== "function") {
    return "";
  }
  const runtimeMode = resolveRuntimeMode();
  if (runtimeMode !== "desktop" && runtimeMode !== "container") {
    return "";
  }

  const versionEls = Array.from(doc.querySelectorAll(ui(UI_TOKENS.global.appVersion)));
  if (!versionEls.length) {
    return "";
  }
  versionEls.forEach(el => {
    if (!String(el.textContent || "").trim()) {
      el.textContent = "Version";
    }
    el.hidden = false;
  });
  const version = runtimeMode === "container"
    ? await fetchContainerInstalledVersion(options)
    : await fetchDesktopInstalledVersion(options);
  if (!version) {
    return "";
  }

  versionEls.forEach(el => {
    el.textContent = `Version ${version}`;
    el.hidden = false;
  });
  return version;
}

/** @deprecated Use {@link hydrateAppVersionLabels}; kept for compatibility. */
export async function hydrateDesktopVersionLabels(doc = document, options = {}) {
  return hydrateAppVersionLabels(doc, options);
}
