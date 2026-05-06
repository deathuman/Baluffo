import { AdminConfig } from "./config/admin-config.js";
import { fetchJson } from "./api-client.js";
import { resolveDesktopRuntimeMode } from "./local-data/runtime-context.js";
import { UI_TOKENS, ui } from "./ui/selectors.js";

const VERSION_RETRY_ATTEMPTS = 12;
const VERSION_RETRY_DELAY_MS = 500;

function waitForDelay(ms) {
  return new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
}

async function fetchInstalledVersion({
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

export async function hydrateDesktopVersionLabels(doc = document, options = {}) {
  if (!doc || typeof doc.querySelectorAll !== "function" || !resolveDesktopRuntimeMode()) {
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
  const version = await fetchInstalledVersion(options);
  if (!version) {
    return "";
  }

  versionEls.forEach(el => {
    el.textContent = `Version ${version}`;
    el.hidden = false;
  });
  return version;
}
