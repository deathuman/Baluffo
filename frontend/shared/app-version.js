import { AdminConfig } from "./config/admin-config.js";
import { fetchJson } from "./api-client.js";
import { awaitDesktopBootstrap } from "./local-data/desktop-client.js";
import { resolveDesktopRuntimeMode } from "./local-data/runtime-context.js";
import { UI_TOKENS, ui } from "./ui/selectors.js";

export async function hydrateDesktopVersionLabels(doc = document) {
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
  if (!(await awaitDesktopBootstrap())) {
    return "";
  }

  try {
    const payload = await fetchJson(AdminConfig.ADMIN_BRIDGE_BASE, "/app/update-status");
    const version = String(payload?.currentVersion || "").trim();
    if (!version) {
      return "";
    }

    versionEls.forEach(el => {
      el.textContent = `Version ${version}`;
      el.hidden = false;
    });
    return version;
  } catch {
    return "";
  }
}
