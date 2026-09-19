import { OPS_TAB_KEYS } from "./health-badges.js";

/**
 * Ops tab selection: hash persistence, aria/visibility toggling and the
 * one-time click wiring for the tab buttons.
 */
export function createOpsTabs({
  state,
  refs,
  loadActiveOpsTabDetail
}) {
  function getOpsTabPanels() {
    return {
      overview: refs.adminOpsTabOverviewEl,
      discovery: refs.adminOpsTabDiscoveryEl,
      "source-policy": refs.adminOpsTabSourcePolicyEl,
      "registry-conflicts": refs.adminOpsTabRegistryConflictsEl,
      dedup: refs.adminOpsTabDedupEl
    };
  }

  function opsTabFromHash() {
    try {
      const value = new URLSearchParams((globalThis.location?.hash || "").replace(/^#\??/, "")).get("ops-tab") || "";
      return OPS_TAB_KEYS.has(value) ? value : "";
    } catch {
      return "";
    }
  }

  function rememberOpsTabInHash(tabKey) {
    try {
      if (!globalThis.history?.replaceState || !globalThis.location) return;
      const params = new URLSearchParams((globalThis.location?.hash || "").replace(/^#\??/, ""));
      if (OPS_TAB_KEYS.has(tabKey) && tabKey !== "overview") params.set("ops-tab", tabKey);
      else params.delete("ops-tab");
      const qs = params.toString();
      globalThis.history.replaceState(
        null,
        "",
        qs ? `#${qs}` : `${globalThis.location.pathname || ""}${globalThis.location.search || ""}`
      );
    } catch {
      // Stub environments without location/history.
    }
  }

  function selectOpsTab(tabKey = "overview") {
    const activeKey = OPS_TAB_KEYS.has(tabKey) ? tabKey : "overview";
    state.adminOpsActiveTab = activeKey;
    rememberOpsTabInHash(activeKey);
    const buttons = Array.isArray(refs.adminOpsTabBtnEls) ? refs.adminOpsTabBtnEls : [];
    buttons.forEach(button => {
      const buttonKey = String(button?.dataset?.opsTab || button?.getAttribute?.("data-ops-tab") || "");
      const active = buttonKey === activeKey;
      button?.setAttribute?.("aria-selected", active ? "true" : "false");
      button?.classList?.toggle?.("active", active);
      if (button) button.tabIndex = active ? 0 : -1;
    });
    Object.entries(getOpsTabPanels()).forEach(([key, panel]) => {
      if (!panel) return;
      const active = key === activeKey;
      panel.hidden = !active;
      panel.classList?.toggle?.("hidden", !active);
      if (active) {
        panel.removeAttribute?.("hidden");
      } else {
        panel.setAttribute?.("hidden", "");
      }
    });
    return loadActiveOpsTabDetail(activeKey).catch(() => {});
  }

  function setupOpsTabs() {
    const buttons = Array.isArray(refs.adminOpsTabBtnEls) ? refs.adminOpsTabBtnEls : [];
    if (!buttons.length || state.adminOpsTabsInitialized) {
      selectOpsTab(opsTabFromHash() || state.adminOpsActiveTab || "overview");
      return;
    }
    state.adminOpsTabsInitialized = true;
    buttons.forEach(button => {
      button?.addEventListener?.("click", () => {
        selectOpsTab(String(button?.dataset?.opsTab || button?.getAttribute?.("data-ops-tab") || "overview"));
      });
    });
    selectOpsTab(opsTabFromHash() || state.adminOpsActiveTab || "overview");
  }

  return {
    selectOpsTab,
    setupOpsTabs
  };
}
