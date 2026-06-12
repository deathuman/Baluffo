import { setTooltip } from "../../shared/ui/index.js";

export function applySavedAdminBridgeState({
  buttonEl,
  state,
  label,
  title,
  viewState = null
}) {
  if (!buttonEl) return;
  const normalized = String(state || "checking").toLowerCase();
  if (viewState && typeof viewState === "object") {
    viewState.adminBridgeButtonState = normalized;
  }
  const tooltipTarget = buttonEl.closest?.("[data-admin-bridge-tooltip]") || buttonEl.parentElement || buttonEl;
  buttonEl.dataset.bridgeState = normalized;
  buttonEl.classList.remove("online", "offline", "checking", "degraded");
  buttonEl.classList.add(normalized);
  buttonEl.textContent = normalized === "checking" || normalized === "degraded"
    ? "Admin"
    : label || (normalized === "online" ? "Admin Online" : "Admin Offline");
  setTooltip(buttonEl, "");
  setTooltip(tooltipTarget, title || label || "Checking admin bridge status");
  const enabled = normalized === "online" || normalized === "degraded";
  buttonEl.disabled = !enabled;
  buttonEl.setAttribute("aria-disabled", enabled ? "false" : "true");
}
