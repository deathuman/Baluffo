import { setTooltip } from "../../shared/ui/index.js?v=5";

export function applyJobsAdminBridgeState({
  buttonEl,
  state,
  label,
  title,
  runtimeState = null
}) {
  if (!buttonEl) return;
  const normalized = String(state || "checking").toLowerCase();
  if (runtimeState && typeof runtimeState === "object") {
    runtimeState.adminBridgeButtonState = normalized;
  }
  const tooltipTarget = buttonEl.closest?.("[data-admin-bridge-tooltip]") || buttonEl.parentElement || buttonEl;
  buttonEl.dataset.bridgeState = normalized;
  buttonEl.classList.remove("online", "offline", "checking", "degraded", "hidden");

  if (normalized === "checking") {
    buttonEl.classList.add("checking");
    buttonEl.textContent = "Admin";
    setTooltip(buttonEl, "");
    setTooltip(tooltipTarget, title || "Checking admin bridge status");
    buttonEl.disabled = true;
    buttonEl.setAttribute("aria-disabled", "true");
    return;
  }
  if (normalized === "degraded") {
    buttonEl.classList.add("degraded", "checking");
    buttonEl.textContent = label || "Admin";
    setTooltip(buttonEl, "");
    setTooltip(tooltipTarget, title || "Admin status delayed; open Admin anyway");
    buttonEl.disabled = false;
    buttonEl.setAttribute("aria-disabled", "false");
    return;
  }

  const enabled = normalized === "online";
  buttonEl.classList.add(enabled ? "online" : "offline");
  buttonEl.textContent = label || (enabled ? "Admin Online" : "Admin Offline");
  setTooltip(buttonEl, "");
  setTooltip(tooltipTarget, title || buttonEl.textContent);
  buttonEl.disabled = !enabled;
  buttonEl.setAttribute("aria-disabled", enabled ? "false" : "true");
}
