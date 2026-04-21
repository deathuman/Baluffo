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
  buttonEl.dataset.bridgeState = normalized;
  buttonEl.classList.remove("online", "offline", "checking");
  buttonEl.classList.add(normalized);
  buttonEl.textContent = label || "Admin Checking...";
  buttonEl.title = title || label || "Checking admin bridge status";
  const enabled = normalized === "online";
  buttonEl.disabled = !enabled;
  buttonEl.setAttribute("aria-disabled", enabled ? "false" : "true");
}
