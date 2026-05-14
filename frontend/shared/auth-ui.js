import { toAuthViewModel } from "./auth-view-model.js";
import { setTooltip } from "./ui/index.js";

export function setAuthStatusViewModel(statusEl, hintEl, avatarEl, text) {
  const { label, hint } = toAuthViewModel(text);
  if (statusEl) statusEl.textContent = label;
  if (hintEl) hintEl.textContent = hint;
  if (avatarEl) {
    const initial = label.charAt(0).toUpperCase();
    avatarEl.textContent = initial && /[A-Z0-9]/.test(initial) ? initial : "U";
  }
}

export function setAuthControlsReady(buttons, ready) {
  const isReady = Boolean(ready);
  buttons.forEach(btn => {
    if (!btn) return;
    btn.disabled = !isReady;
    btn.setAttribute("aria-disabled", isReady ? "false" : "true");
    setTooltip(btn, isReady ? "" : "Local auth provider is starting.");
  });
}
