import { toAuthViewModel } from "../../shared/auth-view-model.js";

export function setSavedAuthStatus(refs, text) {
  const { label, hint } = toAuthViewModel(text);
  if (refs.savedAuthStatusEl) refs.savedAuthStatusEl.textContent = label;
  if (refs.savedAuthStatusHintEl) refs.savedAuthStatusHintEl.textContent = hint;
  if (refs.savedAuthAvatarEl) {
    const initial = label.charAt(0).toUpperCase();
    refs.savedAuthAvatarEl.textContent = initial && /[A-Z0-9]/.test(initial) ? initial : "U";
  }
}

export function toggleSavedAuthButtons(refs, isSignedIn) {
  if (refs.signInBtnEl) refs.signInBtnEl.classList.toggle("hidden", isSignedIn);
  if (refs.signOutBtnEl) refs.signOutBtnEl.classList.toggle("hidden", !isSignedIn);
}

export function setSavedAuthControlsReady(refs, ready) {
  const isReady = Boolean(ready);
  [refs.signInBtnEl, refs.signOutBtnEl].forEach(btn => {
    if (!btn) return;
    btn.disabled = !isReady;
    btn.setAttribute("aria-disabled", isReady ? "false" : "true");
    btn.title = isReady ? "" : "Local auth provider is starting.";
  });
}
