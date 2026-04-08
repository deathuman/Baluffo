import { toAuthViewModel } from "../../shared/auth-view-model.js";

export function toggleJobsAuthButtons(refs, isSignedIn) {
  if (refs.authSignInBtn) refs.authSignInBtn.classList.toggle("hidden", isSignedIn);
  if (refs.authSignOutBtn) refs.authSignOutBtn.classList.toggle("hidden", !isSignedIn);
  if (refs.savedJobsBtn) refs.savedJobsBtn.classList.toggle("hidden", !isSignedIn);
}

export function setJobsAuthControlsReady(refs, ready) {
  const isReady = Boolean(ready);
  [refs.authSignInBtn, refs.authSignOutBtn].forEach(btn => {
    if (!btn) return;
    btn.disabled = !isReady;
    btn.setAttribute("aria-disabled", isReady ? "false" : "true");
    btn.title = isReady ? "" : "Local auth provider is starting.";
  });
}

export { toAuthViewModel as toJobsAuthViewModel };

export function setJobsAuthStatus(refs, text) {
  if (!refs.authStatus) return;
  const { label, hint } = toAuthViewModel(text);

  refs.authStatus.textContent = label;
  if (refs.authStatusHint) {
    refs.authStatusHint.textContent = hint;
  }
  if (refs.authAvatar) {
    const initial = label.charAt(0).toUpperCase();
    refs.authAvatar.textContent = initial && /[A-Z0-9]/.test(initial) ? initial : "U";
  }
}
