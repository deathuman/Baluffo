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

export function toJobsAuthViewModel(text) {
  const raw = String(text || "").trim();
  const model = { label: raw || "Guest", hint: "" };
  const signedInMatch = raw.match(/^signed\s+in\s+as\s+(.+)$/i);
  if (!raw || /^browsing\s+as\s+guest$/i.test(raw) || /^guest$/i.test(raw)) {
    model.label = "Guest";
    model.hint = "Browsing as guest";
    return model;
  }
  if (signedInMatch) {
    model.label = String(signedInMatch[1] || "").trim() || "User";
    model.hint = "Signed in";
  }
  return model;
}

export function setJobsAuthStatus(refs, text) {
  if (!refs.authStatus) return;
  const { label, hint } = toJobsAuthViewModel(text);

  refs.authStatus.textContent = label;
  if (refs.authStatusHint) {
    refs.authStatusHint.textContent = hint;
  }
  if (refs.authAvatar) {
    const initial = label.charAt(0).toUpperCase();
    refs.authAvatar.textContent = initial && /[A-Z0-9]/.test(initial) ? initial : "U";
  }
}
