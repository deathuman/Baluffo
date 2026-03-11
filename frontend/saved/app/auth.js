export function setSavedAuthStatus(refs, text) {
  const raw = String(text || "").trim();
  let label = raw || "Guest";
  let hint = "";
  const signedInMatch = raw.match(/^signed\s+in\s+as\s+(.+)$/i);

  if (!raw || /^browsing\s+as\s+guest$/i.test(raw) || /^guest$/i.test(raw)) {
    label = "Guest";
    hint = "Browsing as guest";
  } else if (signedInMatch) {
    label = String(signedInMatch[1] || "").trim() || "User";
    hint = "Signed in";
  }

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
