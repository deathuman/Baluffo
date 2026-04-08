import { setAuthControlsReady, setAuthStatusViewModel } from "../../shared/auth-ui.js";

export function setSavedAuthStatus(refs, text) {
  setAuthStatusViewModel(refs.savedAuthStatusEl, refs.savedAuthStatusHintEl, refs.savedAuthAvatarEl, text);
}

export function toggleSavedAuthButtons(refs, isSignedIn) {
  if (refs.signInBtnEl) refs.signInBtnEl.classList.toggle("hidden", isSignedIn);
  if (refs.signOutBtnEl) refs.signOutBtnEl.classList.toggle("hidden", !isSignedIn);
}

export function setSavedAuthControlsReady(refs, ready) {
  setAuthControlsReady([refs.signInBtnEl, refs.signOutBtnEl], ready);
}
