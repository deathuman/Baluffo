import { setAuthControlsReady, setAuthStatusViewModel } from "../../shared/auth-ui.js";

export function toggleJobsAuthButtons(refs, isSignedIn) {
  if (refs.authSignInBtn) refs.authSignInBtn.classList.toggle("hidden", isSignedIn);
  if (refs.authSignOutBtn) refs.authSignOutBtn.classList.toggle("hidden", !isSignedIn);
  if (refs.savedJobsBtn) refs.savedJobsBtn.classList.toggle("hidden", !isSignedIn);
}

export function setJobsAuthControlsReady(refs, ready) {
  setAuthControlsReady([refs.authSignInBtn, refs.authSignOutBtn], ready);
}

export function setJobsAuthStatus(refs, text) {
  setAuthStatusViewModel(refs.authStatus, refs.authStatusHint, refs.authAvatar, text);
}
