import {
  setJobsAuthControlsReady,
  setJobsAuthStatus,
  toggleJobsAuthButtons
} from "../auth.js";

export function createJobsAuthController({
  refs,
  userState,
  authReadyPoller,
  jobsAuthService,
  jobsSavedJobsService,
  jobsPageService,
  jobsDispatch,
  JOBS_ACTIONS,
  isJobsApiReady,
  emitDesktopStartupMetric,
  showToast,
  logJobsError,
  getAllJobs,
  applyFiltersAndRender,
  getSkipInitialGuestAuthRerender = () => false,
  setSkipInitialGuestAuthRerender = () => {},
  openJobsCacheDb,
  JOBS_SEEN_STORE,
  loadSeenJobKeys,
  markSeenJob,
  buildSeenRowKey,
  getJobKeyForJob,
  toJobSnapshot,
  sanitizeUrl
}) {
  function setAuthControlsReady(ready) {
    setJobsAuthControlsReady({ authSignInBtn: refs.authSignInBtn, authSignOutBtn: refs.authSignOutBtn }, ready);
  }

  function setAuthStatus(text) {
    setJobsAuthStatus({ authStatus: refs.authStatus, authStatusHint: refs.authStatusHint, authAvatar: refs.authAvatar }, text);
  }

  function toggleAuthButtons(isSignedIn) {
    toggleJobsAuthButtons({
      authSignInBtn: refs.authSignInBtn,
      authSignOutBtn: refs.authSignOutBtn,
      savedJobsBtn: refs.savedJobsBtn
    }, isSignedIn);
  }

  function setGuestNoticeVisible(visible) {
    if (!refs.guestNoticeEl) return;
    refs.guestNoticeEl.hidden = !visible;
  }

  function focusSavedJobsButton() {
    const { savedJobsBtn } = refs;
    if (!savedJobsBtn || savedJobsBtn.classList.contains("hidden")) return;
    try {
      savedJobsBtn.focus({ preventScroll: true });
    } catch {
      savedJobsBtn.focus();
    }
  }

  function shouldWaitForAuth() {
    return !isJobsApiReady() || !jobsPageService.isAvailable();
  }

  function handleSignedOut() {
    const shouldSkipGuestRerender = Boolean(getSkipInitialGuestAuthRerender())
      && getAllJobs().length > 0
      && userState.savedJobKeys.size === 0
      && userState.seenJobKeys.size === 0;
    userState.savedJobKeys = new Set();
    userState.seenJobKeys = new Set();
    setAuthStatus("Browsing as guest");
    setGuestNoticeVisible(true);
    toggleAuthButtons(false);
    setSkipInitialGuestAuthRerender(false);
    if (!shouldSkipGuestRerender && getAllJobs().length) {
      applyFiltersAndRender({ resetPage: false });
    }
  }

  async function handleSignedIn(user) {
    setSkipInitialGuestAuthRerender(false);
    setAuthStatus(`Signed in as ${user.displayName || user.email || "user"}`);
    setGuestNoticeVisible(false);
    toggleAuthButtons(true);

    try {
      const [savedKeysResult, loadedSeenJobKeys, availabilityAttentionResult] = await Promise.all([
        jobsSavedJobsService.getSavedJobKeys(user.uid),
        loadSeenJobKeys(user.uid),
        jobsSavedJobsService.getAvailabilityAttention(user.uid)
      ]);
      userState.savedJobKeys = new Set(savedKeysResult.data || []);
      userState.seenJobKeys = loadedSeenJobKeys;
      const attentionCount = Math.max(0, Number(availabilityAttentionResult?.data?.count || 0));
      if (refs.savedJobsBtn) {
        refs.savedJobsBtn.textContent = attentionCount
          ? `Saved Jobs (${attentionCount})`
          : "Saved Jobs";
        refs.savedJobsBtn.classList.toggle("needs-attention", attentionCount > 0);
        refs.savedJobsBtn.setAttribute(
          "aria-label",
          attentionCount
            ? `Go to saved jobs page; ${attentionCount} availability updates need attention`
            : "Go to saved jobs page"
        );
      }
    } catch (err) {
      logJobsError("Failed to load saved jobs", err);
      showToast("Could not load profile job state.", "error");
      userState.savedJobKeys = new Set();
      userState.seenJobKeys = new Set();
    }

    if (getAllJobs().length) applyFiltersAndRender({ resetPage: false });
  }

  function initAuth() {
    if (shouldWaitForAuth()) {
      emitDesktopStartupMetric("jobs_auth_waiting");
      setAuthStatus("Local auth starting...");
      setGuestNoticeVisible(false);
      toggleAuthButtons(false);
      setAuthControlsReady(false);
      authReadyPoller.schedulePoll();
      return;
    }

    authReadyPoller.stopPoll();
    emitDesktopStartupMetric("jobs_auth_ready");
    setAuthControlsReady(true);
    if (userState.authStateListenerBound) return;
    userState.authStateListenerBound = true;

    jobsAuthService.onAuthStateChanged(async user => {
      userState.currentUser = user || null;
      jobsDispatch.dispatch({
        type: JOBS_ACTIONS.AUTH_CHANGED,
        payload: { uid: userState.currentUser?.uid || "" }
      });

      if (!userState.currentUser) {
        handleSignedOut();
        return;
      }

      await handleSignedIn(userState.currentUser);
    });
  }

  async function signInUser() {
    if (shouldWaitForAuth()) {
      setAuthControlsReady(false);
      authReadyPoller.schedulePoll();
      showToast("Local auth provider is starting. Try again in a moment.", "info");
      return;
    }

    if (!userState.authStateListenerBound) {
      initAuth();
    }
    setAuthControlsReady(true);

    const result = await jobsAuthService.signIn();
    if (!result.ok) {
      if (String(result.error || "").toLowerCase().includes("cancel")) return;
      logJobsError("Sign-in failed", new Error(result.error));
      showToast("Sign-in failed. Please try again.", "error");
      return;
    }

    focusSavedJobsButton();
  }

  async function signOutUser() {
    if (shouldWaitForAuth()) {
      setAuthControlsReady(false);
      authReadyPoller.schedulePoll();
      return;
    }

    if (!userState.authStateListenerBound) {
      initAuth();
    }
    setAuthControlsReady(true);

    const result = await jobsAuthService.signOut();
    if (!result.ok) {
      logJobsError("Sign-out failed", new Error(result.error));
      showToast("Sign-out failed. Please try again.", "error");
    }
  }

  async function markJobSeenFromInteraction(jobKey) {
    const safeJobKey = String(jobKey || "").trim();
    if (!userState.currentUser?.uid || !safeJobKey) return;
    if (userState.seenJobKeys.has(safeJobKey)) return;

    userState.seenJobKeys.add(safeJobKey);
    await markSeenJob(userState.currentUser.uid, safeJobKey, {
      openDb: openJobsCacheDb,
      seenStore: JOBS_SEEN_STORE,
      seenAt: Date.now(),
      buildKey: buildSeenRowKey
    });
    if (getAllJobs().length) applyFiltersAndRender({ resetPage: false });
  }

  async function toggleSaveJob(job) {
    if (!isJobsApiReady()) {
      showToast("Local storage provider unavailable.", "error");
      return;
    }

    if (!userState.currentUser) {
      showToast("Sign in to save jobs.", "info");
      await signInUser();
      return;
    }

    const jobKey = getJobKeyForJob(job);
    const isSaved = userState.savedJobKeys.has(jobKey);

    try {
      if (isSaved) {
        const removeResult = await jobsSavedJobsService.removeSavedJobForUser(userState.currentUser.uid, jobKey);
        if (!removeResult.ok) throw new Error(removeResult.error);
        userState.savedJobKeys.delete(jobKey);
        showToast("Removed from saved jobs.", "success");
      } else {
        const saveResult = await jobsSavedJobsService.saveJobForUser(userState.currentUser.uid, toJobSnapshot(job, { sanitizeUrl }));
        if (!saveResult.ok) throw new Error(saveResult.error);
        userState.savedJobKeys.add(jobKey);
        showToast("Saved job to your profile.", "success");
      }
      jobsDispatch.dispatch({ type: JOBS_ACTIONS.SAVE_TOGGLED, payload: { jobKey } });
      if (getAllJobs().length) applyFiltersAndRender({ resetPage: false });
    } catch (err) {
      logJobsError("Could not toggle saved job", err);
      showToast("Could not update saved jobs right now.", "error");
    }
  }

  return {
    initAuth,
    signInUser,
    signOutUser,
    toggleSaveJob,
    markJobSeenFromInteraction,
    setAuthControlsReady,
    setAuthStatus,
    toggleAuthButtons
  };
}
