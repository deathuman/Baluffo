import {
  setSavedAuthControlsReady,
  setSavedAuthStatus,
  toggleSavedAuthButtons
} from "../auth.js";

export function createSavedAuthController({
  refs,
  viewState,
  savedPageService,
  savedAuthService,
  savedAuthReadyPoller,
  isSavedApiReady,
  savedDispatch,
  SAVED_ACTIONS,
  clearNoteSaveQueues,
  setActivityPanelOpen,
  setCustomJobPanelOpen,
  setCustomJobAvailability,
  updateTimelineScopeButtons,
  renderWorkspaceStats,
  emitSavedStartupMetric,
  markSavedStep = () => {},
  measureSavedStep = () => {},
  markSavedFirstRender = () => {},
  setSourceStatus,
  setActivityStatus,
  renderAuthRequired,
  renderTimeline,
  markSavedFirstInteractive,
  setSavedFilter,
  defaultSavedFilter,
  setSavedSort,
  defaultSavedSort,
  renderSelectedJobHint,
  setBackupButtonsEnabled,
  setSavedFilterBarVisible,
  setSavedSortBarVisible,
  loadTimelinePreferences,
  subscribeToSavedJobs,
  refreshActivityLog,
  timelineScopeAll,
  showToast
}) {
  let initialAuthEventHandled = false;
  let pendingInitialGuestTimer = 0;

  function setAuthStatus(text) {
    setSavedAuthStatus({
      savedAuthStatusEl: refs.savedAuthStatusEl,
      savedAuthStatusHintEl: refs.savedAuthStatusHintEl,
      savedAuthAvatarEl: refs.savedAuthAvatarEl
    }, text);
  }

  function toggleAuthButtons(isSignedIn) {
    toggleSavedAuthButtons({
      signInBtnEl: refs.signInBtnEl,
      signOutBtnEl: refs.signOutBtnEl
    }, isSignedIn);
  }

  function setAuthControlsReady(ready) {
    setSavedAuthControlsReady({
      signInBtnEl: refs.signInBtnEl,
      signOutBtnEl: refs.signOutBtnEl
    }, ready);
  }

  function initializePageChrome() {
    setActivityPanelOpen(false);
    setCustomJobPanelOpen(false);
    setCustomJobAvailability(false);
    updateTimelineScopeButtons();
    renderWorkspaceStats();
  }

  function resetUserScopedViewState() {
    viewState.unsubscribeSavedJobs();
    viewState.unsubscribeSavedJobs = () => {};
    clearNoteSaveQueues();
    viewState.expandedJobKey = null;
    viewState.jobDetailTabByKey = new Map();
    viewState.cachedActivityEntries = [];
    viewState.lastSavedJobsByKey = new Map();
    viewState.savedLifecycleOverlayByJobKey = new Map();
    viewState.savedLifecycleOverlayRequestId = 0;
    viewState.selectedJobKey = "";
    viewState.phaseOverrideContext = null;
    viewState.timelineScope = timelineScopeAll;
    viewState.lastActivityPulse = null;
    setSavedFilter(defaultSavedFilter);
    setSavedSort(defaultSavedSort);
    updateTimelineScopeButtons();
    renderSelectedJobHint();
    renderWorkspaceStats();
  }

  function handleSignedOut() {
    setAuthStatus("Browsing as guest");
    setSourceStatus("Sign in to a local profile to view saved jobs.");
    setActivityStatus("Activity appears after you sign in and change saved jobs.");
    toggleAuthButtons(false);
    setBackupButtonsEnabled(false);
    setCustomJobAvailability(false);
    setCustomJobPanelOpen(false);
    setSavedFilterBarVisible(false);
    setSavedSortBarVisible(false);
    renderAuthRequired("Your saved jobs workspace is stored per local profile, so guest browsing cannot show or edit saved jobs.");
    markSavedFirstRender("auth_required", 0);
    renderTimeline();
  }

  function handleSignedIn(user) {
    setAuthStatus(`Signed in as ${user.displayName || user.email || "user"}`);
    setSourceStatus("Loading your saved jobs...");
    setActivityStatus("Loading activity...");
    toggleAuthButtons(true);
    setBackupButtonsEnabled(true);
    setCustomJobAvailability(true);
    const timelinePrefs = loadTimelinePreferences(user.uid);
    viewState.timelineScope = timelinePrefs.scope;
    setActivityPanelOpen(false, { persist: false });
    updateTimelineScopeButtons();
    renderSelectedJobHint();
    subscribeToSavedJobs(user.uid);
    refreshActivityLog().catch(err => {
      console.error("Failed to load activity:", err);
      setActivityStatus("Could not load activity.");
    });
  }

  function shouldWaitForAuth() {
    return !savedPageService.isAvailable() || !isSavedApiReady();
  }

  function hasPersistedSessionHint() {
    try {
      return Boolean(window.localStorage.getItem("baluffo_current_profile_id"));
    } catch {
      return false;
    }
  }

  function cancelPendingInitialGuestTimer() {
    if (!pendingInitialGuestTimer) return;
    window.clearTimeout(pendingInitialGuestTimer);
    pendingInitialGuestTimer = 0;
  }

  function initSavedJobsPage() {
    markSavedStep("saved_auth_init_start");
    initializePageChrome();

    if (shouldWaitForAuth()) {
      emitSavedStartupMetric("saved_auth_waiting");
      setAuthStatus("Local auth starting...");
      setSourceStatus("Local auth provider is starting...");
      setActivityStatus("Local provider is starting...");
      toggleAuthButtons(false);
      setAuthControlsReady(false);
      savedAuthReadyPoller.schedulePoll();
      setCustomJobAvailability(false);
      setSavedSortBarVisible(false);
      renderAuthRequired("Local auth provider is starting. Please wait...");
      markSavedFirstRender("auth_waiting", 0);
      renderTimeline();
      markSavedStep("saved_auth_init_end", { waiting: true });
      measureSavedStep("saved_auth_init", "saved_auth_init_start", "saved_auth_init_end", {
        waiting: true
      });
      return;
    }

    savedAuthReadyPoller.stopPoll();
    emitSavedStartupMetric("saved_auth_ready");
    setAuthControlsReady(true);
    markSavedFirstInteractive("auth_ready");
    markSavedStep("saved_auth_init_end", { waiting: false });
    measureSavedStep("saved_auth_init", "saved_auth_init_start", "saved_auth_init_end", {
      waiting: false
    });
    if (viewState.savedAuthListenerBound) return;
    viewState.savedAuthListenerBound = true;

    savedAuthService.onAuthStateChanged(user => {
      cancelPendingInitialGuestTimer();
      viewState.currentUser = user || null;
      savedDispatch.dispatch({
        type: SAVED_ACTIONS.AUTH_CHANGED,
        payload: { uid: viewState.currentUser?.uid || "" }
      });

      resetUserScopedViewState();

      if (!viewState.currentUser) {
        const shouldDelayInitialGuestRender =
          !initialAuthEventHandled && hasPersistedSessionHint();
        initialAuthEventHandled = true;
        if (shouldDelayInitialGuestRender) {
          setAuthStatus("Restoring profile...");
          setSourceStatus("Restoring your saved jobs...");
          setActivityStatus("Restoring activity...");
          toggleAuthButtons(false);
          setCustomJobAvailability(false);
          setSavedFilterBarVisible(false);
          setSavedSortBarVisible(false);
          renderAuthRequired("Restoring your local profile. Please wait...");
          markSavedFirstRender("auth_restoring", 0);
          renderTimeline();
          pendingInitialGuestTimer = window.setTimeout(() => {
            pendingInitialGuestTimer = 0;
            if (viewState.currentUser) return;
            handleSignedOut();
          }, 800);
          return;
        }
        handleSignedOut();
        return;
      }

      initialAuthEventHandled = true;
      handleSignedIn(viewState.currentUser);
    });
  }

  async function signInUser() {
    if (shouldWaitForAuth()) {
      setAuthControlsReady(false);
      savedAuthReadyPoller.schedulePoll();
      showToast("Local auth provider is starting. Try again in a moment.", "info");
      return;
    }
    if (!viewState.savedAuthListenerBound) {
      initSavedJobsPage();
    }
    setAuthControlsReady(true);

    const result = await savedAuthService.signIn();
    if (!result.ok) {
      if (String(result.error || "").toLowerCase().includes("cancel")) return;
      console.error("Sign-in failed:", result.error);
      showToast("Sign-in failed.", "error");
      return;
    }

    const focusTarget = refs.addCustomJobBtnEl || refs.signOutBtnEl || refs.jobsPageBtnEl;
    if (!focusTarget) return;
    try {
      focusTarget.focus({ preventScroll: true });
    } catch {
      focusTarget.focus();
    }
  }

  async function signOutUser() {
    if (shouldWaitForAuth()) {
      setAuthControlsReady(false);
      savedAuthReadyPoller.schedulePoll();
      return;
    }
    if (!viewState.savedAuthListenerBound) {
      initSavedJobsPage();
    }
    setAuthControlsReady(true);

    const result = await savedAuthService.signOut();
    if (!result.ok) {
      console.error("Sign-out failed:", result.error);
      showToast("Sign-out failed.", "error");
    }
  }

  return {
    initSavedJobsPage,
    signInUser,
    signOutUser,
    setAuthStatus,
    toggleAuthButtons,
    setAuthControlsReady
  };
}
