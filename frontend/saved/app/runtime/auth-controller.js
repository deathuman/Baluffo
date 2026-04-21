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
    viewState.phaseOverrideArmedGlobal = false;
    viewState.jobDetailTabByKey = new Map();
    viewState.cachedActivityEntries = [];
    viewState.lastSavedJobsByKey = new Map();
    viewState.selectedJobKey = "";
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
    setSourceStatus("Sign in to view your saved jobs.");
    setActivityStatus("Sign in to view history.");
    toggleAuthButtons(false);
    setBackupButtonsEnabled(false);
    setCustomJobAvailability(false);
    setCustomJobPanelOpen(false);
    setSavedFilterBarVisible(false);
    setSavedSortBarVisible(false);
    renderAuthRequired("Sign in to access your custom saved jobs table.");
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

  function initSavedJobsPage() {
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
      renderTimeline();
      return;
    }

    savedAuthReadyPoller.stopPoll();
    emitSavedStartupMetric("saved_auth_ready");
    setAuthControlsReady(true);
    markSavedFirstInteractive("auth_ready");
    if (viewState.savedAuthListenerBound) return;
    viewState.savedAuthListenerBound = true;

    savedAuthService.onAuthStateChanged(user => {
      viewState.currentUser = user || null;
      savedDispatch.dispatch({
        type: SAVED_ACTIONS.AUTH_CHANGED,
        payload: { uid: viewState.currentUser?.uid || "" }
      });

      resetUserScopedViewState();

      if (!viewState.currentUser) {
        handleSignedOut();
        return;
      }

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
