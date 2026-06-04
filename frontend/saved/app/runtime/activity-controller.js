import {
  buildTimelinePrefsKey as buildTimelinePrefsKeyFromActivity,
  normalizeTimelineScope,
  countRecentActivityEntries,
  setActivityPanelOpen as setActivityPanelOpenFromModule,
  setTimelineScope as setTimelineScopeFromModule,
  updateTimelineScopeButtons as updateTimelineScopeButtonsFromModule,
  queueActivityPulse as queueActivityPulseFromModule,
  clearExpiredPulse as clearExpiredPulseFromModule,
  renderSelectedJobHint as renderSelectedJobHintFromModule,
  renderTimeline as renderTimelineFromModule,
  renderActivityEntries as renderActivityEntriesFromModule
} from "../activity.js";

export function createSavedActivityController({
  dom,
  viewState,
  savedPageService,
  setActivityStatus,
  timelinePrefPrefix,
  timelineScopeAll,
  activityHighlightMs,
  renderActivityEntryHtml,
  getReminderMeta,
  loadSavedTimelinePreferences,
  persistSavedTimelinePreferences,
  activityTypeLabel,
  formatActivityDetail,
  formatPhaseTimestamp
}) {
  function buildTimelinePrefsKey(uid) {
    return buildTimelinePrefsKeyFromActivity(timelinePrefPrefix, uid);
  }

  function loadTimelinePreferences(uid) {
    return loadSavedTimelinePreferences(
      timelinePrefPrefix,
      uid,
      normalizeTimelineScope,
      timelineScopeAll
    );
  }

  function persistTimelinePreferences(uid) {
    persistSavedTimelinePreferences(timelinePrefPrefix, uid, normalizeTimelineScope, {
      visible: Boolean(viewState.activityPanelOpen),
      scope: normalizeTimelineScope(viewState.timelineScope)
    });
  }

  function setActivityPanelOpen(open, options = {}) {
    if (open) {
      viewState.timelineScope = viewState.selectedJobKey ? "selected" : timelineScopeAll;
      updateTimelineScopeButtons();
    }
    return setActivityPanelOpenFromModule(open, {
      activityPanelEl: dom.activityPanelEl,
      historyPanelToggleBtnEl: dom.historyPanelToggleBtnEl,
      persist: options.persist,
      currentUser: viewState.currentUser,
      persistTimelinePreferences,
      setActivityPanelOpenState: value => {
        viewState.activityPanelOpen = value;
      }
    });
  }

  function updateTimelineScopeButtons() {
    updateTimelineScopeButtonsFromModule(
      dom.activityScopeBtnEls,
      viewState.timelineScope,
      viewState.selectedJobKey
    );
  }

  function setTimelineScope(nextScope) {
    return setTimelineScopeFromModule(nextScope, {
      selectedJobKey: viewState.selectedJobKey,
      persistTimelinePreferences,
      currentUser: viewState.currentUser,
      updateTimelineScopeState: value => {
        viewState.timelineScope = value;
      },
      updateTimelineScopeButtons
    });
  }

  function queueActivityPulse(jobKey, category) {
    viewState.lastActivityPulse = queueActivityPulseFromModule(jobKey, category);
  }

  function clearExpiredPulse() {
    viewState.lastActivityPulse = clearExpiredPulseFromModule(viewState.lastActivityPulse);
  }

  function renderSelectedJobHint() {
    renderSelectedJobHintFromModule(
      dom.activitySelectedJobEl,
      viewState.selectedJobKey,
      viewState.lastSavedJobsByKey
    );
  }

  function renderWorkspaceStats(jobs = null) {
    if (Array.isArray(jobs)) {
      viewState.savedWorkspaceStatsReady = true;
    }
    const rows = Array.isArray(jobs) ? jobs : Array.from(viewState.lastSavedJobsByKey.values());
    const showStats = Boolean(viewState.currentUser && viewState.savedWorkspaceStatsReady);
    if (dom.savedWorkspaceStripEl) {
      dom.savedWorkspaceStripEl.hidden = !showStats;
      dom.savedWorkspaceStripEl.classList?.toggle?.("hidden", !showStats);
      dom.savedWorkspaceStripEl.setAttribute?.("aria-hidden", showStats ? "false" : "true");
    }
    if (dom.savedMetricTotalEl) dom.savedMetricTotalEl.textContent = String(rows.length);
    if (dom.savedMetricRemindersEl) {
      const dueSoon = rows.filter(job => getReminderMeta(job?.reminderAt).isSoon).length;
      dom.savedMetricRemindersEl.textContent = String(dueSoon);
    }
    if (dom.savedMetricActivityEl) {
      const recentCount = countRecentActivityEntries(viewState.cachedActivityEntries, 24);
      dom.savedMetricActivityEl.textContent = String(recentCount);
      if (dom.activityRecentBadgeEl) {
        dom.activityRecentBadgeEl.textContent = recentCount > 0 ? String(recentCount) : "";
      }
    }
  }

  function renderActivityEntry(entry) {
    return renderActivityEntryHtml(entry, {
      formatPhaseTimestamp,
      lastSavedJobsByKey: viewState.lastSavedJobsByKey,
      formatActivityDetail,
      activityTypeLabel
    });
  }

  function renderActivityEntries(entries) {
    renderActivityEntriesFromModule(entries, {
      activityPanelBodyEl: dom.activityPanelBodyEl,
      lastActivityPulse: viewState.lastActivityPulse,
      renderActivityEntry,
      renderTimeline,
      clearExpiredPulseState: clearExpiredPulse,
      activityHighlightMs
    });
  }

  function renderTimeline() {
    renderTimelineFromModule({
      cachedActivityEntries: viewState.cachedActivityEntries,
      timelineScope: viewState.timelineScope,
      selectedJobKey: viewState.selectedJobKey,
      currentUser: viewState.currentUser,
      setActivityStatus,
      renderActivityEntries
    });
  }

  async function refreshActivityLog() {
    if (!dom.activityPanelBodyEl) return;
    if (!viewState.currentUser || !savedPageService.isAvailable()) {
      setActivityStatus("Sign in to view history.");
      renderTimeline();
      renderWorkspaceStats();
      return;
    }

    setActivityStatus("Loading activity...");
    try {
      const entriesResult = await savedPageService.listActivityForUser(viewState.currentUser.uid, 400);
      if (!entriesResult.ok) throw new Error(entriesResult.error || "Could not load history.");
      viewState.cachedActivityEntries = Array.isArray(entriesResult.data) ? entriesResult.data : [];
      renderTimeline();
      renderWorkspaceStats();
    } catch (err) {
      console.error("Could not load activity log:", err);
      viewState.cachedActivityEntries = [];
      setActivityStatus("Could not load history.");
      renderTimeline();
      renderWorkspaceStats();
    }
  }

  return {
    buildTimelinePrefsKey,
    loadTimelinePreferences,
    setActivityPanelOpen,
    setTimelineScope,
    updateTimelineScopeButtons,
    queueActivityPulse,
    clearExpiredPulse,
    renderSelectedJobHint,
    renderWorkspaceStats,
    refreshActivityLog,
    renderTimeline
  };
}
