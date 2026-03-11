export const TIMELINE_SCOPE_ALL = "all";
export const TIMELINE_SCOPE_SELECTED = "selected";
export const TIMELINE_SCOPE_PHASE = "phase";
export const TIMELINE_SCOPE_NOTES = "notes";
export const TIMELINE_SCOPE_ATTACHMENTS = "attachments";

export function normalizeTimelineScope(value) {
  const raw = String(value || "").trim().toLowerCase();
  if (
    raw === TIMELINE_SCOPE_ALL ||
    raw === TIMELINE_SCOPE_SELECTED ||
    raw === TIMELINE_SCOPE_PHASE ||
    raw === TIMELINE_SCOPE_NOTES ||
    raw === TIMELINE_SCOPE_ATTACHMENTS
  ) {
    return raw;
  }
  return TIMELINE_SCOPE_ALL;
}

export function buildTimelinePrefsKey(prefix, uid) {
  const safeUid = String(uid || "").trim();
  return safeUid ? `${prefix}:${safeUid}` : prefix;
}

export function timelineTypeForEntry(entry) {
  const type = String(entry?.type || "").toLowerCase();
  if (type.includes("phase")) return TIMELINE_SCOPE_PHASE;
  if (type.includes("note")) return TIMELINE_SCOPE_NOTES;
  if (type.includes("attach")) return TIMELINE_SCOPE_ATTACHMENTS;
  return TIMELINE_SCOPE_ALL;
}

export function filterActivityEntriesForScope(entries, scope, currentSelectedJobKey = "") {
  const rows = Array.isArray(entries) ? entries : [];
  const safeScope = normalizeTimelineScope(scope);
  const selectedKey = String(currentSelectedJobKey || "").trim();
  return rows.filter(entry => {
    const entryJobKey = String(entry?.jobKey || "").trim();
    if (safeScope === TIMELINE_SCOPE_SELECTED) {
      return selectedKey && entryJobKey === selectedKey;
    }
    if (safeScope === TIMELINE_SCOPE_PHASE) return timelineTypeForEntry(entry) === TIMELINE_SCOPE_PHASE;
    if (safeScope === TIMELINE_SCOPE_NOTES) return timelineTypeForEntry(entry) === TIMELINE_SCOPE_NOTES;
    if (safeScope === TIMELINE_SCOPE_ATTACHMENTS) return timelineTypeForEntry(entry) === TIMELINE_SCOPE_ATTACHMENTS;
    return true;
  });
}

export function countRecentActivityEntries(entries, withinHours = 24, parseIsoDate = value => new Date(value)) {
  const threshold = Date.now() - Math.max(1, Number(withinHours) || 24) * 60 * 60 * 1000;
  return (Array.isArray(entries) ? entries : []).filter(entry => {
    const parsed = parseIsoDate(entry?.createdAt);
    return parsed && typeof parsed.getTime === "function" && parsed.getTime() >= threshold;
  }).length;
}

export function setActivityPanelOpen(open, deps) {
  const {
    activityPanelEl,
    historyPanelToggleBtnEl,
    persist = true,
    currentUser,
    persistTimelinePreferences,
    setActivityPanelOpenState
  } = deps;
  const nextOpen = Boolean(open);
  setActivityPanelOpenState(nextOpen);
  if (!activityPanelEl) return;
  activityPanelEl.classList.toggle("collapsed", !nextOpen);
  activityPanelEl.setAttribute("aria-hidden", nextOpen ? "false" : "true");
  if (historyPanelToggleBtnEl) {
    historyPanelToggleBtnEl.classList.toggle("active", nextOpen);
    historyPanelToggleBtnEl.setAttribute("aria-expanded", nextOpen ? "true" : "false");
    historyPanelToggleBtnEl.textContent = nextOpen ? "Hide Activity" : "Show Activity";
  }
  if (persist && currentUser) {
    persistTimelinePreferences(currentUser.uid);
  }
}

export function setTimelineScope(nextScope, deps) {
  const {
    selectedJobKey,
    persistTimelinePreferences,
    currentUser,
    updateTimelineScopeState,
    updateTimelineScopeButtons
  } = deps;
  const normalized = normalizeTimelineScope(nextScope);
  if (normalized === TIMELINE_SCOPE_SELECTED && !selectedJobKey) {
    updateTimelineScopeState(TIMELINE_SCOPE_ALL);
  } else {
    updateTimelineScopeState(normalized);
  }
  updateTimelineScopeButtons();
  if (currentUser) {
    persistTimelinePreferences(currentUser.uid);
  }
}

export function updateTimelineScopeButtons(activityScopeBtnEls, timelineScope, selectedJobKey) {
  activityScopeBtnEls.forEach(btn => {
    const scope = normalizeTimelineScope(btn.dataset.timelineScope || TIMELINE_SCOPE_ALL);
    const isDisabled = scope === TIMELINE_SCOPE_SELECTED && !selectedJobKey;
    const isActive = scope === timelineScope;
    btn.disabled = isDisabled;
    btn.classList.toggle("active", isActive);
    btn.setAttribute("aria-pressed", isActive ? "true" : "false");
  });
}

export function queueActivityPulse(jobKey, category) {
  const safeKey = String(jobKey || "").trim();
  const safeCategory = normalizeTimelineScope(category);
  return {
    jobKey: safeKey,
    category: safeCategory,
    expiresAt: Date.now() + 2200
  };
}

export function clearExpiredPulse(lastActivityPulse) {
  if (!lastActivityPulse) return null;
  if (Date.now() > lastActivityPulse.expiresAt) {
    return null;
  }
  return lastActivityPulse;
}

export function renderSelectedJobHint(activitySelectedJobEl, selectedJobKey, lastSavedJobsByKey) {
  if (!activitySelectedJobEl) return;
  if (!selectedJobKey) {
    activitySelectedJobEl.textContent = "Selected: none";
    return;
  }
  const row = lastSavedJobsByKey.get(selectedJobKey);
  const label = row ? `${row.title || "Untitled"} @ ${row.company || "Unknown"}` : selectedJobKey;
  activitySelectedJobEl.textContent = `Selected: ${label}`;
}

export function renderTimeline(deps) {
  const {
    cachedActivityEntries,
    timelineScope,
    selectedJobKey,
    currentUser,
    setActivityStatus,
    renderActivityEntries
  } = deps;
  const entries = filterActivityEntriesForScope(cachedActivityEntries, timelineScope, selectedJobKey);
  if (!currentUser) {
    setActivityStatus("Sign in to view history.");
  } else {
    const scopeLabel = timelineScope === TIMELINE_SCOPE_ALL
      ? "all activity"
      : timelineScope === TIMELINE_SCOPE_SELECTED
        ? "selected job activity"
        : `${timelineScope} activity`;
    setActivityStatus(`Showing ${entries.length} ${scopeLabel}.`);
  }
  renderActivityEntries(entries);
}

export function shouldPulseEntry(entry, lastActivityPulse) {
  const activePulse = clearExpiredPulse(lastActivityPulse);
  if (!activePulse) return false;
  const matchesJob = !activePulse.jobKey || String(entry?.jobKey || "").trim() === activePulse.jobKey;
  if (!matchesJob) return false;
  if (activePulse.category === TIMELINE_SCOPE_ALL) return true;
  return timelineTypeForEntry(entry) === activePulse.category;
}

export function renderActivityEntries(entries, deps) {
  const {
    activityPanelBodyEl,
    lastActivityPulse,
    renderActivityEntry,
    renderTimeline,
    clearExpiredPulseState,
    activityHighlightMs
  } = deps;
  if (!activityPanelBodyEl) return;
  if (!Array.isArray(entries) || entries.length === 0) {
    activityPanelBodyEl.innerHTML = '<div class="muted">No activity yet.</div>';
    return;
  }
  activityPanelBodyEl.innerHTML = entries.map(entry => {
    const pulseClass = shouldPulseEntry(entry, lastActivityPulse) ? "activity-pulse" : "";
    return `
      <div class="activity-entry-wrap ${pulseClass}">
        ${renderActivityEntry(entry)}
      </div>
    `;
  }).join("");
  if (lastActivityPulse) {
    setTimeout(() => {
      clearExpiredPulseState();
      renderTimeline();
    }, activityHighlightMs + 80);
  }
}
