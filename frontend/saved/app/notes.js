const NOTES_RERENDER_SETTLE_MS = 1200;

export function isEditingNotesField(doc = document) {
  if (typeof doc === "undefined") return false;
  return isEditingNotesFieldFromElement(doc.activeElement);
}

export function isEditingNotesFieldFromElement(activeElement) {
  const active = activeElement;
  if (!active || typeof active !== "object") return false;
  const tagName = String(active.tagName || "").toUpperCase();
  const classList = active.classList;
  return (
    tagName === "TEXTAREA" &&
    Boolean(classList && typeof classList.contains === "function" && classList.contains("job-notes-input"))
  );
}

export function shouldDeferSavedJobsRerender(options = {}) {
  const nowMs = Number(options.nowMs) || Date.now();
  const isEditing = Boolean(options.isEditingNotes);
  const inFlightCount = Math.max(0, Number(options.inFlightCount) || 0);
  const pendingCount = Math.max(0, Number(options.pendingCount) || 0);
  const lastInteractionAt = Math.max(0, Number(options.lastInteractionAt) || 0);
  if (isEditing) return true;
  if (inFlightCount > 0 || pendingCount > 0) return true;
  if (lastInteractionAt > 0 && nowMs - lastInteractionAt < NOTES_RERENDER_SETTLE_MS) return true;
  return false;
}

export function queueNotesSave(jobKey, value, deps) {
  const {
    noteSaveState,
    noteAutosaveMs,
    dispatchQueued,
    setNoteSaveState,
    flushNotesSave
  } = deps;
  if (!jobKey) return;
  const safeJobKey = String(jobKey || "").trim();
  if (!safeJobKey) return;
  noteSaveState.lastInteractionAt = Date.now();
  dispatchQueued(safeJobKey);
  const noteValue = String(value || "");
  if (!(noteSaveState.latestValues instanceof Map)) noteSaveState.latestValues = new Map();
  noteSaveState.latestValues.set(safeJobKey, noteValue);
  noteSaveState.pendingValues.set(safeJobKey, noteValue);
  if (!(noteSaveState.saveStatusByJobKey instanceof Map)) noteSaveState.saveStatusByJobKey = new Map();
  noteSaveState.saveStatusByJobKey.set(safeJobKey, "saving");
  setNoteSaveState(safeJobKey, "saving");
  if (noteSaveState.timers.has(safeJobKey)) {
    clearTimeout(noteSaveState.timers.get(safeJobKey));
  }
  const timer = setTimeout(() => {
    flushNotesSave(safeJobKey).catch(() => {});
  }, noteAutosaveMs);
  noteSaveState.timers.set(safeJobKey, timer);
}

export async function flushNotesSave(jobKey, value, deps) {
  const {
    noteSaveState,
    currentUser,
    getPreviousNoteLength,
    updateJobNotes,
    setNoteSaveState,
    dispatchSaved,
    dispatchFailed,
    queueActivityPulse,
    timelineScopeNotes
  } = deps;
  const safeJobKey = String(jobKey || "").trim();
  if (!safeJobKey || !currentUser) return;
  const generation = Number(noteSaveState.generation) || 0;
  const uid = String(currentUser.uid || currentUser.id || "");
  const requestToken = {};
  const isCurrentAccount = () => (
    (Number(noteSaveState.generation) || 0) === generation
    && (typeof deps.getCurrentUser !== "function"
      || String(deps.getCurrentUser()?.uid || deps.getCurrentUser()?.id || "") === uid)
  );
  const isCurrentSave = () => (
    isCurrentAccount() && noteSaveState.inFlight.get(safeJobKey) === requestToken
  );
  noteSaveState.lastInteractionAt = Date.now();
  if (typeof value === "string") {
    if (!(noteSaveState.latestValues instanceof Map)) noteSaveState.latestValues = new Map();
    noteSaveState.latestValues.set(safeJobKey, value);
    noteSaveState.pendingValues.set(safeJobKey, value);
  }
  if (noteSaveState.timers.has(safeJobKey)) {
    clearTimeout(noteSaveState.timers.get(safeJobKey));
    noteSaveState.timers.delete(safeJobKey);
  }
  if (!noteSaveState.pendingValues.has(safeJobKey)) return;
  if (noteSaveState.inFlight.has(safeJobKey)) return;
  noteSaveState.inFlight.set(safeJobKey, requestToken);

  if (!(noteSaveState.saveStatusByJobKey instanceof Map)) noteSaveState.saveStatusByJobKey = new Map();
  noteSaveState.saveStatusByJobKey.set(safeJobKey, "saving");
  setNoteSaveState(safeJobKey, "saving");
  const saveValue = noteSaveState.pendingValues.get(safeJobKey);
  try {
    const previousLength = typeof getPreviousNoteLength === "function"
      ? getPreviousNoteLength(jobKey)
      : undefined;
    const notesResult = await updateJobNotes(currentUser.uid || currentUser.id, safeJobKey, saveValue, {
      previousLength
    });
    if (!notesResult.ok) throw new Error(notesResult.error || "Could not save notes.");
    if (!isCurrentSave()) return;
    if (noteSaveState.pendingValues.get(safeJobKey) === saveValue) {
      noteSaveState.pendingValues.delete(safeJobKey);
      noteSaveState.latestValues?.delete?.(safeJobKey);
      noteSaveState.saveStatusByJobKey.set(safeJobKey, "saved");
      setNoteSaveState(safeJobKey, "saved");
      dispatchSaved(safeJobKey);
      queueActivityPulse(safeJobKey, timelineScopeNotes);
    } else {
      noteSaveState.saveStatusByJobKey.set(safeJobKey, "saving");
      setNoteSaveState(safeJobKey, "saving");
    }
  } catch (err) {
    if (!isCurrentSave()) return;
    console.error("Could not save notes:", err);
    noteSaveState.saveStatusByJobKey.set(safeJobKey, "error");
    setNoteSaveState(safeJobKey, "error");
    dispatchFailed(safeJobKey, err?.message || "Could not save notes.");
  } finally {
    if (noteSaveState.inFlight.get(safeJobKey) === requestToken) {
      noteSaveState.inFlight.delete(safeJobKey);
      if (isCurrentAccount()) noteSaveState.lastInteractionAt = Date.now();
      const hasNewerPendingValue =
        noteSaveState.pendingValues.has(safeJobKey) &&
        noteSaveState.pendingValues.get(safeJobKey) !== saveValue;
      if (hasNewerPendingValue && isCurrentAccount()) {
        setTimeout(() => {
          deps.flushNotesSave(safeJobKey).catch(() => {});
        }, 0);
      }
    }
  }
}

export function clearNoteSaveQueues(noteSaveState) {
  noteSaveState.generation = (Number(noteSaveState.generation) || 0) + 1;
  noteSaveState.timers.forEach(timer => clearTimeout(timer));
  noteSaveState.timers.clear();
  noteSaveState.inFlight.clear();
  noteSaveState.pendingValues.clear();
  noteSaveState.latestValues?.clear?.();
  noteSaveState.saveStatusByJobKey?.clear?.();
  noteSaveState.lastInteractionAt = 0;
}
