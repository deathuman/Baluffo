export const NOTES_RERENDER_SETTLE_MS = 1200;

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
  noteSaveState.lastInteractionAt = Date.now();
  dispatchQueued(jobKey);
  noteSaveState.pendingValues.set(jobKey, String(value || ""));
  setNoteSaveState(jobKey, "saving");
  if (noteSaveState.timers.has(jobKey)) {
    clearTimeout(noteSaveState.timers.get(jobKey));
  }
  const timer = setTimeout(() => {
    flushNotesSave(jobKey).catch(() => {});
  }, noteAutosaveMs);
  noteSaveState.timers.set(jobKey, timer);
}

export async function flushNotesSave(jobKey, value, deps) {
  const {
    noteSaveState,
    currentUser,
    updateJobNotes,
    setNoteSaveState,
    dispatchSaved,
    dispatchFailed,
    queueActivityPulse,
    timelineScopeNotes
  } = deps;
  if (!jobKey || !currentUser) return;
  noteSaveState.lastInteractionAt = Date.now();
  if (typeof value === "string") {
    noteSaveState.pendingValues.set(jobKey, value);
  }
  if (noteSaveState.timers.has(jobKey)) {
    clearTimeout(noteSaveState.timers.get(jobKey));
    noteSaveState.timers.delete(jobKey);
  }
  if (!noteSaveState.pendingValues.has(jobKey)) return;
  if (noteSaveState.inFlight.get(jobKey)) return;
  noteSaveState.inFlight.set(jobKey, true);

  setNoteSaveState(jobKey, "saving");
  const saveValue = noteSaveState.pendingValues.get(jobKey);
  try {
    const notesResult = await updateJobNotes(currentUser.uid, jobKey, saveValue);
    if (!notesResult.ok) throw new Error(notesResult.error || "Could not save notes.");
    if (noteSaveState.pendingValues.get(jobKey) === saveValue) {
      noteSaveState.pendingValues.delete(jobKey);
      setNoteSaveState(jobKey, "saved");
      dispatchSaved(jobKey);
      queueActivityPulse(jobKey, timelineScopeNotes);
    } else {
      setNoteSaveState(jobKey, "saving");
    }
  } catch (err) {
    console.error("Could not save notes:", err);
    setNoteSaveState(jobKey, "error");
    dispatchFailed(jobKey, err?.message || "Could not save notes.");
  } finally {
    noteSaveState.lastInteractionAt = Date.now();
    noteSaveState.inFlight.delete(jobKey);
    if (noteSaveState.pendingValues.has(jobKey) && currentUser) {
      setTimeout(() => {
        deps.flushNotesSave(jobKey).catch(() => {});
      }, 0);
    }
  }
}

export function clearNoteSaveQueues(noteSaveState) {
  noteSaveState.timers.forEach(timer => clearTimeout(timer));
  noteSaveState.timers.clear();
  noteSaveState.inFlight.clear();
  noteSaveState.pendingValues.clear();
  noteSaveState.lastInteractionAt = 0;
}
