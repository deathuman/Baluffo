import {
  clearNoteSaveQueues as clearNoteSaveQueuesFromModule,
  flushNotesSave as flushNotesSaveFromModule,
  queueNotesSave as queueNotesSaveFromModule
} from "../notes.js";

export function createSavedRuntimeNotes(deps) {
  function queueNotesSave(jobKey, value) {
    return queueNotesSaveFromModule(jobKey, value, {
      noteSaveState: deps.noteSaveState,
      noteAutosaveMs: deps.noteAutosaveMs,
      dispatchQueued: safeJobKey => {
        deps.savedDispatch.dispatch({ type: deps.savedActions.NOTES_QUEUED, payload: { jobKey: safeJobKey } });
      },
      setNoteSaveState: deps.setNoteSaveState,
      flushNotesSave
    });
  }

  async function flushNotesSave(jobKey, value) {
    return flushNotesSaveFromModule(jobKey, value, {
      noteSaveState: deps.noteSaveState,
      currentUser: deps.getCurrentUser(),
      getPreviousNoteLength: deps.getPreviousNoteLength,
      updateJobNotes: (uid, safeJobKey, saveValue, options) => deps.updateJobNotes(uid, safeJobKey, saveValue, options),
      setNoteSaveState: deps.setNoteSaveState,
      dispatchSaved: safeJobKey => {
        deps.savedDispatch.dispatch({ type: deps.savedActions.NOTES_SAVED, payload: { jobKey: safeJobKey } });
      },
      dispatchFailed: (safeJobKey, error) => {
        deps.savedDispatch.dispatch({
          type: deps.savedActions.NOTES_SAVE_FAILED,
          payload: { jobKey: safeJobKey, error }
        });
      },
      queueActivityPulse: deps.queueActivityPulse,
      timelineScopeNotes: deps.timelineScopeNotes,
      flushNotesSave
    });
  }

  function clearNoteSaveQueues() {
    clearNoteSaveQueuesFromModule(deps.noteSaveState);
  }

  return {
    queueNotesSave,
    flushNotesSave,
    clearNoteSaveQueues
  };
}
