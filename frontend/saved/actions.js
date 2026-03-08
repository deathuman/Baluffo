export const SAVED_ACTIONS = {
  AUTH_CHANGED: "saved/authChanged",
  CUSTOM_JOB_MUTATED: "saved/customJobMutated",
  NOTES_QUEUED: "saved/notesQueued",
  NOTES_SAVED: "saved/notesSaved",
  NOTES_SAVE_FAILED: "saved/notesSaveFailed",
  ATTACHMENT_MUTATED: "saved/attachmentMutated"
};

export function createSavedDispatcher(initial = {}) {
  const state = {
    authUserId: "",
    pendingNotesCount: 0,
    lastNotesError: "",
    lastAttachmentJobKey: "",
    lastCustomMutationAt: "",
    ...initial
  };

  function dispatch(action) {
    switch (action?.type) {
      case SAVED_ACTIONS.AUTH_CHANGED:
        state.authUserId = String(action?.payload?.uid || "");
        break;
      case SAVED_ACTIONS.CUSTOM_JOB_MUTATED:
        state.lastCustomMutationAt = String(action?.payload?.at || "");
        break;
      case SAVED_ACTIONS.NOTES_QUEUED:
        state.pendingNotesCount = Math.max(0, Number(state.pendingNotesCount || 0) + 1);
        break;
      case SAVED_ACTIONS.NOTES_SAVED:
        state.pendingNotesCount = Math.max(0, Number(state.pendingNotesCount || 0) - 1);
        state.lastNotesError = "";
        break;
      case SAVED_ACTIONS.NOTES_SAVE_FAILED:
        state.pendingNotesCount = Math.max(0, Number(state.pendingNotesCount || 0) - 1);
        state.lastNotesError = String(action?.payload?.error || "");
        break;
      case SAVED_ACTIONS.ATTACHMENT_MUTATED:
        state.lastAttachmentJobKey = String(action?.payload?.jobKey || "");
        break;
      default:
        break;
    }
    return state;
  }

  return {
    dispatch,
    getState: () => ({ ...state })
  };
}
