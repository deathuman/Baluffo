export const JOBS_ACTIONS = {
  AUTH_CHANGED: "jobs/authChanged",
  FILTERS_CHANGED: "jobs/filtersChanged",
  REFRESH_REQUESTED: "jobs/refreshRequested",
  REFRESH_COMPLETED: "jobs/refreshCompleted",
  REFRESH_FAILED: "jobs/refreshFailed",
  SAVE_TOGGLED: "jobs/saveToggled"
};

export function createJobsDispatcher(initial = {}) {
  const state = {
    refreshing: false,
    refreshError: "",
    lastRefreshAt: "",
    authUserId: "",
    lastSaveToggleJobKey: "",
    ...initial
  };

  function dispatch(action) {
    switch (action?.type) {
      case JOBS_ACTIONS.AUTH_CHANGED:
        state.authUserId = String(action?.payload?.uid || "");
        break;
      case JOBS_ACTIONS.FILTERS_CHANGED:
        state.lastFilterHash = String(action?.payload?.signature || "");
        break;
      case JOBS_ACTIONS.REFRESH_REQUESTED:
        state.refreshing = true;
        state.refreshError = "";
        break;
      case JOBS_ACTIONS.REFRESH_COMPLETED:
        state.refreshing = false;
        state.lastRefreshAt = String(action?.payload?.finishedAt || "");
        break;
      case JOBS_ACTIONS.REFRESH_FAILED:
        state.refreshing = false;
        state.refreshError = String(action?.payload?.error || "");
        break;
      case JOBS_ACTIONS.SAVE_TOGGLED:
        state.lastSaveToggleJobKey = String(action?.payload?.jobKey || "");
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
