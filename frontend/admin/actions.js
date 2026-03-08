export const ADMIN_ACTIONS = {
  UNLOCKED: "admin/unlocked",
  LOCKED: "admin/locked",
  OVERVIEW_REFRESHED: "admin/overviewRefreshed",
  DISCOVERY_REFRESHED: "admin/discoveryRefreshed",
  OPS_REFRESHED: "admin/opsRefreshed"
};

export function createAdminDispatcher(initial = {}) {
  const state = {
    isUnlocked: false,
    lastOverviewAt: "",
    lastDiscoveryAt: "",
    lastOpsAt: "",
    ...initial
  };

  function dispatch(action) {
    switch (action?.type) {
      case ADMIN_ACTIONS.UNLOCKED:
        state.isUnlocked = true;
        break;
      case ADMIN_ACTIONS.LOCKED:
        state.isUnlocked = false;
        break;
      case ADMIN_ACTIONS.OVERVIEW_REFRESHED:
        state.lastOverviewAt = String(action?.payload?.at || "");
        break;
      case ADMIN_ACTIONS.DISCOVERY_REFRESHED:
        state.lastDiscoveryAt = String(action?.payload?.at || "");
        break;
      case ADMIN_ACTIONS.OPS_REFRESHED:
        state.lastOpsAt = String(action?.payload?.at || "");
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
