const DEFAULT_ADMIN_BRIDGE_BASE = "http://127.0.0.1:8877";

export function resolveAdminBridgeBase(config = {}) {
  return config?.ADMIN_BRIDGE_BASE ?? DEFAULT_ADMIN_BRIDGE_BASE;
}
