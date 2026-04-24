const RUNTIME_MODE_KEY = "baluffo_runtime_mode";
const RUNTIME_BRIDGE_BASE_KEY = "baluffo_runtime_bridge_base";
const STARTUP_PROBE_KEY = "baluffo_startup_probe_enabled";
const DEFAULT_BRIDGE_HOST = "127.0.0.1";

function resolveUrl(href, fallbackHref = "") {
  const rawHref = String(href || fallbackHref || "").trim();
  if (!rawHref) return null;
  try {
    return new URL(rawHref, fallbackHref || undefined);
  } catch {
    return null;
  }
}

function safeGetItem(storageObject, key) {
  try {
    return String(storageObject?.getItem?.(key) || "").trim();
  } catch {
    return "";
  }
}

function safeSetItem(storageObject, key, value) {
  try {
    storageObject?.setItem?.(key, value);
  } catch {
    // Ignore storage write failures and fall back to current URL state.
  }
}

function getFrontendRuntimeConfig() {
  return globalThis.BALUFFO_FRONTEND_RUNTIME_CONFIG || {};
}

function resolveActiveDesktopRuntimeBridgeParams() {
  const runtimeConfig = getFrontendRuntimeConfig();
  if (!runtimeConfig?.runtime?.desktop) {
    return {};
  }
  const explicitBridgePort = Number(runtimeConfig?.bridge?.port || 0);
  if (
    !Number.isInteger(explicitBridgePort) ||
    explicitBridgePort <= 0 ||
    explicitBridgePort > 65535
  ) {
    return {};
  }
  return {
    bridgePort: String(explicitBridgePort),
    bridgeHost: String(runtimeConfig?.bridge?.host || "").trim() || DEFAULT_BRIDGE_HOST
  };
}

export function resolveDesktopRuntimeMode(
  href = window.location?.href || "",
  { sessionStorageObject = window.sessionStorage, persist = true } = {}
) {
  const url = resolveUrl(href);
  const explicitDesktop = String(url?.searchParams?.get("desktop") || "").trim();
  if (explicitDesktop === "1") {
    if (persist) {
      safeSetItem(sessionStorageObject, RUNTIME_MODE_KEY, "desktop");
    }
    return true;
  }
  return safeGetItem(sessionStorageObject, RUNTIME_MODE_KEY) === "desktop";
}

function resolveDesktopRuntimeBridgeParams(
  href = window.location?.href || "",
  { sessionStorageObject = window.sessionStorage, persist = true } = {}
) {
  const url = resolveUrl(href);
  const explicitBridgePort = String(url?.searchParams?.get("bridgePort") || "").trim();
  const explicitBridgeHost =
    String(url?.searchParams?.get("bridgeHost") || "").trim() || DEFAULT_BRIDGE_HOST;
  if (/^\d+$/.test(explicitBridgePort)) {
    if (persist) {
      safeSetItem(
        sessionStorageObject,
        RUNTIME_BRIDGE_BASE_KEY,
        `http://${explicitBridgeHost}:${explicitBridgePort}`
      );
    }
    return {
      bridgePort: explicitBridgePort,
      bridgeHost: explicitBridgeHost
    };
  }

  const activeRuntimeBridge = resolveActiveDesktopRuntimeBridgeParams();
  if (activeRuntimeBridge.bridgePort && activeRuntimeBridge.bridgeHost) {
    if (persist) {
      safeSetItem(
        sessionStorageObject,
        RUNTIME_BRIDGE_BASE_KEY,
        `http://${activeRuntimeBridge.bridgeHost}:${activeRuntimeBridge.bridgePort}`
      );
    }
    return activeRuntimeBridge;
  }

  const cachedBase = safeGetItem(sessionStorageObject, RUNTIME_BRIDGE_BASE_KEY);
  const parsedBase = resolveUrl(cachedBase);
  const cachedPort = String(parsedBase?.port || "").trim();
  const cachedHost = String(parsedBase?.hostname || "").trim();
  if (/^\d+$/.test(cachedPort) && cachedHost) {
    return {
      bridgePort: cachedPort,
      bridgeHost: cachedHost
    };
  }
  return {};
}

function resolveStartupProbeStickyEnabled(
  href = window.location?.href || "",
  { sessionStorageObject = window.sessionStorage, persist = true } = {}
) {
  const url = resolveUrl(href);
  const explicitProbe = String(url?.searchParams?.get("startupProbe") || "").trim();
  if (explicitProbe === "1") {
    if (persist) {
      safeSetItem(sessionStorageObject, STARTUP_PROBE_KEY, "1");
    }
    return true;
  }
  return safeGetItem(sessionStorageObject, STARTUP_PROBE_KEY) === "1";
}

function resolveDesktopRuntimeQueryParams(
  href = window.location?.href || "",
  { sessionStorageObject = window.sessionStorage, persist = true } = {}
) {
  if (!resolveDesktopRuntimeMode(href, { sessionStorageObject, persist })) {
    return {};
  }

  const params = {
    desktop: "1",
    ...resolveDesktopRuntimeBridgeParams(href, { sessionStorageObject, persist })
  };
  if (resolveStartupProbeStickyEnabled(href, { sessionStorageObject, persist })) {
    params.startupProbe = "1";
  }
  return params;
}

export function appendDesktopRuntimeQueryParams(
  targetUrl,
  {
    currentHref = window.location?.href || "",
    sessionStorageObject = window.sessionStorage,
    persist = true
  } = {}
) {
  const resolvedTarget = targetUrl instanceof URL
    ? new URL(targetUrl.href)
    : resolveUrl(targetUrl, currentHref);
  if (!resolvedTarget) return null;

  const runtimeParams = resolveDesktopRuntimeQueryParams(currentHref, {
    sessionStorageObject,
    persist
  });
  Object.entries(runtimeParams).forEach(([key, value]) => {
    if (!value || resolvedTarget.searchParams.has(key)) return;
    resolvedTarget.searchParams.set(key, value);
  });
  return resolvedTarget;
}
