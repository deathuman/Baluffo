import {
  APPROVED_DESKTOP_PAGE_PATHS,
  DESKTOP_NAVIGATION_BYPASS_WINDOW_MS,
  desktopState
} from "./state.js";
import { appendDesktopRuntimeQueryParams } from "../runtime-context.js";

function resolveDesktopNavigationUrl(target, baseHref = window.location?.href || "") {
  const rawTarget = String(target || "").trim();
  if (!rawTarget) {
    return null;
  }
  try {
    return new URL(rawTarget, baseHref || undefined);
  } catch {
    return null;
  }
}

function isApprovedDesktopPageNavigation(url, currentHref = window.location?.href || "") {
  const targetUrl = url instanceof URL ? url : resolveDesktopNavigationUrl(url, currentHref);
  const currentUrl = resolveDesktopNavigationUrl(currentHref, currentHref);
  if (!targetUrl || !currentUrl || targetUrl.origin !== currentUrl.origin) {
    return false;
  }
  return APPROVED_DESKTOP_PAGE_PATHS.has(String(targetUrl.pathname || "/").toLowerCase());
}

export function clearDesktopNavigationBypass() {
  desktopState.desktopNavigationBypassExpiresAt = 0;
}

export function hasDesktopNavigationBypass() {
  return (
    desktopState.desktopNavigationBypassExpiresAt > 0
    && Date.now() <= desktopState.desktopNavigationBypassExpiresAt
  );
}

function armDesktopNavigationBypass(targetUrl) {
  if (!isApprovedDesktopPageNavigation(targetUrl)) {
    clearDesktopNavigationBypass();
    return false;
  }
  desktopState.desktopNavigationBypassExpiresAt = Date.now() + DESKTOP_NAVIGATION_BYPASS_WINDOW_MS;
  return true;
}

export function armDesktopReloadBypass() {
  desktopState.desktopNavigationBypassExpiresAt = Date.now() + DESKTOP_NAVIGATION_BYPASS_WINDOW_MS;
  return true;
}

export function consumeDesktopNavigationBypass() {
  const hasBypass = hasDesktopNavigationBypass();
  clearDesktopNavigationBypass();
  return hasBypass;
}

export function navigateDesktopPage(
  target,
  {
    locationObject = window.location,
    baseHref = window.location?.href || "",
    sessionStorageObject = window.sessionStorage
  } = {}
) {
  let resolvedTarget = resolveDesktopNavigationUrl(target, baseHref);
  if (resolvedTarget && isApprovedDesktopPageNavigation(resolvedTarget, baseHref)) {
    resolvedTarget = appendDesktopRuntimeQueryParams(resolvedTarget, {
      currentHref: baseHref,
      sessionStorageObject
    });
  }
  const nextHref = resolvedTarget ? resolvedTarget.href : String(target || "");
  armDesktopNavigationBypass(resolvedTarget);
  if (locationObject && typeof locationObject.assign === "function") {
    locationObject.assign(nextHref);
    return nextHref;
  }
  if (locationObject && "href" in locationObject) {
    locationObject.href = nextHref;
  }
  return nextHref;
}
