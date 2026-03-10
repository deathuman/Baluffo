import {
  safeReadJsonLocal,
  safeWriteJsonLocal,
  safeReadSession
} from "../../local-data/storage-gateway.js";

// Saved page state-sync helpers: local/session persistence only.
export function buildSavedTimelinePrefsKey(prefix, uid) {
  return `${String(prefix || "")}:${String(uid || "")}`;
}

export function loadSavedTimelinePreferences(prefix, uid, normalizeScope, fallbackScope) {
  const fallback = { visible: false, scope: String(fallbackScope || "all") };
  const safeUid = String(uid || "").trim();
  if (!safeUid) return fallback;
  const parsed = safeReadJsonLocal(buildSavedTimelinePrefsKey(prefix, safeUid), fallback);
  return {
    visible: Boolean(parsed?.visible),
    scope: normalizeScope(parsed?.scope)
  };
}

export function persistSavedTimelinePreferences(prefix, uid, normalizeScope, nextState) {
  const safeUid = String(uid || "").trim();
  if (!safeUid) return false;
  const payload = {
    visible: Boolean(nextState?.visible),
    scope: normalizeScope(nextState?.scope)
  };
  return safeWriteJsonLocal(buildSavedTimelinePrefsKey(prefix, safeUid), payload);
}

export function readSavedLastJobsUrl(storageKey, fallback = "jobs.html") {
  const url = String(safeReadSession(storageKey, "") || "");
  if (!url) return fallback;
  if (!url.startsWith("/") && !url.startsWith("jobs.html")) return fallback;
  return url;
}

