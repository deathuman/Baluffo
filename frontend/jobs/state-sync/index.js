import {
  safeReadJsonLocal,
  safeWriteJsonLocal,
  safeReadLocal,
  safeWriteLocal,
  safeWriteSession
} from "../../local-data/storage-gateway.js";

export function readQuickFilterPreferences(storageKey, fallback = []) {
  const parsed = safeReadJsonLocal(storageKey, fallback);
  return Array.isArray(parsed) ? parsed : fallback;
}

export function writeQuickFilterPreferences(storageKey, values) {
  return safeWriteJsonLocal(storageKey, values);
}

export function readAutoRefreshAppliedId(storageKey) {
  return String(safeReadLocal(storageKey, "") || "");
}

export function writeAutoRefreshAppliedId(storageKey, value) {
  return safeWriteLocal(storageKey, String(value || ""));
}

export function writeAutoRefreshSignal(storageKey, signal) {
  return safeWriteJsonLocal(storageKey, signal);
}

export function readAutoRefreshSignal(storageKey) {
  return safeReadLocal(storageKey, "");
}

export function rememberJobsUrl(storageKey, url) {
  return safeWriteSession(storageKey, url);
}
