import {
  safeReadLocal,
  safeWriteLocal,
  safeReadSession,
  safeWriteJsonLocal
} from "../../local-data/storage-gateway.js";

// Admin state-sync helpers own local/session persistence used by app orchestration.

export function readSourceFilter(storageKey, fallback = "all") {
  return String(safeReadLocal(storageKey, fallback) || fallback).toLowerCase();
}

export function writeSourceFilter(storageKey, value) {
  return safeWriteLocal(storageKey, String(value || "all").toLowerCase());
}

export function readShowZeroJobs(storageKey) {
  return safeReadLocal(storageKey, "0") === "1";
}

export function writeShowZeroJobs(storageKey, enabled) {
  return safeWriteLocal(storageKey, enabled ? "1" : "0");
}

export function readAdminLastJobsUrl(storageKey, fallback = "jobs.html") {
  const url = String(safeReadSession(storageKey, "") || "");
  if (!url) return fallback;
  if (!url.startsWith("/") && !url.startsWith("jobs.html")) return fallback;
  return url;
}

export function writeJobsAutoRefreshSignal(storageKey, signal) {
  return safeWriteJsonLocal(storageKey, signal);
}
