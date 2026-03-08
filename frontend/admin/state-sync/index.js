import { safeReadLocal, safeWriteLocal } from "../../local-data/storage-gateway.js";

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
