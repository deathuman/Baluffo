import { APPLICATION_STATUSES } from "./constants.js";

export function normalizeApplicationStatus(status) {
  const raw = String(status || "").toLowerCase().trim();
  if (raw === "bookmarked") return "bookmark";
  if (APPLICATION_STATUSES.includes(raw)) return raw;
  return "bookmark";
}

export function canTransitionPhase(currentStatus, nextStatus) {
  const current = normalizeApplicationStatus(currentStatus);
  const next = normalizeApplicationStatus(nextStatus);
  if (current === next) return true;
  if (current === "rejected") return false;
  if (next === "rejected") return true;

  const currentIdx = APPLICATION_STATUSES.indexOf(current);
  const nextIdx = APPLICATION_STATUSES.indexOf(next);
  if (currentIdx < 0 || nextIdx < 0) return false;
  return nextIdx === currentIdx + 1;
}
