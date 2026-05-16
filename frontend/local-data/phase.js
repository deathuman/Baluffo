import {
  canTransitionPhase as canTransitionTrackingPhase,
  normalizeApplicationStatusMirror
} from "./tracking.js";

export function normalizeApplicationStatus(status) {
  return normalizeApplicationStatusMirror(status);
}

export function canTransitionPhase(currentStatus, nextStatus) {
  return canTransitionTrackingPhase(currentStatus, nextStatus);
}
