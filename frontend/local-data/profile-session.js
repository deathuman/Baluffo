import { AdminConfig } from "../../admin-config.js";
import { SESSION_KEY, PROFILE_KEY } from "./constants.js";

export function makeUser(profile) {
  return {
    uid: profile.id,
    displayName: profile.name,
    email: profile.email || ""
  };
}

export function readProfiles() {
  try {
    const raw = localStorage.getItem(PROFILE_KEY);
    const arr = raw ? JSON.parse(raw) : [];
    return Array.isArray(arr) ? arr : [];
  } catch {
    return [];
  }
}

export function writeProfiles(profiles) {
  localStorage.setItem(PROFILE_KEY, JSON.stringify(profiles));
}

export function verifyAdminPin(pin) {
  return String(pin || "") === String(AdminConfig.ADMIN_PIN_DEFAULT || "");
}

export function ensureAdmin(pin) {
  if (!verifyAdminPin(pin)) throw new Error("Invalid admin PIN.");
}

export function getStoredSessionUser() {
  const profileId = localStorage.getItem(SESSION_KEY);
  if (!profileId) return null;
  const profile = readProfiles().find(p => p.id === profileId);
  return profile ? makeUser(profile) : null;
}
