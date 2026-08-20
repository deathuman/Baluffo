/** Shared object-shape guards for ops domain models. */

export function getObjectValue(value) {
  return value && typeof value === "object" && !Array.isArray(value) ? value : {};
}

export function isPlainObject(value) {
  return value && typeof value === "object" && !Array.isArray(value);
}

export function hasUsefulValue(value) {
  if (value === null || value === undefined) return false;
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    return Boolean(normalized)
      && normalized !== "unknown"
      && normalized !== "never"
      && normalized !== "none"
      && normalized !== "not loaded yet";
  }
  return true;
}
