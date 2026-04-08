/**
 * @fileoverview Shared auth view-model utility.
 * Parses auth status text into a consistent view model format.
 */

/**
 * Parses auth status text into a view model.
 * @param {string} text - Auth status text like "Signed in as John" or "Browsing as guest"
 * @returns {{ label: string, hint: string }}
 */
export function toAuthViewModel(text) {
  const raw = String(text || "").trim();
  const model = { label: raw || "Guest", hint: "" };
  const signedInMatch = raw.match(/^signed\s+in\s+as\s+(.+)$/i);
  if (!raw || /^browsing\s+as\s+guest$/i.test(raw) || /^guest$/i.test(raw)) {
    model.label = "Guest";
    model.hint = "Browsing as guest";
    return model;
  }
  if (signedInMatch) {
    model.label = String(signedInMatch[1] || "").trim() || "User";
    model.hint = "Signed in";
  }
  return model;
}
