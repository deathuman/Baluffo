/**
 * @fileoverview Shared text normalization utilities.
 * Provides common string normalization functions used across pages.
 */

/**
 * Normalizes a value to a trimmed, lowercased string.
 * Returns empty string for null/undefined.
 * @param {*} value - Value to normalize
 * @returns {string}
 */
export function normalizeToken(value) {
  return String(value || "").trim().toLowerCase();
}

/**
 * Normalizes an optional text value.
 * Returns empty string for null/undefined, otherwise trimmed string.
 * @param {*} value - Value to normalize
 * @returns {string}
 */
export function normalizeOptionalText(value) {
  return String(value || "").trim();
}
