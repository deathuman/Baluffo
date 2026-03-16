/**
 * Returns HTML for a loading state (spinner + message).
 * @param {string} text - Message to show
 * @returns {string} HTML string
 */
import { escapeHtml } from "../ui/index.js";

export function renderLoadingState(text) {
  return `<div class="loading">${escapeHtml(text)}</div>`;
}
