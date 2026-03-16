/**
 * Returns HTML for an error state with message and optional retry button.
 * Caller is responsible for binding the retry button (e.g. getElementById("retry-fetch-btn")).
 * @param {string} message - Error message to display
 * @returns {string} HTML string
 */
import { escapeHtml } from "../ui/index.js";

export function renderErrorState(message) {
  return `
    <div class="error">
      <p>${escapeHtml(message)}</p>
      <button id="retry-fetch-btn" class="btn retry-btn">Retry</button>
    </div>
  `;
}
