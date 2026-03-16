/**
 * Returns HTML for an empty list state.
 * @param {string} [message="No jobs to show."] - Message to display
 * @returns {string} HTML string
 */
import { escapeHtml } from "../ui/index.js";

export function renderEmptyState(message = "No jobs to show.") {
  return `<div class="empty-state">${escapeHtml(message)}</div>`;
}
