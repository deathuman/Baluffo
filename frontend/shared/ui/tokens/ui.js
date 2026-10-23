/**
 * @fileoverview The `data-ui` attribute-selector factory every token consumer
 * builds its queries from. It lives beside the token tables rather than in the
 * `selectors.js` barrel so a page can take the factory without also taking
 * every page's token registry.
 */

/**
 * Creates a CSS attribute selector for a given UI token.
 * @param {string} token
 * @returns {string}
 */
export const ui = (token) => `[data-ui="${token}"]`;
