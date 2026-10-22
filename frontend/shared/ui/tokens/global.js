/**
 * @fileoverview Handles every packaged page resolves (theme, page nav, app version, auth pill).
 * Split out of `frontend/shared/ui/selectors.js` so a page bundles only the
 * table it resolves instead of every page's.
 */

export const GLOBAL_TOKENS = Object.freeze({
  themeToggle: "theme-toggle-btn",
  bridgeStatus: "admin-bridge-status-badge",
  jobsPageBtn: "jobs-page-btn",
  savedPageBtn: "saved-page-btn",
  adminPageBtn: "admin-page-btn",
  appVersion: "app-version",
  authStatus: "auth-status",
  authStatusHint: "auth-status-hint",
  authAvatar: "auth-avatar",
  authSignInBtn: "auth-sign-in-btn",
  authSignOutBtn: "auth-sign-out-btn",
});
