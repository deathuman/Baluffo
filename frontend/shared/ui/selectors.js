/**
 * @fileoverview Centralized registry of DOM selectors used throughout the Baluffo frontend.
 * This registry prioritizes [data-ui] attributes as "logic handles" to decouple JavaScript from styling/IDs.
 *
 * The tables themselves live in `tokens/`, one per surface. That split is what
 * keeps a packaged page from carrying the other pages' handles: jobs imports
 * `global` + `jobs`, saved `global` + `saved`, admin `global` + `admin`, and the
 * three token tables a page never resolves would otherwise be ~9 KB of strings
 * in every bundle (`docs/startup-probe-architecture.md`). This module stays the
 * documented entry point, and it is the one place that still exposes all four.
 */

import { ADMIN_TOKENS } from "./tokens/admin.js";
import { GLOBAL_TOKENS } from "./tokens/global.js";
import { JOBS_TOKENS } from "./tokens/jobs.js";
import { SAVED_TOKENS } from "./tokens/saved.js";

export { ui } from "./tokens/ui.js";

export const UI_TOKENS = Object.freeze({
  global: GLOBAL_TOKENS,
  jobs: JOBS_TOKENS,
  saved: SAVED_TOKENS,
  admin: ADMIN_TOKENS,
});
