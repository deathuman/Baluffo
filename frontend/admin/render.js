// The overview renderers are imported from their leaf by the Admin shell, so
// this barrel stays the ops/discovery surface that tests and the Ops controller
// share; re-exporting the overview ones here would keep the shell's dependency
// visible to knip as dead code.
export { renderUsersTableHtml } from "./render/overview.js";
export { renderDiscoveryCandidateReviewHtml, renderSourcesTableHtml } from "./render/sources.js";
export { appendAdminLogRow } from "./render/logs.js";
export {
  renderAdminOpsAlerts,
  renderAdminOpsDedupLists,
  renderAdminOpsFetcherMetrics,
  renderAdminOpsHistory,
  renderAdminOpsKpis,
  renderAdminOpsSchedule,
  renderAdminOpsTrends
} from "./render/ops.js";
