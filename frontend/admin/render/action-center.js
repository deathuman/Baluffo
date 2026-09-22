// Presentation for the Admin Action Center: a status chip, a checked-at stamp, and
// the signal rows beneath them.
//
// This module owns presentation only — it takes the evaluated signals and poll
// metadata and returns HTML strings. It never touches the DOM and never mutates its
// inputs, so the controller stays a polling/evaluation surface and the markup is
// testable without constructing one.
//
// The panel this replaces rendered all five states through one function, so a
// passing check was pixel-identical to a failed one, and it used emoji and ASCII
// glyphs (`i`, `⚠`, `⏰`, `↔️`, `❌`, `✓`, `▶`, `🔄`, `📋`, `✕`) where the rest of
// the page uses monochrome inline SVG. Every state now has its own tone and icon,
// and the healthy case — the common one — is a single compact line rather than a
// full-width box.
import { escapeHtml, tooltipAttrs } from "../../shared/ui/index.js";

// Single source of truth for state presentation. `tone` maps to a CSS modifier
// class; `icon` and `chip` are the visible affordances that distinguish states.
const STATE_META = new Map([
  ["healthy", { tone: "ok", icon: "check", chip: "Healthy", line: "All systems operational" }],
  ["partial", { tone: "neutral", icon: "clock", chip: "Partial", line: "" }],
  ["checking", { tone: "neutral", icon: "clock", chip: "Checking", line: "" }],
  ["active-work-delayed", { tone: "neutral", icon: "pause", chip: "Delayed", line: "" }],
  ["unavailable", { tone: "warning", icon: "alert", chip: "Unavailable", line: "" }]
]);

// Monochrome, 24x24, `currentColor` — the same contract the two icon buttons in
// admin.html already use, so icons inherit the theme without per-theme overrides.
export const ACTION_CENTER_ICONS = Object.freeze({
  check: "M9 16.17 4.83 12l-1.42 1.41L9 19 21 7l-1.41-1.41z",
  alert: "M12 2 1 21h22L12 2Zm0 6 6.5 11h-13L12 8Zm-1 3v4h2v-4h-2Zm0 5v2h2v-2h-2Z",
  cross: "M19 6.41 17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z",
  info: "M11 7h2v2h-2V7Zm0 4h2v6h-2v-6Zm1-9a10 10 0 1 0 0 20 10 10 0 0 0 0-20Zm0 18a8 8 0 1 1 0-16 8 8 0 0 1 0 16Z",
  pause: "M6 5h4v14H6V5Zm8 0h4v14h-4V5Z",
  clock: "M12 2a10 10 0 1 0 0 20 10 10 0 0 0 0-20Zm0 18a8 8 0 1 1 0-16 8 8 0 0 1 0 16Zm1-13h-2v6l5 3 1-1.73-4-2.27V7Z",
  refresh: "M17.65 6.35A8 8 0 1 0 19.73 14h-2.08A6 6 0 1 1 12 6c1.66 0 3.14.69 4.22 1.78L13 11h7V4l-2.35 2.35Z",
  copy: "M16 1H4a2 2 0 0 0-2 2v14h2V3h12V1Zm3 4H8a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h11a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2Zm0 16H8V7h11v14Z",
  clipboard: "M16 1H4a2 2 0 0 0-2 2v14h2V3h12V1Zm3 4H8a2 2 0 0 0-2 2v14a2 2 0 0 0 2 2h11a2 2 0 0 0 2-2V7a2 2 0 0 0-2-2Zm0 16H8V7h11v14Z",
  close: "M19 6.41 17.59 5 12 10.59 6.41 5 5 6.41 10.59 12 5 17.59 6.41 19 12 13.41 17.59 19 19 17.59 13.41 12z",
  "arrow-right": "M12 4l-1.41 1.41L16.17 11H4v2h12.17l-5.58 5.59L12 20l8-8-8-8Z"
});

const ACTION_META = new Map([
  ["review", { label: "Review", icon: "arrow-right", dataAction: "review" }],
  ["retry_fetch", { label: "Run Jobs Fetcher", icon: "refresh", dataAction: "retry", preset: "default" }],
  ["retry_failed", { label: "Retry failed", icon: "refresh", dataAction: "retry", preset: "retry_failed" }],
  ["retry_sync", { label: "Retry sync", icon: "refresh", dataAction: "retry", preset: "sync_pull" }],
  ["copy_diagnostics", { label: "Copy diagnostics", icon: "copy", dataAction: "copy-diagnostics" }],
  ["dismiss", { label: "Dismiss", icon: "close", dataAction: "dismiss" }]
]);

export function actionCenterIcon(name, extraClass = "") {
  const path = ACTION_CENTER_ICONS[name] || ACTION_CENTER_ICONS.info;
  const className = extraClass ? `action-center-icon ${extraClass}` : "action-center-icon";
  return `<svg class="${className}" viewBox="0 0 24 24" width="16" height="16" aria-hidden="true" focusable="false"><path fill="currentColor" d="${path}" /></svg>`;
}

// Relative age of the last completed poll. Deliberately local: reusing
// `frontend/saved/render.js:formatRelativeTime` would be a cross-slice import.
export function formatCheckedAt(checkedAtMs, nowMs = Date.now()) {
  const checked = Number(checkedAtMs);
  if (!Number.isFinite(checked) || checked <= 0) return "";
  const elapsedMs = Math.max(0, Number(nowMs) - checked);
  const seconds = Math.floor(elapsedMs / 1000);
  if (seconds < 5) return "checked just now";
  if (seconds < 60) return `checked ${seconds}s ago`;
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `checked ${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  return `checked ${hours}h ago`;
}

// The worst severity present decides the chip, so a single critical signal is not
// masked by healthy ones alongside it.
export function resolveSeverity(signals = []) {
  const list = Array.isArray(signals) ? signals : [];
  if (list.some(signal => signal && signal.severity === "critical")) return "critical";
  if (list.length > 0) return "warning";
  return "healthy";
}

export function severityChipClass(severity) {
  if (severity === "critical") return "critical";
  if (severity === "warning") return "warning";
  return "healthy";
}

// `label` is the headline and `summary` the detail. Both are always rendered: the
// old panel's duplication ("Storage health issue detected — Storage is unhealthy")
// was fixed by making that summary specific rather than by filtering summaries here.
// A text-similarity filter was tried and rejected — it scored the genuinely useful
// sync summary ("Sync needs attention" / "Sync conflict needs review…") as a
// restatement and hid it.
export function renderSignalRow(signal) {
  const severity = signal?.severity === "critical" ? "critical" : "warning";
  const label = String(signal?.label || signal?.id || "").trim();
  const summary = String(signal?.summary || "").trim();

  const detail = summary
    ? `<span class="action-center-signal-summary">${escapeHtml(summary)}</span>`
    : "";

  const actions = Array.isArray(signal?.actions) ? signal.actions : [];
  const actionsHtml = actions
    .map(action => renderActionButton(action, signal?.id))
    .filter(Boolean)
    .join("");

  return `<div class="action-center-signal action-center-signal-${severity}" data-signal="${escapeHtml(String(signal?.id || ""))}">
      <span class="admin-status-chip ${severityChipClass(severity)} action-center-signal-chip">${escapeHtml(severity)}</span>
      <div class="action-center-signal-content">
        <span class="action-center-signal-label">${escapeHtml(label)}</span>
        ${detail}
      </div>
      <div class="action-center-signal-actions">${actionsHtml}</div>
    </div>`;
}

function renderActionButton(action, signalId) {
  const meta = ACTION_META.get(action);
  if (!meta) return "";
  const presetAttr = meta.preset ? ` data-preset="${escapeHtml(meta.preset)}"` : "";
  return `<button class="btn clear-filters-btn action-center-signal-btn" type="button" data-action="${escapeHtml(meta.dataAction)}" data-signal="${escapeHtml(String(signalId || ""))}"${presetAttr}>${actionCenterIcon(meta.icon)}<span>${escapeHtml(meta.label)}</span></button>`;
}

export function renderStatusLine({ state, summary, detail = "" }) {
  const meta = STATE_META.get(state) || STATE_META.get("unavailable");
  const detailHtml = detail
    ? `<span class="action-center-status-detail">${escapeHtml(detail)}</span>`
    : "";
  return `<div class="action-center-status action-center-status-${meta.tone}" data-state="${escapeHtml(state)}">
      ${actionCenterIcon(meta.icon)}
      <span class="action-center-status-text">${escapeHtml(summary || meta.line)}</span>
      ${detailHtml}
    </div>`;
}

export function renderStatusChip({ state, signals = [] }) {
  const hasSignals = Array.isArray(signals) && signals.length > 0;
  const meta = STATE_META.get(state);
  const severity = hasSignals ? resolveSeverity(signals) : null;
  const label = hasSignals
    ? `${signals.length} issue${signals.length === 1 ? "" : "s"}`
    : (meta?.chip || "Checking");
  const chipClass = hasSignals ? severityChipClass(severity) : (meta?.tone === "ok" ? "healthy" : "");
  const title = hasSignals ? `${severity} severity` : (meta?.chip || "Checking");
  return `<span class="admin-status-chip action-center-status-chip ${chipClass}"${tooltipAttrs(title)}>${escapeHtml(label)}</span>`;
}

export function renderCheckedAt(checkedAtMs, nowMs = Date.now()) {
  const text = formatCheckedAt(checkedAtMs, nowMs);
  return text ? `<span class="action-center-checked-at">${escapeHtml(text)}</span>` : "";
}

// A "View all → Ops Health" overflow row lived here. It was unreachable by
// construction, not by mistake in the comparison: `stale_fetch` requires a fetch
// age above 12h and `failed_sources` requires one at or below 12h, so those two can
// never be active together and the signal space tops out at three — exactly the
// display cap. Removed rather than left as an affordance that can never appear; if a
// fifth signal is ever added, the overflow row should come back with it.
export function renderActionCenterBody({ state, signals = [], summary = "", detail = "" }) {
  const list = Array.isArray(signals) ? signals : [];
  if (list.length === 0) return renderStatusLine({ state, summary, detail });
  return list.map(renderSignalRow).join("");
}
