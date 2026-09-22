// Bounded, read-only detail body for one run, rendered into the inspector drawer
// when a run row is clicked.
//
// This module owns presentation only: it takes the row view and its analysis
// payload and returns an HTML string. It never touches the DOM and never mutates
// its inputs, so the same function serves the drawer today and any other host
// tomorrow.
//
// The panel it replaces printed the same facts three times (the progress string
// as a summary line, again as a synthetic "source order" timeline entry, and a
// third time inside the diagnostic hints) plus `Duration` and `Elapsed` side by
// side as two names for one number. Sections are therefore de-duplicated and
// hidden when empty rather than filled with "nothing recorded" placeholders.
import { escapeHtml } from "../../shared/ui/index.js";
import { formatDateTime } from "./ops-shared.js";

const SUMMARY_STAT_LABELS = new Map([
  ["outputCount", "Output"],
  ["sourceCount", "Sources"],
  ["successfulSources", "Sources resolved"],
  ["failedSources", "Failed"],
  ["okWithWarningSources", "Warnings"],
  ["queuedCandidateCount", "Queued candidates"],
  ["failedProbeCount", "Failed probes"],
  ["foundEndpointCount", "Endpoints found"],
  ["generatedCandidateCount", "Candidates generated"],
  ["survivedDedupeCandidateCount", "Survived dedupe"],
  ["probedCandidateCount", "Probed"],
  ["discoverableButDeferredCount", "Deferred"],
  ["completedStageCount", "Stages completed"],
  ["stageIndex", "Stage index"],
  ["stageTotal", "Stages total"],
  ["currentStageKey", "Stage"],
  ["stage", "Stage"],
  ["baselineOutputCount", "Baseline output"],
  ["jobsPageLoadedCount", "Jobs page loaded"],
  ["finalOutputCount", "Final output"],
  ["updatesFound", "Updates found"],
  ["action", "Action"],
  ["activeCount", "Active"],
  ["pendingCount", "Pending"],
  ["rejectedCount", "Rejected"],
  ["error", "Error"]
]);

// A stat grid that grows without bound stops being readable, and the widest real
// payloads carry a handful of keys at most.
const STAT_LIMIT = 8;
const TIMELINE_LIMIT = 5;
const HINT_LIMIT = 5;

// Words that carry no discriminating information on their own: every run of a
// given type repeats them, so their presence in a hint proves nothing about
// whether the hint adds anything to the progress line.
const ECHO_NOISE_TOKENS = new Set([
  "pipeline", "discovery", "fetch", "sync", "task", "run", "runs", "stage", "stages",
  "step", "steps", "status", "completed", "complete", "done", "running", "active",
  "ok", "succeeded", "success", "finished", "started", "and", "the", "with", "for",
  "vs", "base", "comparison", "source", "sources", "output"
]);

function humanizeKey(key) {
  const mapped = SUMMARY_STAT_LABELS.get(key);
  if (mapped) return mapped;
  const spaced = String(key || "").replace(/([a-z0-9])([A-Z])/g, "$1 $2").replace(/[_-]+/g, " ").trim();
  return spaced ? spaced.charAt(0).toUpperCase() + spaced.slice(1) : String(key || "");
}

function formatStatValue(value) {
  if (typeof value === "boolean") return value ? "yes" : "no";
  if (typeof value === "number" && Number.isFinite(value)) return value.toLocaleString();
  return String(value ?? "");
}

function tokenize(value) {
  return String(value || "")
    .toLowerCase()
    .split(/[^a-z0-9]+/)
    .filter(Boolean);
}

// True when a hint only restates the progress line. Substring containment covers
// the verbatim echo; the token check covers the rephrased one ("Pipeline stage
// 3/3 Pipeline completed" against "Pipeline completed (100%) | step 3/3 | ...").
function isProgressEcho(hint, progressText) {
  const hintText = String(hint || "").trim().toLowerCase();
  const progress = String(progressText || "").trim().toLowerCase();
  if (!hintText) return true;
  if (!progress) return false;
  if (progress.includes(hintText) || hintText.includes(progress)) return true;
  const progressTokens = new Set(tokenize(progress));
  const significant = new Set(tokenize(hintText).filter(token => !ECHO_NOISE_TOKENS.has(token)));
  if (!significant.size) return true;
  return Array.from(significant).every(token => progressTokens.has(token));
}

// Timeline entries must be real events with a real stamp. The model synthesises a
// `source: "progress"` entry from the current progress payload; it has no
// timestamp of its own and merely repeats the progress line, so it is dropped
// here rather than in the model, which keeps the copy-diagnostics payload stable.
function selectTimelineEntries(entries, progressText) {
  const rows = Array.isArray(entries) ? entries : [];
  const seen = new Set();
  const selected = [];
  for (const entry of rows) {
    if (!entry || typeof entry !== "object") continue;
    if (!String(entry.timestamp || "").trim()) continue;
    const label = String(entry.label || "").trim();
    if (!label) continue;
    if (entry.source === "progress" && isProgressEcho(`${label} ${entry.detail || ""}`, progressText)) continue;
    const dedupeKey = `${entry.timestamp}|${label}`;
    if (seen.has(dedupeKey)) continue;
    seen.add(dedupeKey);
    selected.push(entry);
    if (selected.length >= TIMELINE_LIMIT) break;
  }
  return selected;
}

function renderStatGrid(summaryCounts, progressText) {
  const source = summaryCounts && typeof summaryCounts === "object" && !Array.isArray(summaryCounts)
    ? summaryCounts
    : {};
  const stats = Object.entries(source)
    .filter(([_key, value]) => value !== "" && value !== null && value !== undefined)
    .slice(0, STAT_LIMIT);
  const cells = stats.map(([key, value]) => `
    <div class="admin-ops-run-detail-stat">
      <span class="admin-ops-run-detail-stat-label">${escapeHtml(humanizeKey(key))}</span>
      <span class="admin-ops-run-detail-stat-value">${escapeHtml(formatStatValue(value))}</span>
    </div>
  `).join("");
  const progress = String(progressText || "").trim()
    ? `<div class="admin-ops-run-detail-stat admin-ops-run-detail-stat-wide">
        <span class="admin-ops-run-detail-stat-label">Progress</span>
        <span class="admin-ops-run-detail-stat-value">${escapeHtml(progressText)}</span>
      </div>`
    : "";
  if (!cells && !progress) return "";
  return `
    <div class="admin-ops-run-detail-section">
      <div class="admin-ops-run-detail-section-title">Evidence</div>
      <div class="admin-ops-run-detail-stats">${cells}${progress}</div>
    </div>
  `;
}

function renderTimeline(entries) {
  if (!entries.length) return "";
  const items = entries.map(entry => {
    const severity = ["critical", "warning", "healthy"].includes(String(entry.severity || ""))
      ? entry.severity
      : "muted";
    const status = entry.status || entry.type || entry.source || "event";
    const detail = entry.detail ? ` · ${escapeHtml(entry.detail)}` : "";
    return `
      <li class="admin-ops-run-timeline-item">
        <span class="admin-ops-run-timeline-time">${escapeHtml(formatDateTime(entry.timestamp))}</span>
        <span class="admin-status-chip ${severity}">${escapeHtml(status)}</span>
        <span class="admin-ops-run-timeline-message">${escapeHtml(entry.label || "")}${detail}</span>
      </li>
    `;
  }).join("");
  return `
    <div class="admin-ops-run-detail-section">
      <div class="admin-ops-run-detail-section-title">Timeline</div>
      <ol class="admin-ops-run-timeline-list">${items}</ol>
    </div>
  `;
}

function renderHints(hints) {
  if (!hints.length) return "";
  const items = hints.map(hint => `<li>${escapeHtml(hint)}</li>`).join("");
  return `
    <div class="admin-ops-run-detail-section admin-ops-run-detail-hints">
      <div class="admin-ops-run-detail-section-title">Diagnostic hints</div>
      <ul>${items}</ul>
    </div>
  `;
}

function formatDurationMs(value) {
  const ms = Number(value);
  if (!Number.isFinite(ms) || ms < 0) return "";
  if (ms < 1000) return `${Math.round(ms)}ms`;
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`;
  const minutes = Math.floor(ms / 60000);
  const seconds = Math.round((ms % 60000) / 1000);
  return `${minutes}m ${seconds}s`;
}

// These two lists are the run's slowest sources and its work items. They used to
// be dumped as `key: value | key: value` strings, which is where most of the old
// panel's noise came from; here each entry reads as one line of prose.
function renderSlowSources(items) {
  const rows = (Array.isArray(items) ? items : [])
    .map(item => ({
      name: String(item?.sourceId || item?.id || item?.name || "").trim(),
      duration: formatDurationMs(item?.durationMs ?? item?.elapsedMs)
    }))
    .filter(row => row.name);
  if (!rows.length) return "";
  const list = rows.map(row => `
    <li class="admin-ops-run-detail-row">
      <span class="admin-ops-run-detail-row-name">${escapeHtml(row.name)}</span>
      ${row.duration ? `<span class="admin-ops-run-detail-row-meta">${escapeHtml(row.duration)}</span>` : ""}
    </li>
  `).join("");
  return `
    <div class="admin-ops-run-detail-section">
      <div class="admin-ops-run-detail-section-title">Slowest sources</div>
      <ul class="admin-ops-run-detail-rows">${list}</ul>
    </div>
  `;
}

function renderWorkItems(items) {
  const rows = (Array.isArray(items) ? items : [])
    .map(item => {
      const name = String(item?.name || item?.id || "").trim();
      const id = String(item?.id || "").trim();
      return {
        name,
        // The id is the stable handle an operator greps for, so it is worth the
        // extra line whenever the human-readable name does not already carry it.
        id: id && id !== name ? id : "",
        status: String(item?.status || "").trim(),
        detail: String(item?.error || item?.message || "").trim()
      };
    })
    .filter(row => row.name);
  if (!rows.length) return "";
  const list = rows.map(row => `
    <li class="admin-ops-run-detail-row">
      <span class="admin-ops-run-detail-row-name">${escapeHtml(row.name)}</span>
      ${row.status ? `<span class="admin-ops-run-detail-row-status">${escapeHtml(row.status)}</span>` : ""}
      ${row.id ? `<span class="admin-ops-run-detail-row-id">${escapeHtml(row.id)}</span>` : ""}
      ${row.detail ? `<span class="admin-ops-run-detail-row-meta">${escapeHtml(row.detail)}</span>` : ""}
    </li>
  `).join("");
  return `
    <div class="admin-ops-run-detail-section">
      <div class="admin-ops-run-detail-section-title">Work items</div>
      <ul class="admin-ops-run-detail-rows">${list}</ul>
    </div>
  `;
}

export function renderRunDetailHtml(view, {
  analysis = null,
  canCopyRunDiagnostics = false
} = {}) {
  if (!view) return "";
  const detail = analysis && typeof analysis === "object" && !Array.isArray(analysis)
    ? analysis
    : (view.analysisPayload && typeof view.analysisPayload === "object" ? view.analysisPayload : {});
  const timing = detail.timing && typeof detail.timing === "object" ? detail.timing : {};
  const runKey = String(view.key || "");

  // `Elapsed` is deliberately absent: for a live run it is the only clock and for
  // a finished run it is the same number as `Duration`. Showing both was the
  // single most confusing thing in the old panel.
  const facts = [
    timing.startedAt ? `<span><strong>Started</strong> ${escapeHtml(formatDateTime(timing.startedAt))}</span>` : "",
    (!view.isRunning && timing.finishedAt)
      ? `<span><strong>Finished</strong> ${escapeHtml(formatDateTime(timing.finishedAt))}</span>`
      : "",
    timing.durationLabel ? `<span><strong>Duration</strong> ${escapeHtml(timing.durationLabel)}</span>` : ""
  ].filter(Boolean).join("");

  const messages = [
    detail.warningSummary ? `<div class="admin-ops-run-detail-warning">${escapeHtml(detail.warningSummary)}</div>` : "",
    detail.failureSummary ? `<div class="admin-ops-run-detail-failure">${escapeHtml(detail.failureSummary)}</div>` : "",
    detail.remediationHint ? `<div class="admin-ops-run-analysis-hint">${escapeHtml(detail.remediationHint)}</div>` : ""
  ].filter(Boolean).join("");

  // The analysis progress label is the rich one ("output 42 | failed 1"); the row
  // cell text is a bare count ("42") that only makes sense inside its column, so
  // it is the fallback rather than the first choice.
  const progressText = String(detail.progressLabel || view.outputOrQueuedText || "").trim();
  const hints = (Array.isArray(detail.diagnosticHints) ? detail.diagnosticHints : [])
    .filter(hint => !isProgressEcho(hint, progressText))
    .slice(0, HINT_LIMIT);
  const timeline = selectTimelineEntries(detail.timelineEntries, progressText);
  const statGrid = renderStatGrid(detail.summaryCounts, progressText);
  const hintList = renderHints(hints);
  const timelineBlock = renderTimeline(timeline);
  const slowBlock = renderSlowSources(detail.slowExamples);
  const workBlock = renderWorkItems(detail.workItemExamples);

  const copyButton = (canCopyRunDiagnostics && runKey)
    ? `<button type="button" class="btn clear-filters-btn admin-ops-run-copy-btn" data-ops-run-diagnostics-copy="${escapeHtml(runKey)}" data-tooltip="Copy bounded diagnostics for this run">Copy</button>`
    : "";
  const abortButton = view.abortable
    ? `<button type="button" class="btn clear-filters-btn admin-ops-run-abort-btn" data-ops-run-abort="${escapeHtml(runKey)}" data-tooltip="Abort this task">Abort</button>`
    : "";
  const actions = copyButton || abortButton
    ? `<div class="admin-ops-run-detail-actions">${copyButton}${abortButton}</div>`
    : "";

  const body = [
    facts ? `<div class="admin-ops-run-detail-meta">${facts}</div>` : "",
    messages,
    statGrid,
    slowBlock,
    workBlock,
    timelineBlock,
    hintList
  ].filter(Boolean).join("");

  return `
    <div class="admin-ops-run-detail" data-ops-run-detail="${escapeHtml(runKey)}">
      <div class="admin-ops-run-detail-head">
        <div class="admin-ops-run-detail-identity">
          <span class="admin-ops-run-detail-type">${escapeHtml(view.typeText || "")}</span>
          <span class="admin-ops-run-detail-id">${escapeHtml(view.runId || "")}</span>
          <span class="admin-status-chip ${escapeHtml(view.statusClass || "healthy")}">${escapeHtml(view.statusText || "")}</span>
        </div>
        ${actions}
      </div>
      ${body || '<div class="muted">No supporting evidence was recorded for this run.</div>'}
    </div>
  `;
}
