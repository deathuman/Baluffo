import { escapeHtml, setTooltip, tooltipAttrs } from "../../shared/ui/index.js";
import {
  buildTaskRunAnalysis,
  buildTaskRunDiagnostics,
  buildTaskRunView
} from "../../shared/task-run-view-model.js";
import {
  formatDiscoverySubtaskProgress,
  formatScrapyStaticSourcesTailBadge,
  formatTaskProgressDetail
} from "../../shared/task-progress.js";
import {
  buildRunStatusTooltip,
  formatCompactDateTime,
  formatDateTime,
  formatDuration,
  formatSignedInt,
  getRunStatusChipClass
} from "./ops-shared.js";
import { renderRunDetailHtml } from "./ops-run-detail.js";

// The run detail body lives in the inspector drawer, which is a single stable
// element in `admin.html` rather than a child of the history container. Callers
// pass it in; the document lookup is only a fallback so the drawer still works if
// a caller forgets the option.
function resolveRunDetailHost(inspectorContentEl) {
  if (inspectorContentEl && typeof inspectorContentEl.addEventListener === "function") {
    return inspectorContentEl;
  }
  const doc = globalThis.document;
  if (!doc || typeof doc.getElementById !== "function") return null;
  const el = doc.getElementById("admin-inspector-content");
  return el && typeof el.addEventListener === "function" ? el : null;
}

const TASK_TYPE_LABELS = new Map([
  ["discovery", "Discovery"],
  ["pipeline", "Pipeline"],
  ["fetch", "Fetch"],
  ["sync", "Sync"]
]);

function formatRunTaskTypeLabel(value) {
  const normalized = String(value || "").trim();
  if (!normalized) return "";
  const mapped = TASK_TYPE_LABELS.get(normalized.toLowerCase());
  if (mapped) return mapped;
  return normalized.charAt(0).toUpperCase() + normalized.slice(1);
}

function isZeroPipelineProgressLabel(value) {
  return /^step 0(?:\s*\|\s*output 0\s*\(baseline 0\))?$/i.test(String(value || "").trim());
}

function firstMeaningfulPipelineLabel(...values) {
  return values.map(value => String(value || "").trim()).find(value => value && !isZeroPipelineProgressLabel(value)) || "";
}

function rowAbortRequested(row) {
  const summary = row?.summary && typeof row.summary === "object" ? row.summary : {};
  const progress = row?.taskProgress && typeof row.taskProgress === "object" ? row.taskProgress : {};
  return Boolean(
    String(summary?.abortRequestedAt || "").trim()
    || String(summary?.abortReason || "").trim()
    || String(row?.stage || "").trim().toLowerCase() === "aborting"
    || String(row?.lifecycleStatus || "").trim().toLowerCase() === "aborting"
    || String(row?.displayStatus || "").trim().toLowerCase() === "aborting"
    || String(progress?.phaseKey || "").trim().toLowerCase() === "aborting"
  );
}

export function renderAdminOpsTrends(trendsEl, runs) {
  if (!trendsEl) return;
  const canPatchInPlace = Boolean(trendsEl && trendsEl.dataset);
  const rows = Array.isArray(runs) ? runs : [];
  const fetchRuns = rows.filter(row => String(row?.type || "") === "fetch");
  const latest = fetchRuns[fetchRuns.length - 1];
  const prev = fetchRuns[fetchRuns.length - 2];
  if (!latest || !prev) {
    if (canPatchInPlace && trendsEl.dataset.opsTrendSig === "insufficient") return;
    if (canPatchInPlace) trendsEl.dataset.opsTrendSig = "insufficient";
    trendsEl.textContent = "Trends: not enough fetch history yet.";
    return;
  }
  const latestOutput = Number(latest?.summary?.outputCount || 0);
  const prevOutput = Number(prev?.summary?.outputCount || 0);
  const latestFailed = Number(latest?.summary?.failedSources || 0);
  const prevFailed = Number(prev?.summary?.failedSources || 0);
  const summaryText =
    `Trends: output Δ ${formatSignedInt(latestOutput - prevOutput)} (latest ${latestOutput.toLocaleString()}); failed sources Δ ${formatSignedInt(latestFailed - prevFailed)}.`;

  const successfulRuns = fetchRuns
    .filter(row => {
      const status = String(row?.status || row?.displayStatus || "ok").toLowerCase();
      const output = Number(row?.summary?.outputCount || 0);
      return status !== "error" && Number.isFinite(output) && output > 0;
    })
    .map(row => {
      const stamp = Date.parse(String(row?.finishedAt || row?.startedAt || ""));
      return {
        output: Number(row?.summary?.outputCount || 0),
        ts: Number.isFinite(stamp) ? stamp : 0
      };
    })
    .sort((a, b) => a.ts - b.ts)
    .slice(-20);

  if (!successfulRuns.length) {
    if (canPatchInPlace && trendsEl.dataset.opsTrendSig === "empty") return;
    if (canPatchInPlace) trendsEl.dataset.opsTrendSig = "empty";
    trendsEl.textContent = "Trends: no successful fetch history yet.";
    return;
  }
  const signature = successfulRuns.map(item => `${item.ts}:${item.output}`).join("|");
  if (canPatchInPlace && trendsEl.dataset.opsTrendSig === signature) return;
  if (canPatchInPlace) trendsEl.dataset.opsTrendSig = signature;

  const width = 640;
  const height = 170;
  const padLeft = 54;
  const padRight = 16;
  const padTop = 18;
  const padBottom = 34;
  const chartW = width - padLeft - padRight;
  const chartH = height - padTop - padBottom;
  const values = successfulRuns.map(item => item.output);
  const rawMinY = Math.min(...values);
  const rawMaxY = Math.max(...values);
  const range = Math.max(1, rawMaxY - rawMinY);
  const pad = Math.max(1, range * 0.18);
  const zoomMinY = Math.max(0, rawMinY - pad);
  const zoomMaxY = rawMaxY + pad;
  const spanY = Math.max(1, zoomMaxY - zoomMinY);

  const points = successfulRuns.map((item, idx) => {
    const x = padLeft + (successfulRuns.length <= 1 ? chartW / 2 : (idx * chartW) / (successfulRuns.length - 1));
    const y = padTop + chartH - ((item.output - zoomMinY) / spanY) * chartH;
    return { x, y, value: item.output, ts: item.ts };
  });

  const linePath = points.length <= 1
    ? `M ${points[0].x.toFixed(2)} ${points[0].y.toFixed(2)}`
    : points.slice(1).reduce((acc, point, idx) => {
      const prevPoint = points[idx];
      const dx = point.x - prevPoint.x;
      const c1x = prevPoint.x + (dx / 3);
      const c1y = prevPoint.y;
      const c2x = prevPoint.x + (2 * dx / 3);
      const c2y = point.y;
      return `${acc} C ${c1x.toFixed(2)} ${c1y.toFixed(2)} ${c2x.toFixed(2)} ${c2y.toFixed(2)} ${point.x.toFixed(2)} ${point.y.toFixed(2)}`;
    }, `M ${points[0].x.toFixed(2)} ${points[0].y.toFixed(2)}`);

  const areaPath = `${linePath} L ${points[points.length - 1].x.toFixed(2)} ${(padTop + chartH).toFixed(2)} L ${points[0].x.toFixed(2)} ${(padTop + chartH).toFixed(2)} Z`;
  const yTicks = [0, 0.5, 1].map(ratio => ({
    y: padTop + chartH - ratio * chartH,
    label: Math.round(zoomMinY + (spanY * ratio))
  }));
  const xLabel = item => (item.ts ? new Date(item.ts).toLocaleDateString(undefined, { month: "short", day: "numeric" }) : "n/a");
  const first = points[0];
  const mid = points[Math.floor((points.length - 1) / 2)];
  const last = points[points.length - 1];
  const pointDots = points.map(point =>
    `<circle cx="${point.x.toFixed(2)}" cy="${point.y.toFixed(2)}" r="2.0" class="admin-ops-trend-dot"><title>${point.value.toLocaleString()} jobs</title></circle>`
  ).join("");

  trendsEl.innerHTML = `
    <div class="admin-ops-trend-summary">${escapeHtml(summaryText)}</div>
    <svg class="admin-ops-trend-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="Successful jobs fetched over time">
      <path class="admin-ops-trend-area" d="${areaPath}" />
      ${yTicks.map(tick => `<line class="admin-ops-trend-grid" x1="${padLeft}" x2="${width - padRight}" y1="${tick.y.toFixed(2)}" y2="${tick.y.toFixed(2)}" />`).join("")}
      ${yTicks.map(tick => `<text class="admin-ops-trend-y-label" x="${padLeft - 8}" y="${(tick.y + 4).toFixed(2)}" text-anchor="end">${tick.label.toLocaleString()}</text>`).join("")}
      <path class="admin-ops-trend-line" d="${linePath}" />
      ${pointDots}
      <text class="admin-ops-trend-x-label" x="${first.x.toFixed(2)}" y="${height - 10}" text-anchor="start">${escapeHtml(xLabel(first))}</text>
      <text class="admin-ops-trend-x-label" x="${mid.x.toFixed(2)}" y="${height - 10}" text-anchor="middle">${escapeHtml(xLabel(mid))}</text>
      <text class="admin-ops-trend-x-label" x="${last.x.toFixed(2)}" y="${height - 10}" text-anchor="end">${escapeHtml(xLabel(last))}</text>
    </svg>
  `;
}

export function renderAdminOpsHistory(historyEl, runsOrModel, options = {}) {
  if (!historyEl) return;
  const onCopyRunDiagnostics = typeof options?.onCopyRunDiagnostics === "function"
    ? options.onCopyRunDiagnostics
    : null;
  const onAbortRun = typeof options?.onAbortRun === "function"
    ? options.onAbortRun
    : null;
  const runDetailHost = resolveRunDetailHost(options?.runDetailHost);
  const model = Array.isArray(runsOrModel)
    ? {
      currentRows: [],
      visibleCompletedRows: runsOrModel,
      olderCompletedRows: []
    }
    : (runsOrModel || {});
  const currentRows = Array.isArray(model.currentRows) ? model.currentRows : [];
  const visibleCompletedRows = Array.isArray(model.visibleCompletedRows) ? model.visibleCompletedRows : [];
  const olderCompletedRows = Array.isArray(model.olderCompletedRows) ? model.olderCompletedRows : [];
  const waitingForTaskState = Boolean(options?.waitingForTaskState);
  const taskStateUnavailable = Boolean(options?.taskStateUnavailable);
  // ponytail: distinguish "bridge hiccup" from "no active tasks": if last fetch failed,
  // we show "Task state temporarily unavailable" instead of the generic empty-state copy.
  const taskStateError = String(options?.taskStateError || "").trim();
  const historyPending = Boolean(options?.historyPending);
  const historyLoaded = options?.historyLoaded !== false;
  const historyError = String(options?.historyError || "").trim();
  const historyFullLoaded = Boolean(options?.historyFullLoaded);
  const canPatchInPlace = Boolean(
    historyEl
    && typeof historyEl.querySelector === "function"
    && typeof historyEl.querySelectorAll === "function"
    && historyEl.dataset
  );
  if (!currentRows.length && !visibleCompletedRows.length && !olderCompletedRows.length && historyLoaded && !historyPending) {
    historyEl.innerHTML = waitingForTaskState
      ? '<div class="admin-ops-loading">Waiting for task state...</div>'
      : taskStateUnavailable
        ? (taskStateError
          ? `<div class="admin-ops-loading">Task state temporarily unavailable${taskStateError ? ` (${escapeHtml(taskStateError)})` : ""}. Retrying.</div>`
          : '<div class="admin-ops-loading">Task state unavailable. Current runs may be stale.</div>')
      : '<div class="no-results">No run history yet.</div>';
    if (canPatchInPlace) {
      delete historyEl.dataset.opsStructureSig;
    }
    return;
  }
  if (!currentRows.length && !visibleCompletedRows.length && !olderCompletedRows.length && !historyLoaded) {
    historyEl.innerHTML = historyPending
      ? '<div class="admin-ops-loading admin-section-loading">Loading recent activity...</div>'
      : historyError
        ? '<div class="admin-ops-loading admin-section-loading">Activity delayed; retrying.</div>'
        : '<div class="admin-ops-loading admin-section-loading">Loading recent activity...</div>';
    if (canPatchInPlace) {
      delete historyEl.dataset.opsStructureSig;
    }
    return;
  }

  const truncateText = (value, limit = 180) => {
    const text = String(value || "").trim();
    if (!text) return "";
    return text.length > limit ? `${text.slice(0, Math.max(0, limit - 1)).trimEnd()}...` : text;
  };

  const formatDiscoveryStageBadge = progress => {
    const counts = progress?.counts && typeof progress.counts === "object" && !Array.isArray(progress.counts)
      ? progress.counts
      : {};
    const stageIndex = Math.max(0, Number(counts?.stageIndex || 0));
    const stageTotal = Math.max(0, Number(counts?.stageTotal || 0));
    return stageIndex > 0 && stageTotal > 0 ? `stage ${stageIndex.toLocaleString()}/${stageTotal.toLocaleString()}` : "";
  };

  const hasOwn = (source, key) => Object.prototype.hasOwnProperty.call(source || {}, key);
  const hasSyncLifecycleCounts = summary => ["activeCount", "pendingCount", "rejectedCount"].some(
    key => hasOwn(summary, key)
  );

  const toRowView = (row, rowArea, index) => {
    const inputIsLive = Boolean(row?.isLive || row?.active);
    if (inputIsLive) {
      row = {
        ...row,
        active: true,
        isLive: true,
        finishedAt: "",
        displayStatus: String(row?.displayStatus || row?.status || "running").trim() || "running"
      };
    }
    const runView = buildTaskRunView(row);
    const rawStatus = String(row?.displayStatus || row?.status || "unknown");
    const statusToken = rawStatus.toLowerCase();
    const summary = row?.summary || {};
    const taskProgress = row?.taskProgress || {};
    const type = String(row?.type || "unknown");
    const syncAction = String(summary?.action || "").trim().toLowerCase();
    const syncLabel = syncAction ? `Sync ${syncAction}` : "Sync";
    const syncCounts = hasSyncLifecycleCounts(summary)
      ? [summary?.activeCount, summary?.pendingCount, summary?.rejectedCount]
          .map(value => Number(value || 0))
          .map(value => value.toLocaleString())
          .join("/")
      : "";
    const currentRunDetail = formatTaskProgressDetail(
      type,
      taskProgress,
      summary,
      type === "fetch" || type === "discovery" || (type === "pipeline" && summary?.activeChildTaskType)
        ? { includeCounts: false }
        : {}
    );
    const currentRunTailBadge = row?.isLive && type === "fetch"
      ? formatScrapyStaticSourcesTailBadge(row?.workItems)
      : "";
    const currentRunStageBadge = row?.isLive && type === "discovery"
      ? formatDiscoveryStageBadge(taskProgress)
      : "";
    const currentRunSubtaskBadge = row?.isLive && type === "discovery"
      ? formatDiscoverySubtaskProgress(taskProgress?.counts)
      : "";
    const liveRunDetail = [
      currentRunDetail,
      currentRunSubtaskBadge,
      currentRunStageBadge,
      currentRunTailBadge
    ].filter(Boolean).join(" | ");
    const pipelineChildDetail = row?.type === "pipeline" && row?.isLive && summary?.activeChildTaskType
      ? currentRunDetail
      : "";
    const progressText = row?.type === "pipeline"
      ? (firstMeaningfulPipelineLabel(pipelineChildDetail, runView.progressLabel, runView.secondaryLabel) || (row?.isLive ? "Pipeline running" : "Pipeline completed"))
      : (row?.isLive && liveRunDetail)
        ? liveRunDetail
        : row?.type === "discovery"
          ? `Review queue: ${Number(summary?.queuedCandidateCount || 0).toLocaleString()}`
          : Number(summary?.outputCount || 0).toLocaleString();
    const rawProgressTitle = runView.progressStale
      ? runView.progressStaleLabel || runView.progressLabel || ""
      : (runView.progressLabel || runView.secondaryLabel || progressText);
    const progressTitle = type === "pipeline"
      ? (firstMeaningfulPipelineLabel(rawProgressTitle) || progressText)
      : rawProgressTitle;
    const statusText = runView.stallProximity === "approaching"
      ? "approaching"
      : (runView.statusLabel || rawStatus);
    const statusClass = `${runView.severity === "critical"
      ? "critical"
      : runView.severity === "warning"
        ? "warning"
        : getRunStatusChipClass(rawStatus)}${runView.stallProximity === "approaching" ? " admin-status-chip-approaching" : ""}`;
    const statusTitle = runView.stallProximity === "approaching"
      ? runView.heartbeatStalenessLabel || buildRunStatusTooltip(row)
      : (runView.remediationHint || buildRunStatusTooltip(row));
    const key = [
      rowArea,
      String(row?.id || ""),
      String(row?.runId || ""),
      type,
      String(row?.startedAt || ""),
      String(row?.finishedAt || ""),
      String(index)
    ].join("|");
    const pipelineOwnedChild = Boolean(
      type !== "pipeline"
      && (
        String(row?.parentTaskType || "").trim().toLowerCase() === "pipeline"
        || String(row?.parentRunId || row?.summary?.pipelineRunId || "").trim()
        || row?.displayOnly
        || row?.summary?.controlPlane
        || String(row?.controlPlaneSource || "").trim() === "pipeline-status"
      )
    );
    const abortRequested = rowAbortRequested(row);
    return {
      key,
      runId: String(row?.runId || row?.id || ""),
      taskType: type,
      rowArea,
      title: runView.title,
      primaryLabel: runView.primaryLabel,
      secondaryLabel: runView.secondaryLabel,
      typeText: formatRunTaskTypeLabel(runView.taskType || type),
      statusText,
      severity: runView.severity,
      statusClass,
      statusTitle,
      isRunning: statusToken === "running" || statusToken === "started",
      abortable: Boolean(
        onAbortRun
        && inputIsLive
        && ["fetch", "discovery", "pipeline"].includes(type)
        && !pipelineOwnedChild
        && !abortRequested
        && String(row?.runId || row?.id || "").trim()
      ),
      durationText: runView.durationLabel || runView.elapsedLabel || formatDuration(Number(row?.elapsedMs ?? row?.durationMs ?? 0)),
      outputOrQueuedText: row?.type === "sync"
        ? syncCounts
          ? `${syncLabel} (${syncCounts})`
          : (runView.progressLabel ? `${syncLabel} (${runView.progressLabel})` : syncLabel)
        : progressText,
      outputOrQueuedTitle: progressTitle,
      failedText: (row?.type === "discovery"
        ? Number(summary?.failedProbeCount || 0)
        : row?.type === "sync"
          ? Number(String(summary?.error || "").trim().length > 0 ? 1 : 0)
          : Number(summary?.failedSources || 0)).toLocaleString(),
      failedTitle: runView.failureSummary || runView.warningSummary || "",
      startedText: formatDateTime(row?.startedAt || ""),
      finishedText: statusToken === "running" || statusToken === "started"
        ? ""
        : formatCompactDateTime(row?.finishedAt || ""),
      // The compact cell text is only a pointer to the real value, so the full
      // localized stamp rides along as a tooltip and in the detail drawer.
      finishedTitle: statusToken === "running" || statusToken === "started"
        ? ""
        : (String(row?.finishedAt || "").trim() ? formatDateTime(row.finishedAt) : ""),
      progressLabel: type === "pipeline"
        ? firstMeaningfulPipelineLabel(runView.progressLabel)
        : (runView.progressLabel || ""),
      warningSummary: runView.warningSummary || "",
      failureSummary: runView.failureSummary || "",
      progressStale: Boolean(runView.progressStale),
      diagnosticHints: Array.isArray(runView.diagnosticHints)
        ? runView.diagnosticHints
            .map(hint => truncateText(hint, 160))
            .filter(hint => hint && !(type === "pipeline" && isZeroPipelineProgressLabel(hint)))
            .slice(0, 5)
        : [],
      diagnosticsPayload: buildTaskRunDiagnostics(row, { rowArea, runView }),
      analysisPayload: buildTaskRunAnalysis(row, { rowArea, runView })
    };
  };

  const currentViews = currentRows.map((row, index) => toRowView(row, "current", index));
  const visibleCompletedViews = visibleCompletedRows.map((row, index) => toRowView(row, "completed", index));
  const olderCompletedViews = olderCompletedRows.map((row, index) => toRowView(row, "completed_older", index));
  const allViews = [...currentViews, ...visibleCompletedViews, ...olderCompletedViews];
  const copyPayloads = new Map(
    allViews.map(view => [view.key, view.diagnosticsPayload])
  );
  const viewByKey = new Map(allViews.map(view => [view.key, view]));
  const selectedRunKey = String(
    options?.selectedRunKey
    || (canPatchInPlace ? historyEl.dataset.opsSelectedRunKey || "" : "")
    || ""
  );
  const selectedView = viewByKey.get(selectedRunKey) || null;

  const structureSignature = JSON.stringify({
    selectedRunKey: selectedView?.key || "",
    waitingForTaskState,
    historyPending,
    historyLoaded,
    historyFullLoaded,
    currentRows: currentViews.map(row => [
      row.key,
      row.abortable ? "abortable" : ""
    ]),
    completedRows: visibleCompletedViews.map(row => [
      row.key,
      row.statusText,
      row.durationText,
      row.outputOrQueuedText,
      row.failedText,
      row.finishedText,
      row.warningSummary,
      row.failureSummary,
      row.diagnosticHints.join("|")
    ]),
    completedOlder: olderCompletedViews.map(row => [
      row.key,
      row.statusText,
      row.durationText,
      row.outputOrQueuedText,
      row.failedText,
      row.finishedText,
      row.warningSummary,
      row.failureSummary,
      row.diagnosticHints.join("|")
    ])
  });

  const updateExistingRows = (views, rowArea) => {
    const rowMap = new Map(
      Array.from(historyEl.querySelectorAll(`.admin-ops-history-row[data-row-area="${rowArea}"]`))
        .map(rowEl => [String(rowEl.dataset.runKey || ""), rowEl])
    );
    views.forEach(view => {
      const rowEl = rowMap.get(view.key);
      if (!rowEl) return;
      rowEl.classList.toggle("admin-ops-history-row-running", view.isRunning);
      rowEl.classList.toggle("admin-ops-history-row-selected", view.key === selectedRunKey);
      rowEl.classList.toggle("admin-ops-progress-stale", Boolean(view.progressStale));
      rowEl.classList.toggle("admin-ops-history-row-approaching", String(view.statusText || "").toLowerCase() === "approaching");
      const cells = rowEl.querySelectorAll(".admin-cell");
      if (cells.length < 6) return;
      cells[0].textContent = view.typeText;
      const chip = cells[1].querySelector(".admin-status-chip");
      if (chip) {
        chip.className = `admin-status-chip ${view.statusClass}`;
        chip.textContent = view.statusText;
        setTooltip(chip, view.statusTitle);
      }
      cells[2].textContent = view.durationText;
      cells[3].textContent = view.outputOrQueuedText;
      setTooltip(cells[3], view.outputOrQueuedTitle);
      cells[4].textContent = view.failedText;
      setTooltip(cells[4], view.failedTitle);
      cells[5].textContent = view.finishedText;
      setTooltip(cells[5], view.finishedTitle);
    });
  };
  // Row buttons keep their per-element handlers, which are refreshed on every
  // repaint. The drawer is different: it is a single long-lived element that the
  // inspector controller paints asynchronously, so the ops renderer cannot reach
  // its buttons with `querySelectorAll` at click time. It gets one delegated
  // listener instead, and the WeakMap lets a repaint swap in fresh callbacks and
  // payloads without stacking duplicate listeners.
  const copyHosts = new WeakMap();
  const bindCopyHost = root => {
    if (!onCopyRunDiagnostics || !root || typeof root.addEventListener !== "function") return;
    const existing = copyHosts.get(root);
    if (existing) {
      existing.onCopy = onCopyRunDiagnostics;
      existing.payloads = copyPayloads;
      return;
    }
    const binding = { onCopy: onCopyRunDiagnostics, payloads: copyPayloads };
    copyHosts.set(root, binding);
    root.addEventListener("click", event => {
      const button = event?.target?.closest?.("[data-ops-run-diagnostics-copy]");
      if (!button) return;
      event?.stopPropagation?.();
      const key = String(button.getAttribute("data-ops-run-diagnostics-copy") || "");
      const payload = binding.payloads.get(key);
      if (payload) binding.onCopy(payload);
    });
  };
  const abortHosts = new WeakMap();
  const bindAbortHost = root => {
    if (!onAbortRun || !root || typeof root.addEventListener !== "function") return;
    const existing = abortHosts.get(root);
    if (existing) {
      existing.onAbort = onAbortRun;
      existing.views = viewByKey;
      return;
    }
    const binding = { onAbort: onAbortRun, views: viewByKey };
    abortHosts.set(root, binding);
    root.addEventListener("click", event => {
      const button = event?.target?.closest?.("[data-ops-run-abort]");
      if (!button) return;
      event?.preventDefault?.();
      event?.stopPropagation?.();
      const key = String(button.getAttribute("data-ops-run-abort") || "");
      const view = binding.views.get(key);
      if (view) binding.onAbort({ taskType: view.taskType, runId: view.runId, key: view.key });
    });
  };
  const attachCopyHandlers = (root = historyEl) => {
    if (!onCopyRunDiagnostics || !root || typeof root.querySelectorAll !== "function") return;
    root.querySelectorAll("[data-ops-run-diagnostics-copy]").forEach(button => {
      button.onclick = event => {
        event?.stopPropagation?.();
        const key = String(button.getAttribute("data-ops-run-diagnostics-copy") || "");
        const payload = copyPayloads.get(key);
        if (payload) onCopyRunDiagnostics(payload);
      };
    });
  };
  const attachAbortHandlers = (root = historyEl) => {
    if (!onAbortRun || !root || typeof root.querySelectorAll !== "function") return;
    root.querySelectorAll("[data-ops-run-abort]").forEach(button => {
      button.onclick = event => {
        event?.preventDefault?.();
        event?.stopPropagation?.();
        const key = String(button.getAttribute("data-ops-run-abort") || "");
        const view = viewByKey.get(key);
        if (view) onAbortRun({ taskType: view.taskType, runId: view.runId, key: view.key });
      };
    });
  };
  const attachRunDetailHostHandlers = () => {
    bindCopyHost(runDetailHost);
    bindAbortHost(runDetailHost);
  };
  // Row click selects the run and publishes its detail body to the inspector
  // drawer. The row handler runs before the document-level inspector delegate, so
  // staging `__runDetailHtml` here is what lets `ENTITY_TYPES.task_run` render it.
  // Rows keep no inline panel: the drawer is the single home for run detail.
  const attachSelectionHandlers = () => {
    if (!historyEl || typeof historyEl.querySelectorAll !== "function") return;
    const rows = Array.from(historyEl.querySelectorAll(".admin-ops-history-row[data-run-key]"));
    rows.forEach(rowEl => {
      const key = String(rowEl.dataset?.runKey || rowEl.getAttribute?.("data-run-key") || "");
      rowEl.onclick = event => {
        if (event?.target?.closest?.("[data-ops-run-diagnostics-copy],[data-ops-run-abort]")) return;
        if (historyEl.dataset) historyEl.dataset.opsSelectedRunKey = key;
        rows.forEach(item => item.classList?.toggle?.("admin-ops-history-row-selected", item === rowEl));
        const view = viewByKey.get(key) || null;
        if (!view) return;
        rowEl.__runDetailHtml = renderRunDetailHtml(view, {
          canCopyRunDiagnostics: Boolean(onCopyRunDiagnostics)
        });
      };
      rowEl.onkeydown = event => {
        if (event?.key !== "Enter" && event?.key !== " ") return;
        event.preventDefault?.();
        rowEl.onclick?.(event);
      };
    });
  };

  if (canPatchInPlace && historyEl.dataset.opsStructureSig === structureSignature) {
    updateExistingRows(currentViews, "current");
    updateExistingRows(visibleCompletedViews, "completed");
    updateExistingRows(olderCompletedViews, "completed_older");
    attachCopyHandlers();
    attachSelectionHandlers();
    attachRunDetailHostHandlers();
    return;
  }

  const olderOpen = canPatchInPlace ? Boolean(historyEl.querySelector(".admin-ops-history-older")?.open) : false;
  const recentOpen = canPatchInPlace ? Boolean(historyEl.querySelector(".admin-ops-history-recent")?.open) : false;
  if (canPatchInPlace) {
    historyEl.dataset.opsStructureSig = structureSignature;
  }

  const renderCappedRows = (views, cap, {
    renderRows = renderCompactRows
  } = {}) => {
    const visible = views.slice(0, cap);
    const overflow = views.slice(cap);
    return `
      ${renderRows(visible)}
      ${overflow.length ? `
        <details class="admin-ops-expand-capped">
          <summary>Show all ${views.length} runs</summary>
          <div class="jobs-table-body">
            ${renderRows(overflow)}
          </div>
        </details>
      ` : ""}
    `;
  };

  const renderCompactRows = views => views.map(view => {
    const outputOrQueuedTitle = view.isRunning ? "" : view.outputOrQueuedTitle;
    const actionButton = view.abortable
      ? `<button type="button" class="btn clear-filters-btn admin-ops-run-abort-btn" data-ops-run-abort="${escapeHtml(view.key)}" data-tooltip="Abort this task">Abort</button>`
      : "";
    return `
      <div class="admin-user-row admin-source-row admin-ops-history-row${view.isRunning ? " admin-ops-history-row-running" : ""}${view.key === selectedView?.key ? " admin-ops-history-row-selected" : ""}${view.progressStale ? " admin-ops-progress-stale" : ""}${String(view.statusText || "").toLowerCase() === "approaching" ? " admin-ops-history-row-approaching" : ""}" data-row-area="${view.rowArea}" data-run-key="${escapeHtml(view.key)}" tabindex="0"${tooltipAttrs("Open this run's details in the inspector")}>
        <div class="admin-cell">${escapeHtml(view.typeText)}</div>
        <div class="admin-cell"><span class="admin-status-chip ${view.statusClass}"${tooltipAttrs(view.statusTitle)}>${escapeHtml(view.statusText)}</span></div>
        <div class="admin-cell">${escapeHtml(view.durationText)}</div>
        <div class="admin-cell"${tooltipAttrs(outputOrQueuedTitle)}>${escapeHtml(view.outputOrQueuedText)}</div>
        <div class="admin-cell"${tooltipAttrs(view.failedTitle)}>${escapeHtml(view.failedText)}</div>
        <div class="admin-cell"${tooltipAttrs(view.finishedTitle)}>${escapeHtml(view.finishedText)}</div>
        <div class="admin-cell admin-ops-history-actions">${actionButton}</div>
      </div>
    `;
  }).join("");

  // Completed rows are emitted flat. They used to be wrapped in a per-row
  // `.admin-ops-history-run` block purely to give each row the max-content width
  // the header grid also had; now that the row grid itself carries the tracks and
  // the body is the sized element, the extra div only added a level for the row
  // click handler to see through.
  const renderCompletedRows = views => renderCompactRows(views);

  historyEl.innerHTML = `
    <div class="admin-ops-current-runs">
      <div class="admin-ops-history-title">Current Runs</div>
      <div class="jobs-table-header">
        <div class="admin-row-header admin-ops-history-header">
          <div>Type</div>
          <div>Status</div>
          <div>Duration</div>
          <div>Progress / Summary</div>
          <div>Failed</div>
          <div>Finished</div>
          <div>Actions</div>
        </div>
      </div>
      <div class="jobs-table-body">
        ${currentViews.length
          ? renderCappedRows(currentViews, 10)
          : (waitingForTaskState
            ? '<div class="admin-ops-loading">Waiting for task state...</div>'
            : taskStateUnavailable
              ? '<div class="admin-ops-loading">Task state unavailable. Current runs may be stale.</div>'
            : '<div class="no-results">No current runs.</div>')}
      </div>
    </div>
    <details class="admin-ops-history-recent admin-ops-completed-runs">
      <summary>Recent Runs${visibleCompletedViews.length ? ` (${visibleCompletedViews.length})` : ""}</summary>
      <div class="jobs-table-header">
        <div class="admin-row-header admin-ops-history-header">
          <div>Type</div>
          <div>Status</div>
          <div>Duration</div>
          <div>Progress / Summary</div>
          <div>Failed</div>
          <div>Finished</div>
          <div>Actions</div>
        </div>
      </div>
      <div class="jobs-table-body">
        ${visibleCompletedViews.length
          ? renderCappedRows(visibleCompletedViews, 5, { renderRows: renderCompletedRows })
          : historyPending
            ? '<div class="admin-ops-loading admin-section-loading">Loading recent run history...</div>'
            : historyLoaded
              ? '<div class="no-results">No completed runs yet.</div>'
              : '<div class="admin-ops-loading admin-section-loading">Recent run history has not loaded yet.</div>'}
      </div>
    </details>
    ${olderCompletedViews.length ? `
      <details class="admin-ops-history-older admin-ops-completed-runs" data-ops-load-older-history>
        <summary>Older runs (${olderCompletedViews.length})</summary>
        <div class="jobs-table-body admin-ops-history-older-scroll">
          ${renderCompletedRows(olderCompletedViews)}
        </div>
      </details>
    ` : (historyLoaded && !historyFullLoaded) ? `
      <details class="admin-ops-history-older admin-ops-completed-runs" data-ops-load-older-history>
        <summary>Older runs</summary>
        <div class="jobs-table-body admin-ops-history-older-scroll">
          <div class="admin-ops-loading admin-section-loading">Open to load older run history.</div>
        </div>
      </details>
    ` : ""}
  `;
  if (canPatchInPlace) {
    const recentDetailsEl = historyEl.querySelector(".admin-ops-history-recent");
    if (recentDetailsEl) recentDetailsEl.open = recentOpen;
    const detailsEl = historyEl.querySelector(".admin-ops-history-older");
    if (detailsEl) detailsEl.open = olderOpen;
  }
  attachCopyHandlers();
  attachAbortHandlers();
  attachSelectionHandlers();
  attachRunDetailHostHandlers();
}
