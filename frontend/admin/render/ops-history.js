import { escapeHtml } from "../../shared/ui/index.js";
import { buildTaskRunView } from "../../shared/task-run-view-model.js";
import {
  formatScrapyStaticSourcesTailBadge,
  formatTaskProgressDetail
} from "../../shared/task-progress.js";
import {
  buildRunStatusTooltip,
  formatDateTime,
  formatDuration,
  formatSignedInt,
  getRunStatusChipClass
} from "./ops-shared.js";

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

export function renderAdminOpsHistory(historyEl, runsOrModel) {
  if (!historyEl) return;
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
  const canPatchInPlace = Boolean(
    historyEl
    && typeof historyEl.querySelector === "function"
    && typeof historyEl.querySelectorAll === "function"
    && historyEl.dataset
  );
  if (!currentRows.length && !visibleCompletedRows.length && !olderCompletedRows.length) {
    historyEl.innerHTML = '<div class="no-results">No run history yet.</div>';
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

  const toRowView = (row, rowArea, index) => {
    const runView = buildTaskRunView(row);
    const rawStatus = String(row?.displayStatus || row?.status || "unknown");
    const statusToken = rawStatus.toLowerCase();
    const summary = row?.summary || {};
    const taskProgress = row?.taskProgress || {};
    const type = String(row?.type || "unknown");
    const syncAction = String(summary?.action || "").trim().toLowerCase();
    const syncLabel = syncAction ? `Sync ${syncAction}` : "Sync";
    const syncCounts = [summary?.activeCount, summary?.pendingCount, summary?.rejectedCount]
      .map(value => Number(value || 0))
      .map(value => value.toLocaleString())
      .join("/");
    const currentRunDetail = formatTaskProgressDetail(
      type,
      taskProgress,
      summary,
      type === "fetch" || type === "discovery" ? { includeCounts: false } : {}
    );
    const currentRunTailBadge = row?.isLive && type === "fetch"
      ? formatScrapyStaticSourcesTailBadge(row?.workItems)
      : "";
    const liveRunDetail = [currentRunDetail, currentRunTailBadge].filter(Boolean).join(" | ");
    const key = [
      rowArea,
      String(row?.id || ""),
      String(row?.runId || ""),
      type,
      String(row?.startedAt || ""),
      String(row?.finishedAt || ""),
      String(index)
    ].join("|");
    return {
      key,
      rowArea,
      title: runView.title,
      primaryLabel: runView.primaryLabel,
      secondaryLabel: runView.secondaryLabel,
      typeText: runView.taskType || type,
      statusText: runView.statusLabel || rawStatus,
      severity: runView.severity,
      statusClass: runView.severity === "critical"
        ? "critical"
        : runView.severity === "warning"
          ? "warning"
          : getRunStatusChipClass(rawStatus),
      statusTitle: runView.remediationHint || buildRunStatusTooltip(row),
      isRunning: statusToken === "running" || statusToken === "started",
      durationText: runView.durationLabel || runView.elapsedLabel || formatDuration(Number(row?.elapsedMs ?? row?.durationMs ?? 0)),
      outputOrQueuedText: (row?.isLive && liveRunDetail)
        ? liveRunDetail
        : row?.type === "discovery"
          ? `Review queue: ${Number(summary?.queuedCandidateCount || 0).toLocaleString()}`
          : row?.type === "sync"
            ? `${syncLabel} (${syncCounts})`
            : Number(summary?.outputCount || 0).toLocaleString(),
      failedText: (row?.type === "discovery"
        ? Number(summary?.failedProbeCount || 0)
        : row?.type === "sync"
          ? Number(String(summary?.error || "").trim().length > 0 ? 1 : 0)
          : Number(summary?.failedSources || 0)).toLocaleString(),
      startedText: formatDateTime(row?.startedAt || ""),
      finishedText: formatDateTime(row?.finishedAt || row?.startedAt || ""),
      progressLabel: runView.progressLabel || "",
      warningSummary: runView.warningSummary || "",
      failureSummary: runView.failureSummary || "",
      diagnosticHints: Array.isArray(runView.diagnosticHints)
        ? runView.diagnosticHints.map(hint => truncateText(hint, 160)).filter(Boolean).slice(0, 5)
        : []
    };
  };

  const currentViews = currentRows.map((row, index) => toRowView(row, "current", index));
  const visibleCompletedViews = visibleCompletedRows.map((row, index) => toRowView(row, "completed", index));
  const olderCompletedViews = olderCompletedRows.map((row, index) => toRowView(row, "completed_older", index));

  const structureSignature = JSON.stringify({
    currentRows: currentViews.map(row => [
      row.key,
      row.statusText,
      row.durationText,
      row.outputOrQueuedText,
      row.failedText,
      row.finishedText
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
      const cells = rowEl.querySelectorAll(".admin-cell");
      if (cells.length < 6) return;
      cells[0].textContent = view.typeText;
      const chip = cells[1].querySelector(".admin-status-chip");
      if (chip) {
        chip.className = `admin-status-chip ${view.statusClass}`;
        chip.textContent = view.statusText;
        if (view.statusTitle) {
          chip.setAttribute("title", view.statusTitle);
        } else {
          chip.removeAttribute("title");
        }
      }
      cells[2].textContent = view.durationText;
      cells[3].textContent = view.outputOrQueuedText;
      cells[4].textContent = view.failedText;
      cells[5].textContent = view.finishedText;
    });
  };

  if (canPatchInPlace && historyEl.dataset.opsStructureSig === structureSignature) {
    updateExistingRows(currentViews, "current");
    updateExistingRows(visibleCompletedViews, "completed");
    updateExistingRows(olderCompletedViews, "completed_older");
    return;
  }

  const olderOpen = canPatchInPlace ? Boolean(historyEl.querySelector(".admin-ops-history-older")?.open) : false;
  if (canPatchInPlace) {
    historyEl.dataset.opsStructureSig = structureSignature;
  }

  const renderCompactRows = views => views.map(view => `
      <div class="admin-user-row admin-source-row admin-ops-history-row${view.isRunning ? " admin-ops-history-row-running" : ""}" data-row-area="${view.rowArea}" data-run-key="${escapeHtml(view.key)}">
        <div class="admin-cell">${escapeHtml(view.typeText)}</div>
        <div class="admin-cell"><span class="admin-status-chip ${view.statusClass}"${view.statusTitle ? ` title="${escapeHtml(view.statusTitle)}"` : ""}>${escapeHtml(view.statusText)}</span></div>
        <div class="admin-cell">${escapeHtml(view.durationText)}</div>
        <div class="admin-cell">${escapeHtml(view.outputOrQueuedText)}</div>
        <div class="admin-cell">${escapeHtml(view.failedText)}</div>
        <div class="admin-cell">${escapeHtml(view.finishedText)}</div>
      </div>
    `).join("");

  const renderCompletedRows = views => views.map(view => {
    const metaItems = [
      view.startedText ? `<span><strong>Started</strong> ${escapeHtml(view.startedText)}</span>` : "",
      view.finishedText ? `<span><strong>Finished</strong> ${escapeHtml(view.finishedText)}</span>` : "",
      view.durationText ? `<span><strong>Duration</strong> ${escapeHtml(view.durationText)}</span>` : ""
    ].filter(Boolean).join("");
    const warningFailureItems = [
      view.warningSummary ? `<div class="admin-ops-run-detail-warning">${escapeHtml(view.warningSummary)}</div>` : "",
      view.failureSummary ? `<div class="admin-ops-run-detail-failure">${escapeHtml(view.failureSummary)}</div>` : ""
    ].filter(Boolean).join("");
    const hintItems = view.diagnosticHints.length
      ? `<ul>${view.diagnosticHints.map(hint => `<li>${escapeHtml(hint)}</li>`).join("")}</ul>`
      : '<div class="muted">No diagnostic hints for this run.</div>';
    return `
      <div class="admin-ops-history-run" data-row-area="${view.rowArea}" data-run-key="${escapeHtml(view.key)}">
        ${renderCompactRows([view])}
        <details class="admin-ops-run-detail">
          <summary>${escapeHtml(view.title)} details</summary>
          <div class="admin-ops-run-detail-body">
            <div class="admin-ops-run-detail-head">
              <div>
                <strong>${escapeHtml(view.primaryLabel)}</strong>
                ${view.secondaryLabel ? `<span>${escapeHtml(view.secondaryLabel)}</span>` : ""}
              </div>
              <span class="admin-status-chip ${view.statusClass}">${escapeHtml(view.statusText)}</span>
            </div>
            <div class="admin-ops-run-detail-meta">${metaItems}</div>
            <div class="admin-ops-run-detail-summary"><strong>Summary</strong> ${escapeHtml(view.progressLabel || view.outputOrQueuedText)}</div>
            ${warningFailureItems || '<div class="muted">No warnings or failures recorded for this run.</div>'}
            <div class="admin-ops-run-detail-hints">
              <strong>Diagnostic hints</strong>
              ${hintItems}
            </div>
          </div>
        </details>
      </div>
    `;
  }).join("");

  historyEl.innerHTML = `
    <div class="admin-ops-current-runs">
      <div class="admin-ops-history-title">Current Runs</div>
      <div class="jobs-table-header">
        <div class="admin-row-header admin-ops-history-header">
          <div>Type</div>
          <div>Status</div>
          <div>Duration</div>
          <div>Output / Review queue</div>
          <div>Failed</div>
          <div>Finished</div>
        </div>
      </div>
      <div class="jobs-table-body">
        ${currentViews.length ? renderCompactRows(currentViews) : '<div class="no-results">No current runs.</div>'}
      </div>
    </div>
    <div class="admin-ops-completed-runs">
      <div class="admin-ops-history-title">Recent Runs</div>
      <div class="jobs-table-header">
        <div class="admin-row-header admin-ops-history-header">
          <div>Type</div>
          <div>Status</div>
          <div>Duration</div>
          <div>Output / Review queue</div>
          <div>Failed</div>
          <div>Finished</div>
        </div>
      </div>
      <div class="jobs-table-body">
        ${visibleCompletedViews.length ? renderCompletedRows(visibleCompletedViews) : '<div class="no-results">No completed runs yet.</div>'}
      </div>
    </div>
    ${olderCompletedViews.length ? `
      <details class="admin-ops-history-older admin-ops-completed-runs">
        <summary>Older runs (${olderCompletedViews.length})</summary>
        <div class="jobs-table-body">
          ${renderCompletedRows(olderCompletedViews)}
        </div>
      </details>
    ` : ""}
  `;
  if (canPatchInPlace) {
    const detailsEl = historyEl.querySelector(".admin-ops-history-older");
    if (detailsEl) detailsEl.open = olderOpen;
  }
}
