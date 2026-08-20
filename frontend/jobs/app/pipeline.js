import { fetchBridge } from "../../shared/api-client.js";
import { formatTaskProgressDetail } from "../../shared/task-progress.js";
import { normalizeToken } from "../../shared/text-utils.js";

export const JOBS_UPDATE_COPY = Object.freeze({
  idleLabel: "Update jobs",
  updatingLabel: "Updating jobs...",
  tooltipDefault: "Find new openings and rebuild the local job list. This usually takes a few minutes; first updates can take up to 1 hour.",
  tooltipWarm: "Find new openings and rebuild the local job list. This usually takes a few minutes.",
  tooltipFirstRun: "First update: find new openings and rebuild the local job list. This can take up to 1 hour.",
  tooltipFirstRunBootstrap: "Preparing first-run jobs: Baluffo is fetching the starter Google Sheets job feed. The first refresh can take several minutes.",
  tooltipBridgeUnavailable: "Update jobs is unavailable because the Admin bridge is not reachable. Start or restart the desktop app, then try again.",
  tooltipBridgeTimedOut: "Update jobs is unavailable because the Admin bridge did not respond in time. Start or restart the desktop app, then try again.",
  completedWithUpdates: "Job update completed. Loading updated listings.",
  completedWithSyncWarning: "Job update completed. Source sync needs attention.",
  startedToast: "Job update started.",
  startFailed: "Could not start job update.",
  abortLabel: "Abort update",
  abortingLabel: "Aborting...",
  // ponytail: separate from updatingLabel so stall copy can append "(no progress
  // for 1m 30s)" without replacing the running label itself.
  stalledSuffixLabel: "No progress"
});

function getJobsUpdateUnavailableTooltip(error) {
  const normalized = String(error || "").trim().toLowerCase();
  if (normalized.includes("timed out") || normalized.includes("timeout")) {
    return JOBS_UPDATE_COPY.tooltipBridgeTimedOut;
  }
  return JOBS_UPDATE_COPY.tooltipBridgeUnavailable;
}

export function getJobsUpdateTooltip({
  bridgeError = "",
  firstRunBootstrapActive = false,
  firstRun = false,
  firstRunKnown = false
} = {}) {
  if (String(bridgeError || "").trim()) return getJobsUpdateUnavailableTooltip(bridgeError);
  if (firstRunBootstrapActive) return JOBS_UPDATE_COPY.tooltipFirstRunBootstrap;
  if (firstRun) return JOBS_UPDATE_COPY.tooltipFirstRun;
  if (firstRunKnown) return JOBS_UPDATE_COPY.tooltipWarm;
  return JOBS_UPDATE_COPY.tooltipDefault;
}

function titleCaseWords(value) {
  return String(value || "")
    .split(/\s+/)
    .filter(Boolean)
    .map(token => token.charAt(0).toUpperCase() + token.slice(1).toLowerCase())
    .join(" ");
}

function normalizePipelineStage(payload) {
  const progress = payload?.progress;
  if (progress && typeof progress === "object") {
    const label = String(progress.label || "").trim();
    if (label) {
      const cleaned = label
        .replace(/^running\s+/i, "")
        .replace(/\.\.\.$/, "")
        .trim();
      if (cleaned) return getUserFacingUpdateStage(cleaned);
    }
  }
  const rawStage = normalizeToken(payload?.stage);
  if (rawStage) {
    return getUserFacingUpdateStage(rawStage);
  }
  return "Updating jobs";
}

function getUserFacingUpdateStage(value) {
  const normalized = normalizeToken(value);
  if (normalized === "discovery" || normalized === "discover" || normalized === "source_discovery") {
    return "Checking sources";
  }
  if (normalized === "fetch" || normalized === "fetching") return "Fetching job listings";
  if (normalized === "sync" || normalized === "sync_push" || normalized === "sync_pull") {
    return "Updating local jobs";
  }
  if (
    normalized === "starting"
    || normalized === "pipeline"
    || normalized === "starting pipeline"
    || normalized === "pipeline starting"
  ) {
    return "Updating jobs";
  }
  return titleCaseWords(String(value || "").replace(/_/g, " "));
}

function clampProgressRatio(value) {
  const numeric = Number(value);
  if (!Number.isFinite(numeric)) return 0;
  return Math.max(0, Math.min(1, numeric));
}

function ensureJobsPipelineButtonChrome(button, idleLabel) {
  if (!button) return null;

  const existingFill = typeof button.querySelector === "function"
    ? button.querySelector('[data-ui="jobs-pipeline-fill"]')
    : null;
  const existingLabel = typeof button.querySelector === "function"
    ? button.querySelector('[data-ui="jobs-pipeline-label"]')
    : null;
  const existingProgress = typeof button.querySelector === "function"
    ? button.querySelector('[data-ui="jobs-pipeline-progress"]')
    : null;
  if (existingFill && existingLabel && existingProgress) {
    return { fillEl: existingFill, labelEl: existingLabel, progressEl: existingProgress };
  }

  const ownerDocument = button.ownerDocument || (typeof document !== "undefined" ? document : null);
  if (!ownerDocument?.createElement || typeof button.replaceChildren !== "function") {
    return null;
  }

  const fillEl = ownerDocument.createElement("span");
  fillEl.className = "jobs-pipeline-btn-fill";
  fillEl.dataset.ui = "jobs-pipeline-fill";
  fillEl.setAttribute?.("aria-hidden", "true");

  const labelEl = ownerDocument.createElement("span");
  labelEl.className = "jobs-pipeline-btn-label";
  labelEl.dataset.ui = "jobs-pipeline-label";
  labelEl.textContent = String(button.textContent || idleLabel || JOBS_UPDATE_COPY.idleLabel);

  const progressEl = ownerDocument.createElement("span");
  progressEl.className = "jobs-pipeline-btn-progress";
  progressEl.dataset.ui = "jobs-pipeline-progress";
  progressEl.hidden = true;

  button.replaceChildren(fillEl, labelEl, progressEl);
  return { fillEl, labelEl, progressEl };
}

function ensureJobsPipelineAbortButton(button) {
  if (!button?.parentElement) return null;
  const existing = button.parentElement.querySelector?.('[data-ui="jobs-pipeline-abort"]');
  if (existing) return existing;
  const ownerDocument = button.ownerDocument || (typeof document !== "undefined" ? document : null);
  if (!ownerDocument?.createElement) return null;
  const abortButton = ownerDocument.createElement("button");
  abortButton.type = "button";
  abortButton.className = "jobs-pipeline-abort-btn";
  abortButton.dataset.ui = "jobs-pipeline-abort";
  abortButton.textContent = "Abort";
  abortButton.hidden = true;
  abortButton.setAttribute?.("aria-label", JOBS_UPDATE_COPY.abortLabel);
  abortButton.setAttribute?.("data-tooltip", JOBS_UPDATE_COPY.abortLabel);
  button.insertAdjacentElement?.("afterend", abortButton);
  return abortButton;
}

function buildPipelineFillState(payload, { running = false } = {}) {
  const active = Boolean(running || payload?.active);
  if (!active) return { mode: "", fill: 0 };

  const progress = payload?.progress && typeof payload.progress === "object" ? payload.progress : {};
  const currentStep = Number(progress.currentStep || 0);
  const totalSteps = Number(progress.totalSteps || 0);
  const progressMode = String(progress.mode || "").trim().toLowerCase();
  const hasDeterminateStep = Number.isFinite(currentStep) && Number.isFinite(totalSteps) && totalSteps > 0 && currentStep > 0;
  const hasDeterminateProgress = hasDeterminateStep || progressMode === "determinate";

  if (!hasDeterminateProgress) {
    return { mode: "indeterminate", fill: 0 };
  }

  const rawPercent = progress.percent;
  const percent = Number(rawPercent);
  const hasPercent = rawPercent !== null && rawPercent !== undefined && String(rawPercent).trim() !== "";
  const ratio = hasPercent && Number.isFinite(percent)
    ? clampProgressRatio(percent / 100)
    : clampProgressRatio(totalSteps > 0 ? currentStep / totalSteps : 0);
  return {
    mode: "determinate",
    fill: ratio
  };
}

function resolvePipelineElapsedReferenceMs(nowMs, referenceAt = "") {
  const clientNowMs = Number(nowMs);
  const referenceMs = Date.parse(String(referenceAt || ""));
  if (Number.isFinite(referenceMs) && (!Number.isFinite(clientNowMs) || referenceMs > clientNowMs)) {
    return referenceMs;
  }
  return clientNowMs;
}

export function formatPipelineElapsed(startedAt, nowMs = Date.now(), referenceAt = "") {
  const startedMs = Date.parse(String(startedAt || ""));
  if (!Number.isFinite(startedMs)) return "";
  const referenceMs = resolvePipelineElapsedReferenceMs(nowMs, referenceAt);
  const elapsedSeconds = Math.max(0, Math.floor((referenceMs - startedMs) / 1000));
  if (elapsedSeconds < 60) return `${elapsedSeconds}s`;
  const minutes = Math.floor(elapsedSeconds / 60);
  const seconds = elapsedSeconds % 60;
  return `${minutes}m ${seconds}s`;
}

export function getPipelineRunningLabel(payload, nowMs = Date.now()) {
  const stage = normalizePipelineStage(payload);
  const elapsed = formatPipelineElapsed(payload?.startedAt, nowMs, payload?.snapshotAt);
  return elapsed ? `${stage}... ${elapsed}` : `${stage}...`;
}

export function formatBlockingTaskProgressLabel(task) {
  const taskType = String(task?.taskType || task?.type || "").trim().toLowerCase();
  return formatTaskProgressDetail(
    taskType,
    task?.taskProgress || {},
    task?.summary || {}
  );
}

export function buildJobsPipelineButtonView(
  payload,
  {
    running = false,
    disabled = false,
    buttonLabel = "",
    progressLabel = "",
    buttonTooltip = "",
    isError = false,
    abortable = false,
    aborting = false,
    nowMs = Date.now()
  } = {}
) {
  const active = Boolean(running || payload?.active);
  const fillState = buildPipelineFillState(payload, { running });
  const liveLabel = String(
    buttonLabel
    || (active
      ? getPipelineRunningLabel(
        {
          ...payload,
          startedAt: String(payload?.startedAt || "")
        },
        nowMs
      )
      : "")
  ).trim();
  const label = aborting
    ? JOBS_UPDATE_COPY.abortingLabel
    : liveLabel;

  return {
    active,
    disabled: Boolean(abortable && !aborting ? false : disabled),
    isError: Boolean(isError),
    label: label || (active ? JOBS_UPDATE_COPY.updatingLabel : JOBS_UPDATE_COPY.idleLabel),
    tooltip: String(buttonTooltip || JOBS_UPDATE_COPY.tooltipDefault).trim(),
    progressLabel: String(progressLabel || "").trim(),
    progressMode: fillState.mode,
    progressFill: fillState.fill
  };
}

export function updateJobsPipelineUi(
  refs,
  {
    pipelinePayload = null,
    running = false,
    disabled = false,
    buttonLabel = "",
    progressLabel = "",
    buttonTooltip = "",
    isError = false,
    abortable = false,
    aborting = false
  } = {}
) {
  const { jobsPipelineRunBtn } = refs || {};
  if (!jobsPipelineRunBtn) return;

  if (!jobsPipelineRunBtn.dataset.idleLabel) {
    jobsPipelineRunBtn.dataset.idleLabel = String(jobsPipelineRunBtn.textContent || JOBS_UPDATE_COPY.idleLabel);
  }
  const idleLabel = String(jobsPipelineRunBtn.dataset.idleLabel || JOBS_UPDATE_COPY.idleLabel);
  const chrome = ensureJobsPipelineButtonChrome(jobsPipelineRunBtn, idleLabel);
  const abortButton = ensureJobsPipelineAbortButton(jobsPipelineRunBtn);
  const fillEl = chrome?.fillEl || null;
  const labelEl = chrome?.labelEl || null;
  const progressEl = chrome?.progressEl || null;
  const view = buildJobsPipelineButtonView(pipelinePayload, {
    running,
    disabled,
    buttonLabel,
    progressLabel,
    buttonTooltip,
    isError,
    abortable,
    aborting
  });

  const nextLabel = view.label || (view.active ? JOBS_UPDATE_COPY.updatingLabel : idleLabel);
  if (labelEl) {
    labelEl.textContent = nextLabel;
  } else {
    jobsPipelineRunBtn.textContent = nextLabel;
  }
  if (progressEl) {
    const progressText = view.active ? String(view.progressLabel || "").trim() : "";
    progressEl.textContent = progressText;
    progressEl.hidden = !progressText;
  }
  jobsPipelineRunBtn.disabled = Boolean(view.disabled);
  jobsPipelineRunBtn.setAttribute("aria-disabled", jobsPipelineRunBtn.disabled ? "true" : "false");
  jobsPipelineRunBtn.setAttribute("aria-busy", view.active ? "true" : "false");
  if (view.tooltip) {
    jobsPipelineRunBtn.setAttribute?.("data-tooltip", view.tooltip);
    jobsPipelineRunBtn.removeAttribute?.("title");
    if (jobsPipelineRunBtn.dataset) jobsPipelineRunBtn.dataset.tooltip = view.tooltip;
  }
  jobsPipelineRunBtn.classList.toggle("running", Boolean(view.active));
  const canAbort = Boolean(abortable && !aborting);
  jobsPipelineRunBtn.classList.toggle("abortable", canAbort);
  jobsPipelineRunBtn.classList.toggle("abort-reveal", false);
  jobsPipelineRunBtn.dataset.abortable = canAbort ? "true" : "false";
  if (canAbort) {
    jobsPipelineRunBtn.dataset.abortLabel = JOBS_UPDATE_COPY.abortLabel;
    jobsPipelineRunBtn.setAttribute?.("data-abort-label", JOBS_UPDATE_COPY.abortLabel);
  } else {
    delete jobsPipelineRunBtn.dataset.abortLabel;
    jobsPipelineRunBtn.removeAttribute?.("data-abort-label");
  }
  jobsPipelineRunBtn.classList.toggle("determinate", view.progressMode === "determinate");
  jobsPipelineRunBtn.classList.toggle("indeterminate", view.progressMode === "indeterminate");
  jobsPipelineRunBtn.classList.toggle("log-error", Boolean(view.isError));

  if (view.progressMode === "determinate") {
    const fillPercent = Math.round(view.progressFill * 100);
    jobsPipelineRunBtn.dataset.progressMode = "determinate";
    jobsPipelineRunBtn.dataset.progressFill = String(fillPercent);
    jobsPipelineRunBtn.style.setProperty("--jobs-pipeline-fill", `${fillPercent}%`);
    if (fillEl) {
      fillEl.dataset.progressMode = "determinate";
      fillEl.style.width = `${fillPercent}%`;
      fillEl.style.opacity = "1";
      fillEl.style.removeProperty("animation");
    }
  } else if (view.progressMode === "indeterminate") {
    jobsPipelineRunBtn.dataset.progressMode = "indeterminate";
    delete jobsPipelineRunBtn.dataset.progressFill;
    jobsPipelineRunBtn.style.removeProperty("--jobs-pipeline-fill");
    if (fillEl) {
      fillEl.dataset.progressMode = "indeterminate";
      fillEl.style.width = "42%";
      fillEl.style.opacity = "1";
      fillEl.style.animation = "jobsPipelineFillSweep 1.15s ease-in-out infinite";
    }
  } else {
    delete jobsPipelineRunBtn.dataset.progressMode;
    delete jobsPipelineRunBtn.dataset.progressFill;
    jobsPipelineRunBtn.style.removeProperty("--jobs-pipeline-fill");
    if (fillEl) {
      delete fillEl.dataset.progressMode;
      fillEl.style.width = "0%";
      fillEl.style.opacity = "0";
      fillEl.style.removeProperty("animation");
    }
  }
  if (abortButton) {
    abortButton.hidden = !Boolean(abortable && !aborting);
    abortButton.disabled = !Boolean(abortable && !aborting);
    abortButton.classList.toggle("visible", Boolean(abortable && !aborting));
  }
}

export function clearJobsPipelinePolling(state) {
  if (state?.pollingTimer) {
    clearTimeout(state.pollingTimer);
    state.pollingTimer = null;
  }
}

export function scheduleJobsPipelineStatusPoll(state, delayMs, pollFn, minDelayMs) {
  clearJobsPipelinePolling(state);
  state.pollingTimer = setTimeout(() => {
    pollFn().catch(() => {});
  }, Math.max(Number(minDelayMs) || 600, Number(delayMs) || Number(minDelayMs) || 600));
}

export async function callJobsBridge(baseUrl, path, options = {}) {
  const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : 1800;
  const response = await fetchBridge(baseUrl, path, {
    method: options.method || "GET",
    body: options.body,
    headers: options.headers,
    allowStatuses: options.allowStatuses,
    timeoutMs
  });
  return response.json();
}
