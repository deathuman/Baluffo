import { fetchBridge } from "../../shared/api-client.js";
import { formatTaskProgressDetail } from "../../shared/task-progress.js";
import { normalizeToken } from "../../shared/text-utils.js";

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
      if (cleaned) return titleCaseWords(cleaned);
    }
  }
  const rawStage = normalizeToken(payload?.stage);
  if (rawStage) {
    if (rawStage === "sync_push") return "Sync Push";
    if (rawStage === "sync_pull") return "Sync Pull";
    return titleCaseWords(rawStage.replace(/_/g, " "));
  }
  return "Pipeline";
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
  if (existingFill && existingLabel) {
    return { fillEl: existingFill, labelEl: existingLabel };
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
  labelEl.textContent = String(button.textContent || idleLabel || "Run Discovery + Fetch + Sync");

  button.replaceChildren(fillEl, labelEl);
  return { fillEl, labelEl };
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

export function formatPipelineElapsed(startedAt, nowMs = Date.now()) {
  const startedMs = Date.parse(String(startedAt || ""));
  if (!Number.isFinite(startedMs)) return "";
  const elapsedSeconds = Math.max(0, Math.floor((Number(nowMs) - startedMs) / 1000));
  if (elapsedSeconds < 60) return `${elapsedSeconds}s`;
  const minutes = Math.floor(elapsedSeconds / 60);
  const seconds = elapsedSeconds % 60;
  return `${minutes}m ${seconds}s`;
}

export function getPipelineRunningLabel(payload, nowMs = Date.now()) {
  const stage = normalizePipelineStage(payload);
  const elapsed = formatPipelineElapsed(payload?.startedAt, nowMs);
  return elapsed ? `${stage} running... ${elapsed}` : `${stage} running...`;
}

export function formatBlockingTaskProgressLabel(task) {
  const taskType = String(task?.taskType || task?.type || "").trim().toLowerCase();
  return formatTaskProgressDetail(
    taskType,
    task?.taskProgress || {},
    task?.summary || {}
  );
}

function getPipelineProgressLabel(payload) {
  const progress = payload?.progress;
  if (progress && typeof progress === "object") {
    const label = String(progress.label || "").trim();
    if (label) return label;
    const current = Number(progress.currentStep || 0);
    const total = Number(progress.totalSteps || 0);
    if (current > 0 && total > 0) return `Step ${current}/${total}`;
  }
  const stage = String(payload?.stage || "").trim();
  if (stage) return `Stage: ${stage}`;
  return "Running pipeline...";
}

export function buildJobsPipelineButtonView(
  payload,
  {
    running = false,
    disabled = false,
    buttonLabel = "",
    progressLabel = "",
    isError = false,
    nowMs = Date.now()
  } = {}
) {
  const active = Boolean(running || payload?.active);
  const fillState = buildPipelineFillState(payload, { running });
  const label = String(
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

  return {
    active,
    disabled: Boolean(disabled),
    isError: Boolean(isError),
    label: label || (active ? "Pipeline Running..." : "Run Discovery + Fetch + Sync"),
    progressLabel: String(progressLabel || getPipelineProgressLabel(payload)).trim(),
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
    isError = false
  } = {}
) {
  const { jobsPipelineRunBtn } = refs || {};
  if (!jobsPipelineRunBtn) return;

  if (!jobsPipelineRunBtn.dataset.idleLabel) {
    jobsPipelineRunBtn.dataset.idleLabel = String(jobsPipelineRunBtn.textContent || "Run Discovery + Fetch + Sync");
  }
  const idleLabel = String(jobsPipelineRunBtn.dataset.idleLabel || "Run Discovery + Fetch + Sync");
  const chrome = ensureJobsPipelineButtonChrome(jobsPipelineRunBtn, idleLabel);
  const fillEl = chrome?.fillEl || null;
  const labelEl = chrome?.labelEl || null;
  const view = buildJobsPipelineButtonView(pipelinePayload, {
    running,
    disabled,
    buttonLabel,
    progressLabel,
    isError
  });

  const nextLabel = view.label || (view.active ? "Pipeline Running..." : idleLabel);
  if (labelEl) {
    labelEl.textContent = nextLabel;
  } else {
    jobsPipelineRunBtn.textContent = nextLabel;
  }
  jobsPipelineRunBtn.disabled = Boolean(view.disabled);
  jobsPipelineRunBtn.setAttribute("aria-disabled", jobsPipelineRunBtn.disabled ? "true" : "false");
  jobsPipelineRunBtn.setAttribute("aria-busy", view.active ? "true" : "false");
  jobsPipelineRunBtn.classList.toggle("running", Boolean(view.active));
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
    timeoutMs
  });
  return response.json();
}
