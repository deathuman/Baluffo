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
  const rawStage = String(payload?.stage || "").trim().toLowerCase();
  if (rawStage) {
    if (rawStage === "sync_push") return "Sync Push";
    if (rawStage === "sync_pull") return "Sync Pull";
    return titleCaseWords(rawStage.replace(/_/g, " "));
  }
  return "Pipeline";
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

export function getPipelineProgressLabel(payload) {
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

export function updateJobsPipelineUi(refs, { running = false, disabled = false, buttonLabel = "", progressLabel = "", isError = false } = {}) {
  const { jobsPipelineRunBtn, jobsPipelineProgressEl } = refs || {};
  if (jobsPipelineRunBtn) {
    if (!jobsPipelineRunBtn.dataset.idleLabel) {
      jobsPipelineRunBtn.dataset.idleLabel = String(jobsPipelineRunBtn.textContent || "Run Discovery + Fetch + Sync");
    }
    const idleLabel = String(jobsPipelineRunBtn.dataset.idleLabel || "Run Discovery + Fetch + Sync");
    jobsPipelineRunBtn.textContent = buttonLabel || (running ? "Pipeline Running..." : idleLabel);
    jobsPipelineRunBtn.disabled = Boolean(disabled);
    jobsPipelineRunBtn.setAttribute("aria-disabled", jobsPipelineRunBtn.disabled ? "true" : "false");
    jobsPipelineRunBtn.classList.toggle("running", Boolean(running));
    jobsPipelineRunBtn.classList.toggle("log-error", Boolean(isError));
  }
  // Deprecated on jobs page: pipeline status is surfaced via button label only.
  void jobsPipelineProgressEl;
  void progressLabel;
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
  const controller = new AbortController();
  const timeoutMs = Number(options.timeoutMs) > 0 ? Number(options.timeoutMs) : 1800;
  const timeoutId = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`${baseUrl}${path}?t=${Date.now()}`, {
      method: options.method || "GET",
      cache: "no-store",
      headers: {
        "Content-Type": "application/json",
        ...(options.headers || {})
      },
      body: options.body ? JSON.stringify(options.body) : undefined,
      signal: controller.signal
    });
    if (!response.ok) {
      throw new Error(`Bridge ${options.method || "GET"} ${path} failed with HTTP ${response.status}`);
    }
    return await response.json();
  } finally {
    clearTimeout(timeoutId);
  }
}
