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
  }
  if (jobsPipelineProgressEl) {
    jobsPipelineProgressEl.textContent = String(progressLabel || "");
    jobsPipelineProgressEl.classList.toggle("running", Boolean(running));
    jobsPipelineProgressEl.classList.toggle("log-error", Boolean(isError));
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
