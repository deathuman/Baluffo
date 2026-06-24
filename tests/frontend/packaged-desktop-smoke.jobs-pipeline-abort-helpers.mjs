import assert from "node:assert/strict";

const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";
const BRIDGE_REQUEST_RETRY_TIMEOUT_MS = 30_000;
const BRIDGE_REQUEST_RETRY_INTERVAL_MS = 500;
const JOBS_ABORT_LABEL = "Abort update";

function isRetryableBridgeRequestError(error) {
  return /ECONNREFUSED|ECONNRESET|ECONNABORTED|ETIMEDOUT|socket hang up/i.test(
    String(error?.message || error || "")
  );
}

async function bridgeRequestWithRetry(apiRequest, method, url, options = {}) {
  const deadline = Date.now() + BRIDGE_REQUEST_RETRY_TIMEOUT_MS;
  let lastError = null;
  while (Date.now() < deadline) {
    try {
      return await apiRequest[method](url, options);
    } catch (error) {
      lastError = error;
      if (!isRetryableBridgeRequestError(error)) throw error;
      await new Promise(resolve => setTimeout(resolve, BRIDGE_REQUEST_RETRY_INTERVAL_MS));
    }
  }
  throw lastError || new Error(`Bridge ${method.toUpperCase()} request timed out: ${url}`);
}

async function fetchPipelineStatus(apiRequest) {
  const response = await bridgeRequestWithRetry(apiRequest, "get", `${BRIDGE_BASE}/tasks/run-jobs-pipeline-status`);
  assert.equal(response.ok(), true, "jobs pipeline status request should succeed");
  return response.json();
}

async function fetchBridgeJson(apiRequest, relativePath, label) {
  const response = await bridgeRequestWithRetry(apiRequest, "get", `${BRIDGE_BASE}${relativePath}`);
  assert.equal(response.ok(), true, `${label} request should succeed`);
  return response.json();
}

async function postBridgeJson(apiRequest, relativePath, data, label) {
  const response = await bridgeRequestWithRetry(apiRequest, "post", `${BRIDGE_BASE}${relativePath}`, { data });
  assert.equal(response.ok(), true, `${label} request should succeed`);
  return response.json();
}

function isActiveTaskRow(row) {
  const finishedAt = String(row?.finishedAt || "").trim();
  const status = String(row?.status || row?.lifecycleStatus || "").trim().toLowerCase();
  return Boolean(row?.active) || (!finishedAt && ["running", "starting"].includes(status));
}

function summarizeTaskRow(row) {
  return [row?.taskType || row?.type || "task", row?.runId || row?.id, row?.status || row?.lifecycleStatus, row?.stage]
    .map(value => String(value || "").trim()).filter(Boolean).join(":");
}

function rowRunId(row) {
  return String(row?.runId || row?.id || "").trim();
}

function rowTaskType(row) {
  return String(row?.taskType || row?.type || "").trim().toLowerCase();
}

async function waitForBridgeTasksIdle(apiRequest, timeoutMs = 120_000, stableMs = 0) {
  const deadline = Date.now() + timeoutMs;
  let lastActiveTasks = [];
  let idleSinceMs = 0;
  while (Date.now() < deadline) {
    const payload = await fetchBridgeJson(apiRequest, "/ops/task-state?view=summary", "task state summary");
    lastActiveTasks = Array.isArray(payload?.tasks) ? payload.tasks.filter(row => isActiveTaskRow(row)) : [];
    if (lastActiveTasks.length === 0) {
      if (Number(stableMs) <= 0) return payload;
      if (!idleSinceMs) idleSinceMs = Date.now();
      if (Date.now() - idleSinceMs >= Number(stableMs)) return payload;
    } else {
      idleSinceMs = 0;
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  const summary = lastActiveTasks.map(summarizeTaskRow).join(", ") || "unknown task";
  throw new Error(`Bridge tasks did not become idle before pipeline launch: ${summary}`);
}

async function waitForIdleOrActiveBootstrapFetch(apiRequest, timeoutMs = 120_000) {
  const deadline = Date.now() + timeoutMs;
  let lastActiveTasks = [];
  while (Date.now() < deadline) {
    const payload = await fetchBridgeJson(apiRequest, "/ops/task-state?view=summary", "task state summary");
    lastActiveTasks = Array.isArray(payload?.tasks) ? payload.tasks.filter(row => isActiveTaskRow(row)) : [];
    const bootstrapFetch = lastActiveTasks.find(row => (
      rowTaskType(row) === "fetch" && /^jobs_bootstrap_[a-f0-9]{10}$/i.test(rowRunId(row))
    ));
    if (bootstrapFetch) return { state: "bootstrap", task: bootstrapFetch, payload };
    if (lastActiveTasks.length === 0) return { state: "idle", task: null, payload };
    await new Promise(resolve => setTimeout(resolve, 1000));
  }
  const summary = lastActiveTasks.map(summarizeTaskRow).join(", ") || "unknown task";
  throw new Error(`Bridge tasks did not become idle or expose an active bootstrap fetch: ${summary}`);
}

async function waitForActiveRun(apiRequest, taskType, runId, timeoutMs = 30_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const payload = await fetchBridgeJson(apiRequest, "/ops/task-state?view=summary", "task state summary");
    const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
    const match = tasks.find(row => (
      rowTaskType(row) === taskType && rowRunId(row) === runId && isActiveTaskRow(row)
    ));
    if (match) return match;
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  throw new Error(`Active ${taskType} run did not appear in task state: ${runId}`);
}

async function waitForRunGoneFromCurrentTasks(apiRequest, runId, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const payload = await fetchBridgeJson(apiRequest, "/ops/task-state?view=summary", "task state summary");
    const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
    if (!tasks.some(row => rowRunId(row) === runId && isActiveTaskRow(row))) return payload;
    await new Promise(resolve => setTimeout(resolve, 500));
  }
  throw new Error(`Run stayed active after abort: ${runId}`);
}

async function abortActiveBootstrapFetches(apiRequest) {
  const payload = await fetchBridgeJson(apiRequest, "/ops/task-state?view=summary", "task state summary");
  const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
  const bootstrapFetches = tasks.filter(row => (
    rowTaskType(row) === "fetch"
    && /^jobs_bootstrap_[a-f0-9]{10}$/i.test(rowRunId(row))
    && isActiveTaskRow(row)
  ));
  for (const row of bootstrapFetches) {
    const runId = rowRunId(row);
    const result = await postBridgeJson(
      apiRequest,
      "/tasks/abort",
      { taskType: "fetch", runId, reason: "packaged_smoke_cleanup" },
      "bootstrap cleanup abort"
    );
    if (!result?.ok && !result?.abortAccepted && !result?.aborted) {
      throw new Error(`Could not abort cleanup bootstrap ${runId}: ${String(result?.error || "abort rejected")}`);
    }
    await waitForRunGoneFromCurrentTasks(apiRequest, runId);
  }
}

export async function waitForBridgeTasksIdleWithBootstrapCleanup(apiRequest, timeoutMs = 120_000, stableMs = 3_000) {
  const deadline = Date.now() + timeoutMs;
  let lastError = null;
  while (Date.now() < deadline) {
    await abortActiveBootstrapFetches(apiRequest);
    try {
      return await waitForBridgeTasksIdle(apiRequest, Math.max(1000, Math.min(15_000, deadline - Date.now())), stableMs);
    } catch (error) {
      lastError = error;
      const payload = await fetchBridgeJson(apiRequest, "/ops/task-state?view=summary", "task state summary");
      const activeTasks = Array.isArray(payload?.tasks) ? payload.tasks.filter(row => isActiveTaskRow(row)) : [];
      const activeBootstrapFetches = activeTasks.filter(row => (
        rowTaskType(row) === "fetch" && /^jobs_bootstrap_[a-f0-9]{10}$/i.test(rowRunId(row))
      ));
      if (activeBootstrapFetches.length === 0) {
        await new Promise(resolve => setTimeout(resolve, 1000));
      }
    }
  }
  throw lastError || new Error("Bridge tasks did not become idle after bootstrap cleanup.");
}

async function assertNoJobAbortFailureToast(page) {
  const failureToasts = await page.locator(".toast").evaluateAll(nodes => (
    nodes
      .map(node => String(node.textContent || "").trim())
      .filter(text => /Job update failed|Could not abort job update/i.test(text))
  ));
  assert.deepEqual(failureToasts, [], `unexpected abort failure toast: ${failureToasts.join("; ")}`);
}

async function waitForPipelineButtonAbortable(page, runId) {
  await page.waitForFunction(
    expectedRunId => {
      const button = document.querySelector("#jobs-pipeline-run-btn");
      const text = String(button?.textContent || "");
      return Boolean(button)
        && !button.disabled
        && button?.dataset?.abortable === "true"
        && button?.dataset?.abortLabel === "Abort update"
        && /Checking sources|Fetching job listings|Updating jobs/i.test(text)
        && !text.includes("Aborting")
        && Boolean(expectedRunId);
    },
    runId,
    { timeout: 45_000 }
  );
}

async function assertPipelineButtonHoverAbortLabel(page, pipelineButton) {
  await pipelineButton.hover();
  const hoverStateHandle = await page.waitForFunction(
    expectedLabel => {
      const button = document.querySelector("#jobs-pipeline-run-btn");
      const label = button?.querySelector?.('[data-ui="jobs-pipeline-label"]');
      if (!button || !label) return null;
      const afterStyle = getComputedStyle(button, "::after");
      const labelStyle = getComputedStyle(label);
      const afterContent = String(afterStyle.content || "").replace(/^(['"])(.*)\1$/, "$2").replace(/\\"/g, "\"");
      const state = {
        abortable: String(button.dataset.abortable || ""),
        abortLabel: String(button.dataset.abortLabel || ""),
        afterContent,
        text: String(button.textContent || "").trim(),
        disabled: Boolean(button.disabled)
      };
      if (
        state.abortable === "true"
        && state.abortLabel === expectedLabel
        && state.afterContent === expectedLabel
        && Number(afterStyle.opacity || 0) > 0.9
        && Number(labelStyle.opacity || 1) < 0.1
        && !state.disabled
      ) {
        return state;
      }
      return null;
    },
    JOBS_ABORT_LABEL,
    { timeout: 10_000 }
  );
  const hoverState = await hoverStateHandle.jsonValue();
  assert.equal(hoverState.abortLabel, JOBS_ABORT_LABEL);
  assert.equal(hoverState.afterContent, JOBS_ABORT_LABEL);
  assert.match(hoverState.text, /Checking sources|Fetching job listings|Updating jobs/i);
}

async function waitForPipelineButtonNotAborting(page) {
  return page.waitForFunction(
    () => {
      const button = document.querySelector("#jobs-pipeline-run-btn");
      const text = String(button?.textContent || "").trim();
      return Boolean(button) && text && !/Aborting/i.test(text);
    },
    null,
    { timeout: 60_000 }
  );
}

export async function dismissJobsFirstRunNotice(page) {
  const overlay = page.locator("[data-jobs-first-run-notice='true']");
  if ((await overlay.count()) === 0) return;
  await overlay.locator("button").first().click({ timeout: 5_000 }).catch(async () => {
    await page.keyboard.press("Escape").catch(() => {});
  });
  await overlay.waitFor({ state: "detached", timeout: 5_000 }).catch(() => {});
}

export async function runJobsMainButtonAbortScenario(apiRequest, page) {
  const readyState = await waitForIdleOrActiveBootstrapFetch(apiRequest);
  const pipelineBefore = await fetchPipelineStatus(apiRequest);
  let runId = rowRunId(readyState.task);
  if (!runId) {
    const started = await postBridgeJson(
      apiRequest,
      "/tasks/run-jobs-bootstrap",
      { source: "jobs_page_abort_smoke", forceBootstrap: true },
      "jobs bootstrap start"
    );
    assert.equal(Boolean(started?.started), true, "bootstrap should start");
    assert.equal(String(started?.taskType || ""), "fetch", "bootstrap task type should be fetch");
    runId = String(started?.runId || "").trim();
    assert.match(runId, /^jobs_bootstrap_[a-f0-9]{10}$/i);
    await waitForActiveRun(apiRequest, "fetch", runId);
  }
  await waitForPipelineButtonAbortable(page, runId);
  const pipelineButton = page.locator("#jobs-pipeline-run-btn");
  await assertPipelineButtonHoverAbortLabel(page, pipelineButton);
  await pipelineButton.click();
  await page.waitForFunction(
    () => /Aborting/i.test(String(document.querySelector("#jobs-pipeline-run-btn")?.textContent || "")),
    null,
    { timeout: 10_000 }
  );
  await pipelineButton.click({ force: true }).catch(() => {});
  const pipelineAfterSecondClick = await fetchPipelineStatus(apiRequest);
  assert.equal(
    Boolean(pipelineAfterSecondClick?.active),
    Boolean(pipelineBefore?.active),
    "second abort-pending click should not start a new pipeline"
  );
  if (String(pipelineBefore?.runId || "")) {
    assert.equal(
      String(pipelineAfterSecondClick?.runId || ""),
      String(pipelineBefore?.runId || ""),
      "second abort-pending click should preserve the prior pipeline run id"
    );
  }
  await waitForRunGoneFromCurrentTasks(apiRequest, runId);
  await waitForPipelineButtonNotAborting(page);
  await assertNoJobAbortFailureToast(page);
  await Promise.all([page.waitForURL(/admin\.html/i, { timeout: 30_000 }), page.locator("#admin-page-btn").click()]);
  await page.locator("#admin-jobs-btn").waitFor({ state: "visible", timeout: 30_000 });
  await Promise.all([page.waitForURL(/jobs\.html/i, { timeout: 30_000 }), page.locator("#admin-jobs-btn").click()]);
  await waitForPipelineButtonNotAborting(page);
  await assertNoJobAbortFailureToast(page);
  await waitForBridgeTasksIdleWithBootstrapCleanup(apiRequest);
  return { runId };
}
