import assert from "node:assert/strict";
import fs from "node:fs/promises";
import path from "node:path";
import { request as playwrightRequest } from "@playwright/test";
import {
  fetchBridgeJson,
  isActiveTaskRow,
  postBridgeJson,
  waitUntil
} from "./helpers/packaged-first-run-smoke-helpers.mjs";

const BRIDGE_BASE = process.env.PACKAGED_DESKTOP_BRIDGE_BASE || "http://127.0.0.1:8877";
const REPORT_PATH =
  process.env.PACKAGED_SMOKE_REPORT_PATH ||
  process.env.PACKAGED_SMOKE_PLAYWRIGHT_REPORT ||
  path.resolve(".tmp/packaged-desktop-smoke/task-abort-schedule-report.json");
const OUTPUT_DIR =
  process.env.PACKAGED_SMOKE_OUTPUT_DIR ||
  process.env.PACKAGED_SMOKE_ARTIFACTS_DIR ||
  path.resolve(".tmp/packaged-desktop-smoke/task-abort-schedule-output");
const ABORT_REASON = "packaged_task_abort_rehearsal";

function rowRunId(row) {
  return String(row?.runId || row?.id || "").trim();
}

function rowTaskType(row) {
  return String(row?.taskType || row?.type || "").trim().toLowerCase();
}

function rowStatus(row) {
  return String(row?.status || row?.lifecycleStatus || "").trim().toLowerCase();
}

function rowTerminalReason(row) {
  return String(row?.terminalReason || row?.summary?.terminalReason || "").trim();
}

function assertCanceledAbortEvidence(row, expectedRunId, label) {
  assert.equal(rowRunId(row), expectedRunId, `${label} row should include the expected run id`);
  assert.equal(rowStatus(row), "canceled", `${label} should be canceled`);
  assert.equal(rowTerminalReason(row), "user_abort_requested", `${label} should record user_abort_requested`);
}

async function waitForActiveRun(apiRequest, taskType, runId) {
  return waitUntil("active task lifecycle row", async () => {
    const payload = await fetchBridgeJson(
      apiRequest,
      BRIDGE_BASE,
      "/ops/task-state?view=summary",
      "task state summary"
    );
    const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
    return tasks.find(row => rowRunId(row) === runId && rowTaskType(row) === taskType && isActiveTaskRow(row)) || null;
  }, 15_000, 500);
}

async function waitForRunGoneFromCurrentTasks(apiRequest, runId) {
  return waitUntil("task leaves current task state", async () => {
    const payload = await fetchBridgeJson(
      apiRequest,
      BRIDGE_BASE,
      "/ops/task-state?view=summary",
      "task state summary"
    );
    const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
    return tasks.some(row => rowRunId(row) === runId && isActiveTaskRow(row)) ? null : true;
  }, 45_000, 500);
}

async function waitForBridgeTasksIdle(apiRequest) {
  return waitUntil("bridge lifecycle idle", async () => {
    const payload = await fetchBridgeJson(
      apiRequest,
      BRIDGE_BASE,
      "/ops/task-state?view=summary",
      "task state summary"
    );
    const tasks = Array.isArray(payload?.tasks) ? payload.tasks : [];
    const activeTasks = tasks.filter(isActiveTaskRow);
    return activeTasks.length === 0 ? true : null;
  }, 60_000, 500);
}

async function waitForHistoryRun(apiRequest, taskType, runId) {
  return waitUntil("terminal lifecycle history row", async () => {
    const payload = await fetchBridgeJson(apiRequest, BRIDGE_BASE, "/ops/history?limit=100", "ops history");
    const runs = Array.isArray(payload?.runs) ? payload.runs : [];
    return runs.find(row => rowRunId(row) === runId && rowTaskType(row) === taskType) || null;
  }, 45_000, 500);
}

async function fetchSchedule(apiRequest) {
  return fetchBridgeJson(apiRequest, BRIDGE_BASE, "/tasks/jobs-pipeline-schedule", "jobs pipeline schedule");
}

async function fetchPipelineStatus(apiRequest) {
  return fetchBridgeJson(apiRequest, BRIDGE_BASE, "/tasks/run-jobs-pipeline-status", "jobs pipeline status");
}

async function waitForScheduledPipelineRunId(apiRequest) {
  return waitUntil("scheduled pipeline trigger", async () => {
    const payload = await fetchSchedule(apiRequest);
    const runId = String(payload?.status?.lastTriggerRunId || "").trim();
    return runId ? { runId, payload } : null;
  }, 30_000, 500);
}

async function waitForPipelineTerminal(apiRequest, runId) {
  return waitUntil("scheduled pipeline terminal status", async () => {
    const payload = await fetchPipelineStatus(apiRequest);
    const currentRunId = String(payload?.runId || "").trim();
    const stage = String(payload?.stage || "").trim().toLowerCase();
    const error = String(payload?.error || "").trim();
    if (currentRunId !== runId) return null;
    if (error || stage === "error") {
      throw new Error(`Scheduled Jobs pipeline entered error state: ${error || stage}`);
    }
    if (!payload?.active && stage && stage !== "starting") {
      return payload;
    }
    return null;
  }, 120_000, 1_000);
}

async function runScenario(name, slug, scenarios, callback) {
  const started = Date.now();
  const scenario = { name, slug, status: "passed", durationMs: 0, error: "" };
  try {
    Object.assign(scenario, await callback());
  } catch (error) {
    scenario.status = "failed";
    scenario.error = String(error?.stack || error?.message || error);
    throw error;
  } finally {
    scenario.durationMs = Date.now() - started;
    scenarios.push(scenario);
  }
}

async function runTaskAbortScenario(apiRequest) {
  const started = await postBridgeJson(
    apiRequest,
    BRIDGE_BASE,
    "/tasks/run-jobs-bootstrap",
    { source: "jobs_first_run" },
    "jobs bootstrap start"
  );
  assert.equal(Boolean(started?.started), true, "bootstrap should start");
  assert.equal(String(started?.taskType || ""), "fetch", "bootstrap task type should be fetch");
  assert.equal(
    String(started?.smokeMode || ""),
    "controlled-heartbeat-success",
    "bootstrap smoke mode should keep a deterministic long heartbeat"
  );
  const runId = String(started?.runId || "").trim();
  assert.match(runId, /^jobs_bootstrap_[a-f0-9]{10}$/i);

  const activeRow = await waitForActiveRun(apiRequest, "fetch", runId);
  assert.equal(rowRunId(activeRow), runId, "active lifecycle row should match started bootstrap run");

  const abortPayload = await postBridgeJson(
    apiRequest,
    BRIDGE_BASE,
    "/tasks/abort",
    { taskType: "fetch", runId, reason: ABORT_REASON },
    "task abort"
  );
  assert.equal(Boolean(abortPayload?.abortAccepted), true, "abort should be accepted");
  assert.equal(String(abortPayload?.runId || ""), runId, "abort response should target the active run");
  assert.equal(String(abortPayload?.terminalReason || ""), "user_abort_requested");

  await waitForRunGoneFromCurrentTasks(apiRequest, runId);
  const historyRow = await waitForHistoryRun(apiRequest, "fetch", runId);
  assertCanceledAbortEvidence(historyRow, runId, "fetch history");

  return { runId, abortState: String(abortPayload?.state || "") };
}

async function runSchedulerScenario(apiRequest) {
  await waitForBridgeTasksIdle(apiRequest);
  const enabled = await postBridgeJson(
    apiRequest,
    BRIDGE_BASE,
    "/tasks/jobs-pipeline-schedule",
    { enabled: true, intervalHours: 1 },
    "jobs pipeline schedule enable"
  );
  assert.equal(Boolean(enabled?.savedConfig?.enabled), true, "schedule should save enabled=true");
  assert.equal(Number(enabled?.savedConfig?.intervalHours), 1, "schedule should save one-hour interval");

  const trigger = String(enabled?.status?.lastTriggerRunId || "").trim()
    ? { runId: String(enabled.status.lastTriggerRunId).trim(), payload: enabled }
    : await waitForScheduledPipelineRunId(apiRequest);
  assert.match(trigger.runId, /^pipeline_[a-z0-9_]+/i, "schedule should trigger a pipeline run id");

  const terminalPayload = await waitForPipelineTerminal(apiRequest, trigger.runId);
  assert.equal(Boolean(terminalPayload?.active), false, "scheduled pipeline should finish");
  assert.notEqual(String(terminalPayload?.stage || "").trim().toLowerCase(), "error");
  assert.equal(String(terminalPayload?.error || "").trim(), "");

  const statusPayload = await fetchSchedule(apiRequest);
  assert.equal(String(statusPayload?.status?.lastTriggerRunId || ""), trigger.runId);
  assert.equal(Boolean(statusPayload?.status?.pending), false, "schedule should not leave pending work");
  assert.equal(String(statusPayload?.status?.lastTriggerError || ""), "");

  const disabled = await postBridgeJson(
    apiRequest,
    BRIDGE_BASE,
    "/tasks/jobs-pipeline-schedule",
    { enabled: false, intervalHours: 1 },
    "jobs pipeline schedule disable"
  );
  assert.equal(Boolean(disabled?.savedConfig?.enabled), false, "schedule should save enabled=false");
  assert.equal(Boolean(disabled?.status?.pending), false, "disabled schedule should clear pending work");

  return { runId: trigger.runId, terminalStage: String(terminalPayload?.stage || "") };
}

async function writeReport(report) {
  await fs.mkdir(path.dirname(REPORT_PATH), { recursive: true });
  await fs.mkdir(OUTPUT_DIR, { recursive: true });
  await fs.writeFile(REPORT_PATH, `${JSON.stringify(report, null, 2)}\n`, "utf8");
}

async function main() {
  const report = {
    ok: false,
    startedAt: new Date().toISOString(),
    finishedAt: "",
    scenarios: [],
    errors: [],
    artifacts: { outputDir: OUTPUT_DIR }
  };
  let apiRequest;
  try {
    await fs.mkdir(OUTPUT_DIR, { recursive: true });
    apiRequest = await playwrightRequest.newContext();
    await runScenario(
      "Packaged bridge aborts an active fetch task",
      "packaged-task-abort-rehearsal",
      report.scenarios,
      () => runTaskAbortScenario(apiRequest)
    );
    await runScenario(
      "Packaged bridge scheduler launches one Jobs pipeline",
      "packaged-jobs-pipeline-schedule-rehearsal",
      report.scenarios,
      () => runSchedulerScenario(apiRequest)
    );
    report.ok = report.scenarios.every(scenario => scenario.status === "passed");
  } catch (error) {
    report.errors.push(String(error?.stack || error?.message || error));
    report.ok = false;
  } finally {
    if (apiRequest) {
      try {
        const disabled = await postBridgeJson(
          apiRequest,
          BRIDGE_BASE,
          "/tasks/jobs-pipeline-schedule",
          { enabled: false, intervalHours: 1 },
          "jobs pipeline schedule cleanup"
        );
        report.scheduleCleanup = {
          enabled: Boolean(disabled?.savedConfig?.enabled),
          pending: Boolean(disabled?.status?.pending)
        };
      } catch (error) {
        report.scheduleCleanupError = String(error?.message || error);
      }
      await apiRequest.dispose();
    }
    report.finishedAt = new Date().toISOString();
    await writeReport(report);
  }
  if (!report.ok) {
    process.exitCode = 1;
  }
}

await main();
