function defaultWait(delayMs) {
  return new Promise(resolve => setTimeout(resolve, delayMs));
}

export async function runJobAvailabilityCheck(
  service,
  availabilityId,
  { maxPolls = 80, pollIntervalMs = 750, wait = defaultWait, onProgress = () => {} } = {}
) {
  const started = await service.checkJobAvailability(availabilityId);
  if (!started?.ok) return started;
  const runId = String(started.data?.runId || "");
  if (!runId) return { ok: false, data: null, error: "Availability check did not return a run ID." };
  onProgress({ status: "running", runId });
  for (let attempt = 0; attempt < maxPolls; attempt += 1) {
    const status = await service.getJobAvailabilityCheckStatus(runId);
    if (!status?.ok) return status;
    if (String(status.data?.status || "") !== "running") return status;
    await wait(pollIntervalMs);
  }
  return { ok: false, data: null, error: "Availability check timed out." };
}

export function availabilityCheckResultLabel(payload) {
  const status = String(payload?.status || "");
  if (status === "failed") return "Availability check failed.";
  const classification = String(payload?.result?.classification || "").replaceAll("_", " ");
  return classification ? `Availability check completed: ${classification}.` : "Availability check completed.";
}

export function availabilityCheckWasApplied(payload) {
  return Boolean(payload?.status === "succeeded" && payload?.result?.applied);
}
