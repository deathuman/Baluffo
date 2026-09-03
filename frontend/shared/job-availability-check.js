function defaultWait(delayMs) {
  return new Promise(resolve => setTimeout(resolve, delayMs));
}

// Verdict tones map to toast styling: "success" (green) only for definitive
// live evidence, "error" (red) only for closed/failed evidence, "info"
// (neutral) for inconclusive outcomes that need a human decision.
const VERDICTS = {
  direct_live: {
    tone: "success",
    message: "Verified live — the page loads and shows this job open."
  },
  direct_closed: {
    tone: "error",
    message: "The job page reports closed."
  },
  direct_closed_ambiguous: {
    tone: "error",
    message: "The page suggests this job is closed."
  },
  direct_unverified: {
    tone: "info",
    message: "Couldn't verify — the site didn't confirm the job (slow, blocked, or no visible apply signal)."
  },
  generic_redirect: {
    tone: "info",
    message: "Couldn't verify — the link redirects to a general careers page."
  },
  anti_bot: {
    tone: "info",
    message: "Couldn't verify — the site blocked the automated check."
  },
  invalid_public_url: {
    tone: "info",
    message: "This link can't be checked automatically."
  },
  check_failed: {
    tone: "error",
    message: "The availability check failed — try again."
  }
};

const INCONCLUSIVE_CLASSIFICATIONS = new Set([
  "direct_unverified",
  "generic_redirect",
  "anti_bot",
  "invalid_public_url"
]);

function verdictForClassification(classification, confidence) {
  if (classification === "direct_closed" && confidence !== "definitive") {
    return VERDICTS.direct_closed_ambiguous;
  }
  return VERDICTS[classification]
    || { tone: "info", message: "Couldn't verify — the site didn't confirm the job." };
}

/**
 * Map a terminal check payload to a human verdict.
 * Returns { tone, message, classification, conclusive }.
 */
export function availabilityCheckVerdict(payload) {
  const status = String(payload?.status || "");
  if (status !== "succeeded") {
    return {
      tone: VERDICTS.check_failed.tone,
      message: VERDICTS.check_failed.message,
      classification: "check_failed",
      conclusive: false
    };
  }
  const result = payload?.result && typeof payload.result === "object" ? payload.result : {};
  const classification = String(result.classification || "");
  const confidence = String(result.availabilityEvidence?.confidence || result.confidence || "");
  const verdict = verdictForClassification(classification, confidence);
  if (result.applied === false && result.enforced === true && verdict.tone === "success") {
    // A definitive live result existed but a newer lifecycle entry already
    // supersedes it; the row state did not change from this check.
    return {
      tone: "info",
      message: "No change — a newer result already applies.",
      classification,
      conclusive: true
    };
  }
  return {
    tone: verdict.tone,
    message: verdict.message,
    classification,
    conclusive: !INCONCLUSIVE_CLASSIFICATIONS.has(classification) && classification !== "check_failed"
  };
}

export async function runJobAvailabilityCheck(
  service,
  availabilityId,
  { maxWallMs = 0, pollIntervalMs = 750, wait = defaultWait, onProgress = () => {} } = {}
) {
  const startedAt = Date.now();
  const reportProgress = () => {
    onProgress({
      status: "running",
      elapsedS: Math.max(1, Math.round((Date.now() - startedAt) / 1000))
    });
  };
  const started = await service.checkJobAvailability(availabilityId);
  if (!started?.ok) return started;
  const runId = String(started.data?.runId || "");
  if (!runId) return { ok: false, data: null, error: "Availability check did not return a run ID." };
  reportProgress();
  // No fixed poll count: keep polling while the backend reports "running".
  // Backend runs are bounded (HTTP timeout + pacing), so "running" cannot
  // hang forever; maxWallMs is an optional test/emergency backstop.
  for (;;) {
    await wait(pollIntervalMs);
    if (maxWallMs > 0 && Date.now() - startedAt > maxWallMs) {
      return { ok: false, data: null, error: "Availability check timed out." };
    }
    const status = await service.getJobAvailabilityCheckStatus(runId);
    if (!status?.ok) return status;
    if (String(status.data?.status || "") !== "running") return status;
    reportProgress();
  }
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
