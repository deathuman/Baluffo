import { showToast } from "../../../shared/ui/index.js";

export function createSavedMutations(deps) {
  const notify = deps.showToast || showToast;

  async function requestOverrideReason({ title, description }) {
    if (typeof deps.requestTextInputDialog === "function") {
      return deps.requestTextInputDialog({
        title,
        description,
        label: "Reason (optional)",
        submitLabel: "Override",
        defaultValue: "",
        placeholder: "Optional reason",
        required: false
      });
    }
    const ok = await deps.requestConfirmationDialog({
      title,
      description,
      confirmLabel: "Override"
    });
    return ok ? "" : null;
  }

  async function confirmRemoveSavedJob(jobKey, savedJob) {
    if (typeof deps.requestConfirmationDialog !== "function") return true;
    const title = String(savedJob?.title || "").trim();
    const company = String(savedJob?.company || "").trim();
    const label = [title, company].filter(Boolean).join(" at ");
    const description = label
      ? `Remove "${label}" from your saved jobs? You can undo immediately after removal.`
      : "Remove this job from your saved jobs? You can undo immediately after removal.";
    return Boolean(await deps.requestConfirmationDialog({
      title: "Remove saved job?",
      description,
      confirmLabel: "Remove job",
      cancelLabel: "Cancel"
    }));
  }

  async function removeSavedJob(jobKey) {
    if (!deps.viewState.currentUser) {
      notify("Sign in required.", "error");
      return;
    }
    const safeJobKey = String(jobKey || "").trim();
    const removedSnapshot = deps.viewState.lastSavedJobsByKey.get(safeJobKey) || null;
    const confirmed = await confirmRemoveSavedJob(safeJobKey, removedSnapshot);
    if (!confirmed) return;
    try {
      const removeResult = await deps.savedPageService.removeSavedJobForUser(deps.viewState.currentUser.uid, safeJobKey);
      if (!removeResult.ok) throw new Error(removeResult.error || "Could not remove job.");
      if (deps.viewState.phaseOverrideContext?.jobKey === safeJobKey) {
        deps.viewState.phaseOverrideContext = null;
      }
      if (deps.viewState.trackingOverrideContext?.jobKey === safeJobKey) {
        deps.viewState.trackingOverrideContext = null;
      }
      notify("Removed saved job.", "success", {
        durationMs: 6500,
        actionLabel: "Undo",
        onAction: async () => {
          if (!deps.viewState.currentUser || !removedSnapshot) return;
          try {
            const restoreResult = await deps.savedPageService.saveJobForUser(deps.viewState.currentUser.uid, removedSnapshot);
            if (!restoreResult.ok) throw new Error(restoreResult.error || "Could not restore job.");
            notify("Saved job restored.", "success");
          } catch (restoreErr) {
            console.error("Could not restore removed job:", restoreErr);
            notify("Could not restore removed job.", "error");
          }
        }
      });
    } catch (err) {
      console.error("Could not remove saved job:", err);
      notify("Could not remove job.", "error");
    }
  }

  async function updatePhase(jobKey, phase, options = {}) {
    if (!deps.viewState.currentUser) {
      notify("Sign in required.", "error");
      return;
    }

    const safeJobKey = String(jobKey || "").trim();
    if (!safeJobKey) {
      notify("Invalid saved job key.", "error");
      return;
    }
    const row = deps.viewState.lastSavedJobsByKey.get(safeJobKey);
    if (!row) {
      notify("Saved job not found. Refresh and retry.", "error");
      return;
    }
    const currentPhase = deps.normalizePhase(row?.pipelinePhase || row?.applicationStatus);
    const currentOutcome = deps.normalizeOutcome(row?.outcomeStatus || row?.applicationStatus);
    const normalized = deps.normalizePhase(phase);
    if (normalized === currentPhase) {
      return;
    }
    const regularAllowed = deps.canTransition(currentPhase, normalized, currentOutcome);
    const overrideContext = deps.viewState.trackingOverrideContext || deps.viewState.phaseOverrideContext || null;
    const overrideRequested =
      options.overrideThisTransition &&
      String(overrideContext?.jobKey || "") === safeJobKey &&
      String(overrideContext?.kind || "phase") === "phase" &&
      String(overrideContext?.phase || "") === normalized;
    if (!regularAllowed && !overrideRequested) {
      const nextContext = {
        kind: "phase",
        jobKey: safeJobKey,
        phase: normalized,
        fromPhase: currentPhase,
        fromOutcome: currentOutcome
      };
      deps.viewState.phaseOverrideContext = nextContext;
      deps.viewState.trackingOverrideContext = nextContext;
      deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
      return;
    }

    let overrideReason = "";
    if (!regularAllowed && overrideRequested) {
      const from = deps.phaseLabels[currentPhase] || currentPhase;
      const to = deps.phaseLabels[normalized] || normalized;
      const reason = await requestOverrideReason({
        title: "Override phase lock?",
        description: `${from} -> ${to}`
      });
      if (reason === null) {
        deps.viewState.phaseOverrideContext = null;
        deps.viewState.trackingOverrideContext = null;
        deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
        return;
      }
      overrideReason = String(reason || "").trim();
    }

    try {
      const interviewTimestamp = deps.needsInterviewTimestamp(normalized)
        ? await deps.requestInterviewTimestamp(normalized, row?.phaseTimestamps?.[normalized] || "")
        : "";
      if (deps.needsInterviewTimestamp(normalized) && !interviewTimestamp) {
        return;
      }
      const previousPhaseTimestamp = String(row?.phaseTimestamps?.[currentPhase] || "").trim();
      const updateOptions = {
        override: !regularAllowed && overrideRequested,
        overrideReason
      };
      if (interviewTimestamp) {
        updateOptions.preserveTimestamp = interviewTimestamp;
      }
      const updateResult = typeof deps.savedPageService.updateApplicationTracking === "function"
        ? await deps.savedPageService.updateApplicationTracking(
          deps.viewState.currentUser.uid,
          safeJobKey,
          { pipelinePhase: normalized },
          updateOptions
        )
        : await deps.savedPageService.updateApplicationStatus(
          deps.viewState.currentUser.uid,
          safeJobKey,
          normalized,
          updateOptions
        );
      if (!updateResult.ok) throw new Error(updateResult.error || "Could not update phase.");
      if (overrideRequested) {
        deps.viewState.phaseOverrideContext = null;
        deps.viewState.trackingOverrideContext = null;
      }
      const previousPhase = currentPhase;
      notify(`Phase updated to ${deps.phaseLabels[normalized] || normalized}.`, "success", {
        durationMs: 6500,
        actionLabel: "Revert",
        onAction: async () => {
          if (!deps.viewState.currentUser) return;
          try {
            const revertOptions = {
              override: true,
              cleanupPhase: normalized,
              preserveTimestamp: previousPhaseTimestamp,
              eventType: "phase_reverted"
            };
            const revertResult = typeof deps.savedPageService.updateApplicationTracking === "function"
              ? await deps.savedPageService.updateApplicationTracking(
                deps.viewState.currentUser.uid,
                safeJobKey,
                { pipelinePhase: previousPhase },
                revertOptions
              )
              : await deps.savedPageService.updateApplicationStatus(
                deps.viewState.currentUser.uid,
                safeJobKey,
                previousPhase,
                revertOptions
              );
            if (!revertResult.ok) throw new Error(revertResult.error || "Could not revert phase.");
            notify(`Phase reverted to ${deps.phaseLabels[previousPhase] || previousPhase}.`, "success");
            await deps.refreshActivityLog();
            deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
          } catch (revertErr) {
            console.error("Could not revert phase change:", revertErr);
            notify("Could not revert phase.", "error");
          }
        }
      });
      deps.queueActivityPulse(safeJobKey, deps.timelineScopePhase);
      await deps.refreshActivityLog();
    } catch (err) {
      console.error("Could not update phase:", err);
      notify(err?.message || "Could not update phase.", "error");
    } finally {
      deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
    }
  }

  async function updateOutcome(jobKey, outcomeStatus, options = {}) {
    if (!deps.viewState.currentUser) {
      notify("Sign in required.", "error");
      return;
    }

    const safeJobKey = String(jobKey || "").trim();
    if (!safeJobKey) {
      notify("Invalid saved job key.", "error");
      return;
    }
    const row = deps.viewState.lastSavedJobsByKey.get(safeJobKey);
    if (!row) {
      notify("Saved job not found. Refresh and retry.", "error");
      return;
    }
    if (typeof deps.savedPageService.updateApplicationTracking !== "function") {
      notify("Outcome tracking is not available in this runtime.", "error");
      return;
    }

    const currentOutcome = deps.normalizeOutcome(row?.outcomeStatus || row?.applicationStatus);
    const normalized = deps.normalizeOutcome(outcomeStatus);
    if (normalized === currentOutcome) {
      return;
    }
    const regularAllowed = deps.canSetOutcome(currentOutcome, normalized);
    const overrideContext = deps.viewState.trackingOverrideContext || null;
    const overrideRequested =
      options.overrideThisTransition &&
      String(overrideContext?.jobKey || "") === safeJobKey &&
      String(overrideContext?.kind || "") === "outcome" &&
      String(overrideContext?.outcomeStatus || "") === normalized;
    if (!regularAllowed && !overrideRequested) {
      deps.viewState.trackingOverrideContext = {
        kind: "outcome",
        jobKey: safeJobKey,
        outcomeStatus: normalized,
        fromOutcome: currentOutcome,
        fromPhase: deps.normalizePhase(row?.pipelinePhase || row?.applicationStatus)
      };
      deps.viewState.phaseOverrideContext = null;
      deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
      return;
    }

    let overrideReason = "";
    if (!regularAllowed && overrideRequested) {
      const from = deps.outcomeLabels[currentOutcome] || currentOutcome;
      const to = deps.outcomeLabels[normalized] || normalized;
      const reason = await requestOverrideReason({
        title: "Override outcome lock?",
        description: `${from} -> ${to}`
      });
      if (reason === null) {
        deps.viewState.trackingOverrideContext = null;
        deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
        return;
      }
      overrideReason = String(reason || "").trim();
    }

    try {
      const previousOutcome = currentOutcome;
      const previousOutcomeTimestamp = String(row?.outcomeTimestamps?.[currentOutcome] || "").trim();
      const updateResult = await deps.savedPageService.updateApplicationTracking(
        deps.viewState.currentUser.uid,
        safeJobKey,
        { outcomeStatus: normalized },
        {
          override: !regularAllowed && overrideRequested,
          overrideReason
        }
      );
      if (!updateResult.ok) throw new Error(updateResult.error || "Could not update outcome.");
      if (overrideRequested) {
        deps.viewState.trackingOverrideContext = null;
      }
      notify(`Outcome updated to ${deps.outcomeLabels[normalized] || normalized}.`, "success", {
        durationMs: 6500,
        actionLabel: "Revert",
        onAction: async () => {
          if (!deps.viewState.currentUser) return;
          try {
            const revertResult = await deps.savedPageService.updateApplicationTracking(
              deps.viewState.currentUser.uid,
              safeJobKey,
              { outcomeStatus: previousOutcome },
              {
                override: true,
                preserveOutcomeTimestamp: previousOutcomeTimestamp,
                eventType: "outcome_reverted"
              }
            );
            if (!revertResult.ok) throw new Error(revertResult.error || "Could not revert outcome.");
            notify(`Outcome reverted to ${deps.outcomeLabels[previousOutcome] || previousOutcome}.`, "success");
            await deps.refreshActivityLog();
            deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
          } catch (revertErr) {
            console.error("Could not revert outcome change:", revertErr);
            notify("Could not revert outcome.", "error");
          }
        }
      });
      deps.queueActivityPulse(safeJobKey, deps.timelineScopePhase);
      await deps.refreshActivityLog();
    } catch (err) {
      console.error("Could not update outcome:", err);
      notify(err?.message || "Could not update outcome.", "error");
    } finally {
      deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
    }
  }

  return {
    removeSavedJob,
    updatePhase,
    updateOutcome
  };
}
