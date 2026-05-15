import { showToast } from "../../../shared/ui/index.js";

export function createSavedMutations(deps) {
  const notify = deps.showToast || showToast;

  async function removeSavedJob(jobKey) {
    if (!deps.viewState.currentUser) {
      notify("Sign in required.", "error");
      return;
    }
    const removedSnapshot = deps.viewState.lastSavedJobsByKey.get(String(jobKey || "")) || null;
    try {
      const removeResult = await deps.savedPageService.removeSavedJobForUser(deps.viewState.currentUser.uid, jobKey);
      if (!removeResult.ok) throw new Error(removeResult.error || "Could not remove job.");
      if (deps.viewState.phaseOverrideContext?.jobKey === String(jobKey || "").trim()) {
        deps.viewState.phaseOverrideContext = null;
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
    const currentPhase = deps.normalizePhase(row?.applicationStatus);
    const normalized = deps.normalizePhase(phase);
    if (normalized === currentPhase) {
      return;
    }
    const regularAllowed = deps.canTransition(currentPhase, normalized);
    const overrideContext = deps.viewState.phaseOverrideContext || null;
    const overrideRequested =
      options.overrideThisTransition &&
      String(overrideContext?.jobKey || "") === safeJobKey &&
      String(overrideContext?.phase || "") === normalized;
    if (!regularAllowed && !overrideRequested) {
      deps.viewState.phaseOverrideContext = {
        jobKey: safeJobKey,
        phase: normalized,
        fromPhase: currentPhase
      };
      deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
      return;
    }

    if (!regularAllowed && overrideRequested) {
      const from = deps.phaseLabels[currentPhase] || currentPhase;
      const to = deps.phaseLabels[normalized] || normalized;
      const ok = await deps.requestConfirmationDialog({
        title: "Override phase lock?",
        description: `${from} -> ${to}`,
        confirmLabel: "Override"
      });
      if (!ok) {
        deps.viewState.phaseOverrideContext = null;
        deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
        return;
      }
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
        override: !regularAllowed && overrideRequested
      };
      if (interviewTimestamp) {
        updateOptions.preserveTimestamp = interviewTimestamp;
      }
      const updateResult = await deps.savedPageService.updateApplicationStatus(
        deps.viewState.currentUser.uid,
        safeJobKey,
        normalized,
        updateOptions
      );
      if (!updateResult.ok) throw new Error(updateResult.error || "Could not update phase.");
      if (overrideRequested) {
        deps.viewState.phaseOverrideContext = null;
      }
      const previousPhase = currentPhase;
      notify(`Phase updated to ${deps.phaseLabels[normalized] || normalized}.`, "success", {
        durationMs: 6500,
        actionLabel: "Revert",
        onAction: async () => {
          if (!deps.viewState.currentUser) return;
          try {
            const revertResult = await deps.savedPageService.updateApplicationStatus(deps.viewState.currentUser.uid, safeJobKey, previousPhase, {
              override: true,
              cleanupPhase: normalized,
              preserveTimestamp: previousPhaseTimestamp
            });
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

  return {
    removeSavedJob,
    updatePhase
  };
}
