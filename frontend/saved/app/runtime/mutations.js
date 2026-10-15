import { showToast } from "../../../shared/ui/index.js";

export function createSavedMutations(deps) {
  const phaseOptions = deps.phaseOptions || [];
  const isPipelinePhaseRewind = deps.isPipelinePhaseRewind || ((currentPhase, nextPhase) => {
    const currentIndex = phaseOptions.indexOf(currentPhase);
    const nextIndex = phaseOptions.indexOf(nextPhase);
    return currentIndex >= 0 && nextIndex >= 0 && nextIndex < currentIndex;
  });
  const clearFuturePhaseTimestamps = deps.clearFuturePhaseTimestamps || ((timestamps, phase) => {
    const targetIndex = phaseOptions.indexOf(phase);
    const nextTimestamps = { ...(timestamps || {}) };
    if (targetIndex < 0) return nextTimestamps;
    for (const key of Object.keys(nextTimestamps)) {
      if (phaseOptions.indexOf(key) > targetIndex) delete nextTimestamps[key];
    }
    return nextTimestamps;
  });
  const toApplicationStatusMirror = deps.toApplicationStatusMirror || ((phase, outcome) => (
    outcome !== "active" ? outcome : phase
  ));
  const notify = deps.showToast || showToast;
  const inFlightRemovalByUserAndJob = new Set();
  const inFlightTrackingByJobKey = new Set();

  function getPendingTrackingMap() {
    if (!(deps.viewState.pendingTrackingByJobKey instanceof Map)) {
      deps.viewState.pendingTrackingByJobKey = new Map();
    }
    return deps.viewState.pendingTrackingByJobKey;
  }

  function beginPendingTracking(jobKey, entry) {
    const pendingMap = getPendingTrackingMap();
    if (pendingMap.get(jobKey)?.state === "pending") return null;
    pendingMap.set(jobKey, entry);
    inFlightTrackingByJobKey.add(jobKey);
    try {
      deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
    } catch (err) {
      pendingMap.delete(jobKey);
      inFlightTrackingByJobKey.delete(jobKey);
      throw err;
    }
    return entry;
  }

  function reconcilePendingTracking(jobKey, entry, succeeded, error = "") {
    const pendingMap = getPendingTrackingMap();
    if (pendingMap.get(jobKey) !== entry) return;
    if (succeeded) {
      const canonicalRow = deps.viewState.lastSavedJobsByKey.get(jobKey);
      const originalRow = entry.originalJob;
      const canonicalStillAtStart = canonicalRow && originalRow && (
        canonicalRow === originalRow || (
          canonicalRow.pipelinePhase === originalRow.pipelinePhase
          && canonicalRow.outcomeStatus === originalRow.outcomeStatus
          && canonicalRow.applicationStatus === originalRow.applicationStatus
          && JSON.stringify(canonicalRow.phaseTimestamps || {}) === JSON.stringify(originalRow.phaseTimestamps || {})
          && JSON.stringify(canonicalRow.outcomeTimestamps || {}) === JSON.stringify(originalRow.outcomeTimestamps || {})
        )
      );
      if (canonicalStillAtStart) {
        deps.viewState.lastSavedJobsByKey.set(jobKey, {
          ...canonicalRow,
          pipelinePhase: entry.optimisticJob.pipelinePhase,
          outcomeStatus: entry.optimisticJob.outcomeStatus,
          applicationStatus: entry.optimisticJob.applicationStatus,
          phaseTimestamps: entry.optimisticJob.phaseTimestamps,
          outcomeTimestamps: entry.optimisticJob.outcomeTimestamps,
          updatedAt: entry.optimisticJob.updatedAt,
          trackingUpdatedAt: entry.optimisticJob.trackingUpdatedAt,
          lastActivityAt: entry.optimisticJob.lastActivityAt
        });
      }
      pendingMap.delete(jobKey);
    } else {
      entry.state = "error";
      entry.error = error;
    }
    deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
  }

  function withOptimisticTracking(row, patch, options = {}) {
    const nowIso = new Date().toISOString();
    const phaseTimestamps = { ...(row?.phaseTimestamps || {}) };
    const outcomeTimestamps = { ...(row?.outcomeTimestamps || {}) };
    let pipelinePhase = deps.normalizePhase(row?.pipelinePhase || row?.applicationStatus);
    let outcomeStatus = deps.normalizeOutcome(row?.outcomeStatus || row?.applicationStatus);

    if (patch.pipelinePhase) {
      const nextPhase = deps.normalizePhase(patch.pipelinePhase);
      if (nextPhase !== pipelinePhase) {
        if (options.preserveTimestamp) {
          const nextIndex = phaseOptions.indexOf(nextPhase);
          if (nextIndex >= 0) {
            for (const futurePhase of phaseOptions.slice(nextIndex + 1)) delete phaseTimestamps[futurePhase];
          }
          phaseTimestamps[nextPhase] = options.preserveTimestamp;
        } else if (options.override && isPipelinePhaseRewind(pipelinePhase, nextPhase)) {
          const rewound = clearFuturePhaseTimestamps(phaseTimestamps, nextPhase);
          for (const key of Object.keys(phaseTimestamps)) delete phaseTimestamps[key];
          Object.assign(phaseTimestamps, rewound);
          phaseTimestamps[nextPhase] = phaseTimestamps[nextPhase] || nowIso;
        } else {
          phaseTimestamps[nextPhase] = nowIso;
        }
        pipelinePhase = nextPhase;
      }
    }
    if (patch.outcomeStatus) {
      const nextOutcome = deps.normalizeOutcome(patch.outcomeStatus);
      if (nextOutcome !== outcomeStatus) {
        if (nextOutcome !== "active") outcomeTimestamps[nextOutcome] = options.preserveOutcomeTimestamp || nowIso;
        outcomeStatus = nextOutcome;
      }
    }

    return {
      ...row,
      pipelinePhase,
      outcomeStatus,
      applicationStatus: toApplicationStatusMirror(pipelinePhase, outcomeStatus),
      phaseTimestamps,
      outcomeTimestamps,
      updatedAt: nowIso,
      trackingUpdatedAt: nowIso,
      lastActivityAt: nowIso
    };
  }

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
    const initiatingUid = String(deps.viewState.currentUser?.uid || "").trim();
    if (!initiatingUid) {
      notify("Sign in required.", "error");
      return;
    }
    const safeJobKey = String(jobKey || "").trim();
    if (!safeJobKey) return;
    const removalKey = `${initiatingUid}\u0000${safeJobKey}`;
    if (inFlightRemovalByUserAndJob.has(removalKey)) return;
    inFlightRemovalByUserAndJob.add(removalKey);
    const currentSnapshot = deps.viewState.lastSavedJobsByKey.get(safeJobKey) || null;
    const removedSnapshot = currentSnapshot ? { ...currentSnapshot } : null;
    const isInitiatingUserCurrent = () => (
      String(deps.viewState.currentUser?.uid || "").trim() === initiatingUid
    );
    try {
      const confirmed = await confirmRemoveSavedJob(safeJobKey, removedSnapshot);
      if (!confirmed || !isInitiatingUserCurrent()) return;
      const removeResult = await deps.savedPageService.removeSavedJobForUser(initiatingUid, safeJobKey);
      if (!isInitiatingUserCurrent()) return;
      if (!removeResult.ok) throw new Error(removeResult.error || "Could not remove job.");
      if (deps.viewState.phaseOverrideContext?.jobKey === safeJobKey) {
        deps.viewState.phaseOverrideContext = null;
      }
      if (deps.viewState.trackingOverrideContext?.jobKey === safeJobKey) {
        deps.viewState.trackingOverrideContext = null;
      }
      let undoConsumed = false;
      notify("Removed saved job.", "success", {
        durationMs: 6500,
        actionLabel: "Undo",
        onAction: async () => {
          if (undoConsumed) return;
          undoConsumed = true;
          if (!isInitiatingUserCurrent() || !removedSnapshot) return;
          try {
            const restoreResult = await deps.savedPageService.saveJobForUser(initiatingUid, removedSnapshot);
            if (!isInitiatingUserCurrent()) return;
            if (!restoreResult.ok) throw new Error(restoreResult.error || "Could not restore job.");
            notify("Saved job restored.", "success");
          } catch (restoreErr) {
            if (!isInitiatingUserCurrent()) return;
            console.error("Could not restore removed job:", restoreErr);
            notify("Could not restore removed job.", "error");
          }
        }
      });
    } catch (err) {
      if (!isInitiatingUserCurrent()) return;
      console.error("Could not remove saved job:", err);
      notify("Could not remove job.", "error");
    } finally {
      inFlightRemovalByUserAndJob.delete(removalKey);
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
    if (inFlightTrackingByJobKey.has(safeJobKey)) return;
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

    inFlightTrackingByJobKey.add(safeJobKey);
    let overrideReason = "";
    let pendingEntry = null;
    try {
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
      const optimisticJob = withOptimisticTracking(row, { pipelinePhase: normalized }, updateOptions);
      pendingEntry = beginPendingTracking(safeJobKey, {
        state: "pending",
        kind: "phase",
        phase: normalized,
        originalJob: row,
        optimisticJob
      });
      if (!pendingEntry) return;
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
      reconcilePendingTracking(safeJobKey, pendingEntry, true);
      pendingEntry = null;
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
              eventType: "phase_reverted",
              revertedFromPhase: normalized,
              restoredPhase: previousPhase,
              removedPhaseTimestampFor: normalized,
              restoredPhaseTimestamp: previousPhaseTimestamp
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
      if (pendingEntry) {
        reconcilePendingTracking(safeJobKey, pendingEntry, false, err?.message || "Could not update phase.");
      } else {
        deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
      }
      notify(err?.message || "Could not update phase.", "error");
    } finally {
      inFlightTrackingByJobKey.delete(safeJobKey);
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
    if (inFlightTrackingByJobKey.has(safeJobKey)) return;
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

    inFlightTrackingByJobKey.add(safeJobKey);
    let pendingEntry = null;
    let overrideReason = "";
    try {
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

      const previousOutcome = currentOutcome;
      const previousOutcomeTimestamp = String(row?.outcomeTimestamps?.[currentOutcome] || "").trim();
      const updateOptions = {
        override: !regularAllowed && overrideRequested,
        overrideReason
      };
      const optimisticJob = withOptimisticTracking(row, { outcomeStatus: normalized }, updateOptions);
      pendingEntry = beginPendingTracking(safeJobKey, {
        state: "pending",
        kind: "outcome",
        outcomeStatus: normalized,
        originalJob: row,
        optimisticJob
      });
      if (!pendingEntry) return;

      const updateResult = await deps.savedPageService.updateApplicationTracking(
        deps.viewState.currentUser.uid,
        safeJobKey,
        { outcomeStatus: normalized },
        updateOptions
      );
      if (!updateResult.ok) throw new Error(updateResult.error || "Could not update outcome.");
      reconcilePendingTracking(safeJobKey, pendingEntry, true);
      pendingEntry = null;
      if (overrideRequested) {
        deps.viewState.trackingOverrideContext = null;
        deps.viewState.phaseOverrideContext = null;
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
                eventType: "outcome_reverted",
                revertedFromOutcome: normalized,
                restoredOutcome: previousOutcome,
                restoredOutcomeTimestamp: previousOutcomeTimestamp
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
      if (pendingEntry) {
        reconcilePendingTracking(safeJobKey, pendingEntry, false, err?.message || "Could not update outcome.");
      } else {
        deps.renderSavedJobs(Array.from(deps.viewState.lastSavedJobsByKey.values()));
      }
      notify(err?.message || "Could not update outcome.", "error");
    } finally {
      inFlightTrackingByJobKey.delete(safeJobKey);
    }
  }

  return {
    removeSavedJob,
    updatePhase,
    updateOutcome
  };
}
