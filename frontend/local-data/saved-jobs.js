import { sanitizeLocationField } from "../jobs/domain.js";
import {
  canSetOutcomeStatus,
  canTransitionPipelinePhase,
  normalizeOutcomeStatus,
  normalizePipelinePhase,
  normalizeTrackingFields,
  splitApplicationStatus,
  toApplicationStatusMirror
} from "./tracking.js";

export function createSavedJobsDomain(deps) {
  const {
    withStore,
    listSavedJobs,
    ensureCurrentUser,
    notifySavedJobsChanged,
    addActivityLog,
    generateJobKey,
    normalizeSectorValue,
    normalizeCustomSourceLabel,
    sanitizeJobUrl,
    nowIso,
    normalizeIsoOrNow,
    toPlainObject,
    isClearlyLowerQualityImported
  } = deps;

  function normalizeSavedJobRecord(uid, row, fallback = null) {
    const source = toPlainObject(row);
    const base = fallback ? toPlainObject(fallback) : {};
    const inputForKey = {
      ...base,
      ...source,
      jobKey: source.jobKey || base.jobKey || "",
      keySalt: source.keySalt || ""
    };
    const jobKey = generateJobKey(inputForKey);
    const savedAt = normalizeIsoOrNow(source.savedAt || base.savedAt, nowIso());
    const tracking = normalizeTrackingFields(source, base, {
      savedAt,
      nowIso,
      normalizeIsoOrNow
    });

    const isCustom = source.isCustom === true || (source.isCustom == null ? base.isCustom === true : false);
    return {
      pk: `${uid}::${jobKey}`,
      profileId: uid,
      jobKey,
      title: String(source.title ?? base.title ?? "").trim(),
      company: String(source.company ?? base.company ?? "").trim(),
      sector: normalizeSectorValue(source.sector ?? base.sector, source.companyType ?? base.companyType),
      companyType: String(source.companyType ?? base.companyType ?? "Tech").trim() || "Tech",
      city: sanitizeLocationField(source.city ?? base.city ?? "", "city"),
      country: sanitizeLocationField(source.country ?? base.country ?? "", "country"),
      workType: String(source.workType ?? base.workType ?? "Onsite").trim() || "Onsite",
      contractType: String(source.contractType ?? base.contractType ?? "Unknown").trim() || "Unknown",
      jobLink: sanitizeJobUrl(source.jobLink ?? base.jobLink ?? ""),
      profession: String(source.profession ?? base.profession ?? "").trim(),
      isCustom,
      customSourceLabel: isCustom
        ? normalizeCustomSourceLabel(source.customSourceLabel ?? base.customSourceLabel)
        : "",
      reminderAt: normalizeIsoOrNow(source.reminderAt ?? base.reminderAt, ""),
      contactedAt: normalizeIsoOrNow(source.contactedAt ?? base.contactedAt, ""),
      updatedBy: String(source.updatedBy ?? base.updatedBy ?? "").trim(),
      pipelinePhase: tracking.pipelinePhase,
      outcomeStatus: tracking.outcomeStatus,
      applicationStatus: tracking.applicationStatus,
      phaseTimestamps: tracking.phaseTimestamps,
      outcomeTimestamps: tracking.outcomeTimestamps,
      notes: String(source.notes ?? base.notes ?? ""),
      attachmentsCount: Math.max(0, Number(source.attachmentsCount ?? base.attachmentsCount) || 0),
      savedAt,
      updatedAt: normalizeIsoOrNow(source.updatedAt ?? base.updatedAt, nowIso()),
      contentUpdatedAt: tracking.contentUpdatedAt,
      trackingUpdatedAt: tracking.trackingUpdatedAt,
      notesUpdatedAt: tracking.notesUpdatedAt,
      lastActivityAt: tracking.lastActivityAt
    };
  }

  function mergeSavedJobRows(uid, existingRow, importedRow) {
    const existing = normalizeSavedJobRecord(uid, existingRow);
    const imported = normalizeSavedJobRecord(uid, importedRow, existing);
    if (isClearlyLowerQualityImported(existing, imported)) {
      return existing;
    }
    const merged = {
      ...existing,
      ...imported,
      pk: existing.pk,
      profileId: uid,
      jobKey: existing.jobKey,
      savedAt: normalizeIsoOrNow(existing.savedAt || imported.savedAt, nowIso()),
      updatedAt: nowIso()
    };
    merged.phaseTimestamps = {
      ...toPlainObject(existing.phaseTimestamps),
      ...toPlainObject(imported.phaseTimestamps)
    };
    if (!merged.phaseTimestamps.bookmark) {
      merged.phaseTimestamps.bookmark = merged.savedAt;
    }
    const tracking = normalizeTrackingFields(merged, existing, {
      savedAt: merged.savedAt,
      nowIso,
      normalizeIsoOrNow
    });
    Object.assign(merged, tracking);
    return merged;
  }

  async function saveJobForUser(uid, job, options = {}) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");

    const jobKey = generateJobKey(job);
    const pk = `${uid}::${jobKey}`;
    const currentIso = nowIso();

    let existingSnapshot = null;
    let savedSnapshot = null;
    await withStore("saved_jobs", "readwrite", (store, done, fail) => {
      const getReq = store.get(pk);
      getReq.onsuccess = () => {
        const existing = getReq.result || null;
        existingSnapshot = existing ? { ...existing } : null;
        const incomingSavedAt = String(job?.savedAt || "").trim();
        const savedAt = existing?.savedAt || incomingSavedAt || currentIso;
        const tracking = normalizeTrackingFields(job || {}, existing || {}, {
          savedAt,
          nowIso: () => currentIso,
          normalizeIsoOrNow
        });
        const payload = {
          pk,
          profileId: uid,
          jobKey,
          title: job.title || "",
          company: job.company || "",
          sector: normalizeSectorValue(job.sector, job.companyType),
          companyType: job.companyType || "Tech",
          city: sanitizeLocationField(job.city || "", "city"),
          country: sanitizeLocationField(job.country || "", "country"),
          workType: job.workType || "Onsite",
          contractType: job.contractType || "Unknown",
          jobLink: sanitizeJobUrl(job.jobLink || ""),
          profession: job.profession || "",
          isCustom: existing?.isCustom === true ? true : Boolean(job.isCustom),
          customSourceLabel: (existing?.isCustom === true || Boolean(job.isCustom))
            ? normalizeCustomSourceLabel(job.customSourceLabel || existing?.customSourceLabel)
            : "",
          reminderAt: String(job.reminderAt || existing?.reminderAt || "").trim(),
          contactedAt: String(job.contactedAt || existing?.contactedAt || "").trim(),
          updatedBy: String(job.updatedBy || existing?.updatedBy || "").trim(),
          pipelinePhase: tracking.pipelinePhase,
          outcomeStatus: tracking.outcomeStatus,
          applicationStatus: tracking.applicationStatus,
          phaseTimestamps: tracking.phaseTimestamps,
          outcomeTimestamps: tracking.outcomeTimestamps,
          notes: existing?.notes ?? String(job.notes || ""),
          attachmentsCount: Number.isFinite(existing?.attachmentsCount)
            ? existing.attachmentsCount
            : Math.max(0, Number(job?.attachmentsCount) || 0),
          savedAt,
          updatedAt: currentIso,
          contentUpdatedAt: currentIso,
          trackingUpdatedAt: tracking.trackingUpdatedAt,
          notesUpdatedAt: tracking.notesUpdatedAt,
          lastActivityAt: tracking.lastActivityAt || savedAt
        };
        savedSnapshot = { ...payload };
        const putReq = store.put(payload);
        putReq.onsuccess = () => done(jobKey);
        putReq.onerror = () => fail(putReq.error || new Error("Could not save job."));
      };
      getReq.onerror = () => fail(getReq.error || new Error("Could not read existing saved job."));
    });

    let eventType = String(options?.eventType || "").trim();
    if (!eventType) {
      const hadExisting = Boolean(existingSnapshot);
      if (savedSnapshot?.isCustom && hadExisting) {
        eventType = "custom_job_updated";
      } else {
        eventType = savedSnapshot?.isCustom ? "custom_job_created" : "job_saved";
      }
    }
    await addActivityLog(uid, eventType, savedSnapshot || { jobKey, title: job.title, company: job.company }, {
      isCustom: Boolean(savedSnapshot?.isCustom)
    });
    const previousReminder = String(existingSnapshot?.reminderAt || "").trim();
    const nextReminder = String(savedSnapshot?.reminderAt || "").trim();
    if (!previousReminder && nextReminder) {
      await addActivityLog(uid, "reminder_set", savedSnapshot, { reminderAt: nextReminder });
    } else if (previousReminder && !nextReminder) {
      await addActivityLog(uid, "reminder_cleared", savedSnapshot, {});
    } else if (previousReminder && nextReminder && previousReminder !== nextReminder) {
      await addActivityLog(uid, "reminder_set", savedSnapshot, { reminderAt: nextReminder });
    }
    await notifySavedJobsChanged(uid);
    return jobKey;
  }

  async function removeSavedJobForUser(uid, jobKey) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");
    const pk = `${uid}::${jobKey}`;

    let removedSnapshot = null;
    await withStore("saved_jobs", "readonly", (store, done, fail) => {
      const req = store.get(pk);
      req.onsuccess = () => {
        removedSnapshot = req.result || null;
        done();
      };
      req.onerror = () => fail(req.error || new Error("Could not load saved job before remove."));
    });

    await withStore("saved_jobs", "readwrite", (store, done, fail) => {
      const req = store.delete(pk);
      req.onsuccess = () => done();
      req.onerror = () => fail(req.error || new Error("Could not remove saved job."));
    });

    if (removedSnapshot) {
      const eventType = removedSnapshot?.isCustom ? "custom_job_removed" : "job_removed";
      await addActivityLog(uid, eventType, removedSnapshot, {
        fromStatus: removedSnapshot.applicationStatus || "bookmark",
        fromPhase: removedSnapshot.pipelinePhase || "bookmark",
        fromOutcome: removedSnapshot.outcomeStatus || "active",
        isCustom: Boolean(removedSnapshot?.isCustom)
      });
    }
    await notifySavedJobsChanged(uid);
  }

  async function getSavedJobKeys(uid) {
    const rows = await listSavedJobs(uid);
    return new Set(rows.map(r => r.jobKey));
  }

  function subscribeSavedJobs(uid, onChange, onError, listeners) {
    const entry = {
      type: "saved",
      uid,
      callback: onChange
    };
    listeners.add(entry);
    listSavedJobs(uid).then(onChange).catch(onError);
    return () => listeners.delete(entry);
  }

  async function updateApplicationStatus(uid, jobKey, status, options = {}) {
    const split = splitApplicationStatus(status);
    if (split.outcomeStatus !== "active") {
      return updateApplicationTracking(uid, jobKey, { outcomeStatus: split.outcomeStatus }, options);
    }
    return updateApplicationTracking(uid, jobKey, { pipelinePhase: split.pipelinePhase }, options);
  }

  async function updateApplicationTracking(uid, jobKey, trackingUpdate = {}, options = {}) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");
    const allowOverride = Boolean(options && options.override);
    const cleanupPhase = String(options?.cleanupPhase || "").trim();
    const preserveTimestamp = String(options?.preserveTimestamp || "").trim();
    const preserveOutcomeTimestamp = String(options?.preserveOutcomeTimestamp || "").trim();
    const overrideReason = String(options?.overrideReason || "").trim();
    const eventTypeOverride = String(options?.eventType || "").trim();
    const revertedFromPhase = String(options?.revertedFromPhase || "").trim();
    const restoredPhase = String(options?.restoredPhase || "").trim();
    const removedPhaseTimestampFor = String(options?.removedPhaseTimestampFor || "").trim();
    const restoredPhaseTimestamp = String(options?.restoredPhaseTimestamp || "").trim();
    const revertedFromOutcome = String(options?.revertedFromOutcome || "").trim();
    const restoredOutcome = String(options?.restoredOutcome || "").trim();
    const restoredOutcomeTimestamp = String(options?.restoredOutcomeTimestamp || "").trim();
    const pk = `${uid}::${jobKey}`;

    let logPayload = null;
    await withStore("saved_jobs", "readwrite", (store, done, fail) => {
      const getReq = store.get(pk);
      getReq.onsuccess = () => {
        const current = getReq.result;
        if (!current) {
          fail(new Error("Saved job not found."));
          return;
        }
        const previousTracking = normalizeTrackingFields(current, {}, {
          savedAt: current.savedAt,
          nowIso,
          normalizeIsoOrNow
        });
        const nextPhase = trackingUpdate.pipelinePhase == null
          ? previousTracking.pipelinePhase
          : normalizePipelinePhase(trackingUpdate.pipelinePhase);
        const nextOutcome = trackingUpdate.outcomeStatus == null
          ? previousTracking.outcomeStatus
          : normalizeOutcomeStatus(trackingUpdate.outcomeStatus);
        const phaseChanged = nextPhase !== previousTracking.pipelinePhase;
        const outcomeChanged = nextOutcome !== previousTracking.outcomeStatus;
        if (!phaseChanged && !outcomeChanged) {
          done();
          return;
        }
        if (phaseChanged && !allowOverride && !canTransitionPipelinePhase(
          previousTracking.pipelinePhase,
          nextPhase,
          previousTracking.outcomeStatus
        )) {
          fail(new Error("Invalid phase transition. Use override for backward or skipped transitions."));
          return;
        }
        if (outcomeChanged && !allowOverride && !canSetOutcomeStatus(previousTracking.outcomeStatus, nextOutcome)) {
          fail(new Error("Invalid outcome transition. Use override for terminal outcome changes."));
          return;
        }
        const currentIso = nowIso();
        const next = {
          ...current,
          pipelinePhase: nextPhase,
          outcomeStatus: nextOutcome,
          applicationStatus: toApplicationStatusMirror(nextPhase, nextOutcome),
          phaseTimestamps: {
            ...previousTracking.phaseTimestamps
          },
          outcomeTimestamps: {
            ...previousTracking.outcomeTimestamps
          },
          updatedAt: currentIso,
          trackingUpdatedAt: currentIso
        };
        if (cleanupPhase) {
          delete next.phaseTimestamps[cleanupPhase];
        }
        if (phaseChanged) {
          next.phaseTimestamps[nextPhase] = preserveTimestamp || currentIso;
        }
        if (outcomeChanged && nextOutcome !== "active") {
          next.outcomeTimestamps[nextOutcome] = preserveOutcomeTimestamp || currentIso;
        }
        logPayload = {
          profileId: uid,
          jobKey: current.jobKey || jobKey,
          title: current.title || "",
          company: current.company || "",
          previousPhase: previousTracking.pipelinePhase,
          nextPhase,
          previousOutcome: previousTracking.outcomeStatus,
          nextOutcome,
          phaseChanged,
          outcomeChanged,
          overrideUsed: allowOverride,
          overrideReason,
          overrideReasonProvided: Boolean(overrideReason)
        };
        const putReq = store.put(next);
        putReq.onsuccess = () => done();
        putReq.onerror = () => fail(putReq.error || new Error("Could not update application status."));
      };
      getReq.onerror = () => fail(getReq.error || new Error("Could not load saved job."));
    });

    if (logPayload) {
      const eventType = eventTypeOverride
        || (logPayload.outcomeChanged ? "outcome_changed" : "phase_changed");
      const details = {
        previousPhase: logPayload.previousPhase,
        nextPhase: logPayload.nextPhase,
        previousOutcome: logPayload.previousOutcome,
        nextOutcome: logPayload.nextOutcome,
        previousStatus: toApplicationStatusMirror(logPayload.previousPhase, logPayload.previousOutcome),
        nextStatus: toApplicationStatusMirror(logPayload.nextPhase, logPayload.nextOutcome),
        overrideUsed: logPayload.overrideUsed,
        overrideReason: logPayload.overrideReason,
        overrideReasonProvided: logPayload.overrideReasonProvided
      };
      if (eventType === "phase_reverted") {
        details.revertedFromPhase = normalizePipelinePhase(revertedFromPhase || logPayload.previousPhase);
        details.restoredPhase = normalizePipelinePhase(restoredPhase || logPayload.nextPhase);
        details.removedPhaseTimestampFor = normalizePipelinePhase(removedPhaseTimestampFor || cleanupPhase || logPayload.previousPhase);
        details.restoredPhaseTimestamp = restoredPhaseTimestamp || preserveTimestamp || "";
      }
      if (eventType === "outcome_reverted") {
        details.revertedFromOutcome = normalizeOutcomeStatus(revertedFromOutcome || logPayload.previousOutcome);
        details.restoredOutcome = normalizeOutcomeStatus(restoredOutcome || logPayload.nextOutcome);
        details.restoredOutcomeTimestamp = restoredOutcomeTimestamp || preserveOutcomeTimestamp || "";
      }
      await addActivityLog(uid, eventType, logPayload, details);
    }
    await notifySavedJobsChanged(uid);
  }

  async function updateAttachmentMetadata(uid, jobKey, attachmentsCount) {
    const pk = `${uid}::${jobKey}`;
    const safeCount = Math.max(0, Number(attachmentsCount) || 0);

    await withStore("saved_jobs", "readwrite", (store, done, fail) => {
      const getReq = store.get(pk);
      getReq.onsuccess = () => {
        const current = getReq.result;
        if (!current) {
          fail(new Error("Saved job not found."));
          return;
        }
        const next = {
          ...current,
          attachmentsCount: safeCount,
          updatedAt: nowIso()
        };
        const putReq = store.put(next);
        putReq.onsuccess = () => done();
        putReq.onerror = () => fail(putReq.error || new Error("Could not update attachment metadata."));
      };
      getReq.onerror = () => fail(getReq.error || new Error("Could not load saved job."));
    });

    await notifySavedJobsChanged(uid);
  }

  async function updateJobNotes(uid, jobKey, notes, _options = {}) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");
    const pk = `${uid}::${jobKey}`;

    let logPayload = null;
    await withStore("saved_jobs", "readwrite", (store, done, fail) => {
      const getReq = store.get(pk);
      getReq.onsuccess = () => {
        const current = getReq.result;
        if (!current) {
          fail(new Error("Saved job not found."));
          return;
        }
        const previousNotes = String(current.notes || "");
        const nextNotes = String(notes || "");
        const next = {
          ...current,
          notes: nextNotes,
          notesUpdatedAt: nowIso()
        };
        logPayload = {
          profileId: uid,
          jobKey: current.jobKey || jobKey,
          title: current.title || "",
          company: current.company || "",
          notesChanged: previousNotes !== nextNotes,
          previousLength: previousNotes.length,
          nextLength: nextNotes.length
        };
        const putReq = store.put(next);
        putReq.onsuccess = () => done();
        putReq.onerror = () => fail(putReq.error || new Error("Could not update notes."));
      };
      getReq.onerror = () => fail(getReq.error || new Error("Could not load saved job."));
    });

    if (logPayload && logPayload.notesChanged) {
      await addActivityLog(uid, "note_updated", logPayload, {
        previousLength: logPayload.previousLength,
        nextLength: logPayload.nextLength,
        debounceWindow: true
      });
    }
    await notifySavedJobsChanged(uid);
  }

  return {
    normalizeSavedJobRecord,
    mergeSavedJobRows,
    saveJobForUser,
    removeSavedJobForUser,
    getSavedJobKeys,
    subscribeSavedJobs,
    updateApplicationStatus,
    updateApplicationTracking,
    updateAttachmentMetadata,
    updateJobNotes
  };
}
