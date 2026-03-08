export function createSavedJobsDomain(deps) {
  const {
    withStore,
    listSavedJobs,
    ensureCurrentUser,
    notifySavedJobsChanged,
    addActivityLog,
    generateJobKey,
    normalizeApplicationStatus,
    canTransitionPhase,
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
    const mergedPhaseTimestamps = {
      ...toPlainObject(base.phaseTimestamps),
      ...toPlainObject(source.phaseTimestamps)
    };
    if (!mergedPhaseTimestamps.bookmark) {
      mergedPhaseTimestamps.bookmark = savedAt;
    }

    const isCustom = source.isCustom === true || (source.isCustom == null ? base.isCustom === true : false);
    return {
      pk: `${uid}::${jobKey}`,
      profileId: uid,
      jobKey,
      title: String(source.title ?? base.title ?? "").trim(),
      company: String(source.company ?? base.company ?? "").trim(),
      sector: normalizeSectorValue(source.sector ?? base.sector, source.companyType ?? base.companyType),
      companyType: String(source.companyType ?? base.companyType ?? "Tech").trim() || "Tech",
      city: String(source.city ?? base.city ?? "").trim(),
      country: String(source.country ?? base.country ?? "").trim(),
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
      applicationStatus: normalizeApplicationStatus(source.applicationStatus ?? base.applicationStatus),
      phaseTimestamps: mergedPhaseTimestamps,
      notes: String(source.notes ?? base.notes ?? ""),
      attachmentsCount: Math.max(0, Number(source.attachmentsCount ?? base.attachmentsCount) || 0),
      savedAt,
      updatedAt: normalizeIsoOrNow(source.updatedAt ?? base.updatedAt, nowIso())
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
        const phaseTimestamps = existing?.phaseTimestamps && typeof existing.phaseTimestamps === "object"
          ? { ...existing.phaseTimestamps }
          : {};
        if (!existing && job?.phaseTimestamps && typeof job.phaseTimestamps === "object") {
          Object.assign(phaseTimestamps, job.phaseTimestamps);
        }
        if (!phaseTimestamps.bookmark) {
          phaseTimestamps.bookmark = savedAt;
        }
        const applicationStatus = existing
          ? normalizeApplicationStatus(existing.applicationStatus)
          : normalizeApplicationStatus(job?.applicationStatus);
        const payload = {
          pk,
          profileId: uid,
          jobKey,
          title: job.title || "",
          company: job.company || "",
          sector: normalizeSectorValue(job.sector, job.companyType),
          companyType: job.companyType || "Tech",
          city: job.city || "",
          country: job.country || "",
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
          applicationStatus,
          phaseTimestamps,
          notes: existing?.notes ?? String(job.notes || ""),
          attachmentsCount: Number.isFinite(existing?.attachmentsCount)
            ? existing.attachmentsCount
            : Math.max(0, Number(job?.attachmentsCount) || 0),
          savedAt,
          updatedAt: currentIso
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
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");
    const nextStatus = normalizeApplicationStatus(status);
    const allowOverride = Boolean(options && options.override);
    const cleanupPhase = String(options?.cleanupPhase || "").trim();
    const preserveTimestamp = String(options?.preserveTimestamp || "").trim();
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
        const previousStatus = normalizeApplicationStatus(current.applicationStatus);
        if (!allowOverride && !canTransitionPhase(previousStatus, nextStatus)) {
          fail(new Error("Invalid phase transition. Use override for backward or skipped transitions."));
          return;
        }
        const next = {
          ...current,
          applicationStatus: nextStatus,
          phaseTimestamps: {
            ...(current.phaseTimestamps && typeof current.phaseTimestamps === "object" ? current.phaseTimestamps : {})
          },
          updatedAt: current.updatedAt || current.savedAt || nowIso()
        };
        if (cleanupPhase) {
          delete next.phaseTimestamps[cleanupPhase];
        }
        next.phaseTimestamps[nextStatus] = preserveTimestamp || nowIso();
        logPayload = {
          profileId: uid,
          jobKey: current.jobKey || jobKey,
          title: current.title || "",
          company: current.company || "",
          previousStatus,
          nextStatus,
          overrideUsed: allowOverride
        };
        const putReq = store.put(next);
        putReq.onsuccess = () => done();
        putReq.onerror = () => fail(putReq.error || new Error("Could not update application status."));
      };
      getReq.onerror = () => fail(getReq.error || new Error("Could not load saved job."));
    });

    if (logPayload) {
      await addActivityLog(uid, "phase_changed", logPayload, {
        previousStatus: logPayload.previousStatus,
        nextStatus: logPayload.nextStatus,
        overrideUsed: logPayload.overrideUsed
      });
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

  async function updateJobNotes(uid, jobKey, notes) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");
    const pk = `${uid}::${jobKey}`;

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
          notes: String(notes || ""),
          updatedAt: nowIso()
        };
        const putReq = store.put(next);
        putReq.onsuccess = () => done();
        putReq.onerror = () => fail(putReq.error || new Error("Could not update notes."));
      };
      getReq.onerror = () => fail(getReq.error || new Error("Could not load saved job."));
    });

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
    updateAttachmentMetadata,
    updateJobNotes
  };
}
