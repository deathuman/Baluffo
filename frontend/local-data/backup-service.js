export function createBackupDomain(deps) {
  const {
    withStore,
    ensureCurrentUser,
    readProfiles,
    listSavedJobs,
    listAttachmentMetadata,
    listAllActivityForProfile,
    notifySavedJobsChanged,
    updateAttachmentMetadata,
    parseBackupPayload,
    buildProfileBackupPayload,
    normalizeIsoOrNow,
    toPlainObject,
    areSavedRowsEquivalent,
    serializeAttachmentWithBlob,
    stripAttachmentBlob,
    stripAttachmentPk,
    deserializeAttachmentBlob,
    nowIso,
    toActivityId,
    toAttachmentId,
    normalizeSavedJobRecord,
    mergeSavedJobRows,
    normalizeImportedAttachmentRow,
    toAttachmentFingerprint
  } = deps;

  function normalizeImportedActivityEntry(uid, row) {
    const source = toPlainObject(row);
    const createdAt = normalizeIsoOrNow(source.createdAt, nowIso());
    const type = String(source.type || "event").trim() || "event";
    const jobKey = String(source.jobKey || "").trim();
    const title = String(source.title || "").trim();
    const company = String(source.company || "").trim();
    const details = toPlainObject(source.details);
    return {
      id: String(source.id || "").trim() || toActivityId(uid, type, jobKey),
      profileId: uid,
      type,
      jobKey,
      title,
      company,
      createdAt,
      details
    };
  }

  function toActivityFingerprint(row) {
    return [
      String(row?.profileId || ""),
      String(row?.type || ""),
      String(row?.jobKey || ""),
      String(row?.title || ""),
      String(row?.company || ""),
      String(row?.createdAt || ""),
      JSON.stringify(toPlainObject(row?.details))
    ].join("|");
  }

  async function exportProfileData(uid, options = {}) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");

    const profiles = readProfiles();
    const profile = profiles.find(p => p.id === uid);
    const savedJobs = await listSavedJobs(uid);
    const includeFiles = Boolean(options.includeFiles);
    const attachments = await listAttachmentMetadata(uid);
    const serializedAttachments = includeFiles
      ? await Promise.all(attachments.map(row => serializeAttachmentWithBlob(row, stripAttachmentPk)))
      : attachments.map(row => stripAttachmentBlob(row, stripAttachmentPk));
    const activityLog = await listAllActivityForProfile(uid);

    return buildProfileBackupPayload(uid, {
      backupSchemaVersion: deps.backupSchemaVersion,
      nowIso,
      normalizeSavedJobRecord,
      includeFiles,
      profile: profile || { id: uid, name: user.displayName || uid, email: user.email || "" },
      savedJobs,
      attachments: serializedAttachments,
      activityLog
    });
  }

  async function mergeImportedJobs(uid, importedRows) {
    const existingRows = await listSavedJobs(uid);
    const existingByKey = new Map(existingRows.map(row => [String(row.jobKey || ""), row]));
    const writes = [];
    const summary = {
      created: 0,
      updated: 0,
      skippedInvalid: 0
    };
    const warnings = [];

    for (const rawRow of importedRows) {
      const source = toPlainObject(rawRow);
      const title = String(source.title || "").trim();
      const company = String(source.company || "").trim();
      if (!title || !company) {
        summary.skippedInvalid += 1;
        warnings.push("Skipped malformed saved job (missing title/company).");
        continue;
      }
      const normalized = normalizeSavedJobRecord(uid, source);
      const key = String(normalized.jobKey || "");
      if (!key) {
        summary.skippedInvalid += 1;
        warnings.push("Skipped malformed saved job (missing jobKey).");
        continue;
      }
      const existing = existingByKey.get(key);
      if (!existing) {
        summary.created += 1;
        existingByKey.set(key, normalized);
        writes.push(normalized);
        continue;
      }
      const merged = mergeSavedJobRows(uid, existing, source);
      if (!areSavedRowsEquivalent(existing, merged)) {
        summary.updated += 1;
        existingByKey.set(key, merged);
        writes.push(merged);
      }
    }

    if (writes.length > 0) {
      await withStore("saved_jobs", "readwrite", (store, done, fail) => {
        try {
          writes.forEach(row => store.put(row));
          done();
        } catch (err) {
          fail(err);
        }
      });
    }

    return { summary, warnings, rowsByKey: existingByKey };
  }

  async function mergeImportedAttachments(uid, importedRows) {
    const normalizedRows = importedRows
      .map(row => normalizeImportedAttachmentRow(uid, row, normalizeIsoOrNow, toPlainObject, deserializeAttachmentBlob))
      .filter(Boolean);
    if (normalizedRows.length === 0) return { added: 0, hydrated: 0 };

    const existing = await listAttachmentMetadata(uid);
    const existingFingerprints = new Set(existing.map(toAttachmentFingerprint));
    const existingIds = new Set(existing.map(row => String(row.id || "")));
    const existingByComposite = new Map(
      existing.map(row => [`${String(row?.jobKey || "")}::${String(row?.id || "")}`, row])
    );
    const writes = [];
    let hydrated = 0;
    for (const row of normalizedRows) {
      const composite = `${row.jobKey}::${row.id}`;
      const existingSameId = existingByComposite.get(composite) || null;
      if (existingSameId) {
        const existingHasBlob = existingSameId.blob instanceof Blob;
        const incomingHasBlob = row.blob instanceof Blob;
        if (!existingHasBlob && incomingHasBlob) {
          writes.push({
            pk: `${uid}::${row.jobKey}::${row.id}`,
            id: row.id,
            profileId: uid,
            jobKey: row.jobKey,
            name: row.name,
            type: row.type,
            size: row.size,
            createdAt: row.createdAt || existingSameId.createdAt || nowIso(),
            blob: row.blob
          });
          hydrated += 1;
        }
        continue;
      }

      const fingerprint = toAttachmentFingerprint(row);
      if (existingFingerprints.has(fingerprint)) continue;
      let next = row;
      if (existingIds.has(next.id)) {
        const replacementId = toAttachmentId(next.name);
        next = {
          ...next,
          id: replacementId,
          pk: `${uid}::${next.jobKey}::${replacementId}`
        };
      }
      existingIds.add(next.id);
      existingFingerprints.add(toAttachmentFingerprint(next));
      existingByComposite.set(`${next.jobKey}::${next.id}`, next);
      writes.push(next);
    }

    if (writes.length > 0) {
      await withStore("attachments", "readwrite", (store, done, fail) => {
        try {
          writes.forEach(row => store.put(row));
          done();
        } catch (err) {
          fail(err);
        }
      });
    }
    const added = Math.max(0, writes.length - hydrated);
    return { added, hydrated };
  }

  async function mergeImportedActivity(uid, importedRows) {
    const normalizedRows = importedRows
      .map(row => normalizeImportedActivityEntry(uid, row))
      .filter(Boolean);
    if (normalizedRows.length === 0) return { added: 0 };

    const existingRows = await listAllActivityForProfile(uid);
    const existingFingerprints = new Set(existingRows.map(toActivityFingerprint));
    const existingIds = new Set(existingRows.map(row => String(row.id || "")));
    const writes = [];
    for (const row of normalizedRows) {
      const fingerprint = toActivityFingerprint(row);
      if (existingFingerprints.has(fingerprint)) continue;
      let next = row;
      if (existingIds.has(next.id)) {
        next = { ...next, id: toActivityId(uid, next.type, next.jobKey) };
      }
      existingIds.add(next.id);
      existingFingerprints.add(toActivityFingerprint(next));
      writes.push(next);
    }

    if (writes.length > 0) {
      await withStore("activity_log", "readwrite", (store, done, fail) => {
        try {
          writes.forEach(row => store.put(row));
          done();
        } catch (err) {
          fail(err);
        }
      });
    }
    return { added: writes.length };
  }

  async function syncAttachmentCountsForJobs(uid, jobKeys) {
    if (!jobKeys || jobKeys.size === 0) return;
    const meta = await listAttachmentMetadata(uid);
    const countsByKey = new Map();
    meta.forEach(att => {
      const key = String(att?.jobKey || "");
      if (!key) return;
      countsByKey.set(key, (countsByKey.get(key) || 0) + 1);
    });
    for (const jobKey of jobKeys) {
      const count = countsByKey.get(jobKey) || 0;
      await updateAttachmentMetadata(uid, jobKey, count);
    }
  }

  async function importProfileData(uid, payload) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");
    const parsed = parseBackupPayload(payload);

    const mergeJobsResult = await mergeImportedJobs(uid, parsed.savedJobs);
    const attachmentMerge = await mergeImportedAttachments(uid, parsed.attachments);
    const activityMerge = await mergeImportedActivity(uid, parsed.activityLog);

    await syncAttachmentCountsForJobs(uid, new Set(Array.from(mergeJobsResult.rowsByKey.keys())));
    await notifySavedJobsChanged(uid);

    return {
      schemaVersion: parsed.schemaVersion,
      created: mergeJobsResult.summary.created,
      updated: mergeJobsResult.summary.updated,
      skippedInvalid: mergeJobsResult.summary.skippedInvalid,
      historyAdded: activityMerge.added,
      attachmentsAdded: attachmentMerge.added,
      attachmentsHydrated: attachmentMerge.hydrated || 0,
      warnings: parsed.warnings.concat(mergeJobsResult.warnings)
    };
  }

  return {
    exportProfileData,
    importProfileData
  };
}
