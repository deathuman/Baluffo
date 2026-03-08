export function createAdminDomain(deps) {
  const {
    ensureAdmin,
    readProfiles,
    writeProfiles,
    listAllSavedJobs,
    listAllAttachments,
    withStore,
    ensureAdminUserRow,
    utf8ByteLength,
    getAttachmentByteSize,
    sessionKey,
    getCurrentUser,
    setCurrentUser,
    notifyAuthChanged,
    notifySavedJobsChanged
  } = deps;

  async function getAdminOverview(pin) {
    ensureAdmin(pin);
    const profiles = readProfiles();
    const allSavedJobs = await listAllSavedJobs();
    const allAttachments = await listAllAttachments();

    const usersById = new Map();
    profiles.forEach(profile => {
      usersById.set(profile.id, {
        uid: profile.id,
        name: profile.name || profile.id,
        email: profile.email || "",
        savedJobsCount: 0,
        notesBytes: 0,
        attachmentsCount: 0,
        attachmentsBytes: 0,
        totalBytes: 0
      });
    });

    allSavedJobs.forEach(row => {
      const entry = ensureAdminUserRow(usersById, row.profileId, "Unknown Profile");
      entry.savedJobsCount += 1;
      entry.notesBytes += utf8ByteLength(row.notes || "");
    });

    allAttachments.forEach(row => {
      const entry = ensureAdminUserRow(usersById, row.profileId, "Unknown Profile");
      entry.attachmentsCount += 1;
      entry.attachmentsBytes += getAttachmentByteSize(row);
    });

    const users = Array.from(usersById.values()).map(row => ({
      ...row,
      totalBytes: row.notesBytes + row.attachmentsBytes
    }));
    users.sort((a, b) => {
      const byteDelta = b.totalBytes - a.totalBytes;
      if (byteDelta !== 0) return byteDelta;
      return String(a.name || "").localeCompare(String(b.name || ""));
    });

    const totals = users.reduce((acc, row) => ({
      usersCount: acc.usersCount + 1,
      savedJobsCount: acc.savedJobsCount + row.savedJobsCount,
      notesBytes: acc.notesBytes + row.notesBytes,
      attachmentsCount: acc.attachmentsCount + row.attachmentsCount,
      attachmentsBytes: acc.attachmentsBytes + row.attachmentsBytes,
      totalBytes: acc.totalBytes + row.totalBytes
    }), {
      usersCount: 0,
      savedJobsCount: 0,
      notesBytes: 0,
      attachmentsCount: 0,
      attachmentsBytes: 0,
      totalBytes: 0
    });

    return { users, totals };
  }

  async function deleteSavedJobsForProfile(uid) {
    await withStore("saved_jobs", "readwrite", (store, done, fail) => {
      const index = store.index("by_profile");
      const cursorReq = index.openCursor(IDBKeyRange.only(uid));
      cursorReq.onsuccess = event => {
        const cursor = event.target.result;
        if (!cursor) {
          done();
          return;
        }
        const delReq = cursor.delete();
        delReq.onsuccess = () => cursor.continue();
        delReq.onerror = () => fail(delReq.error || new Error("Could not delete saved job row."));
      };
      cursorReq.onerror = () => fail(cursorReq.error || new Error("Could not iterate saved jobs."));
    });
  }

  async function deleteAttachmentsForProfile(uid) {
    await withStore("attachments", "readwrite", (store, done, fail) => {
      const index = store.index("by_profile_job");
      const range = IDBKeyRange.bound([uid, ""], [uid, "\uffff"]);
      const cursorReq = index.openCursor(range);
      cursorReq.onsuccess = event => {
        const cursor = event.target.result;
        if (!cursor) {
          done();
          return;
        }
        const delReq = cursor.delete();
        delReq.onsuccess = () => cursor.continue();
        delReq.onerror = () => fail(delReq.error || new Error("Could not delete attachment row."));
      };
      cursorReq.onerror = () => fail(cursorReq.error || new Error("Could not iterate attachments."));
    });
  }

  async function wipeAccountAdmin(pin, uid) {
    ensureAdmin(pin);
    const targetUid = String(uid || "");
    if (!targetUid) throw new Error("Missing account id.");

    const profiles = readProfiles();
    const nextProfiles = profiles.filter(profile => profile.id !== targetUid);
    writeProfiles(nextProfiles);

    await deleteSavedJobsForProfile(targetUid);
    await deleteAttachmentsForProfile(targetUid);
    await withStore("activity_log", "readwrite", (store, done, fail) => {
      const index = store.index("by_profile");
      const cursorReq = index.openCursor(IDBKeyRange.only(targetUid));
      cursorReq.onsuccess = event => {
        const cursor = event.target.result;
        if (!cursor) {
          done();
          return;
        }
        const delReq = cursor.delete();
        delReq.onsuccess = () => cursor.continue();
        delReq.onerror = () => fail(delReq.error || new Error("Could not delete activity log row."));
      };
      cursorReq.onerror = () => fail(cursorReq.error || new Error("Could not iterate activity log."));
    });

    if (getCurrentUser()?.uid === targetUid) {
      localStorage.removeItem(sessionKey);
      setCurrentUser(null);
      notifyAuthChanged();
    } else {
      await notifySavedJobsChanged(targetUid).catch(() => {
        // No active listeners for this uid is expected.
      });
    }
  }

  return {
    getAdminOverview,
    wipeAccountAdmin
  };
}
