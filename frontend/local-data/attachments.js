import { stripAttachmentPk, dedupeAttachmentRows, attachmentDedupeKey } from "./indexeddb-adapter.js";

export function createAttachmentsDomain(deps) {
  const {
    withStore,
    ensureCurrentUser,
    hashFNV1a,
    nowIso,
    updateAttachmentMetadata,
    addActivityLog
  } = deps;

  function toAttachmentId(fileName) {
    const base = `${fileName || "file"}|${Date.now()}|${Math.random().toString(36).slice(2)}`;
    return `att_${hashFNV1a(base)}`;
  }

  async function listAttachmentsForJob(uid, jobKey) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");

    return withStore("attachments", "readonly", (store, done, fail) => {
      const index = store.index("by_profile_job");
      const request = index.getAll(IDBKeyRange.only([uid, jobKey]));
      request.onsuccess = () => {
        const rows = dedupeAttachmentRows(request.result || [])
          .map(stripAttachmentPk)
          .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")));
        done(rows);
      };
      request.onerror = () => fail(request.error || new Error("Could not list attachments."));
    });
  }

  async function addAttachmentForJob(uid, jobKey, fileMeta, blob) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");
    const attachmentId = toAttachmentId(fileMeta?.name);

    await withStore("attachments", "readwrite", (store, done, fail) => {
      const putReq = store.put({
        id: attachmentId,
        profileId: uid,
        jobKey,
        name: String(fileMeta?.name || "file"),
        type: String(fileMeta?.type || "application/octet-stream"),
        size: Number(fileMeta?.size) || 0,
        createdAt: nowIso(),
        blob
      });
      putReq.onsuccess = () => done();
      putReq.onerror = () => fail(putReq.error || new Error("Could not save attachment."));
    });

    const attachments = await listAttachmentsForJob(uid, jobKey);
    await updateAttachmentMetadata(uid, jobKey, attachments.length);
    await addActivityLog(uid, "attachment_added", { jobKey }, {
      fileName: String(fileMeta?.name || "file"),
      size: Number(fileMeta?.size) || 0
    });
    return attachmentId;
  }

  async function deleteAttachmentForJob(uid, jobKey, attachmentId) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");

    await withStore("attachments", "readwrite", (store, done, fail) => {
      const getReq = store.get(attachmentId);
      getReq.onsuccess = () => {
        const row = getReq.result;
        if (!row || row.profileId !== uid || row.jobKey !== jobKey) {
          fail(new Error("Attachment not found."));
          return;
        }
        const targetKey = attachmentDedupeKey(row);
        const index = store.index("by_profile_job");
        const cursorReq = index.openCursor(IDBKeyRange.only([uid, jobKey]));
        cursorReq.onsuccess = event => {
          const cursor = event.target.result;
          if (!cursor) {
            done();
            return;
          }
          const currentRow = cursor.value;
          if (attachmentDedupeKey(currentRow) !== targetKey) {
            cursor.continue();
            return;
          }
          const delReq = cursor.delete();
          delReq.onsuccess = () => cursor.continue();
          delReq.onerror = () => fail(delReq.error || new Error("Could not delete attachment."));
        };
        cursorReq.onerror = () => fail(cursorReq.error || new Error("Could not iterate attachments."));
      };
      getReq.onerror = () => fail(getReq.error || new Error("Could not load attachment."));
    });

    const attachments = await listAttachmentsForJob(uid, jobKey);
    await updateAttachmentMetadata(uid, jobKey, attachments.length);
    await addActivityLog(uid, "attachment_deleted", { jobKey }, { attachmentId });
  }

  async function getAttachmentBlob(uid, jobKey, attachmentId) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");

    return withStore("attachments", "readonly", (store, done, fail) => {
      const req = store.get(attachmentId);
      req.onsuccess = () => {
        const row = req.result;
        if (!row || row.profileId !== uid || row.jobKey !== jobKey) {
          fail(new Error("Attachment not found."));
          return;
        }
        done(row.blob || null);
      };
      req.onerror = () => fail(req.error || new Error("Could not read attachment."));
    });
  }

  function normalizeImportedAttachmentRow(uid, row, normalizeIsoOrNow, toPlainObject, deserializeAttachmentBlob) {
    const source = toPlainObject(row);
    const jobKey = String(source.jobKey || "").trim();
    if (!jobKey) return null;
    const attachmentId = String(source.id || "").trim() || toAttachmentId(source.name);
    return {
      pk: `${uid}::${jobKey}::${attachmentId}`,
      id: attachmentId,
      profileId: uid,
      jobKey,
      name: String(source.name || "file"),
      type: String(source.type || "application/octet-stream"),
      size: Number(source.size) || 0,
      createdAt: normalizeIsoOrNow(source.createdAt, nowIso()),
      blob: deserializeAttachmentBlob(source)
    };
  }

  function toAttachmentFingerprint(row) {
    return [
      String(row?.profileId || ""),
      String(row?.jobKey || ""),
      String(row?.name || ""),
      String(row?.type || ""),
      String(Number(row?.size) || 0),
      String(row?.createdAt || "")
    ].join("|");
  }

  return {
    toAttachmentId,
    listAttachmentsForJob,
    addAttachmentForJob,
    deleteAttachmentForJob,
    getAttachmentBlob,
    normalizeImportedAttachmentRow,
    toAttachmentFingerprint
  };
}
