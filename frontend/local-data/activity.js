import { stripAttachmentPk } from "./indexeddb-adapter.js";

export function createActivityDomain(deps) {
  const {
    withStore,
    ensureCurrentUser,
    hashFNV1a,
    nowIso
  } = deps;

  function toActivityId(uid, type, jobKey = "") {
    const seed = `${uid}|${type}|${jobKey}|${Date.now()}|${Math.random().toString(36).slice(2)}`;
    return `log_${hashFNV1a(seed)}`;
  }

  async function addActivityLog(uid, type, job, details = {}) {
    const jobKey = String(job?.jobKey || details.jobKey || "");
    const safeDetails = details && typeof details === "object" ? details : {};
    const entry = {
      id: toActivityId(uid, type, jobKey),
      profileId: uid,
      type: String(type || "event"),
      jobKey,
      title: String(job?.title || safeDetails.title || ""),
      company: String(job?.company || safeDetails.company || ""),
      createdAt: nowIso(),
      details: safeDetails
    };
    await withStore("activity_log", "readwrite", (store, done, fail) => {
      const req = store.put(entry);
      req.onsuccess = () => done();
      req.onerror = () => fail(req.error || new Error("Could not write activity log."));
    });
  }

  async function listActivityForUser(uid, limit = 300) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");
    const safeLimit = Math.max(1, Math.min(2000, Number(limit) || 300));
    return withStore("activity_log", "readonly", (store, done, fail) => {
      const index = store.index("by_profile");
      const request = index.getAll(IDBKeyRange.only(uid));
      request.onsuccess = () => {
        const rows = (request.result || [])
          .map(stripAttachmentPk)
          .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")))
          .slice(0, safeLimit);
        done(rows);
      };
      request.onerror = () => fail(request.error || new Error("Could not list activity log."));
    });
  }

  async function listAllActivityForProfile(uid) {
    return withStore("activity_log", "readonly", (store, done, fail) => {
      const index = store.index("by_profile");
      const request = index.getAll(IDBKeyRange.only(uid));
      request.onsuccess = () => done((request.result || []).map(stripAttachmentPk));
      request.onerror = () => fail(request.error || new Error("Could not list activity log entries."));
    });
  }

  return {
    toActivityId,
    addActivityLog,
    listActivityForUser,
    listAllActivityForProfile
  };
}
