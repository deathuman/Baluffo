(function () {
  const DB_NAME = "baluffo_jobs_local";
  const DB_VERSION = 2;
  const BACKUP_SCHEMA_VERSION = 2;
  const SESSION_KEY = "baluffo_current_profile_id";
  const PROFILE_KEY = "baluffo_profiles";
  const ADMIN_PIN = "1234";

  const APPLICATION_STATUSES = [
    "bookmark",
    "applied",
    "interview_1",
    "interview_2",
    "offer",
    "rejected"
  ];

  function normalizeApplicationStatus(status) {
    const raw = String(status || "").toLowerCase().trim();
    if (raw === "bookmarked") return "bookmark";
    if (APPLICATION_STATUSES.includes(raw)) return raw;
    return "bookmark";
  }

  function canTransitionPhase(currentStatus, nextStatus) {
    const current = normalizeApplicationStatus(currentStatus);
    const next = normalizeApplicationStatus(nextStatus);
    if (current === next) return true;
    if (current === "rejected") return false;
    if (next === "rejected") return true;

    const currentIdx = APPLICATION_STATUSES.indexOf(current);
    const nextIdx = APPLICATION_STATUSES.indexOf(next);
    if (currentIdx < 0 || nextIdx < 0) return false;
    return nextIdx === currentIdx + 1;
  }

  const listeners = new Set();
  let currentUser = null;
  let dbPromise = null;
  const hasIndexedDb = typeof window.indexedDB !== "undefined";

  function getDb() {
    if (!hasIndexedDb) {
      return Promise.reject(new Error("IndexedDB not supported in this browser."));
    }
    if (dbPromise) return dbPromise;

    dbPromise = new Promise((resolve, reject) => {
      const request = indexedDB.open(DB_NAME, DB_VERSION);

      request.onupgradeneeded = event => {
        const db = event.target.result;

        if (!db.objectStoreNames.contains("saved_jobs")) {
          const store = db.createObjectStore("saved_jobs", { keyPath: "pk" });
          store.createIndex("by_profile", "profileId", { unique: false });
        }

        if (!db.objectStoreNames.contains("attachments")) {
          const store = db.createObjectStore("attachments", { keyPath: "id" });
          store.createIndex("by_profile_job", ["profileId", "jobKey"], { unique: false });
        }

        if (!db.objectStoreNames.contains("activity_log")) {
          const store = db.createObjectStore("activity_log", { keyPath: "id" });
          store.createIndex("by_profile", "profileId", { unique: false });
        }
      };

      request.onsuccess = () => resolve(request.result);
      request.onerror = () => reject(request.error || new Error("IndexedDB open failed."));
    });

    return dbPromise;
  }

  function makeUser(profile) {
    return {
      uid: profile.id,
      displayName: profile.name,
      email: profile.email || ""
    };
  }

  function readProfiles() {
    try {
      const raw = localStorage.getItem(PROFILE_KEY);
      const arr = raw ? JSON.parse(raw) : [];
      return Array.isArray(arr) ? arr : [];
    } catch {
      return [];
    }
  }

  function writeProfiles(profiles) {
    localStorage.setItem(PROFILE_KEY, JSON.stringify(profiles));
  }

  function verifyAdminPin(pin) {
    return String(pin || "") === ADMIN_PIN;
  }

  function ensureAdmin(pin) {
    if (!verifyAdminPin(pin)) throw new Error("Invalid admin PIN.");
  }

  function getStoredSessionUser() {
    const profileId = localStorage.getItem(SESSION_KEY);
    if (!profileId) return null;
    const profile = readProfiles().find(p => p.id === profileId);
    return profile ? makeUser(profile) : null;
  }

  function notifyAuthChanged() {
    listeners.forEach(l => {
      if (l.type === "auth") l.callback(currentUser);
    });
  }

  async function notifySavedJobsChanged(uid) {
    const rows = await listSavedJobs(uid);
    listeners.forEach(l => {
      if (l.type === "saved" && l.uid === uid) {
        l.callback(rows);
      }
    });
  }

  function hashFNV1a(input) {
    let hash = 2166136261;
    for (let i = 0; i < input.length; i++) {
      hash ^= input.charCodeAt(i);
      hash += (hash << 1) + (hash << 4) + (hash << 7) + (hash << 8) + (hash << 24);
    }
    return (hash >>> 0).toString(16).padStart(8, "0");
  }

  function sanitizeJobUrl(url) {
    if (!url) return "";
    try {
      const parsed = new URL(url);
      return parsed.protocol === "https:" || parsed.protocol === "http:" ? parsed.href : "";
    } catch {
      return "";
    }
  }

  function generateJobKey(job) {
    const explicit = String(job?.jobKey || "").trim();
    if (/^job_[a-f0-9]{8}$/i.test(explicit)) {
      return explicit.toLowerCase();
    }
    const salt = String(job?.keySalt || "").trim().toLowerCase();
    const canonicalLink = sanitizeJobUrl(job.jobLink || "").toLowerCase();
    const fallback = [
      job.title || "",
      job.company || "",
      job.city || "",
      job.country || "",
    ].join("|").toLowerCase();
    const seedBase = canonicalLink || fallback;
    const seed = salt ? `${seedBase}|${salt}` : seedBase;
    return `job_${hashFNV1a(seed)}`;
  }

  function buildAttachmentPath(uid, jobKey, filename) {
    const base = `users/${uid}/saved_jobs/${jobKey}`;
    return filename ? `${base}/${filename}` : `${base}/`;
  }

  function nowIso() {
    return new Date().toISOString();
  }

  function normalizeSectorValue(sector, companyType = "") {
    const raw = String(sector || "").trim();
    const lower = raw.toLowerCase();
    if (lower === "game" || lower === "game company" || lower === "gaming") return "Game";
    if (lower === "tech" || lower === "tech company" || lower === "technology") return "Tech";
    const ct = String(companyType || "").trim().toLowerCase();
    if (ct === "game" || ct === "game company") return "Game";
    if (ct === "tech" || ct === "tech company") return "Tech";
    return raw || "Tech";
  }

  function normalizeCustomSourceLabel(label) {
    const text = String(label || "").trim();
    return text || "Personal";
  }

  function ensureCurrentUser() {
    if (!currentUser) throw new Error("Not signed in.");
    return currentUser;
  }

  async function withStore(storeName, mode, fn) {
    const db = await getDb();
    return new Promise((resolve, reject) => {
      const tx = db.transaction(storeName, mode);
      const store = tx.objectStore(storeName);

      let settled = false;
      function done(value) {
        if (settled) return;
        settled = true;
        resolve(value);
      }
      function fail(err) {
        if (settled) return;
        settled = true;
        reject(err);
      }

      try {
        fn(store, done, fail);
      } catch (err) {
        fail(err);
      }

      tx.onerror = () => fail(tx.error || new Error(`IndexedDB transaction failed: ${storeName}`));
      tx.onabort = () => fail(tx.error || new Error(`IndexedDB transaction aborted: ${storeName}`));
    });
  }

  async function listSavedJobs(uid) {
    return withStore("saved_jobs", "readonly", (store, done, fail) => {
      const index = store.index("by_profile");
      const request = index.getAll(IDBKeyRange.only(uid));
      request.onsuccess = () => {
        const rows = (request.result || []).map(stripPk);
        rows.sort((a, b) => (b.savedAt || "").localeCompare(a.savedAt || ""));
        done(rows);
      };
      request.onerror = () => fail(request.error || new Error("Could not list saved jobs."));
    });
  }

  async function listAllSavedJobs() {
    return withStore("saved_jobs", "readonly", (store, done, fail) => {
      const request = store.getAll();
      request.onsuccess = () => done((request.result || []).map(stripPk));
      request.onerror = () => fail(request.error || new Error("Could not list all saved jobs."));
    });
  }

  async function listAllAttachments() {
    return withStore("attachments", "readonly", (store, done, fail) => {
      const request = store.getAll();
      request.onsuccess = () => done((request.result || []).map(stripAttachmentPk));
      request.onerror = () => fail(request.error || new Error("Could not list all attachments."));
    });
  }

  async function listAttachmentMetadata(uid) {
    return withStore("attachments", "readonly", (store, done, fail) => {
      const index = store.index("by_profile_job");
      const range = IDBKeyRange.bound([uid, ""], [uid, "\uffff"]);
      const request = index.getAll(range);
      request.onsuccess = () => done(dedupeAttachmentRows(request.result || []));
      request.onerror = () => fail(request.error || new Error("Could not list attachment metadata."));
    });
  }

  function stripPk(row) {
    if (!row) return row;
    const copy = { ...row };
    delete copy.pk;
    return copy;
  }

  function stripAttachmentPk(row) {
    if (!row) return row;
    const copy = { ...row };
    delete copy.pk;
    return copy;
  }

  function attachmentDedupeKey(row) {
    return [
      String(row?.profileId || ""),
      String(row?.jobKey || ""),
      String(row?.name || "").trim().toLowerCase(),
      String(row?.type || "").trim().toLowerCase(),
      String(Number(row?.size) || 0)
    ].join("|");
  }

  function dedupeAttachmentRows(rows) {
    const byKey = new Map();
    (Array.isArray(rows) ? rows : []).forEach(row => {
      const key = attachmentDedupeKey(row);
      if (!key) return;
      const current = byKey.get(key);
      if (!current) {
        byKey.set(key, row);
        return;
      }
      const currentHasBlob = current?.blob instanceof Blob;
      const nextHasBlob = row?.blob instanceof Blob;
      if (nextHasBlob && !currentHasBlob) {
        byKey.set(key, row);
        return;
      }
      if (nextHasBlob === currentHasBlob) {
        const currentDate = new Date(String(current?.createdAt || "")).getTime();
        const nextDate = new Date(String(row?.createdAt || "")).getTime();
        if (Number.isFinite(nextDate) && (!Number.isFinite(currentDate) || nextDate > currentDate)) {
          byKey.set(key, row);
        }
      }
    });
    return Array.from(byKey.values());
  }

  function toAttachmentId(fileName) {
    const base = `${fileName || "file"}|${Date.now()}|${Math.random().toString(36).slice(2)}`;
    return `att_${hashFNV1a(base)}`;
  }

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

  function chooseProfile(existingProfiles, input) {
    const trimmed = (input || "").trim();
    if (!trimmed) return null;

    const existing = existingProfiles.find(p => p.name.toLowerCase() === trimmed.toLowerCase());
    if (existing) return existing;

    const slug = trimmed.toLowerCase().replace(/[^a-z0-9]+/g, "_").replace(/^_+|_+$/g, "");
    const id = `local_${slug || hashFNV1a(trimmed)}`;
    return { id, name: trimmed, email: "" };
  }

  async function signIn() {
    const profiles = readProfiles();
    const label = profiles.length
      ? `Enter profile name to sign in (existing or new).\nExisting: ${profiles.map(p => p.name).join(", ")}`
      : "Enter a profile name to create local sign-in:";

    const input = window.prompt(label, profiles[0]?.name || "");
    const profile = chooseProfile(profiles, input);
    if (!profile) throw new Error("Sign-in cancelled.");

    const alreadyExists = profiles.some(p => p.id === profile.id);
    if (!alreadyExists) {
      profiles.push(profile);
      writeProfiles(profiles);
    }

    localStorage.setItem(SESSION_KEY, profile.id);
    currentUser = makeUser(profile);
    notifyAuthChanged();
    return { user: currentUser };
  }

  async function signOut() {
    localStorage.removeItem(SESSION_KEY);
    currentUser = null;
    notifyAuthChanged();
  }

  function onAuthStateChanged(callback) {
    const entry = { type: "auth", callback };
    listeners.add(entry);
    callback(currentUser);
    return () => listeners.delete(entry);
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

  function subscribeSavedJobs(uid, onChange, onError) {
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
            ...(current.phaseTimestamps && typeof current.phaseTimestamps === "object" ? current.phaseTimestamps : {}),
          },
          // Keep row ordering stable when changing phase.
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

  function normalizeIsoOrNow(value, fallback = "") {
    const text = String(value || "").trim();
    if (!text) return fallback;
    const parsed = new Date(text);
    return Number.isNaN(parsed.getTime()) ? fallback : parsed.toISOString();
  }

  function toPlainObject(value) {
    if (!value || typeof value !== "object" || Array.isArray(value)) return {};
    return value;
  }

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

  function isClearlyLowerQualityImported(existingRow, importedRow) {
    const hasExistingRequired = Boolean(String(existingRow?.title || "").trim()) && Boolean(String(existingRow?.company || "").trim());
    const hasImportedRequired = Boolean(String(importedRow?.title || "").trim()) && Boolean(String(importedRow?.company || "").trim());
    return hasExistingRequired && !hasImportedRequired;
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

  function areSavedRowsEquivalent(a, b) {
    if (!a || !b) return false;
    const fields = [
      "jobKey", "title", "company", "sector", "companyType", "city", "country", "workType",
      "contractType", "jobLink", "profession", "isCustom", "customSourceLabel", "reminderAt",
      "contactedAt", "updatedBy", "applicationStatus", "notes", "attachmentsCount", "savedAt"
    ];
    for (const field of fields) {
      if (String(a[field] ?? "") !== String(b[field] ?? "")) return false;
    }
    return JSON.stringify(toPlainObject(a.phaseTimestamps)) === JSON.stringify(toPlainObject(b.phaseTimestamps));
  }

  function normalizeImportedAttachmentRow(uid, row) {
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

  function parseBackupPayload(payload) {
    if (!payload || typeof payload !== "object") {
      throw new Error("Invalid import payload.");
    }
    const warnings = [];
    const schemaVersion = Number(payload.schemaVersion || payload.version || 1) || 1;
    const savedJobs = Array.isArray(payload.savedJobs) ? payload.savedJobs : [];
    const attachments = Array.isArray(payload.attachments) ? payload.attachments : [];
    const activityLog = Array.isArray(payload.activityLog) ? payload.activityLog : [];
    return {
      schemaVersion,
      savedJobs,
      attachments,
      activityLog,
      warnings
    };
  }

  function buildProfileBackupPayload(uid, context) {
    const includeFiles = Boolean(context.includeFiles);
    const exportedAt = nowIso();
    const savedJobs = context.savedJobs.map(row => normalizeSavedJobRecord(uid, row));
    const customJobs = savedJobs.filter(row => row.isCustom).length;
    const attachmentsCount = context.attachments.length;
    const historyEvents = context.activityLog.length;
    return {
      version: BACKUP_SCHEMA_VERSION,
      schemaVersion: BACKUP_SCHEMA_VERSION,
      exportedAt,
      includesFiles: includeFiles,
      counts: {
        savedJobs: savedJobs.length,
        customJobs,
        historyEvents,
        attachments: attachmentsCount
      },
      profile: context.profile,
      savedJobs,
      attachments: context.attachments,
      activityLog: context.activityLog
    };
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
      ? await Promise.all(attachments.map(serializeAttachmentWithBlob))
      : attachments.map(stripAttachmentBlob);
    const activityLog = await listAllActivityForProfile(uid);

    return buildProfileBackupPayload(uid, {
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
        warnings.push(`Skipped malformed saved job (missing title/company).`);
        continue;
      }
      const normalized = normalizeSavedJobRecord(uid, source);
      const key = String(normalized.jobKey || "");
      if (!key) {
        summary.skippedInvalid += 1;
        warnings.push(`Skipped malformed saved job (missing jobKey).`);
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
      .map(row => normalizeImportedAttachmentRow(uid, row))
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
      if (existingFingerprints.has(fingerprint)) {
        continue;
      }
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

  function utf8ByteLength(input) {
    const text = String(input || "");
    if (typeof TextEncoder !== "undefined") {
      return new TextEncoder().encode(text).length;
    }
    return unescape(encodeURIComponent(text)).length;
  }

  function getAttachmentByteSize(row) {
    const meta = Number(row?.size) || 0;
    if (meta > 0) return meta;
    const blobSize = Number(row?.blob?.size) || 0;
    return blobSize > 0 ? blobSize : 0;
  }

  function ensureAdminUserRow(map, uid, fallbackName = "Unknown Profile") {
    if (!map.has(uid)) {
      map.set(uid, {
        uid,
        name: fallbackName,
        email: "",
        savedJobsCount: 0,
        notesBytes: 0,
        attachmentsCount: 0,
        attachmentsBytes: 0,
        totalBytes: 0
      });
    }
    return map.get(uid);
  }

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

    if (currentUser?.uid === targetUid) {
      localStorage.removeItem(SESSION_KEY);
      currentUser = null;
      notifyAuthChanged();
    } else {
      await notifySavedJobsChanged(targetUid).catch(() => {
        // No active listeners for this uid is expected.
      });
    }
  }

  function stripAttachmentBlob(row) {
    const copy = stripAttachmentPk(row);
    delete copy.blob;
    return copy;
  }

  async function serializeAttachmentWithBlob(row) {
    const base = stripAttachmentBlob(row);
    if (!row.blob) return base;
    const dataUrl = await blobToDataUrl(row.blob);
    return { ...base, blobDataUrl: dataUrl };
  }

  function blobToDataUrl(blob) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = () => resolve(String(reader.result || ""));
      reader.onerror = () => reject(reader.error || new Error("Could not serialize blob."));
      reader.readAsDataURL(blob);
    });
  }

  function deserializeAttachmentBlob(row) {
    if (!row?.blobDataUrl) return null;
    return dataUrlToBlob(row.blobDataUrl);
  }

  function dataUrlToBlob(dataUrl) {
    const parts = String(dataUrl).split(",");
    if (parts.length !== 2) return null;
    const header = parts[0];
    const body = parts[1];
    const mimeMatch = header.match(/data:(.*?);base64/);
    const mime = mimeMatch ? mimeMatch[1] : "application/octet-stream";
    const bytes = atob(body);
    const arr = new Uint8Array(bytes.length);
    for (let i = 0; i < bytes.length; i++) arr[i] = bytes.charCodeAt(i);
    return new Blob([arr], { type: mime });
  }

  window.addEventListener("storage", event => {
    if (event.key === SESSION_KEY) {
      currentUser = getStoredSessionUser();
      notifyAuthChanged();
    }
  });

  currentUser = getStoredSessionUser();

  window.JobAppLocalData = {
    APPLICATION_STATUSES,
    isReady: () => hasIndexedDb,
    getCurrentUser: () => currentUser,
    onAuthStateChanged,
    signIn,
    signOut,
    saveJobForUser,
    removeSavedJobForUser,
    getSavedJobKeys,
    subscribeSavedJobs,
    generateJobKey,
    buildAttachmentPath,
    updateApplicationStatus,
    canTransitionPhase,
    updateAttachmentMetadata,
    updateJobNotes,
    listAttachmentsForJob,
    addAttachmentForJob,
    deleteAttachmentForJob,
    getAttachmentBlob,
    listActivityForUser,
    exportProfileData,
    importProfileData,
    verifyAdminPin,
    getAdminOverview,
    wipeAccountAdmin
  };
})();
