(function () {
  const DB_NAME = "baluffo_jobs_local";
  const DB_VERSION = 1;
  const SESSION_KEY = "baluffo_current_profile_id";
  const PROFILE_KEY = "baluffo_profiles";

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
    const canonicalLink = sanitizeJobUrl(job.jobLink || "").toLowerCase();
    const fallback = [
      job.title || "",
      job.company || "",
      job.city || "",
      job.country || ""
    ].join("|").toLowerCase();
    const seed = canonicalLink || fallback;
    return `job_${hashFNV1a(seed)}`;
  }

  function buildAttachmentPath(uid, jobKey, filename) {
    const base = `users/${uid}/saved_jobs/${jobKey}`;
    return filename ? `${base}/${filename}` : `${base}/`;
  }

  function nowIso() {
    return new Date().toISOString();
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

  async function listAttachmentMetadata(uid) {
    return withStore("attachments", "readonly", (store, done, fail) => {
      const index = store.index("by_profile_job");
      const range = IDBKeyRange.bound([uid, ""], [uid, "\uffff"]);
      const request = index.getAll(range);
      request.onsuccess = () => done(request.result || []);
      request.onerror = () => fail(request.error || new Error("Could not list attachment metadata."));
    });
  }

  function stripPk(row) {
    if (!row) return row;
    const copy = { ...row };
    delete copy.pk;
    return copy;
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

  async function saveJobForUser(uid, job) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");

    const jobKey = generateJobKey(job);
    const pk = `${uid}::${jobKey}`;
    const currentIso = nowIso();

    await withStore("saved_jobs", "readwrite", (store, done, fail) => {
      const getReq = store.get(pk);
      getReq.onsuccess = () => {
        const existing = getReq.result || null;
        const payload = {
          pk,
          profileId: uid,
          jobKey,
          title: job.title || "",
          company: job.company || "",
          companyType: job.companyType || "Tech",
          city: job.city || "",
          country: job.country || "",
          workType: job.workType || "Onsite",
          contractType: job.contractType || "Unknown",
          jobLink: sanitizeJobUrl(job.jobLink || ""),
          applicationStatus: normalizeApplicationStatus(existing?.applicationStatus),
          notes: existing?.notes || "",
          attachmentsCount: Number.isFinite(existing?.attachmentsCount) ? existing.attachmentsCount : 0,
          savedAt: existing?.savedAt || currentIso,
          updatedAt: currentIso
        };
        const putReq = store.put(payload);
        putReq.onsuccess = () => done(jobKey);
        putReq.onerror = () => fail(putReq.error || new Error("Could not save job."));
      };
      getReq.onerror = () => fail(getReq.error || new Error("Could not read existing saved job."));
    });

    await notifySavedJobsChanged(uid);
    return jobKey;
  }

  async function removeSavedJobForUser(uid, jobKey) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");
    const pk = `${uid}::${jobKey}`;

    await withStore("saved_jobs", "readwrite", (store, done, fail) => {
      const req = store.delete(pk);
      req.onsuccess = () => done();
      req.onerror = () => fail(req.error || new Error("Could not remove saved job."));
    });

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

  async function updateApplicationStatus(uid, jobKey, status) {
    const nextStatus = normalizeApplicationStatus(status);
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
          applicationStatus: nextStatus,
          updatedAt: nowIso()
        };
        const putReq = store.put(next);
        putReq.onsuccess = () => done();
        putReq.onerror = () => fail(putReq.error || new Error("Could not update application status."));
      };
      getReq.onerror = () => fail(getReq.error || new Error("Could not load saved job."));
    });

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

  async function exportProfileData(uid) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");

    const profiles = readProfiles();
    const profile = profiles.find(p => p.id === uid);
    const savedJobs = await listSavedJobs(uid);
    const attachments = await listAttachmentMetadata(uid);

    return {
      version: 1,
      exportedAt: nowIso(),
      profile: profile || { id: uid, name: user.displayName || uid, email: user.email || "" },
      savedJobs,
      attachments
    };
  }

  async function importProfileData(uid, payload) {
    const user = ensureCurrentUser();
    if (uid !== user.uid) throw new Error("User mismatch.");
    if (!payload || typeof payload !== "object") {
      throw new Error("Invalid import payload.");
    }

    const savedJobs = Array.isArray(payload.savedJobs) ? payload.savedJobs : [];

    await withStore("saved_jobs", "readwrite", (store, done, fail) => {
      try {
        for (const row of savedJobs) {
          const jobKey = row.jobKey || generateJobKey(row);
          const pk = `${uid}::${jobKey}`;
          store.put({
            pk,
            profileId: uid,
            jobKey,
            title: row.title || "",
            company: row.company || "",
            companyType: row.companyType || "Tech",
            city: row.city || "",
            country: row.country || "",
            workType: row.workType || "Onsite",
            contractType: row.contractType || "Unknown",
            jobLink: sanitizeJobUrl(row.jobLink || ""),
            applicationStatus: normalizeApplicationStatus(row.applicationStatus),
            notes: row.notes || "",
            attachmentsCount: Math.max(0, Number(row.attachmentsCount) || 0),
            savedAt: row.savedAt || nowIso(),
            updatedAt: nowIso()
          });
        }
        done();
      } catch (err) {
        fail(err);
      }
    });

    await notifySavedJobsChanged(uid);
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
    updateAttachmentMetadata,
    exportProfileData,
    importProfileData
  };
})();
