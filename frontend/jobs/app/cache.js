export function isJobsCacheStale(savedAt, ttlMs) {
  if (!savedAt) return true;
  return (Date.now() - Number(savedAt)) > Math.max(0, Number(ttlMs) || 0);
}

export function buildSeenRowKey(profileId, jobKey) {
  return `${String(profileId || "").trim()}::${String(jobKey || "").trim()}`;
}

export function openJobsCacheDb({ indexedDb = indexedDB, dbName, dbVersion, cacheStore, seenStore } = {}) {
  if (typeof indexedDb === "undefined") return Promise.resolve(null);
  return new Promise((resolve, reject) => {
    const request = indexedDb.open(dbName, dbVersion);

    request.onupgradeneeded = event => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains(cacheStore)) {
        db.createObjectStore(cacheStore, { keyPath: "id" });
      }
      if (!db.objectStoreNames.contains(seenStore)) {
        const store = db.createObjectStore(seenStore, { keyPath: "pk" });
        store.createIndex("profileId", "profileId", { unique: false });
      }
    };

    request.onsuccess = () => resolve(request.result);
    request.onerror = () => reject(request.error || new Error("Could not open jobs cache database."));
  });
}

export async function readJobsCache(options = {}) {
  const { openDb, cacheStore, cacheKey } = options;
  try {
    const db = await openDb();
    if (!db) return null;

    return await new Promise((resolve, reject) => {
      const tx = db.transaction(cacheStore, "readonly");
      const store = tx.objectStore(cacheStore);
      const request = store.get(cacheKey);

      request.onsuccess = () => {
        const row = request.result;
        resolve({
          jobs: Array.isArray(row?.jobs) ? row.jobs : null,
          savedAt: Number(row?.savedAt) || 0
        });
      };
      request.onerror = () => reject(request.error || new Error("Could not read jobs cache."));
    });
  } catch {
    return null;
  }
}

export async function writeJobsCache(jobs, options = {}) {
  if (!Array.isArray(jobs) || jobs.length === 0) return;
  const { openDb, cacheStore, cacheKey, now = Date.now() } = options;

  try {
    const db = await openDb();
    if (!db) return;

    await new Promise((resolve, reject) => {
      const tx = db.transaction(cacheStore, "readwrite");
      const store = tx.objectStore(cacheStore);
      const request = store.put({
        id: cacheKey,
        savedAt: now,
        jobs
      });

      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error || new Error("Could not write jobs cache."));
    });
  } catch {
    // Ignore cache write failures.
  }
}

export async function loadSeenJobKeys(profileId, options = {}) {
  const safeProfileId = String(profileId || "").trim();
  if (!safeProfileId) return new Set();
  const { openDb, seenStore } = options;

  try {
    const db = await openDb();
    if (!db || !db.objectStoreNames.contains(seenStore)) return new Set();

    return await new Promise((resolve, reject) => {
      const tx = db.transaction(seenStore, "readonly");
      const store = tx.objectStore(seenStore);
      const index = store.index("profileId");
      const request = index.getAll(safeProfileId);

      request.onsuccess = () => {
        const rows = Array.isArray(request.result) ? request.result : [];
        resolve(new Set(rows.map(row => String(row?.jobKey || "").trim()).filter(Boolean)));
      };
      request.onerror = () => reject(request.error || new Error("Could not read seen jobs."));
    });
  } catch {
    return new Set();
  }
}

export async function markSeenJob(profileId, jobKey, options = {}) {
  const safeProfileId = String(profileId || "").trim();
  const safeJobKey = String(jobKey || "").trim();
  if (!safeProfileId || !safeJobKey) return;
  const {
    openDb,
    seenStore,
    seenAt = Date.now(),
    buildKey = buildSeenRowKey
  } = options;

  try {
    const db = await openDb();
    if (!db || !db.objectStoreNames.contains(seenStore)) return;

    await new Promise((resolve, reject) => {
      const tx = db.transaction(seenStore, "readwrite");
      const store = tx.objectStore(seenStore);
      const request = store.put({
        pk: buildKey(safeProfileId, safeJobKey),
        profileId: safeProfileId,
        jobKey: safeJobKey,
        seenAt: Number(seenAt) || Date.now()
      });

      request.onsuccess = () => resolve();
      request.onerror = () => reject(request.error || new Error("Could not persist seen job."));
    });
  } catch {
    // Ignore seen-state write failures.
  }
}

export async function markSeenJobsBulk(profileId, jobKeys, options = {}) {
  const safeProfileId = String(profileId || "").trim();
  if (!safeProfileId || !Array.isArray(jobKeys) || jobKeys.length === 0) return;
  const {
    openDb,
    seenStore,
    seenAt = Date.now(),
    buildKey = buildSeenRowKey
  } = options;

  const uniqueKeys = Array.from(new Set(jobKeys.map(key => String(key || "").trim()).filter(Boolean)));
  if (uniqueKeys.length === 0) return;

  try {
    const db = await openDb();
    if (!db || !db.objectStoreNames.contains(seenStore)) return;

    await new Promise((resolve, reject) => {
      const tx = db.transaction(seenStore, "readwrite");
      const store = tx.objectStore(seenStore);
      uniqueKeys.forEach(key => {
        store.put({
          pk: buildKey(safeProfileId, key),
          profileId: safeProfileId,
          jobKey: key,
          seenAt: Number(seenAt) || Date.now()
        });
      });
      tx.oncomplete = () => resolve();
      tx.onerror = () => reject(tx.error || new Error("Could not persist seen jobs."));
      tx.onabort = () => reject(tx.error || new Error("Seen jobs write transaction aborted."));
    });
  } catch {
    // Ignore seen-state write failures.
  }
}
