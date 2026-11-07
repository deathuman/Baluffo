// Single source of truth for the Jobs IndexedDB contract. runtime.js and the
// warm seeder both consume these, so a schema bump cannot land in only one of
// the app, the measurement harness, or their repair paths.
export const JOBS_CACHE_DB = "baluffo_jobs_cache";
export const JOBS_CACHE_DB_VERSION = 2;
export const JOBS_CACHE_STORE = "jobs_feed";
export const JOBS_SEEN_STORE = "jobs_seen";
export const JOBS_CACHE_KEY = "latest";

export function isJobsCacheStale(savedAt, ttlMs) {
  if (!savedAt) return true;
  return (Date.now() - Number(savedAt)) > Math.max(0, Number(ttlMs) || 0);
}

export function buildSeenRowKey(profileId, jobKey) {
  return `${String(profileId || "").trim()}::${String(jobKey || "").trim()}`;
}

const PROFILE_INDEX = "profileId";

async function hasJobsCacheSchema(db, { cacheStore, seenStore }) {
  if (!db.objectStoreNames.contains(cacheStore) || !db.objectStoreNames.contains(seenStore)) {
    return false;
  }
  const tx = db.transaction(seenStore, "readonly");
  const hasIndex = tx.objectStore(seenStore).indexNames.contains(PROFILE_INDEX);
  await new Promise(resolve => {
    tx.oncomplete = resolve;
    tx.onerror = resolve;
    tx.onabort = resolve;
  });
  return hasIndex;
}

function deleteJobsCacheDb(indexedDb, dbName) {
  return new Promise((resolve, reject) => {
    const request = indexedDb.deleteDatabase(dbName);
    request.onsuccess = () => resolve();
    request.onerror = () =>
      reject(request.error || new Error("Could not reset jobs cache database."));
    request.onblocked = () =>
      reject(new Error("Could not reset jobs cache database: blocked by another connection."));
  });
}

async function readSeenRowsForRepair(db, { seenStore }) {
  if (!db.objectStoreNames.contains(seenStore)) return [];
  try {
    return await new Promise((resolve, reject) => {
      const tx = db.transaction(seenStore, "readonly");
      let rows = [];
      const request = tx.objectStore(seenStore).getAll();
      request.onsuccess = () => {
        rows = Array.isArray(request.result) ? request.result : [];
      };
      // Resolve on completion, not on the request: the connection must have no
      // pending transaction left when the repair closes it to delete the DB,
      // or deleteDatabase fires `blocked` against our own read.
      tx.oncomplete = () => resolve(rows);
      tx.onerror = () => reject(tx.error || new Error("Could not read seen jobs."));
      tx.onabort = () => reject(tx.error || new Error("Could not read seen jobs."));
    });
  } catch {
    // A store we cannot read is still repairable; only its rows are lost.
    return [];
  }
}

function restoreSeenRows(db, { seenStore, rows }) {
  if (rows.length === 0) return Promise.resolve();
  return new Promise((resolve, reject) => {
    const tx = db.transaction(seenStore, "readwrite");
    const store = tx.objectStore(seenStore);
    for (const row of rows) {
      const profileId = String(row?.profileId || "").trim();
      const jobKey = String(row?.jobKey || "").trim();
      // Rows written by markSeenJob carry pk; rebuild it for any legacy row
      // that predates the key helper so the recreated store keeps it addressable.
      if (!profileId || !jobKey) continue;
      store.put({
        ...row,
        pk: String(row?.pk || "").trim() || buildSeenRowKey(profileId, jobKey),
        profileId,
        jobKey
      });
    }
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error || new Error("Could not restore seen jobs."));
    tx.onabort = () => reject(tx.error || new Error("Could not restore seen jobs."));
  });
}

function openJobsCacheDbHandle({ indexedDb, dbName, dbVersion, cacheStore, seenStore }) {
  return new Promise((resolve, reject) => {
    const request = indexedDb.open(dbName, dbVersion);
    let settled = false;

    const fail = error => {
      if (settled) return;
      settled = true;
      reject(error);
    };

    request.onupgradeneeded = event => {
      const db = event.target.result;
      if (!db.objectStoreNames.contains(cacheStore)) {
        db.createObjectStore(cacheStore, { keyPath: "id" });
      }
      const seen = db.objectStoreNames.contains(seenStore)
        ? event.target.transaction.objectStore(seenStore)
        : db.createObjectStore(seenStore, { keyPath: "pk" });
      if (!seen.indexNames.contains(PROFILE_INDEX)) {
        seen.createIndex(PROFILE_INDEX, PROFILE_INDEX, { unique: false });
      }
    };

    request.onblocked = () =>
      fail(new Error("Could not open jobs cache database: blocked by another connection."));
    request.onsuccess = () => {
      const db = request.result;
      if (settled) {
        db.close();
        return;
      }
      settled = true;
      db.onversionchange = () => db.close();
      resolve(db);
    };
    request.onerror = () =>
      fail(request.error || new Error("Could not open jobs cache database."));
  });
}

export async function openJobsCacheDb({ indexedDb = indexedDB, dbName, dbVersion, cacheStore, seenStore } = {}) {
  if (typeof indexedDb === "undefined") return null;
  const options = { indexedDb, dbName, dbVersion, cacheStore, seenStore };
  let db = await openJobsCacheDbHandle(options);
  if (await hasJobsCacheSchema(db, options)) return db;

  // The delete-and-recreate repair must not discard seen-state: jobs_seen is
  // user-visible dedupe history, not data the feed can rebuild from a fetch.
  const seenRows = await readSeenRowsForRepair(db, options);
  db.close();
  await deleteJobsCacheDb(indexedDb, dbName);
  db = await openJobsCacheDbHandle(options);
  if (await hasJobsCacheSchema(db, options)) {
    try {
      await restoreSeenRows(db, { seenStore, rows: seenRows });
    } catch {
      // The schema is repaired either way; a failed restore must not strand the
      // caller on a cache it cannot open.
    }
    return db;
  }

  db.close();
  throw new Error("Could not repair jobs cache schema.");
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
