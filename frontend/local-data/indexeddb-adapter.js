export function stripPk(row) {
  if (!row) return row;
  const copy = { ...row };
  delete copy.pk;
  return copy;
}

export function stripAttachmentPk(row) {
  if (!row) return row;
  const copy = { ...row };
  delete copy.pk;
  return copy;
}

export function attachmentDedupeKey(row) {
  return [
    String(row?.profileId || ""),
    String(row?.jobKey || ""),
    String(row?.name || "").trim().toLowerCase(),
    String(row?.type || "").trim().toLowerCase(),
    String(Number(row?.size) || 0)
  ].join("|");
}

export function dedupeAttachmentRows(rows) {
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

export function createIndexedDbAdapter({ dbName, dbVersion }) {
  const hasIndexedDb = typeof window.indexedDB !== "undefined";
  let dbPromise = null;

  function getDb() {
    if (!hasIndexedDb) {
      return Promise.reject(new Error("IndexedDB not supported in this browser."));
    }
    if (dbPromise) return dbPromise;

    dbPromise = new Promise((resolve, reject) => {
      const request = indexedDB.open(dbName, dbVersion);

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

  return {
    isReady: () => hasIndexedDb,
    withStore,
    listSavedJobs,
    listAllSavedJobs,
    listAllAttachments,
    listAttachmentMetadata
  };
}
