import { assertLocalDataRuntime } from "./runtime-contract.js";

export function toResult(data, error = "") {
  const normalizedError = String(error || "");
  return {
    ok: !normalizedError,
    data,
    error: normalizedError
  };
}

export function getLocalDataApi() {
  // Intentional compatibility boundary: runtime continues to expose this global for now.
  return window.JobAppLocalData || null;
}

export function getValidatedLocalDataApi(label = "window.JobAppLocalData") {
  const api = getLocalDataApi();
  if (!api) {
    return null;
  }
  return assertLocalDataRuntime(api, label);
}

function ensureApi(api, actionName) {
  if (!api) {
    throw new Error(`Local data API unavailable for ${actionName}.`);
  }
}

export const authService = {
  isReady() {
    const api = getValidatedLocalDataApi("local data runtime");
    return Boolean(api && typeof api.isReady === "function" && api.isReady());
  },
  getCurrentUser() {
    const api = getLocalDataApi();
    return api && typeof api.getCurrentUser === "function" ? api.getCurrentUser() : null;
  },
  onAuthStateChanged(callback) {
    const api = getLocalDataApi();
    ensureApi(api, "onAuthStateChanged");
    return api.onAuthStateChanged(callback);
  },
  async signIn() {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "signIn");
      await api.signIn();
      return toResult(true);
    } catch (err) {
      return toResult(null, err?.message || "Sign in failed.");
    }
  },
  async signOut() {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "signOut");
      await api.signOut();
      return toResult(true);
    } catch (err) {
      return toResult(null, err?.message || "Sign out failed.");
    }
  }
};

export const savedJobsService = {
  async saveJobForUser(uid, job) {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "saveJobForUser");
      await api.saveJobForUser(uid, job);
      return toResult(true);
    } catch (err) {
      return toResult(null, err?.message || "Could not save job.");
    }
  },
  async removeSavedJobForUser(uid, jobKey) {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "removeSavedJobForUser");
      await api.removeSavedJobForUser(uid, jobKey);
      return toResult(true);
    } catch (err) {
      return toResult(null, err?.message || "Could not remove saved job.");
    }
  },
  async getSavedJobKeys(uid) {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "getSavedJobKeys");
      const keys = await api.getSavedJobKeys(uid);
      return toResult(Array.isArray(keys) ? keys : []);
    } catch (err) {
      return toResult([], err?.message || "Could not load saved jobs.");
    }
  },
  subscribeSavedJobs(uid, callback) {
    const api = getLocalDataApi();
    ensureApi(api, "subscribeSavedJobs");
    return api.subscribeSavedJobs(uid, callback);
  }
};

export const attachmentsService = {
  async listAttachmentsForJob(uid, jobKey) {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "listAttachmentsForJob");
      const items = await api.listAttachmentsForJob(uid, jobKey);
      return toResult(Array.isArray(items) ? items : []);
    } catch (err) {
      return toResult([], err?.message || "Could not list attachments.");
    }
  },
  async addAttachmentForJob(uid, jobKey, file) {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "addAttachmentForJob");
      const item = await api.addAttachmentForJob(uid, jobKey, file);
      return toResult(item || null);
    } catch (err) {
      return toResult(null, err?.message || "Could not add attachment.");
    }
  }
};

export const historyService = {
  async listActivityForUser(uid, limit = 80) {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "listActivityForUser");
      const rows = await api.listActivityForUser(uid, limit);
      return toResult(Array.isArray(rows) ? rows : []);
    } catch (err) {
      return toResult([], err?.message || "Could not load activity.");
    }
  }
};

export const backupService = {
  async exportProfileData(uid, options = {}) {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "exportProfileData");
      const payload = await api.exportProfileData(uid, options);
      return toResult(payload || {});
    } catch (err) {
      return toResult(null, err?.message || "Could not export backup.");
    }
  },
  async importProfileData(uid, payload) {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "importProfileData");
      const result = await api.importProfileData(uid, payload);
      return toResult(result || {});
    } catch (err) {
      return toResult(null, err?.message || "Could not import backup.");
    }
  }
};

export const adminService = {
  verifyAdminPin(pin) {
    const api = getLocalDataApi();
    ensureApi(api, "verifyAdminPin");
    return Boolean(api.verifyAdminPin(pin));
  },
  async getAdminOverview(pin) {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "getAdminOverview");
      const data = await api.getAdminOverview(pin);
      return toResult(data || { users: [], totals: {} });
    } catch (err) {
      return toResult({ users: [], totals: {} }, err?.message || "Could not load admin overview.");
    }
  },
  async wipeAccountAdmin(pin, uid) {
    try {
      const api = getLocalDataApi();
      ensureApi(api, "wipeAccountAdmin");
      await api.wipeAccountAdmin(pin, uid);
      return toResult(true);
    } catch (err) {
      return toResult(null, err?.message || "Could not wipe account.");
    }
  }
};
