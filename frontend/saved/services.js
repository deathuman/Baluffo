import {
  getLocalDataApi,
  toResult,
  authService
} from "../local-data/services.js";

export function isSavedApiReady() {
  return authService.isReady();
}

export const savedPageService = {
  isAvailable() {
    return Boolean(getLocalDataApi());
  },
  subscribeSavedJobs(uid, onChange, onError) {
    const api = getLocalDataApi();
    if (!api || typeof api.subscribeSavedJobs !== "function") {
      if (typeof onError === "function") {
        onError(new Error("Local data API unavailable for subscribeSavedJobs."));
      }
      return () => {};
    }
    return api.subscribeSavedJobs(uid, onChange, onError);
  },
  canTransitionPhase(currentPhase, nextPhase) {
    const api = getLocalDataApi();
    if (!api || typeof api.canTransitionPhase !== "function") return null;
    return Boolean(api.canTransitionPhase(currentPhase, nextPhase));
  },
  async saveJobForUser(uid, job, options = {}) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.saveJobForUser !== "function") {
        throw new Error("Local data API unavailable for saveJobForUser.");
      }
      const data = await api.saveJobForUser(uid, job, options);
      return toResult(data);
    } catch (err) {
      return toResult(null, err?.message || "Could not save job.");
    }
  },
  async removeSavedJobForUser(uid, jobKey) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.removeSavedJobForUser !== "function") {
        throw new Error("Local data API unavailable for removeSavedJobForUser.");
      }
      await api.removeSavedJobForUser(uid, jobKey);
      return toResult(true);
    } catch (err) {
      return toResult(null, err?.message || "Could not remove saved job.");
    }
  },
  async updateApplicationStatus(uid, jobKey, status, options = {}) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.updateApplicationStatus !== "function") {
        throw new Error("Local data API unavailable for updateApplicationStatus.");
      }
      await api.updateApplicationStatus(uid, jobKey, status, options);
      return toResult(true);
    } catch (err) {
      return toResult(null, err?.message || "Could not update application status.");
    }
  },
  async updateJobNotes(uid, jobKey, notes) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.updateJobNotes !== "function") {
        throw new Error("Local data API unavailable for updateJobNotes.");
      }
      await api.updateJobNotes(uid, jobKey, notes);
      return toResult(true);
    } catch (err) {
      return toResult(null, err?.message || "Could not update notes.");
    }
  },
  async listAttachmentsForJob(uid, jobKey) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.listAttachmentsForJob !== "function") {
        throw new Error("Local data API unavailable for listAttachmentsForJob.");
      }
      const data = await api.listAttachmentsForJob(uid, jobKey);
      return toResult(Array.isArray(data) ? data : []);
    } catch (err) {
      return toResult([], err?.message || "Could not list attachments.");
    }
  },
  async addAttachmentForJob(uid, jobKey, fileMeta, blob) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.addAttachmentForJob !== "function") {
        throw new Error("Local data API unavailable for addAttachmentForJob.");
      }
      const data = await api.addAttachmentForJob(uid, jobKey, fileMeta, blob);
      return toResult(data);
    } catch (err) {
      return toResult(null, err?.message || "Could not add attachment.");
    }
  },
  async getAttachmentBlob(uid, jobKey, attachmentId) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.getAttachmentBlob !== "function") {
        throw new Error("Local data API unavailable for getAttachmentBlob.");
      }
      const data = await api.getAttachmentBlob(uid, jobKey, attachmentId);
      if (data && typeof data === "object" && data.blob instanceof Blob) {
        return toResult({
          blob: data.blob,
          filename: String(data.filename || ""),
          contentType: String(data.contentType || data.blob?.type || "")
        });
      }
      return toResult({
        blob: data instanceof Blob ? data : null,
        filename: "",
        contentType: data instanceof Blob ? String(data.type || "") : ""
      });
    } catch (err) {
      return toResult(null, err?.message || "Could not read attachment.");
    }
  },
  getAttachmentDownloadUrl(uid, jobKey, attachmentId) {
    const api = getLocalDataApi();
    if (!api || typeof api.getAttachmentDownloadUrl !== "function") return "";
    return String(api.getAttachmentDownloadUrl(uid, jobKey, attachmentId) || "");
  },
  async deleteAttachmentForJob(uid, jobKey, attachmentId) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.deleteAttachmentForJob !== "function") {
        throw new Error("Local data API unavailable for deleteAttachmentForJob.");
      }
      await api.deleteAttachmentForJob(uid, jobKey, attachmentId);
      return toResult(true);
    } catch (err) {
      return toResult(null, err?.message || "Could not delete attachment.");
    }
  },
  async listActivityForUser(uid, limit = 400) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.listActivityForUser !== "function") {
        throw new Error("Local data API unavailable for listActivityForUser.");
      }
      const data = await api.listActivityForUser(uid, limit);
      return toResult(Array.isArray(data) ? data : []);
    } catch (err) {
      return toResult([], err?.message || "Could not load activity.");
    }
  },
  async exportProfileData(uid, options = {}) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.exportProfileData !== "function") {
        throw new Error("Local data API unavailable for exportProfileData.");
      }
      const data = await api.exportProfileData(uid, options);
      return toResult(data || {});
    } catch (err) {
      return toResult(null, err?.message || "Could not export backup.");
    }
  },
  async importProfileData(uid, payload) {
    try {
      const api = getLocalDataApi();
      if (!api || typeof api.importProfileData !== "function") {
        throw new Error("Local data API unavailable for importProfileData.");
      }
      const data = await api.importProfileData(uid, payload);
      return toResult(data || {});
    } catch (err) {
      return toResult(null, err?.message || "Could not import backup.");
    }
  }
};

export { authService as savedAuthService };
