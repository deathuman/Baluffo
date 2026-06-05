import { APPLICATION_STATUSES } from "../../../local-data/constants.js";
import {
  requestProfileLoadFailureAction,
  requestProfileName,
  requestTextInputDialog
} from "../../../local-data/profile-name-dialog.js";
import { createLocalDataRuntime } from "../../../local-data/runtime-contract.js";
import { buildAttachmentPath, generateJobKey } from "../../../local-data/job-utils.js";
import { canTransitionPhase, normalizeApplicationStatus } from "../../../local-data/phase.js";
import {
  AUTH_LISTENERS,
  BASE_URL,
  SAVED_SUBSCRIPTIONS,
  SESSION_KEY,
  desktopState
} from "./state.js";
import { normalizeDesktopSession, stopDesktopLifecycle } from "./lifecycle.js";

export function toErrorMessage(error, fallback) {
  return error?.message || String(error || "") || fallback;
}

function buildAttachmentContentUrl(uid, jobKey, attachmentId, options = {}) {
  const query = new URLSearchParams({
    uid: String(uid || ""),
    jobKey: String(jobKey || ""),
    attachmentId: String(attachmentId || "")
  });
  if (options.download) {
    query.set("download", "1");
  }
  return `${BASE_URL}/attachments/content?${query.toString()}`;
}

function parseFilenameFromContentDisposition(value) {
  const text = String(value || "");
  if (!text) return "";
  const utfMatch = text.match(/filename\*\s*=\s*UTF-8''([^;]+)/i);
  if (utfMatch && utfMatch[1]) {
    try {
      return decodeURIComponent(String(utfMatch[1]).trim());
    } catch {
      return String(utfMatch[1]).trim();
    }
  }
  const quotedMatch = text.match(/filename\s*=\s*"([^"]+)"/i);
  if (quotedMatch && quotedMatch[1]) return String(quotedMatch[1]).trim();
  const plainMatch = text.match(/filename\s*=\s*([^;]+)/i);
  return plainMatch && plainMatch[1] ? String(plainMatch[1]).trim() : "";
}

function getFileExtension(name) {
  const idx = String(name || "").lastIndexOf(".");
  return idx === -1 ? "" : String(name).slice(idx + 1).toLowerCase();
}

function isImageAttachmentMeta(attachment) {
  const type = String(attachment?.type || "").toLowerCase();
  if (type === "image/png" || type === "image/jpeg") return true;
  const ext = getFileExtension(attachment?.name || "");
  return ext === "png" || ext === "jpg" || ext === "jpeg";
}

async function requestJson(path, options = {}) {
  const { timeoutMs, ...fetchOptions } = options || {};
  const timeout = Math.max(0, Number(timeoutMs) || 0);
  const controller = timeout > 0 && !fetchOptions.signal && typeof AbortController !== "undefined"
    ? new AbortController()
    : null;
  let timeoutId = null;
  if (controller) {
    timeoutId = window.setTimeout(() => {
      controller.abort();
    }, timeout);
    timeoutId?.unref?.();
    fetchOptions.signal = controller.signal;
  }
  let response;
  try {
    response = await fetch(`${BASE_URL}${path}`, {
      ...fetchOptions,
      headers: {
        "Content-Type": "application/json",
        ...(fetchOptions.headers || {})
      }
    });
  } catch (error) {
    if (controller?.signal?.aborted) {
      throw new Error("Desktop local-data request timed out.");
    }
    throw error;
  } finally {
    if (timeoutId !== null) {
      window.clearTimeout(timeoutId);
    }
  }
  const payload = await response.json().catch(() => ({}));
  if (!response.ok || payload?.ok === false) {
    throw new Error(String(payload?.error || response.statusText || "Request failed."));
  }
  return payload;
}

async function fetchAttachmentBlob(uid, jobKey, attachmentId) {
  const response = await fetch(buildAttachmentContentUrl(uid, jobKey, attachmentId));
  if (!response.ok) {
    let errorMessage = "Could not read attachment.";
    try {
      const payload = await response.json();
      errorMessage = String(payload?.error || errorMessage);
    } catch {
      // ignore
    }
    throw new Error(errorMessage);
  }
  const blob = await response.blob();
  return {
    blob,
    filename: parseFilenameFromContentDisposition(response.headers.get("Content-Disposition")),
    contentType: response.headers.get("Content-Type") || blob.type || ""
  };
}

function notifyAuthChanged() {
  try {
    window.localStorage.setItem(SESSION_KEY, desktopState.currentUser?.uid || "");
  } catch {
    // no-op
  }
  AUTH_LISTENERS.forEach(listener => {
    listener(desktopState.currentUser);
  });
}

async function fetchDesktopSessionPayload() {
  return requestJson("/session");
}

async function fetchCurrentUser() {
  const payload = await fetchDesktopSessionPayload();
  desktopState.desktopSession = normalizeDesktopSession(payload.desktopSession);
  if (!desktopState.desktopSession) {
    stopDesktopLifecycle();
  }
  return payload.user || null;
}

async function listDesktopProfiles() {
  const payload = await requestJson("/profiles");
  return Array.isArray(payload.profiles) ? payload.profiles : [];
}

export function commitAuthState(user, revision = null) {
  if (revision !== null && Number(revision) !== desktopState.authStateRevision) {
    return desktopState.currentUser;
  }
  desktopState.currentUser = user || null;
  notifyAuthChanged();
  return desktopState.currentUser;
}

export async function refreshCurrentUser(options = {}) {
  const revision = Object.prototype.hasOwnProperty.call(options, "revision")
    ? Number(options.revision)
    : null;
  const user = await fetchCurrentUser();
  return commitAuthState(user, revision);
}

async function listSavedJobs(uid) {
  const payload = await requestJson(`/saved-jobs?uid=${encodeURIComponent(String(uid || ""))}`);
  return Array.isArray(payload.rows) ? payload.rows : [];
}

async function pollSavedSubscriptions() {
  for (const subscription of Array.from(SAVED_SUBSCRIPTIONS)) {
    try {
      const rows = await listSavedJobs(subscription.uid);
      const serialized = JSON.stringify(rows);
      if (serialized === subscription.lastPayload) {
        continue;
      }
      subscription.lastPayload = serialized;
      subscription.onChange(rows);
    } catch (error) {
      if (typeof subscription.onError === "function") {
        subscription.onError(error);
      }
    }
  }
}

function ensurePolling() {
  if (desktopState.pollingStarted) {
    return;
  }
  desktopState.pollingStarted = true;
  window.setInterval(() => {
    pollSavedSubscriptions().catch(() => {});
  }, 1500);
}

function fileToDataUrl(file) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(reader.error || new Error("Could not read file."));
    reader.readAsDataURL(file);
  });
}

export function createDesktopLocalDataApi() {
  if (desktopState.desktopApi) {
    return desktopState.desktopApi;
  }
  desktopState.desktopApi = createLocalDataRuntime({
    APPLICATION_STATUSES,
    isReady() {
      return desktopState.desktopBootstrapStatus === "ready";
    },
    getCurrentUser() {
      return desktopState.currentUser;
    },
    onAuthStateChanged(callback) {
      AUTH_LISTENERS.add(callback);
      callback(desktopState.currentUser);
      return () => AUTH_LISTENERS.delete(callback);
    },
    async signIn() {
      let defaultValue = String(desktopState.currentUser?.displayName || "").trim();
      const completeSignIn = async name => {
        const trimmedName = String(name || "").trim();
        if (!trimmedName) {
          throw new Error("Sign-in cancelled.");
        }
        const payload = await requestJson("/sign-in", {
          method: "POST",
          body: JSON.stringify({ name: trimmedName })
        });
        desktopState.authStateRevision += 1;
        return { user: commitAuthState(payload.user || null) };
      };

      while (true) {
        let existingProfiles = [];
        let description = "Enter a profile name to sign in or create a local desktop profile. Signing in keeps your seen and saved jobs on this device.";
        try {
          existingProfiles = await listDesktopProfiles();
        } catch {
          const action = await requestProfileLoadFailureAction({
            title: "Sign in",
            description: "Could not load existing local profiles. Retry to load them again, create a new local profile, or cancel sign-in."
          });
          if (action === "retry") {
            continue;
          }
          if (action === "create") {
            const name = await requestTextInputDialog({
              title: "Create profile",
              description: "Could not load existing local profiles. Create a new local profile for this device to continue.",
              label: "New profile name",
              submitLabel: "Create profile",
              defaultValue
            });
            return completeSignIn(name);
          }
          throw new Error("Sign-in cancelled.");
        }

        if (existingProfiles.length) {
          const currentProfile = existingProfiles.find(profile => Boolean(profile?.isCurrent));
          defaultValue = defaultValue
            || String(currentProfile?.displayName || currentProfile?.name || "").trim()
            || String(existingProfiles[0]?.displayName || existingProfiles[0]?.name || "").trim();
          description = "Choose an existing local profile or create a new one. Signing in keeps your seen jobs, saved jobs, notes, reminders, and attachments on this device.";
        }

        const name = await requestProfileName({
          title: "Sign in",
          description,
          existingProfiles,
          defaultValue
        });
        return completeSignIn(name);
      }
    },
    async signOut() {
      await requestJson("/sign-out", { method: "POST", body: "{}" });
      desktopState.authStateRevision += 1;
      commitAuthState(null);
    },
    async saveJobForUser(uid, job, options = {}) {
      const payload = await requestJson("/saved-jobs/save", {
        method: "POST",
        body: JSON.stringify({ uid, job, options })
      });
      await pollSavedSubscriptions();
      return String(payload.jobKey || generateJobKey(job));
    },
    async removeSavedJobForUser(uid, jobKey) {
      await requestJson("/saved-jobs/remove", {
        method: "POST",
        body: JSON.stringify({ uid, jobKey })
      });
      await pollSavedSubscriptions();
    },
    async getSavedJobKeys(uid) {
      const payload = await requestJson(`/saved-job-keys?uid=${encodeURIComponent(String(uid || ""))}`);
      return Array.isArray(payload.keys) ? payload.keys : [];
    },
    subscribeSavedJobs(uid, onChange, onError) {
      ensurePolling();
      const subscription = {
        uid: String(uid || ""),
        onChange,
        onError,
        lastPayload: ""
      };
      SAVED_SUBSCRIPTIONS.add(subscription);
      listSavedJobs(uid).then(rows => {
        subscription.lastPayload = JSON.stringify(rows);
        onChange(rows);
      }).catch(error => {
        if (typeof onError === "function") {
          onError(error);
        }
      });
      return () => SAVED_SUBSCRIPTIONS.delete(subscription);
    },
    generateJobKey,
    buildAttachmentPath,
    canTransitionPhase,
    async updateApplicationStatus(uid, jobKey, status, options = {}) {
      await requestJson("/saved-jobs/status", {
        method: "POST",
        body: JSON.stringify({ uid, jobKey, status: normalizeApplicationStatus(status), options })
      });
      await pollSavedSubscriptions();
    },
    async updateApplicationTracking(uid, jobKey, tracking, options = {}) {
      await requestJson("/saved-jobs/tracking", {
        method: "POST",
        body: JSON.stringify({ uid, jobKey, tracking, options })
      });
      await pollSavedSubscriptions();
    },
    async updateJobNotes(uid, jobKey, notes, options = {}) {
      await requestJson("/saved-jobs/notes", {
        method: "POST",
        body: JSON.stringify({ uid, jobKey, notes, options })
      });
      await pollSavedSubscriptions();
    },
    async listAttachmentsForJob(uid, jobKey) {
      const payload = await requestJson(`/attachments?uid=${encodeURIComponent(String(uid || ""))}&jobKey=${encodeURIComponent(String(jobKey || ""))}`);
      const rows = Array.isArray(payload.rows) ? payload.rows : [];
      return Promise.all(
        rows.map(async row => {
          if (!isImageAttachmentMeta(row)) {
            return row;
          }
          try {
            const attachmentId = String(row?.id || "");
            if (!attachmentId) return row;
            const data = await fetchAttachmentBlob(uid, jobKey, attachmentId);
            return { ...row, blob: data.blob };
          } catch {
            return row;
          }
        })
      );
    },
    async addAttachmentForJob(uid, jobKey, fileMeta, blob) {
      const blobDataUrl = await fileToDataUrl(blob);
      const payload = await requestJson("/attachments/add", {
        method: "POST",
        body: JSON.stringify({ uid, jobKey, fileMeta, blobDataUrl })
      });
      await pollSavedSubscriptions();
      return String(payload.attachmentId || "");
    },
    async getAttachmentBlob(uid, jobKey, attachmentId) {
      return fetchAttachmentBlob(uid, jobKey, attachmentId);
    },
    getAttachmentDownloadUrl(uid, jobKey, attachmentId) {
      return buildAttachmentContentUrl(uid, jobKey, attachmentId, { download: true });
    },
    getAttachmentOpenUrl(uid, jobKey, attachmentId) {
      return buildAttachmentContentUrl(uid, jobKey, attachmentId);
    },
    async deleteAttachmentForJob(uid, jobKey, attachmentId) {
      await requestJson("/attachments/delete", {
        method: "POST",
        body: JSON.stringify({ uid, jobKey, attachmentId })
      });
      await pollSavedSubscriptions();
    },
    async listActivityForUser(uid, limit = 300) {
      const payload = await requestJson(`/activity?uid=${encodeURIComponent(String(uid || ""))}&limit=${encodeURIComponent(String(limit || 300))}`);
      return Array.isArray(payload.rows) ? payload.rows : [];
    },
    async exportProfileData(uid, options = {}) {
      const payload = await requestJson("/backup/export", {
        method: "POST",
        body: JSON.stringify({ uid, options })
      });
      return payload.payload || {};
    },
    getBackupExportUrl(uid, options = {}) {
      const query = new URLSearchParams({
        uid: String(uid || ""),
        includeFiles: options?.includeFiles ? "1" : "0"
      });
      return `${BASE_URL}/backup/export-file?${query.toString()}`;
    },
    async importProfileData(uid, payload) {
      const response = await requestJson("/backup/import", {
        method: "POST",
        body: JSON.stringify({ uid, payload })
      });
      await pollSavedSubscriptions();
      return response.result || {};
    },
    async getAdminOverview(options = {}) {
      const body = {};
      if (options?.detail) body.detail = options.detail;
      const payload = await requestJson("/admin/overview", {
        method: "POST",
        body: JSON.stringify(body),
        timeoutMs: options?.timeoutMs
      });
      return payload.overview || { users: [], totals: {} };
    },
    async wipeAccountAdmin(uid) {
      const payload = await requestJson("/admin/wipe", {
        method: "POST",
        body: JSON.stringify({ uid })
      });
      desktopState.authStateRevision += 1;
      commitAuthState(payload.user || null);
      await pollSavedSubscriptions();
    }
  }, "desktop local data runtime");
  return desktopState.desktopApi;
}
