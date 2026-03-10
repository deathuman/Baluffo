import {
  DB_NAME,
  DB_VERSION,
  BACKUP_SCHEMA_VERSION,
  SESSION_KEY,
  APPLICATION_STATUSES
} from "./frontend/local-data/constants.js";
import { createIndexedDbAdapter } from "./frontend/local-data/indexeddb-adapter.js";
import { normalizeApplicationStatus, canTransitionPhase } from "./frontend/local-data/phase.js";
import {
  makeUser,
  readProfiles,
  writeProfiles,
  verifyAdminPin,
  ensureAdmin,
  getStoredSessionUser
} from "./frontend/local-data/profile-session.js";
import {
  hashFNV1a,
  sanitizeJobUrl,
  generateJobKey,
  buildAttachmentPath,
  nowIso,
  normalizeSectorValue,
  normalizeCustomSourceLabel
} from "./frontend/local-data/job-utils.js";
import {
  normalizeIsoOrNow,
  toPlainObject,
  isClearlyLowerQualityImported,
  areSavedRowsEquivalent,
  parseBackupPayload,
  buildProfileBackupPayload,
  stripAttachmentBlob,
  serializeAttachmentWithBlob,
  deserializeAttachmentBlob
} from "./frontend/local-data/backup-import-export.js";
import { utf8ByteLength, getAttachmentByteSize, ensureAdminUserRow } from "./frontend/local-data/admin-overview.js";
import { createAuthDomain } from "./frontend/local-data/auth.js";
import { createActivityDomain } from "./frontend/local-data/activity.js";
import { createSavedJobsDomain } from "./frontend/local-data/saved-jobs.js";
import { createAttachmentsDomain } from "./frontend/local-data/attachments.js";
import { createBackupDomain } from "./frontend/local-data/backup-service.js";
import { createAdminDomain } from "./frontend/local-data/admin-service.js";

let browserApiInitialized = false;
let browserApi = null;

export function initBrowserLocalDataClient() {
  if (browserApiInitialized && browserApi) {
    return browserApi;
  }
  browserApiInitialized = true;
  const listeners = new Set();
  let currentUser = null;

  const indexedDbAdapter = createIndexedDbAdapter({ dbName: DB_NAME, dbVersion: DB_VERSION });
  const hasIndexedDb = indexedDbAdapter.isReady();
  const withStore = indexedDbAdapter.withStore;
  const listSavedJobs = indexedDbAdapter.listSavedJobs;
  const listAllSavedJobs = indexedDbAdapter.listAllSavedJobs;
  const listAllAttachments = indexedDbAdapter.listAllAttachments;
  const listAttachmentMetadata = indexedDbAdapter.listAttachmentMetadata;

  async function notifySavedJobsChanged(uid) {
    const rows = await listSavedJobs(uid);
    listeners.forEach(l => {
      if (l.type === "saved" && l.uid === uid) {
        l.callback(rows);
      }
    });
  }

  function ensureCurrentUser() {
    if (!currentUser) throw new Error("Not signed in.");
    return currentUser;
  }

  const authDomain = createAuthDomain({
    listeners,
    getCurrentUser: () => currentUser,
    setCurrentUser: value => {
      currentUser = value;
    },
    makeUser,
    readProfiles,
    writeProfiles,
    hashFNV1a,
    sessionKey: SESSION_KEY
  });

  const activityDomain = createActivityDomain({
    withStore,
    ensureCurrentUser,
    hashFNV1a,
    nowIso
  });

  const savedJobsDomain = createSavedJobsDomain({
    withStore,
    listSavedJobs,
    ensureCurrentUser,
    notifySavedJobsChanged,
    addActivityLog: activityDomain.addActivityLog,
    generateJobKey,
    normalizeApplicationStatus,
    canTransitionPhase,
    normalizeSectorValue,
    normalizeCustomSourceLabel,
    sanitizeJobUrl,
    nowIso,
    normalizeIsoOrNow,
    toPlainObject,
    isClearlyLowerQualityImported
  });

  const attachmentsDomain = createAttachmentsDomain({
    withStore,
    ensureCurrentUser,
    hashFNV1a,
    nowIso,
    updateAttachmentMetadata: savedJobsDomain.updateAttachmentMetadata,
    addActivityLog: activityDomain.addActivityLog
  });

  const backupDomain = createBackupDomain({
    withStore,
    ensureCurrentUser,
    readProfiles,
    listSavedJobs,
    listAttachmentMetadata,
    listAllActivityForProfile: activityDomain.listAllActivityForProfile,
    notifySavedJobsChanged,
    updateAttachmentMetadata: savedJobsDomain.updateAttachmentMetadata,
    parseBackupPayload,
    buildProfileBackupPayload,
    normalizeIsoOrNow,
    toPlainObject,
    areSavedRowsEquivalent,
    serializeAttachmentWithBlob,
    stripAttachmentBlob,
    stripAttachmentPk: row => {
      const copy = { ...row };
      delete copy.pk;
      return copy;
    },
    deserializeAttachmentBlob,
    nowIso,
    toActivityId: activityDomain.toActivityId,
    toAttachmentId: attachmentsDomain.toAttachmentId,
    normalizeSavedJobRecord: savedJobsDomain.normalizeSavedJobRecord,
    mergeSavedJobRows: savedJobsDomain.mergeSavedJobRows,
    normalizeImportedAttachmentRow: attachmentsDomain.normalizeImportedAttachmentRow,
    toAttachmentFingerprint: attachmentsDomain.toAttachmentFingerprint,
    backupSchemaVersion: BACKUP_SCHEMA_VERSION
  });

  const adminDomain = createAdminDomain({
    ensureAdmin,
    readProfiles,
    writeProfiles,
    listAllSavedJobs,
    listAllAttachments,
    withStore,
    ensureAdminUserRow,
    utf8ByteLength,
    getAttachmentByteSize,
    sessionKey: SESSION_KEY,
    getCurrentUser: () => currentUser,
    setCurrentUser: value => {
      currentUser = value;
    },
    notifyAuthChanged: authDomain.notifyAuthChanged,
    notifySavedJobsChanged
  });

  authDomain.bindStorageSync(getStoredSessionUser);
  currentUser = getStoredSessionUser();

  browserApi = {
    APPLICATION_STATUSES,
    isReady: () => hasIndexedDb,
    getCurrentUser: () => currentUser,
    onAuthStateChanged: authDomain.onAuthStateChanged,
    signIn: authDomain.signIn,
    signOut: authDomain.signOut,
    saveJobForUser: savedJobsDomain.saveJobForUser,
    removeSavedJobForUser: savedJobsDomain.removeSavedJobForUser,
    getSavedJobKeys: savedJobsDomain.getSavedJobKeys,
    subscribeSavedJobs: (uid, onChange, onError) =>
      savedJobsDomain.subscribeSavedJobs(uid, onChange, onError, listeners),
    generateJobKey,
    buildAttachmentPath,
    updateApplicationStatus: savedJobsDomain.updateApplicationStatus,
    canTransitionPhase,
    updateAttachmentMetadata: savedJobsDomain.updateAttachmentMetadata,
    updateJobNotes: savedJobsDomain.updateJobNotes,
    listAttachmentsForJob: attachmentsDomain.listAttachmentsForJob,
    addAttachmentForJob: attachmentsDomain.addAttachmentForJob,
    deleteAttachmentForJob: attachmentsDomain.deleteAttachmentForJob,
    getAttachmentBlob: attachmentsDomain.getAttachmentBlob,
    listActivityForUser: activityDomain.listActivityForUser,
    exportProfileData: backupDomain.exportProfileData,
    importProfileData: backupDomain.importProfileData,
    verifyAdminPin,
    getAdminOverview: adminDomain.getAdminOverview,
    wipeAccountAdmin: adminDomain.wipeAccountAdmin
  };
  window.JobAppLocalData = browserApi;
  return browserApi;
}
