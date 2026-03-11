export const LOCAL_DATA_RUNTIME_METHODS = Object.freeze([
  "isReady",
  "getCurrentUser",
  "onAuthStateChanged",
  "signIn",
  "signOut",
  "saveJobForUser",
  "removeSavedJobForUser",
  "getSavedJobKeys",
  "subscribeSavedJobs",
  "generateJobKey",
  "buildAttachmentPath",
  "canTransitionPhase",
  "updateApplicationStatus",
  "updateJobNotes",
  "listAttachmentsForJob",
  "addAttachmentForJob",
  "getAttachmentBlob",
  "getAttachmentOpenUrl",
  "getAttachmentDownloadUrl",
  "deleteAttachmentForJob",
  "listActivityForUser",
  "exportProfileData",
  "getBackupExportUrl",
  "importProfileData",
  "verifyAdminPin",
  "getAdminOverview",
  "wipeAccountAdmin"
]);

export const LOCAL_DATA_RUNTIME_VALUE_KEYS = Object.freeze([
  "APPLICATION_STATUSES"
]);

export function assertLocalDataRuntime(api, label = "local data runtime") {
  if (!api || typeof api !== "object") {
    throw new Error(`Invalid ${label}: expected an object runtime API.`);
  }
  const missingMethods = LOCAL_DATA_RUNTIME_METHODS.filter(name => typeof api[name] !== "function");
  if (missingMethods.length) {
    throw new Error(`Invalid ${label}: missing methods ${missingMethods.join(", ")}.`);
  }
  const statuses = api.APPLICATION_STATUSES;
  if (!Array.isArray(statuses) || !statuses.every(value => typeof value === "string")) {
    throw new Error(`Invalid ${label}: APPLICATION_STATUSES must be a string array.`);
  }
  return api;
}

export function createLocalDataRuntime(api, label = "local data runtime") {
  return assertLocalDataRuntime(api, label);
}
