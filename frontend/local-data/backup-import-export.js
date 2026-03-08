export function normalizeIsoOrNow(value, fallback = "") {
  const text = String(value || "").trim();
  if (!text) return fallback;
  const parsed = new Date(text);
  return Number.isNaN(parsed.getTime()) ? fallback : parsed.toISOString();
}

export function toPlainObject(value) {
  if (!value || typeof value !== "object" || Array.isArray(value)) return {};
  return value;
}

export function isClearlyLowerQualityImported(existingRow, importedRow) {
  const hasExistingRequired = Boolean(String(existingRow?.title || "").trim()) && Boolean(String(existingRow?.company || "").trim());
  const hasImportedRequired = Boolean(String(importedRow?.title || "").trim()) && Boolean(String(importedRow?.company || "").trim());
  return hasExistingRequired && !hasImportedRequired;
}

export function areSavedRowsEquivalent(a, b) {
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

export function parseBackupPayload(payload) {
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

export function buildProfileBackupPayload(uid, context) {
  const includeFiles = Boolean(context.includeFiles);
  const exportedAt = context.nowIso();
  const savedJobs = context.savedJobs.map(row => context.normalizeSavedJobRecord(uid, row));
  const customJobs = savedJobs.filter(row => row.isCustom).length;
  const attachmentsCount = context.attachments.length;
  const historyEvents = context.activityLog.length;
  return {
    version: context.backupSchemaVersion,
    schemaVersion: context.backupSchemaVersion,
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

export function stripAttachmentBlob(row, stripAttachmentPk) {
  const copy = stripAttachmentPk(row);
  delete copy.blob;
  return copy;
}

export async function serializeAttachmentWithBlob(row, stripAttachmentPk) {
  const base = stripAttachmentBlob(row, stripAttachmentPk);
  if (!row.blob) return base;
  const dataUrl = await blobToDataUrl(row.blob);
  return { ...base, blobDataUrl: dataUrl };
}

export function blobToDataUrl(blob) {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result || ""));
    reader.onerror = () => reject(reader.error || new Error("Could not serialize blob."));
    reader.readAsDataURL(blob);
  });
}

export function deserializeAttachmentBlob(row) {
  if (!row?.blobDataUrl) return null;
  return dataUrlToBlob(row.blobDataUrl);
}

export function dataUrlToBlob(dataUrl) {
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
