import {
  sanitizeBackupFileName,
  parseDataUrl,
  toDataUrl,
  utf8Encode,
  utf8Decode,
  getCrc32,
  buildZipStoreOnly,
  parseZipStoreOnly
} from "../../saved-zip-utils.js";

// Saved data-source owns backup file parsing/packing and keeps zip details out of app.js.
export async function parseBackupInputFile(file) {
  const name = String(file?.name || "").toLowerCase();
  const type = String(file?.type || "").toLowerCase();
  const isZip = name.endsWith(".zip") || type === "application/zip" || type === "application/x-zip-compressed";
  if (!isZip) {
    const text = await file.text();
    return JSON.parse(text);
  }
  return readBackupPayloadFromZip(file);
}

export async function buildBackupZipBlob(payload, options = {}) {
  const parseDataUrlFn = options.parseDataUrl || parseDataUrl;
  const sanitizeBackupFileNameFn = options.sanitizeBackupFileName || sanitizeBackupFileName;
  const getCrc32Fn = options.getCrc32 || getCrc32;
  const utf8EncodeFn = options.utf8Encode || utf8Encode;
  const buildZipStoreOnlyFn = options.buildZipStoreOnly || buildZipStoreOnly;
  const packPayload = JSON.parse(JSON.stringify(payload || {}));
  const attachments = Array.isArray(packPayload.attachments) ? packPayload.attachments : [];
  const entries = [];
  const filePathByFingerprint = new Map();

  attachments.forEach((att, idx) => {
    const dataUrl = String(att?.blobDataUrl || "");
    if (!dataUrl) return;
    const parsed = parseDataUrlFn(dataUrl);
    if (!parsed) return;
    const safeName = sanitizeBackupFileNameFn(att?.name || `attachment-${idx + 1}`);
    const crc = getCrc32Fn(parsed.bytes);
    const fingerprint = `${String(parsed.mime || "").toLowerCase()}|${parsed.bytes.length}|${crc}`;
    let filePath = filePathByFingerprint.get(fingerprint) || "";
    if (!filePath) {
      filePath = `files/${String(att?.id || `att_${idx + 1}`)}-${safeName}`;
      entries.push({ name: filePath, bytes: parsed.bytes });
      filePathByFingerprint.set(fingerprint, filePath);
    }
    att.filePath = filePath;
    delete att.blobDataUrl;
  });

  packPayload.packageFormat = "zip-v1";
  packPayload.includesFiles = true;
  entries.unshift({
    name: "backup.json",
    bytes: utf8EncodeFn(JSON.stringify(packPayload, null, 2))
  });
  return buildZipStoreOnlyFn(entries);
}

export async function readBackupPayloadFromZip(file, options = {}) {
  const parseZipStoreOnlyFn = options.parseZipStoreOnly || parseZipStoreOnly;
  const utf8DecodeFn = options.utf8Decode || utf8Decode;
  const toDataUrlFn = options.toDataUrl || toDataUrl;
  const bytes = new Uint8Array(await file.arrayBuffer());
  const files = parseZipStoreOnlyFn(bytes);
  const backupEntry = files.get("backup.json");
  if (!backupEntry) {
    throw new Error("ZIP backup is missing backup.json.");
  }
  const payload = JSON.parse(utf8DecodeFn(backupEntry));
  const attachments = Array.isArray(payload.attachments) ? payload.attachments : [];
  for (const att of attachments) {
    if (!att || typeof att !== "object") continue;
    if (att.blobDataUrl) continue;
    const filePath = String(att.filePath || "").trim();
    if (!filePath) continue;
    const content = files.get(filePath);
    if (!content) continue;
    att.blobDataUrl = toDataUrlFn(content, String(att.type || "application/octet-stream"));
  }
  return payload;
}
