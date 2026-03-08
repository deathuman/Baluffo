export async function parseBackupInputFile(file) {
  const name = String(file?.name || "").toLowerCase();
  const type = String(file?.type || "").toLowerCase();
  const isZip = name.endsWith(".zip") || type === "application/zip" || type === "application/x-zip-compressed";
  if (!isZip) {
    const text = await file.text();
    return JSON.parse(text);
  }
  return null;
}

export async function buildBackupZipBlob(payload, options = {}) {
  const {
    parseDataUrl,
    sanitizeBackupFileName,
    getCrc32,
    utf8Encode,
    buildZipStoreOnly
  } = options;
  const packPayload = JSON.parse(JSON.stringify(payload || {}));
  const attachments = Array.isArray(packPayload.attachments) ? packPayload.attachments : [];
  const entries = [];
  const filePathByFingerprint = new Map();

  attachments.forEach((att, idx) => {
    const dataUrl = String(att?.blobDataUrl || "");
    if (!dataUrl) return;
    const parsed = parseDataUrl(dataUrl);
    if (!parsed) return;
    const safeName = sanitizeBackupFileName(att?.name || `attachment-${idx + 1}`);
    const crc = getCrc32(parsed.bytes);
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
    bytes: utf8Encode(JSON.stringify(packPayload, null, 2))
  });
  return buildZipStoreOnly(entries);
}

export async function readBackupPayloadFromZip(file, options = {}) {
  const { parseZipStoreOnly, utf8Decode, toDataUrl } = options;
  const bytes = new Uint8Array(await file.arrayBuffer());
  const files = parseZipStoreOnly(bytes);
  const backupEntry = files.get("backup.json");
  if (!backupEntry) {
    throw new Error("ZIP backup is missing backup.json.");
  }
  const payload = JSON.parse(utf8Decode(backupEntry));
  const attachments = Array.isArray(payload.attachments) ? payload.attachments : [];
  for (const att of attachments) {
    if (!att || typeof att !== "object") continue;
    if (att.blobDataUrl) continue;
    const filePath = String(att.filePath || "").trim();
    if (!filePath) continue;
    const content = files.get(filePath);
    if (!content) continue;
    att.blobDataUrl = toDataUrl(content, String(att.type || "application/octet-stream"));
  }
  return payload;
}
