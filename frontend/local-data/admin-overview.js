export function utf8ByteLength(input) {
  const text = String(input || "");
  if (typeof TextEncoder !== "undefined") {
    return new TextEncoder().encode(text).length;
  }
  return unescape(encodeURIComponent(text)).length;
}

export function getAttachmentByteSize(row) {
  const meta = Number(row?.size) || 0;
  if (meta > 0) return meta;
  const blobSize = Number(row?.blob?.size) || 0;
  return blobSize > 0 ? blobSize : 0;
}

export function ensureAdminUserRow(map, uid, fallbackName = "Unknown Profile") {
  if (!map.has(uid)) {
    map.set(uid, {
      uid,
      name: fallbackName,
      email: "",
      savedJobsCount: 0,
      notesBytes: 0,
      attachmentsCount: 0,
      attachmentsBytes: 0,
      totalBytes: 0
    });
  }
  return map.get(uid);
}
