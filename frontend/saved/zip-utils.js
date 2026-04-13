export function utf8Encode(text) {
    return new TextEncoder().encode(String(text || ""));
  }

  export function utf8Decode(bytes) {
    return new TextDecoder().decode(bytes);
  }

  export function sanitizeBackupFileName(name) {
    const raw = String(name || "").trim() || "file.bin";
    return raw.replace(/[^a-zA-Z0-9._-]/g, "_");
  }

  export function parseDataUrl(dataUrl) {
    const text = String(dataUrl || "");
    const parts = text.split(",");
    if (parts.length !== 2) return null;
    const header = parts[0];
    const body = parts[1];
    if (!/;base64$/i.test(header)) return null;
    const mimeMatch = header.match(/^data:([^;]+);base64$/i);
    const mime = mimeMatch ? String(mimeMatch[1] || "").trim().toLowerCase() : "application/octet-stream";
    const binary = atob(body);
    const out = new Uint8Array(binary.length);
    for (let i = 0; i < binary.length; i++) out[i] = binary.charCodeAt(i);
    return { bytes: out, mime };
  }

  export function toDataUrl(bytes, mimeType) {
    let binary = "";
    const chunkSize = 0x8000;
    for (let i = 0; i < bytes.length; i += chunkSize) {
      const chunk = bytes.subarray(i, i + chunkSize);
      binary += String.fromCharCode(...chunk);
    }
    return `data:${mimeType};base64,${btoa(binary)}`;
  }

  const CRC32_TABLE = (() => {
    const table = new Uint32Array(256);
    for (let n = 0; n < 256; n++) {
      let c = n;
      for (let k = 0; k < 8; k++) {
        c = (c & 1) ? (0xedb88320 ^ (c >>> 1)) : (c >>> 1);
      }
      table[n] = c >>> 0;
    }
    return table;
  })();

  export function getCrc32(bytes) {
    let crc = -1;
    for (let i = 0; i < bytes.length; i++) {
      crc = (crc >>> 8) ^ CRC32_TABLE[(crc ^ bytes[i]) & 0xff];
    }
    return (crc ^ -1) >>> 0;
  }

  function makeDosTimeDate(date = new Date()) {
    const year = Math.max(1980, date.getFullYear());
    const month = date.getMonth() + 1;
    const day = date.getDate();
    const hours = date.getHours();
    const mins = date.getMinutes();
    const secs = Math.floor(date.getSeconds() / 2);
    const dosTime = (hours << 11) | (mins << 5) | secs;
    const dosDate = ((year - 1980) << 9) | (month << 5) | day;
    return { dosTime, dosDate };
  }

  function concatUint8(parts) {
    const total = parts.reduce((sum, p) => sum + p.length, 0);
    const out = new Uint8Array(total);
    let offset = 0;
    for (const part of parts) {
      out.set(part, offset);
      offset += part.length;
    }
    return out;
  }

  export function buildZipStoreOnly(entries) {
    const localParts = [];
    const centralParts = [];
    let offset = 0;
    const now = new Date();

    for (const entry of entries) {
      const nameBytes = utf8Encode(entry.name);
      const dataBytes = entry.bytes instanceof Uint8Array ? entry.bytes : new Uint8Array();
      const crc = getCrc32(dataBytes);
      const size = dataBytes.length >>> 0;
      const { dosTime, dosDate } = makeDosTimeDate(now);

      const localHeader = new Uint8Array(30 + nameBytes.length);
      const lh = new DataView(localHeader.buffer);
      lh.setUint32(0, 0x04034b50, true);
      lh.setUint16(4, 20, true);
      lh.setUint16(6, 0, true);
      lh.setUint16(8, 0, true);
      lh.setUint16(10, dosTime, true);
      lh.setUint16(12, dosDate, true);
      lh.setUint32(14, crc, true);
      lh.setUint32(18, size, true);
      lh.setUint32(22, size, true);
      lh.setUint16(26, nameBytes.length, true);
      lh.setUint16(28, 0, true);
      localHeader.set(nameBytes, 30);
      localParts.push(localHeader, dataBytes);

      const central = new Uint8Array(46 + nameBytes.length);
      const ch = new DataView(central.buffer);
      ch.setUint32(0, 0x02014b50, true);
      ch.setUint16(4, 20, true);
      ch.setUint16(6, 20, true);
      ch.setUint16(8, 0, true);
      ch.setUint16(10, 0, true);
      ch.setUint16(12, dosTime, true);
      ch.setUint16(14, dosDate, true);
      ch.setUint32(16, crc, true);
      ch.setUint32(20, size, true);
      ch.setUint32(24, size, true);
      ch.setUint16(28, nameBytes.length, true);
      ch.setUint16(30, 0, true);
      ch.setUint16(32, 0, true);
      ch.setUint16(34, 0, true);
      ch.setUint16(36, 0, true);
      ch.setUint32(38, 0, true);
      ch.setUint32(42, offset, true);
      central.set(nameBytes, 46);
      centralParts.push(central);

      offset += localHeader.length + dataBytes.length;
    }

    const centralData = concatUint8(centralParts);
    const eocd = new Uint8Array(22);
    const e = new DataView(eocd.buffer);
    e.setUint32(0, 0x06054b50, true);
    e.setUint16(4, 0, true);
    e.setUint16(6, 0, true);
    e.setUint16(8, entries.length, true);
    e.setUint16(10, entries.length, true);
    e.setUint32(12, centralData.length, true);
    e.setUint32(16, offset, true);
    e.setUint16(20, 0, true);

    return new Blob([concatUint8([...localParts, centralData, eocd])], { type: "application/zip" });
  }

  export function parseZipStoreOnly(bytes) {
    const dv = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
    let eocdOffset = -1;
    for (let i = bytes.length - 22; i >= Math.max(0, bytes.length - 65557); i--) {
      if (dv.getUint32(i, true) === 0x06054b50) {
        eocdOffset = i;
        break;
      }
    }
    if (eocdOffset < 0) throw new Error("Invalid ZIP: end of central directory not found.");

    const totalEntries = dv.getUint16(eocdOffset + 10, true);
    const centralSize = dv.getUint32(eocdOffset + 12, true);
    const centralOffset = dv.getUint32(eocdOffset + 16, true);
    const out = new Map();

    let ptr = centralOffset;
    const centralEnd = centralOffset + centralSize;
    for (let i = 0; i < totalEntries && ptr < centralEnd; i++) {
      if (dv.getUint32(ptr, true) !== 0x02014b50) {
        throw new Error("Invalid ZIP: bad central directory header.");
      }
      const method = dv.getUint16(ptr + 10, true);
      const compressedSize = dv.getUint32(ptr + 20, true);
      const fileNameLen = dv.getUint16(ptr + 28, true);
      const extraLen = dv.getUint16(ptr + 30, true);
      const commentLen = dv.getUint16(ptr + 32, true);
      const localOffset = dv.getUint32(ptr + 42, true);
      const nameBytes = bytes.subarray(ptr + 46, ptr + 46 + fileNameLen);
      const fileName = utf8Decode(nameBytes);
      ptr += 46 + fileNameLen + extraLen + commentLen;

      if (method !== 0) throw new Error(`Unsupported ZIP compression for ${fileName}.`);
      if (dv.getUint32(localOffset, true) !== 0x04034b50) {
        throw new Error("Invalid ZIP: bad local header.");
      }

      const localNameLen = dv.getUint16(localOffset + 26, true);
      const localExtraLen = dv.getUint16(localOffset + 28, true);
      const dataOffset = localOffset + 30 + localNameLen + localExtraLen;
      const fileBytes = bytes.subarray(dataOffset, dataOffset + compressedSize);
      out.set(fileName, new Uint8Array(fileBytes));
    }
    return out;
  }
