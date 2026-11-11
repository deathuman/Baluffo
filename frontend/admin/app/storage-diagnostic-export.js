/**
 * Client-side incident export for the completed exact storage diagnostic.
 *
 * The bridge response already contains only aggregate table and generation
 * totals, so the export sends that payload unchanged and adds a small,
 * versioned envelope for incident attachments. No new bridge route or rescan is
 * involved.
 */

const EXPORT_KIND = "baluffo.storage-diagnostic";
const EXPORT_SCHEMA_VERSION = 1;

function asDate(value) {
  if (value instanceof Date) {
    return Number.isNaN(value.getTime()) ? null : value;
  }
  if (value === null || value === undefined || value === "") return null;
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? null : date;
}

function fileStamp(value) {
  const date = asDate(value);
  return date ? date.toISOString().replace(/[:.]/g, "-") : "";
}

function buildStorageDiagnosticExport(storageDiagnostic, exportedAt = new Date()) {
  const exported = asDate(exportedAt) || new Date();
  return {
    kind: EXPORT_KIND,
    schemaVersion: EXPORT_SCHEMA_VERSION,
    exportedAt: exported.toISOString(),
    storageDiagnostic
  };
}

export function storageDiagnosticExportFilename(storageDiagnostic, exportedAt = new Date()) {
  const stamp = fileStamp(storageDiagnostic?.capturedAt) || fileStamp(exportedAt) || fileStamp(new Date());
  return `baluffo-storage-diagnostic-${stamp}.json`;
}

export function serializeStorageDiagnosticExport(storageDiagnostic, exportedAt = new Date()) {
  return `${JSON.stringify(buildStorageDiagnosticExport(storageDiagnostic, exportedAt), null, 2)}\n`;
}

export function downloadStorageDiagnosticJson(filename, content, {
  documentRef = globalThis.document,
  urlApi = globalThis.URL
} = {}) {
  if (!documentRef?.createElement || !urlApi?.createObjectURL) {
    throw new Error("The browser download API is unavailable");
  }
  const blob = new Blob([content], { type: "application/json" });
  const url = urlApi.createObjectURL(blob);
  const link = documentRef.createElement("a");
  link.href = url;
  link.download = filename;
  link.rel = "noopener";
  documentRef.body?.appendChild?.(link);
  try {
    link.click();
  } finally {
    link.remove?.();
    urlApi.revokeObjectURL?.(url);
  }
}
