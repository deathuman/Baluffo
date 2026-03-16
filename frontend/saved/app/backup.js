/**
 * Saved page backup: export and import profile data (saved jobs, history, attachments).
 * Runtime passes deps (currentUser, services, UI callbacks) and calls runExportBackup / runImportBackup.
 */
import { buildBackupZipBlob, parseBackupInputFile } from "../data-source.js";

/**
 * @param {object} deps
 * @param {object|null} deps.currentUser
 * @param {import("../services.js").SavedPageService} deps.savedPageService
 * @param {boolean} deps.includeFiles - from export-include-files checkbox
 * @param {function(string, string, object?): void} deps.showToast
 */
export async function runExportBackup(deps) {
  const { currentUser, savedPageService, includeFiles, showToast } = deps;
  if (!currentUser || !savedPageService?.isAvailable()) return;

  try {
    const directExportUrl = savedPageService.getBackupExportUrl(currentUser.uid, { includeFiles });
    if (directExportUrl) {
      window.open(directExportUrl, "_blank", "noopener,noreferrer");
      showToast("Backup export started.", "success", { durationMs: 2600 });
      return;
    }
    const payloadResult = await savedPageService.exportProfileData(currentUser.uid, {
      includeFiles
    });
    if (!payloadResult.ok) throw new Error(payloadResult.error || "Could not export backup.");
    const payload = payloadResult.data || {};
    const date = new Date().toISOString().slice(0, 10);
    let blob;
    let filename;
    if (includeFiles) {
      blob = await buildBackupZipBlob(payload);
      filename = `baluffo-backup-${currentUser.uid}-${date}.zip`;
    } else {
      const text = JSON.stringify(payload, null, 2);
      blob = new Blob([text], { type: "application/json" });
      filename = `baluffo-backup-${currentUser.uid}-${date}.json`;
    }
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(url);
    const counts = payload?.counts || {};
    const schemaVersion = payload?.schemaVersion ?? payload?.version ?? "?";
    const jobsCount = Number(counts.savedJobs) || 0;
    const historyCount = Number(counts.historyEvents) || 0;
    const attachmentsCount = Number(counts.attachments) || 0;
    showToast(
      `Backup exported (v${schemaVersion}) · Jobs ${jobsCount} · History ${historyCount} · Attachments ${attachmentsCount}`,
      "success",
      { durationMs: 4600 }
    );
  } catch (err) {
    console.error("Backup export failed:", err);
    showToast("Could not export backup.", "error");
  }
}

/**
 * @param {File|null} file
 * @param {object} deps
 * @param {object|null} deps.currentUser
 * @param {import("../services.js").SavedPageService} deps.savedPageService
 * @param {function(string, string, object?): void} deps.showToast
 * @param {function(): Promise<void>} deps.refreshActivityLog
 */
export async function runImportBackup(file, deps) {
  const { currentUser, savedPageService, showToast, refreshActivityLog } = deps;
  if (!currentUser || !savedPageService?.isAvailable()) return;

  try {
    const payload = await parseBackupInputFile(file);
    const resultEnvelope = await savedPageService.importProfileData(currentUser.uid, payload);
    if (!resultEnvelope.ok) throw new Error(resultEnvelope.error || "Could not import backup.");
    const result = resultEnvelope.data || {};
    const created = Number(result?.created) || 0;
    const updated = Number(result?.updated) || 0;
    const skippedInvalid = Number(result?.skippedInvalid) || 0;
    const historyAdded = Number(result?.historyAdded) || 0;
    const attachmentsAdded = Number(result?.attachmentsAdded) || 0;
    const attachmentsHydrated = Number(result?.attachmentsHydrated) || 0;
    const warningCount = Array.isArray(result?.warnings) ? result.warnings.length : 0;
    showToast(
      `Backup imported · Created ${created} · Updated ${updated} · Skipped ${skippedInvalid} · History +${historyAdded} · Attachments +${attachmentsAdded} · Files hydrated ${attachmentsHydrated}`,
      "success",
      { durationMs: 6400 }
    );
    if (warningCount > 0) {
      showToast(`${warningCount} non-fatal import warnings.`, "info", { durationMs: 4200 });
    }
    await refreshActivityLog();
  } catch (err) {
    console.error("Backup import failed:", err);
    showToast("Could not import backup file.", "error");
  }
}
