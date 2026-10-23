/**
 * The desktop update surface: the header panel, its model, the release-notes
 * dialog it can open and the click diagnostics that record a click landing
 * before the panel is bound.
 *
 * `runtime/boot.js` reaches this module through a dynamic `import()` once the
 * feed has painted rows (or after its fallback timer), so the shell's parse no
 * longer carries ~60 KB of update code that cannot matter before first paint.
 * The dependencies that are exclusive to this surface -- the release-notes
 * dialog and the confirmation dialog -- are imported here for the same reason.
 */
import { fetchJson, postJson } from "../../shared/api-client.js";
import { awaitDesktopBootstrap } from "../../shared/local-data/desktop-client.js";
import { bindAsyncClick, showToast } from "../../shared/ui/index.js";
import { openReleaseNotesDialog } from "../../shared/ui/release-notes-dialog.js";
import { requestConfirmationDialog } from "../../local-data/profile-name-dialog.js";
import { createJobsDesktopUpdateController } from "./desktop-update.js";

export function createJobsDesktopUpdateSurface({
  refs,
  documentObject,
  emitDiagnostic,
  baseUrl,
  isDesktopRuntimeMode,
  openExternalUrl
}) {
  return createJobsDesktopUpdateController({
    refs,
    documentObject,
    emitDiagnostic,
    baseUrl,
    fetchJson,
    postJson,
    bindAsyncClick,
    showToast,
    requestConfirmationDialog,
    isDesktopRuntimeMode,
    awaitDesktopBootstrap,
    showReleaseNotesDialog: options => openReleaseNotesDialog(options),
    openExternalUrl
  });
}
