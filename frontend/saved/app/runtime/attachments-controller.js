import {
  uploadAttachments as uploadAttachmentsFromModule,
  getAttachmentPreviewUrl as getAttachmentPreviewUrlFromModule,
  clearAttachmentPreviewUrls as clearAttachmentPreviewUrlsFromModule,
  renderAttachmentList as renderAttachmentListFromModule,
  renderAttachmentError as renderAttachmentErrorFromModule
} from "../attachments.js";
import { escapeHtml, showToast } from "../../../shared/ui/index.js";

export function createSavedAttachmentsController({
  dom,
  viewState,
  savedPageService,
  savedDispatch,
  savedActions,
  queueActivityPulse,
  timelineScopeAttachments,
  maxAttachmentsPerJob,
  maxAttachmentBytes,
  attachmentPreviewUrls,
  cssEscape,
  setSelectedJobKey
}) {
  const attachmentMutationJobKeys = new Set();
  const attachmentListRequestVersions = new Map();

  function getAttachmentPreviewUrl(jobKey, attachment) {
    return getAttachmentPreviewUrlFromModule(jobKey, attachment, attachmentPreviewUrls);
  }

  function clearAttachmentPreviewUrls(jobKey) {
    clearAttachmentPreviewUrlsFromModule(jobKey, attachmentPreviewUrls);
  }

  function renderAttachmentList(jobKey, attachments) {
    viewState.loadedAttachmentJobKeys.add(String(jobKey || ""));
    viewState.loadingAttachmentJobKeys.delete(String(jobKey || ""));
    return renderAttachmentListFromModule(jobKey, attachments, {
      savedJobsListEl: dom.savedJobsListEl,
      cssEscape,
      clearAttachmentPreviewUrls,
      getAttachmentPreviewUrl,
      escapeHtml,
      bindAttachmentActionButtons
    });
  }

  function renderAttachmentError(jobKey) {
    const safeJobKey = String(jobKey || "");
    // Drop the loaded marker so a later Refresh actually re-fetches. Keeping it
    // would make hydrateAttachmentListForJob early-return and the retry a no-op.
    viewState.loadedAttachmentJobKeys.delete(safeJobKey);
    viewState.loadingAttachmentJobKeys.delete(safeJobKey);
    return renderAttachmentErrorFromModule(safeJobKey, {
      savedJobsListEl: dom.savedJobsListEl,
      cssEscape
    });
  }

  function getAttachmentListRequestVersion(jobKey) {
    return attachmentListRequestVersions.get(jobKey) || 0;
  }

  function invalidateAttachmentListRequests(jobKey) {
    const nextVersion = getAttachmentListRequestVersion(jobKey) + 1;
    attachmentListRequestVersions.set(jobKey, nextVersion);
    // Let a post-mutation retry start even if the superseded read has not settled.
    viewState.loadingAttachmentJobKeys.delete(jobKey);
    return nextVersion;
  }

  async function hydrateAttachmentLists(jobs) {
    const currentUser = viewState.currentUser;
    if (!currentUser || !Array.isArray(jobs)) return;

    for (const job of jobs) {
      const jobKey = String(job.jobKey || job.id || "");
      if (!jobKey) continue;
      if (viewState.loadingAttachmentJobKeys.has(jobKey)) continue;
      viewState.loadingAttachmentJobKeys.add(jobKey);
      const requestVersion = getAttachmentListRequestVersion(jobKey);
      try {
        const rowsResult = await savedPageService.listAttachmentsForJob(currentUser.uid, jobKey);
        if (viewState.currentUser?.uid !== currentUser.uid ||
            getAttachmentListRequestVersion(jobKey) !== requestVersion) continue;
        if (!rowsResult?.ok) throw new Error(rowsResult?.error || "Could not load attachments.");
        renderAttachmentList(jobKey, rowsResult.data || []);
      } catch (err) {
        if (viewState.currentUser?.uid !== currentUser.uid ||
            getAttachmentListRequestVersion(jobKey) !== requestVersion) continue;
        console.error("Could not list attachments:", err);
        renderAttachmentError(jobKey);
      } finally {
        if (viewState.currentUser?.uid === currentUser.uid &&
            getAttachmentListRequestVersion(jobKey) === requestVersion) {
          viewState.loadingAttachmentJobKeys.delete(jobKey);
        }
      }
    }
  }

  async function hydrateAttachmentListForJob(jobKey, options = {}) {
    const safeJobKey = String(jobKey || "").trim();
    const currentUser = viewState.currentUser;
    if (!safeJobKey || !currentUser) return;
    if (!options.force && viewState.loadedAttachmentJobKeys.has(safeJobKey)) return;
    if (viewState.loadingAttachmentJobKeys.has(safeJobKey)) return;
    viewState.loadingAttachmentJobKeys.add(safeJobKey);
    const requestVersion = getAttachmentListRequestVersion(safeJobKey);
    try {
      const rowsResult = await savedPageService.listAttachmentsForJob(currentUser.uid, safeJobKey);
      if (viewState.currentUser?.uid !== currentUser.uid ||
          getAttachmentListRequestVersion(safeJobKey) !== requestVersion) return;
      if (!rowsResult?.ok) throw new Error(rowsResult?.error || "Could not load attachments.");
      renderAttachmentList(safeJobKey, rowsResult.data || []);
    } catch (err) {
      if (viewState.currentUser?.uid !== currentUser.uid ||
          getAttachmentListRequestVersion(safeJobKey) !== requestVersion) return;
      console.error("Could not list attachments:", err);
      renderAttachmentError(safeJobKey);
    } finally {
      if (viewState.currentUser?.uid === currentUser.uid &&
          getAttachmentListRequestVersion(safeJobKey) === requestVersion) {
        viewState.loadingAttachmentJobKeys.delete(safeJobKey);
      }
    }
  }

  async function uploadAttachments(jobKey, files) {
    const safeJobKey = String(jobKey || "").trim();
    const currentUser = viewState.currentUser;
    if (!safeJobKey || !currentUser || attachmentMutationJobKeys.has(safeJobKey)) {
      if (attachmentMutationJobKeys.has(safeJobKey)) {
        showToast("An attachment update is already in progress.", "info");
      }
      return;
    }
    attachmentMutationJobKeys.add(safeJobKey);
    try {
      return await uploadAttachmentsFromModule(safeJobKey, files, {
        currentUser,
        listAttachmentsForJob: (uid, key) => savedPageService.listAttachmentsForJob(uid, key),
        maxAttachmentsPerJob,
        maxAttachmentBytes,
        addAttachmentForJob: (uid, key, meta, file) => (
          savedPageService.addAttachmentForJob(uid, key, meta, file)
        ),
        renderAttachmentList: (key, attachments) => {
          if (viewState.currentUser?.uid !== currentUser.uid) return;
          renderAttachmentList(key, attachments);
        },
        showToast,
        onMutationCommitted: key => {
          const nextRefreshVersion = invalidateAttachmentListRequests(key);
          if (viewState.currentUser?.uid !== currentUser.uid) return;
          viewState.loadedAttachmentJobKeys.add(String(key || ""));
          savedDispatch.dispatch({
            type: savedActions.ATTACHMENT_MUTATED,
            payload: { jobKey: key }
          });
          queueActivityPulse(key, timelineScopeAttachments);
          return nextRefreshVersion;
        },
        onMutationRefreshFailed: key => {
          if (viewState.currentUser?.uid !== currentUser.uid) return;
          viewState.loadedAttachmentJobKeys.delete(String(key || ""));
          showToast("Attachment saved, but the list could not refresh. Use Refresh to try again.", "error");
        },
        getMutationRefreshVersion: getAttachmentListRequestVersion,
        isCurrentUser: user => viewState.currentUser?.uid === user?.uid
      });
    } finally {
      attachmentMutationJobKeys.delete(safeJobKey);
    }
  }

  async function openAttachment(jobKey, attachmentId) {
    if (!viewState.currentUser) return;
    try {
      const directUrl = savedPageService.getAttachmentOpenUrl(
        viewState.currentUser.uid,
        jobKey,
        attachmentId
      );
      if (directUrl) {
        window.open(directUrl, "_blank", "noopener,noreferrer");
        return;
      }
      const blobResult = await savedPageService.getAttachmentBlob(
        viewState.currentUser.uid,
        jobKey,
        attachmentId
      );
      if (!blobResult.ok) throw new Error(blobResult.error || "Could not read attachment.");
      const blob = blobResult.data?.blob;
      if (!blob) {
        showToast("Attachment data not available.", "error");
        return;
      }
      const url = URL.createObjectURL(blob);
      window.open(url, "_blank", "noopener,noreferrer");
      setTimeout(() => URL.revokeObjectURL(url), 60_000);
    } catch (err) {
      console.error("Could not open attachment:", err);
      showToast("Could not open attachment.", "error");
    }
  }

  async function downloadAttachment(jobKey, attachmentId, filename) {
    if (!viewState.currentUser) return;
    try {
      const directUrl = savedPageService.getAttachmentDownloadUrl(
        viewState.currentUser.uid,
        jobKey,
        attachmentId
      );
      if (directUrl) {
        window.open(directUrl, "_blank", "noopener,noreferrer");
        return;
      }

      const blobResult = await savedPageService.getAttachmentBlob(
        viewState.currentUser.uid,
        jobKey,
        attachmentId
      );
      if (!blobResult.ok) throw new Error(blobResult.error || "Could not read attachment.");
      const blob = blobResult.data?.blob;
      if (!blob) {
        showToast("Attachment data not available.", "error");
        return;
      }
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = blobResult.data?.filename || filename || "attachment";
      document.body.appendChild(anchor);
      anchor.click();
      anchor.remove();
      setTimeout(() => URL.revokeObjectURL(url), 1000);
    } catch (err) {
      console.error("Could not download attachment:", err);
      showToast("Could not download attachment.", "error");
    }
  }

  async function deleteAttachment(jobKey, attachmentId) {
    const safeJobKey = String(jobKey || "").trim();
    const currentUser = viewState.currentUser;
    if (!currentUser || !safeJobKey || attachmentMutationJobKeys.has(safeJobKey)) {
      if (attachmentMutationJobKeys.has(safeJobKey)) {
        showToast("An attachment update is already in progress.", "info");
      }
      return;
    }

    attachmentMutationJobKeys.add(safeJobKey);
    try {
      let deleteResult;
      try {
        deleteResult = await savedPageService.deleteAttachmentForJob(
          currentUser.uid,
          safeJobKey,
          attachmentId
        );
        if (!deleteResult?.ok) throw new Error(deleteResult?.error || "Could not delete attachment.");
      } catch (err) {
        console.error("Could not delete attachment:", err);
        if (viewState.currentUser?.uid === currentUser.uid) {
          showToast("Could not delete attachment.", "error");
        }
        return;
      }

      const refreshVersion = invalidateAttachmentListRequests(safeJobKey);
      if (viewState.currentUser?.uid !== currentUser.uid) return;
      viewState.loadedAttachmentJobKeys.add(safeJobKey);
      savedDispatch.dispatch({ type: savedActions.ATTACHMENT_MUTATED, payload: { jobKey: safeJobKey } });
      queueActivityPulse(safeJobKey, timelineScopeAttachments);
      showToast("Attachment removed.", "success");

      try {
        const nextResult = await savedPageService.listAttachmentsForJob(currentUser.uid, safeJobKey);
        if (viewState.currentUser?.uid !== currentUser.uid ||
            getAttachmentListRequestVersion(safeJobKey) !== refreshVersion) return;
        if (!nextResult?.ok) throw new Error(nextResult?.error || "Could not list attachments.");
        renderAttachmentList(safeJobKey, nextResult.data || []);
      } catch (err) {
        if (viewState.currentUser?.uid !== currentUser.uid) return;
        console.error("Could not refresh attachments after delete:", err);
        viewState.loadedAttachmentJobKeys.delete(safeJobKey);
        showToast("Attachment deleted, but the list could not refresh. Use Refresh to try again.", "error");
      }
    } finally {
      attachmentMutationJobKeys.delete(safeJobKey);
    }
  }

  function bindAttachmentActionButtons() {
    const { savedJobsListEl } = dom;
    if (!savedJobsListEl) return;

    savedJobsListEl.querySelectorAll(".att-open-btn").forEach(btn => {
      btn.onclick = async () => {
        const jobKey = btn.dataset.jobKey || "";
        setSelectedJobKey(jobKey, { rerenderTimeline: false });
        await openAttachment(jobKey, btn.dataset.attachmentId || "");
      };
    });

    savedJobsListEl.querySelectorAll(".att-download-btn").forEach(btn => {
      btn.onclick = async () => {
        const jobKey = btn.dataset.jobKey || "";
        setSelectedJobKey(jobKey, { rerenderTimeline: false });
        await downloadAttachment(
          jobKey,
          btn.dataset.attachmentId || "",
          btn.dataset.fileName || "attachment"
        );
      };
    });

    savedJobsListEl.querySelectorAll(".att-delete-btn").forEach(btn => {
      btn.onclick = async () => {
        const jobKey = btn.dataset.jobKey || "";
        setSelectedJobKey(jobKey, { rerenderTimeline: false });
        await deleteAttachment(jobKey, btn.dataset.attachmentId || "");
      };
    });

    // Manual refresh. `force` is required: hydrateAttachmentListForJob returns
    // early for a key already in loadedAttachmentJobKeys, so without it the
    // button would look broken the moment a list had been hydrated once.
    savedJobsListEl.querySelectorAll(".att-refresh-btn").forEach(btn => {
      btn.onclick = async () => {
        const jobKey = btn.dataset.jobKey || "";
        setSelectedJobKey(jobKey, { rerenderTimeline: false });
        await hydrateAttachmentListForJob(jobKey, { force: true });
      };
    });
  }

  return {
    hydrateAttachmentLists,
    hydrateAttachmentListForJob,
    uploadAttachments,
    renderAttachmentList,
    renderAttachmentError,
    bindAttachmentActionButtons
  };
}
