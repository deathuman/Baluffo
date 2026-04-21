import {
  hydrateAttachmentLists as hydrateAttachmentListsFromModule,
  uploadAttachments as uploadAttachmentsFromModule,
  getAttachmentPreviewUrl as getAttachmentPreviewUrlFromModule,
  clearAttachmentPreviewUrls as clearAttachmentPreviewUrlsFromModule,
  renderAttachmentList as renderAttachmentListFromModule
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
  function getAttachmentPreviewUrl(jobKey, attachment) {
    return getAttachmentPreviewUrlFromModule(jobKey, attachment, attachmentPreviewUrls);
  }

  function clearAttachmentPreviewUrls(jobKey) {
    clearAttachmentPreviewUrlsFromModule(jobKey, attachmentPreviewUrls);
  }

  function renderAttachmentList(jobKey, attachments) {
    return renderAttachmentListFromModule(jobKey, attachments, {
      savedJobsListEl: dom.savedJobsListEl,
      cssEscape,
      clearAttachmentPreviewUrls,
      getAttachmentPreviewUrl,
      escapeHtml,
      bindAttachmentActionButtons
    });
  }

  async function hydrateAttachmentLists(jobs) {
    return hydrateAttachmentListsFromModule(jobs, {
      currentUser: viewState.currentUser,
      listAttachmentsForJob: (uid, jobKey) => savedPageService.listAttachmentsForJob(uid, jobKey),
      renderAttachmentList
    });
  }

  async function uploadAttachments(jobKey, files) {
    return uploadAttachmentsFromModule(jobKey, files, {
      currentUser: viewState.currentUser,
      listAttachmentsForJob: (uid, safeJobKey) => savedPageService.listAttachmentsForJob(uid, safeJobKey),
      maxAttachmentsPerJob,
      maxAttachmentBytes,
      addAttachmentForJob: (uid, safeJobKey, meta, file) => (
        savedPageService.addAttachmentForJob(uid, safeJobKey, meta, file)
      ),
      renderAttachmentList,
      showToast,
      dispatchAttachmentMutated: safeJobKey => {
        savedDispatch.dispatch({
          type: savedActions.ATTACHMENT_MUTATED,
          payload: { jobKey: safeJobKey }
        });
      },
      queueActivityPulse,
      timelineScopeAttachments
    });
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
    if (!viewState.currentUser) return;
    try {
      const deleteResult = await savedPageService.deleteAttachmentForJob(
        viewState.currentUser.uid,
        jobKey,
        attachmentId
      );
      if (!deleteResult.ok) throw new Error(deleteResult.error || "Could not delete attachment.");
      const nextResult = await savedPageService.listAttachmentsForJob(viewState.currentUser.uid, jobKey);
      if (!nextResult.ok) throw new Error(nextResult.error || "Could not list attachments.");
      renderAttachmentList(jobKey, nextResult.data);
      showToast("Attachment removed.", "success");
      savedDispatch.dispatch({ type: savedActions.ATTACHMENT_MUTATED, payload: { jobKey } });
      queueActivityPulse(jobKey, timelineScopeAttachments);
    } catch (err) {
      console.error("Could not delete attachment:", err);
      showToast("Could not delete attachment.", "error");
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
  }

  return {
    hydrateAttachmentLists,
    uploadAttachments,
    renderAttachmentList,
    bindAttachmentActionButtons
  };
}
