export const ALLOWED_ATTACHMENT_EXTENSIONS = new Set(["pdf", "doc", "docx", "txt", "png", "jpg", "jpeg"]);

export function getFileExtension(name) {
  const idx = String(name || "").lastIndexOf(".");
  if (idx === -1) return "";
  return String(name).slice(idx + 1).toLowerCase();
}

export function formatFileSize(bytes) {
  const value = Number(bytes) || 0;
  if (value < 1024) return `${value} B`;
  if (value < 1024 * 1024) return `${(value / 1024).toFixed(1)} KB`;
  return `${(value / (1024 * 1024)).toFixed(1)} MB`;
}

export function isImageAttachment(attachment) {
  const type = String(attachment?.type || "").toLowerCase();
  if (type === "image/png" || type === "image/jpeg") return true;
  const ext = getFileExtension(attachment?.name || "");
  return ext === "png" || ext === "jpg" || ext === "jpeg";
}

export function isAllowedAttachment(file) {
  const ext = getFileExtension(file?.name || "");
  if (ALLOWED_ATTACHMENT_EXTENSIONS.has(ext)) return true;
  const type = String(file?.type || "").toLowerCase();
  return (
    type === "application/pdf" ||
    type === "application/msword" ||
    type === "application/vnd.openxmlformats-officedocument.wordprocessingml.document" ||
    type === "text/plain" ||
    type === "image/png" ||
    type === "image/jpeg"
  );
}

export async function hydrateAttachmentLists(jobs, deps) {
  const {
    currentUser,
    listAttachmentsForJob,
    renderAttachmentList
  } = deps;
  if (!currentUser || !Array.isArray(jobs)) return;

  for (const job of jobs) {
    const jobKey = String(job.jobKey || job.id || "");
    if (!jobKey) continue;
    try {
      const rowsResult = await listAttachmentsForJob(currentUser.uid, jobKey);
      renderAttachmentList(jobKey, rowsResult.ok ? rowsResult.data : []);
    } catch (err) {
      console.error("Could not list attachments:", err);
      renderAttachmentList(jobKey, []);
    }
  }
}

export async function uploadAttachments(jobKey, files, deps) {
  const {
    currentUser,
    listAttachmentsForJob,
    maxAttachmentsPerJob,
    maxAttachmentBytes,
    addAttachmentForJob,
    renderAttachmentList,
    showToast,
    dispatchAttachmentMutated,
    queueActivityPulse,
    timelineScopeAttachments
  } = deps;
  if (!currentUser || !jobKey || !Array.isArray(files) || files.length === 0) return;

  let currentList = [];
  try {
    const currentListResult = await listAttachmentsForJob(currentUser.uid, jobKey);
    currentList = currentListResult.ok ? currentListResult.data : [];
  } catch {
    currentList = [];
  }

  const remainingSlots = maxAttachmentsPerJob - currentList.length;
  if (remainingSlots <= 0) {
    showToast(`Max ${maxAttachmentsPerJob} attachments per job.`, "error");
    return;
  }

  let accepted = 0;
  for (const file of files) {
    if (accepted >= remainingSlots) {
      showToast(`Max ${maxAttachmentsPerJob} attachments per job.`, "error");
      break;
    }
    if (!isAllowedAttachment(file)) {
      showToast(`Unsupported file type: ${file.name}`, "error");
      continue;
    }
    if (file.size > maxAttachmentBytes) {
      showToast(`File too large: ${file.name}`, "error");
      continue;
    }

    try {
      const addResult = await addAttachmentForJob(
        currentUser.uid,
        jobKey,
        { name: file.name, type: file.type, size: file.size },
        file
      );
      if (!addResult.ok) throw new Error(addResult.error || `Could not upload ${file.name}`);
      accepted += 1;
    } catch (err) {
      console.error("Attachment upload failed:", err);
      showToast(`Could not upload ${file.name}`, "error");
    }
  }

  try {
    const nextResult = await listAttachmentsForJob(currentUser.uid, jobKey);
    if (!nextResult.ok) throw new Error(nextResult.error || "Could not refresh attachments.");
    renderAttachmentList(jobKey, nextResult.data);
    showToast("Attachments updated.", "success");
    dispatchAttachmentMutated(jobKey);
    queueActivityPulse(jobKey, timelineScopeAttachments);
  } catch {
    showToast("Could not refresh attachments.", "error");
  }
}

export function getAttachmentPreviewUrl(jobKey, attachment, attachmentPreviewUrls) {
  if (!isImageAttachment(attachment)) return "";
  if (!(attachment.blob instanceof Blob)) return "";
  try {
    const url = URL.createObjectURL(attachment.blob);
    const key = String(jobKey || "");
    const urls = attachmentPreviewUrls.get(key) || [];
    urls.push(url);
    attachmentPreviewUrls.set(key, urls);
    return url;
  } catch {
    return "";
  }
}

export function clearAttachmentPreviewUrls(jobKey, attachmentPreviewUrls) {
  const key = String(jobKey || "");
  const urls = attachmentPreviewUrls.get(key) || [];
  urls.forEach(url => {
    try {
      URL.revokeObjectURL(url);
    } catch {
      // no-op
    }
  });
  attachmentPreviewUrls.delete(key);
}

export function renderAttachmentList(jobKey, attachments, deps) {
  const {
    savedJobsListEl,
    cssEscape,
    clearAttachmentPreviewUrls,
    getAttachmentPreviewUrl,
    escapeHtml,
    bindAttachmentActionButtons
  } = deps;
  const container = savedJobsListEl?.querySelector(`.attachments-list[data-job-key="${cssEscape(jobKey)}"]`);
  if (!container) return;
  clearAttachmentPreviewUrls(jobKey);

  if (!attachments || attachments.length === 0) {
    container.innerHTML = '<div class="muted">No attachments yet.</div>';
    return;
  }

  container.innerHTML = attachments.map(att => {
    const id = escapeHtml(att.id || "");
    const name = escapeHtml(att.name || "attachment");
    const size = formatFileSize(att.size || 0);
    const previewUrl = getAttachmentPreviewUrl(jobKey, att);
    const previewHtml = previewUrl
      ? `<img class="attachment-preview" src="${escapeHtml(previewUrl)}" alt="${name} preview" loading="lazy">`
      : "";
    return `
      <div class="attachment-item">
        <div class="attachment-meta">
          ${previewHtml}
          <span class="attachment-name">${name}</span>
          <span class="attachment-size">${size}</span>
        </div>
        <div class="attachment-actions">
          <button class="btn back-btn att-open-btn" data-job-key="${escapeHtml(jobKey)}" data-attachment-id="${id}">Open</button>
          <button class="btn back-btn att-download-btn" data-job-key="${escapeHtml(jobKey)}" data-attachment-id="${id}" data-file-name="${name}">Download</button>
          <button class="btn back-btn att-delete-btn" data-job-key="${escapeHtml(jobKey)}" data-attachment-id="${id}">Delete</button>
        </div>
      </div>
    `;
  }).join("");
  bindAttachmentActionButtons();
}
