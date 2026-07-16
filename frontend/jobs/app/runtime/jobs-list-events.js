export function setupJobsListDelegation({
  jobsList,
  jobRowSelector,
  saveJobBtnSelector,
  availabilityCheckSelector = "[data-ui='job-availability-check-btn']",
  originalLinkSelector = "[data-ui='job-original-link-btn']",
  sanitizeUrl,
  getJobById,
  onToggleSaveJob,
  onOpenJobLink,
  onMarkJobSeen,
  onCheckAvailability = async () => {}
}) {
  if (!jobsList) return;

  jobsList.addEventListener("click", event => {
    const target = event.target;
    if (!(target instanceof Element)) return;

    const saveJobBtn = target.closest(saveJobBtnSelector);
    if (saveJobBtn) {
      event.preventDefault();
      event.stopPropagation();
      const jobId = saveJobBtn.dataset.jobId || "";
      const job = getJobById(jobId);
      if (job) {
        onToggleSaveJob(job).catch(() => {});
      }
      return;
    }

    const availabilityCheckBtn = target.closest(availabilityCheckSelector);
    if (availabilityCheckBtn) {
      event.preventDefault();
      event.stopPropagation();
      availabilityCheckBtn.disabled = true;
      Promise.resolve(onCheckAvailability(availabilityCheckBtn.dataset.availabilityId || ""))
        .finally(() => { availabilityCheckBtn.disabled = false; });
      return;
    }

    const originalLinkBtn = target.closest(originalLinkSelector);
    if (originalLinkBtn) {
      event.preventDefault();
      event.stopPropagation();
      const openLink = sanitizeUrl(originalLinkBtn.dataset.jobLink || "");
      const jobRow = originalLinkBtn.closest(jobRowSelector);
      const jobKey = String(jobRow?.dataset.jobKey || "");
      if (jobKey) onMarkJobSeen(jobKey);
      if (openLink && typeof onOpenJobLink === "function") {
        Promise.resolve(onOpenJobLink(openLink, jobRow)).catch(() => {});
      }
      return;
    }

    const jobRow = target.closest(jobRowSelector);
    if (!jobRow || target.closest(saveJobBtnSelector)) return;
    const link = jobRow.dataset.jobLink;
    if (!link) return;
    const jobKey = String(jobRow.dataset.jobKey || "").trim();
    const openLink = sanitizeUrl(link) || link;
    if (typeof onOpenJobLink === "function") {
      Promise.resolve(onOpenJobLink(openLink, jobRow)).catch(() => {});
    } else {
      window.open(openLink, "_blank", "noopener,noreferrer");
    }
    onMarkJobSeen(jobKey).catch(() => {});
  });

  jobsList.addEventListener("keydown", event => {
    if (event.key !== "Enter") return;
    const target = event.target;
    if (!(target instanceof Element)) return;

    const jobRow = target.closest(jobRowSelector);
    if (!jobRow || target.closest(saveJobBtnSelector)) return;
    const link = jobRow.dataset.jobLink;
    if (!link) return;
    const jobKey = String(jobRow.dataset.jobKey || "").trim();
    const openLink = sanitizeUrl(link) || link;
    if (typeof onOpenJobLink === "function") {
      Promise.resolve(onOpenJobLink(openLink, jobRow)).catch(() => {});
    } else {
      window.open(openLink, "_blank", "noopener,noreferrer");
    }
    onMarkJobSeen(jobKey).catch(() => {});
  });
}
