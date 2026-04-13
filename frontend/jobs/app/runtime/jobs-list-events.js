export function setupJobsListDelegation({
  jobsList,
  jobRowSelector,
  saveJobBtnSelector,
  sanitizeUrl,
  getJobById,
  onToggleSaveJob,
  onOpenJobLink,
  onMarkJobSeen
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
