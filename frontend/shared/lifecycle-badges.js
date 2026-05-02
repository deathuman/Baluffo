import { escapeHtml } from "./ui/index.js";

function formatDateForStatus(value) {
  const parsed = new Date(String(value || ""));
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
}

export function getLifecycleBadgeMeta(record, options = {}) {
  const { showPreservedSkipped = false } = options;
  const lifecycleEvent = String(record?.lifecycleEvent || "").trim().toLowerCase();
  const lifecycleReason = String(record?.lifecycleReason || "").trim().toLowerCase();
  const status = String(record?.status || "active").trim().toLowerCase() || "active";

  if (lifecycleEvent === "reappeared") {
    return {
      label: "Reappeared",
      cssClass: "reappeared",
      title: "Reappeared in the latest fetch"
    };
  }
  if (lifecycleEvent === "preserved" && lifecycleReason === "source_failed") {
    return {
      label: "Preserved because source failed",
      cssClass: "preserved",
      title: "Kept visible because the source failed in the latest fetch"
    };
  }
  if (showPreservedSkipped && lifecycleEvent === "preserved" && lifecycleReason === "source_skipped") {
    return {
      label: "Preserved because source skipped",
      cssClass: "preserved",
      title: "Kept visible because the source was skipped in the latest fetch"
    };
  }
  if (status === "likely_removed") {
    const removedDate = formatDateForStatus(record?.removedAt);
    return {
      label: "Recently removed",
      cssClass: "likely-removed",
      title: removedDate ? `Recently removed since ${removedDate}` : "Recently removed"
    };
  }
  if (status === "archived") {
    const removedDate = formatDateForStatus(record?.removedAt);
    return {
      label: "Archived",
      cssClass: "archived",
      title: removedDate ? `Archived after removal on ${removedDate}` : "Archived"
    };
  }
  return null;
}

export function renderLifecycleBadgeHtml(record, options = {}) {
  const meta = getLifecycleBadgeMeta(record, options);
  if (!meta) return "";
  return `<span class="job-lifecycle-badge ${meta.cssClass}" title="${escapeHtml(meta.title)}">${escapeHtml(meta.label)}</span>`;
}
