import { escapeHtml, tooltipAttrs } from "./ui/index.js";

function formatDateForStatus(value) {
  const parsed = new Date(String(value || ""));
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toLocaleDateString("en-US", { year: "numeric", month: "short", day: "numeric" });
}

function formatRelativeLastSeen(value, now = Date.now()) {
  const parsed = new Date(String(value || ""));
  if (Number.isNaN(parsed.getTime())) return "";
  const nowMs = now instanceof Date ? now.getTime() : Number(now);
  const deltaMs = Math.max(0, (Number.isFinite(nowMs) ? nowMs : Date.now()) - parsed.getTime());
  const deltaMin = Math.round(deltaMs / 60000);
  if (deltaMin < 1) return "just now";
  if (deltaMin < 60) return `${deltaMin}m ago`;
  const deltaHours = Math.round(deltaMin / 60);
  if (deltaHours < 48) return `${deltaHours}h ago`;
  return `${Math.round(deltaHours / 24)}d ago`;
}

function appendLastSeenCopy(title, record, options = {}) {
  if (!options.includeLastSeenAt) return title;
  const relative = formatRelativeLastSeen(record?.lastSeenAt, options.now);
  if (!relative) return title;
  return `${title}; last seen ${relative}`;
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
      title: appendLastSeenCopy("Reappeared in the latest fetch", record, options)
    };
  }
  if (lifecycleEvent === "preserved" && lifecycleReason === "source_failed") {
    return {
      label: "Preserved because source failed",
      cssClass: "preserved",
      title: appendLastSeenCopy("Kept visible because the source failed in the latest fetch", record, options)
    };
  }
  if (showPreservedSkipped && lifecycleEvent === "preserved" && lifecycleReason === "source_skipped") {
    return {
      label: "Preserved because source skipped",
      cssClass: "preserved",
      title: appendLastSeenCopy("Kept visible because the source was skipped in the latest fetch", record, options)
    };
  }
  if (status === "likely_removed") {
    const removedDate = formatDateForStatus(record?.removedAt);
    const title = removedDate ? `Recently removed since ${removedDate}` : "Recently removed";
    return {
      label: "Recently removed",
      cssClass: "likely-removed",
      title: appendLastSeenCopy(title, record, options)
    };
  }
  if (status === "archived") {
    const removedDate = formatDateForStatus(record?.removedAt);
    const title = removedDate ? `Archived after removal on ${removedDate}` : "Archived";
    return {
      label: "Archived",
      cssClass: "archived",
      title: appendLastSeenCopy(title, record, options)
    };
  }
  return null;
}

export function renderLifecycleBadgeHtml(record, options = {}) {
  const meta = getLifecycleBadgeMeta(record, options);
  if (!meta) return "";
  return `<span class="job-lifecycle-badge ${meta.cssClass}"${tooltipAttrs(meta.title)}>${escapeHtml(meta.label)}</span>`;
}
