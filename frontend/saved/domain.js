export function toCanonicalCountry(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const upper = raw.toUpperCase();
  if (upper === "NETHERLANDS") return "NL";
  if (upper === "UNITED STATES" || upper === "USA" || upper === "US") return "US";
  if (upper === "UNITED KINGDOM" || upper === "UK" || upper === "GB") return "GB";
  return raw.length === 2 ? upper : raw;
}

export function normalizeReminderInput(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return "";
  return parsed.toISOString();
}

export function toDatetimeLocalValue(value, parseIsoDate) {
  const parsed = parseIsoDate(value);
  if (!parsed) return "";
  const offsetMs = parsed.getTimezoneOffset() * 60 * 1000;
  const local = new Date(parsed.getTime() - offsetMs);
  return local.toISOString().slice(0, 16);
}

export function normalizeCustomJobInput(values, options = {}) {
  const title = String(values?.title || "").trim();
  const company = String(values?.company || "").trim();
  const reminderAt = normalizeReminderInput(values?.reminderAt);
  return {
    title,
    company,
    city: String(values?.city || "").trim(),
    country: toCanonicalCountry(values?.country),
    workType: String(values?.workType || "").trim() || "Onsite",
    contractType: String(values?.contractType || "").trim() || "Unknown",
    sector: String(values?.sector || "").trim() || "Tech",
    profession: String(values?.profession || "").trim(),
    jobLink: String(values?.jobLink || "").trim(),
    notes: String(values?.notes || "").trim(),
    reminderAt,
    isCustom: true,
    customSourceLabel: String(options.customSourceLabel || "Custom")
  };
}

export function activityTypeLabel(type) {
  switch (type) {
    case "job_saved": return "Saved";
    case "job_removed": return "Removed";
    case "phase_changed": return "Phase Changed";
    case "attachment_added": return "Attachment Added";
    case "attachment_deleted": return "Attachment Deleted";
    case "custom_job_created": return "Custom Job";
    case "custom_job_removed": return "Custom Job Removed";
    case "custom_job_updated": return "Custom Job Updated";
    case "custom_job_duplicated": return "Custom Job Duplicated";
    case "reminder_set": return "Reminder Set";
    case "reminder_cleared": return "Reminder Cleared";
    default: return "Event";
  }
}

export function formatActivityDetail(entry, options = {}) {
  const type = String(entry?.type || "event");
  const details = entry?.details && typeof entry.details === "object" ? entry.details : {};
  const normalizePhase = options.normalizePhase || (value => value);
  const phaseLabels = options.phaseLabels || {};
  const formatPhaseTimestamp = options.formatPhaseTimestamp || (() => "");
  if (type === "phase_changed") {
    const from = phaseLabels[normalizePhase(details.previousStatus)] || "Unknown";
    const to = phaseLabels[normalizePhase(details.nextStatus)] || "Unknown";
    const override = details.overrideUsed ? " (override)" : "";
    return `${from} -> ${to}${override}`;
  }
  if (type === "job_removed") {
    const from = phaseLabels[normalizePhase(details.fromStatus)] || "Saved";
    return `Removed from ${from}`;
  }
  if (type === "custom_job_created") return "Created custom job entry";
  if (type === "custom_job_removed") return "Deleted custom job entry";
  if (type === "custom_job_updated") return "Updated custom job fields";
  if (type === "custom_job_duplicated") return "Created a duplicate custom entry";
  if (type === "reminder_set") {
    if (details.reminderAt) return `Reminder set for ${formatPhaseTimestamp(details.reminderAt) || "scheduled time"}`;
    return "Reminder set";
  }
  if (type === "reminder_cleared") return "Reminder removed";
  if (type === "attachment_added") return `Uploaded ${String(details.fileName || "file")}`;
  if (type === "attachment_deleted") return "Deleted an attachment";
  return "Job table updated";
}
