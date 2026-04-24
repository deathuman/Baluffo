export const SAVED_FILTER_ALL = "all";
const SAVED_FILTER_CUSTOM = "custom";
const SAVED_FILTER_IMPORTED = "imported";
export const SORT_UPDATED = "updated";
const SORT_SAVED = "saved";
const SORT_REMINDER = "reminder";
const SORT_PERSONAL = "personal";
export const REMINDER_SOON_HOURS = 72;

export function isCustomJob(job) {
  return Boolean(job && job.isCustom);
}

export function filterSavedJobs(jobs, filter) {
  if (!Array.isArray(jobs)) return [];
  if (filter === SAVED_FILTER_CUSTOM) return jobs.filter(isCustomJob);
  if (filter === SAVED_FILTER_IMPORTED) return jobs.filter(job => !isCustomJob(job));
  return jobs;
}

export function isValidSavedFilter(value) {
  return value === SAVED_FILTER_ALL || value === SAVED_FILTER_CUSTOM || value === SAVED_FILTER_IMPORTED;
}

export function isValidSavedSort(value) {
  return value === SORT_UPDATED || value === SORT_SAVED || value === SORT_REMINDER || value === SORT_PERSONAL;
}

function getReminderWeight(reminderAt, { parseIsoDate, now = Date.now }) {
  const parsed = parseIsoDate(reminderAt);
  if (!parsed) return 3;
  const diff = parsed.getTime() - now();
  if (diff < 0) return 2;
  if (diff <= REMINDER_SOON_HOURS * 60 * 60 * 1000) return 0;
  return 1;
}

export function sortSavedJobs(jobs, mode, { parseIsoDate }) {
  const rows = Array.isArray(jobs) ? [...jobs] : [];
  const byKey = (a, b) => String(a?.jobKey || "").localeCompare(String(b?.jobKey || ""));
  const byUpdated = (a, b) => String(b.updatedAt || b.savedAt || "").localeCompare(String(a.updatedAt || a.savedAt || ""));
  const bySaved = (a, b) => String(b.savedAt || "").localeCompare(String(a.savedAt || ""));
  const byTitle = (a, b) => String(a.title || "").localeCompare(String(b.title || ""));

  if (mode === SORT_SAVED) {
    return rows.sort((a, b) => bySaved(a, b) || byTitle(a, b) || byKey(a, b));
  }
  if (mode === SORT_PERSONAL) {
    return rows.sort((a, b) => {
      const customA = isCustomJob(a) ? 0 : 1;
      const customB = isCustomJob(b) ? 0 : 1;
      if (customA !== customB) return customA - customB;
      return byUpdated(a, b) || byTitle(a, b) || byKey(a, b);
    });
  }
  if (mode === SORT_REMINDER) {
    return rows.sort((a, b) => {
      const reminderA = getReminderWeight(a.reminderAt, { parseIsoDate });
      const reminderB = getReminderWeight(b.reminderAt, { parseIsoDate });
      if (reminderA !== reminderB) return reminderA - reminderB;
      return byUpdated(a, b) || byTitle(a, b) || byKey(a, b);
    });
  }
  return rows.sort((a, b) => byUpdated(a, b) || byTitle(a, b) || byKey(a, b));
}
