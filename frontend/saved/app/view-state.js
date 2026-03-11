export const SAVED_FILTER_ALL = "all";
export const SAVED_FILTER_CUSTOM = "custom";
export const SAVED_FILTER_IMPORTED = "imported";
export const SORT_UPDATED = "updated";
export const SORT_SAVED = "saved";
export const SORT_REMINDER = "reminder";
export const SORT_PERSONAL = "personal";

export function isCustomJob(job) {
  return Boolean(job && job.isCustom);
}

export function filterSavedJobs(jobs, filter) {
  if (!Array.isArray(jobs)) return [];
  if (filter === SAVED_FILTER_CUSTOM) return jobs.filter(isCustomJob);
  if (filter === SAVED_FILTER_IMPORTED) return jobs.filter(job => !isCustomJob(job));
  return jobs;
}
