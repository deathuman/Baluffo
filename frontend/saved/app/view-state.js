export {
  REMINDER_SOON_HOURS,
  SAVED_FILTER_ALL,
  SORT_UPDATED,
  buildSavedJobViewModel,
  filterSavedJobViews,
  isCustomJob,
  isValidSavedFilter,
  isValidSavedSort,
  sortSavedJobViews
} from "./view-model.js";

import {
  buildSavedJobViewModel,
  filterSavedJobViews,
  sortSavedJobViews
} from "./view-model.js";

export function filterSavedJobs(jobs, filter) {
  const views = (Array.isArray(jobs) ? jobs : []).map(job => buildSavedJobViewModel(job));
  return filterSavedJobViews(views, filter).map(view => view.job);
}

export function sortSavedJobs(jobs, mode, { parseIsoDate }) {
  const views = (Array.isArray(jobs) ? jobs : []).map(job => buildSavedJobViewModel(job, { parseIsoDate }));
  return sortSavedJobViews(views, mode).map(view => view.job);
}
