export {
  REMINDER_SOON_HOURS,
  SAVED_FILTER_ALL,
  SAVED_GROUP_NONE,
  SAVED_GROUP_STAGE,
  SORT_UPDATED,
  buildSavedJobViewModel,
  filterSavedJobViews,
  groupSavedJobViews,
  isCustomJob,
  isValidSavedFilter,
  isValidSavedGroup,
  isValidSavedSort,
  normalizeSavedGroup,
  sortSavedJobViews
} from "./view-model.js";

import {
  buildSavedJobViewModel,
  filterSavedJobViews,
  groupSavedJobViews,
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

export function groupSavedJobs(jobs, mode, { parseIsoDate } = {}) {
  const views = (Array.isArray(jobs) ? jobs : []).map(job => buildSavedJobViewModel(job, { parseIsoDate }));
  return groupSavedJobViews(views, mode).map(group => ({
    key: group.key,
    label: group.label,
    count: group.count,
    jobs: group.views.map(view => view.job)
  }));
}
