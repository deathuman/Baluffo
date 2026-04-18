export function getLiveTaskWorkItems(payload) {
  if (Array.isArray(payload?.workItems)) return payload.workItems;
  return [];
}

export function getTaskStateRows(payload) {
  return Array.isArray(payload?.tasks) ? payload.tasks : [];
}

export function hasActiveTaskStateRows(payload) {
  return getTaskStateRows(payload).some(task => Boolean(task?.active));
}
