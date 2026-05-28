const PRESETS_KEY = "baluffo_jobs_filter_presets";
const RECENT_VIEWS_KEY = "baluffo_jobs_recent_views";
const MAX_PRESETS = 10;
const MAX_RECENT = 10;

function readJson(key, fallback) {
  try {
    const raw = localStorage.getItem(key);
    if (!raw) return fallback;
    const parsed = JSON.parse(raw);
    return parsed && typeof parsed === "object" ? parsed : fallback;
  } catch {
    return fallback;
  }
}

function writeJson(key, value) {
  try {
    localStorage.setItem(key, JSON.stringify(value));
  } catch {
    // localStorage unavailable
  }
}

function stripDefaultFilters(state, defaultFilters) {
  const f = state?.filters || {};
  const d = defaultFilters || {};
  const cleaned = {};
  if (f.workType && f.workType !== d.workType) cleaned.workType = f.workType;
  if (f.lifecycleStatus && f.lifecycleStatus !== (d.lifecycleStatus || "active"))
    cleaned.lifecycleStatus = f.lifecycleStatus;
  if (Array.isArray(f.countries) && f.countries.length > 0) cleaned.countries = f.countries.slice();
  if (f.city) cleaned.city = f.city;
  if (f.sector && f.sector !== d.sector) cleaned.sector = f.sector;
  if (f.profession) cleaned.profession = f.profession;
  if (f.newOnly) cleaned.newOnly = true;
  if (f.excludeInternship) cleaned.excludeInternship = true;
  if (f.search) cleaned.search = f.search;
  if (f.sort && f.sort !== "relevance" && f.sort !== d.sort) cleaned.sort = f.sort;
  return cleaned;
}

function applyPresetFilters(preset, currentState) {
  const next = {
    ...currentState,
    currentPage: 1,
    filters: { ...currentState.filters }
  };
  const p = preset || {};
  if (p.workType !== undefined) next.filters.workType = p.workType;
  if (p.lifecycleStatus !== undefined) next.filters.lifecycleStatus = p.lifecycleStatus;
  if (Array.isArray(p.countries)) next.filters.countries = p.countries;
  if (p.city !== undefined) next.filters.city = p.city;
  if (p.sector !== undefined) next.filters.sector = p.sector;
  if (p.profession !== undefined) next.filters.profession = p.profession;
  if (p.newOnly !== undefined) next.filters.newOnly = Boolean(p.newOnly);
  if (p.excludeInternship !== undefined) next.filters.excludeInternship = Boolean(p.excludeInternship);
  if (p.search !== undefined) next.filters.search = p.search;
  if (p.sort !== undefined) next.filters.sort = p.sort;
  return next;
}

export function saveFilterPreset(name, state, defaultFilters) {
  const presets = readJson(PRESETS_KEY, {});
  const cleanName = String(name || "").trim();
  if (!cleanName) return false;
  if (Object.keys(presets).length >= MAX_PRESETS) {
    const oldest = Object.entries(presets)
      .sort(([, a], [, b]) => (a?.savedAt || 0) - (b?.savedAt || 0))[0];
    if (oldest) delete presets[oldest[0]];
  }
  presets[cleanName] = {
    filters: stripDefaultFilters(state, defaultFilters),
    savedAt: Date.now(),
    label: cleanName
  };
  writeJson(PRESETS_KEY, presets);
  return true;
}

export function loadFilterPreset(name) {
  const presets = readJson(PRESETS_KEY, {});
  const entry = presets[String(name || "").trim()];
  return entry?.filters || null;
}

export function deleteFilterPreset(name) {
  const presets = readJson(PRESETS_KEY, {});
  const cleanName = String(name || "").trim();
  if (!presets[cleanName]) return false;
  delete presets[cleanName];
  writeJson(PRESETS_KEY, presets);
  return true;
}

export function listFilterPresets() {
  const presets = readJson(PRESETS_KEY, {});
  return Object.entries(presets)
    .map(([name, entry]) => ({
      name,
      label: String(entry?.label || name),
      savedAt: Number(entry?.savedAt || 0),
      filters: entry?.filters || {}
    }))
    .sort((a, b) => b.savedAt - a.savedAt);
}

export function applyFilterPreset(name, currentState) {
  const filters = loadFilterPreset(name);
  if (!filters) return null;
  return applyPresetFilters(filters, currentState);
}

export function recordRecentView(url, label, context = "jobs") {
  const views = readJson(RECENT_VIEWS_KEY, []);
  if (!Array.isArray(views)) return;
  const entry = {
    url: String(url || ""),
    label: String(label || url || "").slice(0, 80),
    context: String(context || "jobs"),
    visitedAt: Date.now()
  };
  const filtered = views.filter(v => v && v.url !== entry.url);
  filtered.unshift(entry);
  if (filtered.length > MAX_RECENT) filtered.length = MAX_RECENT;
  writeJson(RECENT_VIEWS_KEY, filtered);
}

export function getRecentViews(limit = 5) {
  const views = readJson(RECENT_VIEWS_KEY, []);
  if (!Array.isArray(views)) return [];
  return views.slice(0, Math.max(1, Number(limit) || 5));
}

export function clearRecentViews() {
  writeJson(RECENT_VIEWS_KEY, []);
}

export { applyPresetFilters, stripDefaultFilters };
