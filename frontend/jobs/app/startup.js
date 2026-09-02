import { resolveDesktopRuntimeMode } from "../../shared/local-data/runtime-context.js";

export function isDesktopRuntimeMode(href = window.location.href) {
  return resolveDesktopRuntimeMode(href);
}

export function scheduleNonCriticalStartup(windowObject, callback, {
  idleTimeout = 1500,
  fallbackDelayMs = 900
} = {}) {
  const run = () => {
    windowObject.setTimeout(() => {
      callback();
    }, 0);
  };
  if (typeof windowObject.requestIdleCallback === "function") {
    windowObject.requestIdleCallback(run, { timeout: idleTimeout });
    return;
  }
  windowObject.setTimeout(run, fallbackDelayMs);
}

export function parseJobsPageUrlState(search, {
  defaultFilters,
  normalizeLifecycleStatus
}) {
  const params = new URLSearchParams(search);
  const page = parseInt(params.get("page"), 10);

  return {
    currentPage: !Number.isNaN(page) && page > 0 ? page : 1,
    filters: {
      ...defaultFilters,
      workType: params.get("workType") || "",
      lifecycleStatus: normalizeLifecycleStatus(params.get("lifecycleStatus"), "active"),
      countries: Array.from(new Set(params.getAll("country").filter(Boolean))),
      city: params.get("city") || "",
      sector: params.get("sector") || "",
      profession: params.get("profession") || "",
      newOnly: params.get("newOnly") === "1",
      excludeInternship: params.get("excludeInternship") === "1",
      search: params.get("search") || "",
      sort: params.get("sort") || "relevance"
    }
  };
}

export function buildJobsPageUrl(pathname, state) {
  const params = new URLSearchParams();

  if (state.currentPage > 1) params.set("page", String(state.currentPage));
  if (state.filters.workType) params.set("workType", state.filters.workType);
  if (state.filters.lifecycleStatus && state.filters.lifecycleStatus !== "active") {
    params.set("lifecycleStatus", state.filters.lifecycleStatus);
  }
  state.filters.countries.forEach(country => params.append("country", country));
  if (state.filters.city) params.set("city", state.filters.city);
  if (state.filters.sector) params.set("sector", state.filters.sector);
  if (state.filters.profession) params.set("profession", state.filters.profession);
  if (state.filters.newOnly) params.set("newOnly", "1");
  if (state.filters.excludeInternship) params.set("excludeInternship", "1");
  if (state.filters.search) params.set("search", state.filters.search);
  if (state.filters.sort && state.filters.sort !== "relevance") params.set("sort", state.filters.sort);

  const query = params.toString();
  return query ? `${pathname}?${query}` : pathname;
}

export function getJobsLastUpdatedText(timestamp, now = Date.now()) {
  if (!timestamp || !Number.isFinite(Number(timestamp))) {
    return "";
  }

  const dt = new Date(Number(timestamp));
  if (Number.isNaN(dt.getTime())) {
    return "";
  }

  const time = dt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
  const reference = new Date(now);
  const startOfToday = new Date(reference.getFullYear(), reference.getMonth(), reference.getDate());
  const startOfYesterday = new Date(startOfToday.getTime() - 24 * 60 * 60 * 1000);
  const startOfYear = new Date(reference.getFullYear(), 0, 1);

  let dayPart = "";
  if (dt >= startOfToday) {
    dayPart = "";
  } else if (dt >= startOfYesterday) {
    dayPart = "Yesterday ";
  } else if (dt >= startOfYear) {
    dayPart = `${dt.toLocaleDateString([], { month: "short", day: "numeric" })} `;
  } else {
    dayPart = `${dt.toLocaleDateString([], { month: "short", day: "numeric", year: "numeric" })} `;
  }
  return `Last updated: ${dayPart}${time}`;
}

export function parseAutoRefreshSignal(rawValue) {
  if (!rawValue) return null;
  try {
    const parsed = JSON.parse(rawValue);
    if (!parsed || typeof parsed !== "object") return null;
    const signalId = String(parsed.id || "").trim();
    if (!signalId) return null;
    if (String(parsed.source || "").trim() !== "admin_fetcher") return null;
    return {
      id: signalId,
      finishedAt: String(parsed.finishedAt || "")
    };
  } catch {
    return null;
  }
}

export function getAutoRefreshStatusText(signal) {
  const completedAt = signal?.finishedAt ? new Date(signal.finishedAt) : null;
  const completedLabel = completedAt && !Number.isNaN(completedAt.getTime())
    ? completedAt.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" })
    : "";
  const statusTail = completedLabel ? ` (${completedLabel})` : "";
  return `New feed available from admin fetcher${statusTail}. Reloading jobs...`;
}
