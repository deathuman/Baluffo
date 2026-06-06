function safeFormatLogTimestamp(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  const parsed = Date.parse(text);
  if (!Number.isFinite(parsed)) return "";
  return new Date(parsed).toLocaleTimeString();
}

export function appendAdminLogRow(container, event, options = {}) {
  if (!container) return;
  const maxRows = Number(options.maxRows || 80);
  const normalizeLogLevel = options.normalizeLogLevel || (value => value);
  const toLocalTime = options.toLocalTime || safeFormatLogTimestamp;
  const formatLogEventText = options.formatLogEventText || (row => String(row?.message || ""));

  const row = document.createElement("div");
  row.className = `admin-fetcher-line ${normalizeLogLevel(event.level)}`;
  row.dataset.timestamp = event.timestamp;
  row.dataset.level = event.level;
  row.dataset.scope = event.scope;
  row.dataset.sourceId = event.sourceId;

  const stamp = document.createElement("span");
  stamp.className = "admin-fetcher-time";
  const parsedTimestamp = new Date(event.timestamp || "");
  const timestampText = options.toLocalTime
    ? (Number.isNaN(parsedTimestamp.getTime()) ? safeFormatLogTimestamp(event.timestamp) : toLocalTime(parsedTimestamp))
    : toLocalTime(event.timestamp);
  stamp.textContent = timestampText;

  const text = document.createElement("span");
  text.className = "admin-fetcher-text";
  text.textContent = formatLogEventText(event);

  row.append(stamp, text);

  const normalizedLevel = normalizeLogLevel(event.level);
  if (normalizedLevel === "error" || normalizedLevel === "warn") {
    const detailText = JSON.stringify({
      level: normalizedLevel,
      scope: event.scope,
      sourceId: event.sourceId,
      message: formatLogEventText(event),
      timestamp: event.timestamp
    }, null, 2);
    let detail = null;

    row.setAttribute("role", "button");
    row.setAttribute("tabindex", "0");
    row.setAttribute("aria-expanded", "false");
    row.addEventListener("click", () => {
      const expanded = row.classList.toggle("expanded");
      row.setAttribute("aria-expanded", expanded ? "true" : "false");
      if (expanded && !detail) {
        detail = document.createElement("div");
        detail.className = "fetcher-log-detail";
        detail.textContent = detailText;
        row.appendChild(detail);
      }
    });
  }
  container.appendChild(row);

  while (container.children.length > maxRows) {
    container.removeChild(container.firstChild);
  }
  container.scrollTop = container.scrollHeight;
}
