function safeFormatLogTimestamp(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  const parsed = Date.parse(text);
  if (!Number.isFinite(parsed)) return "";
  return new Date(parsed).toLocaleTimeString();
}

export function appendAdminLogRow(container, event, options = {}) {
  if (!container) return;
  const maxRows = Number(options.maxRows || 220);
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
  container.appendChild(row);

  while (container.children.length > maxRows) {
    container.removeChild(container.firstChild);
  }
  container.scrollTop = container.scrollHeight;
}
