function getDocumentTarget(documentTarget = globalThis?.document) {
  if (!documentTarget || !documentTarget.body || typeof documentTarget.createElement !== "function") {
    return null;
  }
  return documentTarget;
}

function isFocusableElement(value) {
  return Boolean(value && typeof value === "object" && typeof value.focus === "function");
}

function normalizeExternalUrl(value) {
  const text = String(value || "").trim();
  if (!text) return "";
  try {
    const parsed = new URL(text);
    return parsed.protocol === "http:" || parsed.protocol === "https:" ? parsed.toString() : "";
  } catch {
    return "";
  }
}

function appendPlainText(parent, doc, text) {
  const value = String(text || "");
  if (!value) return;
  if (typeof doc.createTextNode === "function") {
    parent.appendChild(doc.createTextNode(value));
    return;
  }
  const span = doc.createElement("span");
  span.textContent = value;
  parent.appendChild(span);
}

function openExternalUrlTarget(url, openExternalUrl, windowTarget) {
  if (!url) return null;
  if (typeof openExternalUrl === "function") {
    return openExternalUrl(url);
  }
  if (typeof windowTarget?.open === "function") {
    return windowTarget.open(url, "_blank", "noopener,noreferrer");
  }
  return null;
}

function createExternalLink(doc, url, label, openExternalUrl, windowTarget) {
  const safeUrl = normalizeExternalUrl(url);
  if (!safeUrl) return null;
  const anchor = doc.createElement("a");
  anchor.className = "release-notes-dialog-link";
  anchor.href = safeUrl;
  anchor.target = "_blank";
  anchor.rel = "noopener noreferrer";
  anchor.textContent = String(label || safeUrl);
  anchor.addEventListener("click", event => {
    if (!safeUrl) return;
    event.preventDefault();
    openExternalUrlTarget(safeUrl, openExternalUrl, windowTarget);
  });
  return anchor;
}

function appendInlineMarkdown(parent, text, { doc, openExternalUrl, windowTarget }) {
  const source = String(text || "");
  const tokenPattern = /(\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)|`([^`]+)`)/g;
  let cursor = 0;
  let match = tokenPattern.exec(source);
  while (match) {
    appendPlainText(parent, doc, source.slice(cursor, match.index));
    if (match[4] !== undefined) {
      const code = doc.createElement("code");
      code.textContent = match[4];
      parent.appendChild(code);
    } else {
      const anchor = createExternalLink(doc, match[3], match[2], openExternalUrl, windowTarget);
      if (anchor) {
        parent.appendChild(anchor);
      } else {
        appendPlainText(parent, doc, match[0]);
      }
    }
    cursor = tokenPattern.lastIndex;
    match = tokenPattern.exec(source);
  }
  appendPlainText(parent, doc, source.slice(cursor));
}

export function renderReleaseNotesMarkdown(container, markdown, options = {}) {
  const doc = options.documentTarget || container?.ownerDocument || globalThis?.document;
  if (!container || !doc || typeof doc.createElement !== "function") {
    return container;
  }
  const inlineOptions = {
    doc,
    openExternalUrl: options.openExternalUrl,
    windowTarget: options.windowTarget,
  };
  const lines = String(markdown || "").replace(/\r\n?/g, "\n").split("\n");
  let paragraphLines = [];
  let listEl = null;

  const flushParagraph = () => {
    if (!paragraphLines.length) return;
    const paragraph = doc.createElement("p");
    paragraph.className = "release-notes-dialog-paragraph";
    appendInlineMarkdown(paragraph, paragraphLines.join(" "), inlineOptions);
    container.appendChild(paragraph);
    paragraphLines = [];
  };

  const closeList = () => {
    listEl = null;
  };

  lines.forEach(line => {
    const trimmed = String(line || "").trim();
    if (!trimmed) {
      flushParagraph();
      closeList();
      return;
    }
    const headingMatch = trimmed.match(/^(#{1,3})\s+(.*)$/);
    if (headingMatch) {
      flushParagraph();
      closeList();
      const level = Math.max(3, Math.min(5, headingMatch[1].length + 2));
      const heading = doc.createElement(`h${level}`);
      heading.className = "release-notes-dialog-heading";
      appendInlineMarkdown(heading, headingMatch[2], inlineOptions);
      container.appendChild(heading);
      return;
    }
    const listMatch = trimmed.match(/^[-*]\s+(.*)$/);
    if (listMatch) {
      flushParagraph();
      if (!listEl) {
        listEl = doc.createElement("ul");
        listEl.className = "release-notes-dialog-list";
        container.appendChild(listEl);
      }
      const item = doc.createElement("li");
      item.className = "release-notes-dialog-list-item";
      appendInlineMarkdown(item, listMatch[1], inlineOptions);
      listEl.appendChild(item);
      return;
    }
    paragraphLines.push(trimmed);
  });

  flushParagraph();
  return container;
}

function formatPublishedAt(publishedAt) {
  const raw = String(publishedAt || "").trim();
  if (!raw) return "";
  const parsed = new Date(raw);
  if (Number.isNaN(parsed.getTime())) return raw;
  return parsed.toLocaleString([], {
    year: "numeric",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit"
  });
}

export function openReleaseNotesDialog({
  title = "Release notes",
  markdown = "",
  publishedAt = "",
  releaseNotesUrl = "",
  openExternalUrl,
  fallbackMessage = "Release notes are unavailable for this build.",
  documentTarget,
  windowTarget = globalThis?.window
} = {}) {
  const doc = getDocumentTarget(documentTarget);
  const safeReleaseNotesUrl = normalizeExternalUrl(releaseNotesUrl);
  if (!doc) {
    if (safeReleaseNotesUrl) {
      openExternalUrlTarget(safeReleaseNotesUrl, openExternalUrl, windowTarget);
    }
    return null;
  }

  const overlay = doc.createElement("div");
  overlay.className = "popup-overlay release-notes-dialog-overlay";
  overlay.dataset.releaseNotesDialog = "true";

  const panel = doc.createElement("div");
  panel.className = "popup release-notes-dialog";
  panel.setAttribute("role", "dialog");
  panel.setAttribute("aria-modal", "true");
  panel.setAttribute("aria-labelledby", "release-notes-dialog-title");

  const heading = doc.createElement("h2");
  heading.id = "release-notes-dialog-title";
  heading.className = "release-notes-dialog-title";
  heading.textContent = String(title || "Release notes");

  const publishedAtText = formatPublishedAt(publishedAt);
  const publishedAtEl = doc.createElement("p");
  publishedAtEl.className = "release-notes-dialog-published-at";
  publishedAtEl.textContent = publishedAtText ? `Published ${publishedAtText}` : "";

  const body = doc.createElement("div");
  body.className = "release-notes-dialog-body";

  if (String(markdown || "").trim()) {
    renderReleaseNotesMarkdown(body, markdown, {
      documentTarget: doc,
      openExternalUrl,
      windowTarget,
    });
  } else {
    const emptyState = doc.createElement("p");
    emptyState.className = "release-notes-dialog-empty";
    emptyState.textContent = String(fallbackMessage || "Release notes are unavailable for this build.");
    body.appendChild(emptyState);
  }

  const actions = doc.createElement("div");
  actions.className = "release-notes-dialog-actions";

  const closeBtn = doc.createElement("button");
  closeBtn.type = "button";
  closeBtn.className = "btn back-btn release-notes-dialog-close";
  closeBtn.textContent = "Close";

  const openBtn = doc.createElement("button");
  openBtn.type = "button";
  openBtn.className = "btn back-btn release-notes-dialog-open";
  openBtn.textContent = "Open on GitHub";

  let finished = false;
  const previousActiveElement = isFocusableElement(doc.activeElement) ? doc.activeElement : null;

  function cleanup() {
    if (finished) return;
    finished = true;
    doc.removeEventListener("keydown", onKeyDown, true);
    overlay.remove();
    if (previousActiveElement && (typeof doc.contains !== "function" || doc.contains(previousActiveElement))) {
      try {
        previousActiveElement.focus({ preventScroll: true });
      } catch {
        previousActiveElement.focus();
      }
    }
  }

  function onKeyDown(event) {
    if (event.key !== "Escape") return;
    event.preventDefault();
    cleanup();
  }

  closeBtn.addEventListener("click", cleanup);
  overlay.addEventListener("click", event => {
    if (event.target === overlay) {
      cleanup();
    }
  });
  doc.addEventListener("keydown", onKeyDown, true);

  actions.appendChild(closeBtn);
  if (safeReleaseNotesUrl) {
    openBtn.addEventListener("click", () => {
      openExternalUrlTarget(safeReleaseNotesUrl, openExternalUrl, windowTarget);
    });
    actions.appendChild(openBtn);
  }

  panel.appendChild(heading);
  if (publishedAtText) {
    panel.appendChild(publishedAtEl);
  }
  panel.appendChild(body);
  panel.appendChild(actions);
  overlay.appendChild(panel);
  doc.body.appendChild(overlay);

  const focusClose = () => {
    try {
      closeBtn.focus({ preventScroll: true });
    } catch {
      closeBtn.focus();
    }
  };
  if (typeof windowTarget?.requestAnimationFrame === "function") {
    windowTarget.requestAnimationFrame(focusClose);
  } else {
    setTimeout(focusClose, 0);
  }

  return {
    close: cleanup,
    overlay,
    panel,
    body,
  };
}
