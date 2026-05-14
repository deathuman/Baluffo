const CONTROLLER_KEY = "__baluffoTooltipController";
const TOOLTIP_ID = "baluffo-global-tooltip";

function getTooltipText(target) {
  return String(
    target?.getAttribute?.("data-tooltip")
    || target?.dataset?.tooltip
    || ""
  ).trim();
}

function hasTooltip(target) {
  return Boolean(getTooltipText(target));
}

function closestTooltipTarget(start, root) {
  let node = start;
  while (node && node !== root) {
    if (node.nodeType === 1 && hasTooltip(node)) return node;
    node = node.parentElement || node.parentNode;
  }
  return null;
}

function eventTarget(event, documentTarget) {
  const path = typeof event?.composedPath === "function" ? event.composedPath() : [];
  return path[0] || event?.target || documentTarget?.activeElement || null;
}

function viewportSize(windowTarget, documentTarget) {
  return {
    width: Number(windowTarget?.innerWidth || documentTarget?.documentElement?.clientWidth || 1024),
    height: Number(windowTarget?.innerHeight || documentTarget?.documentElement?.clientHeight || 768),
  };
}

function measuredRect(el, fallbackWidth = 220, fallbackHeight = 42) {
  if (typeof el?.getBoundingClientRect === "function") {
    const rect = el.getBoundingClientRect();
    const width = Number(rect?.width || (Number(rect?.right) - Number(rect?.left)) || fallbackWidth);
    const height = Number(rect?.height || (Number(rect?.bottom) - Number(rect?.top)) || fallbackHeight);
    return {
      left: Number(rect?.left || 0),
      top: Number(rect?.top || 0),
      right: Number(rect?.right || Number(rect?.left || 0) + width),
      bottom: Number(rect?.bottom || Number(rect?.top || 0) + height),
      width,
      height,
    };
  }
  return { left: 0, top: 0, right: fallbackWidth, bottom: fallbackHeight, width: fallbackWidth, height: fallbackHeight };
}

function clamp(value, min, max) {
  if (max < min) return min;
  return Math.min(Math.max(value, min), max);
}

export function installGlobalTooltipController({
  documentTarget = globalThis.document,
  windowTarget = globalThis.window,
} = {}) {
  const doc = documentTarget;
  if (!doc?.body) return null;
  if (doc[CONTROLLER_KEY]) return doc[CONTROLLER_KEY];

  let portal = null;
  let activeTarget = null;
  let previousDescribedBy = null;
  let mutationObserver = null;

  function ensurePortal() {
    if (portal?.parentNode) return portal;
    portal = doc.createElement("div");
    portal.id = TOOLTIP_ID;
    portal.className = "baluffo-tooltip-portal";
    portal.setAttribute("role", "tooltip");
    portal.setAttribute("aria-hidden", "true");
    doc.body.appendChild(portal);
    return portal;
  }

  function restoreDescribedBy() {
    if (!activeTarget) return;
    if (previousDescribedBy) {
      activeTarget.setAttribute?.("aria-describedby", previousDescribedBy);
    } else {
      activeTarget.removeAttribute?.("aria-describedby");
    }
  }

  function applyDescribedBy(target) {
    previousDescribedBy = target.getAttribute?.("aria-describedby") || "";
    const ids = previousDescribedBy.split(/\s+/).filter(Boolean);
    if (!ids.includes(TOOLTIP_ID)) ids.push(TOOLTIP_ID);
    target.setAttribute?.("aria-describedby", ids.join(" "));
  }

  function positionTooltip(target) {
    if (!portal) return;
    const targetRect = measuredRect(target, 0, 0);
    const portalRect = measuredRect(portal);
    const viewport = viewportSize(windowTarget, doc);
    const margin = 12;
    const gap = 10;
    const width = Math.min(portalRect.width, Math.max(0, viewport.width - margin * 2));
    const height = portalRect.height;
    let placement = "top";
    let top = targetRect.top - height - gap;
    if (top < margin) {
      placement = "bottom";
      top = targetRect.bottom + gap;
    }
    top = clamp(top, margin, viewport.height - height - margin);
    const left = clamp(
      targetRect.left + targetRect.width / 2 - width / 2,
      margin,
      viewport.width - width - margin
    );
    portal.dataset.placement = placement;
    portal.classList?.toggle?.("baluffo-tooltip-bottom", placement === "bottom");
    portal.style.left = `${Math.round(left)}px`;
    portal.style.top = `${Math.round(top)}px`;
  }

  function hideTooltip() {
    if (!activeTarget || !portal) return;
    restoreDescribedBy();
    portal.classList?.remove?.("visible");
    portal.setAttribute("aria-hidden", "true");
    portal.textContent = "";
    activeTarget = null;
    previousDescribedBy = null;
  }

  function showTooltip(target) {
    if (!target || !hasTooltip(target)) {
      hideTooltip();
      return;
    }
    const sameTarget = activeTarget === target;
    if (activeTarget && !sameTarget) hideTooltip();
    activeTarget = target;
    const node = ensurePortal();
    node.textContent = getTooltipText(target);
    node.setAttribute("aria-hidden", "false");
    if (!sameTarget) applyDescribedBy(target);
    positionTooltip(target);
    windowTarget?.requestAnimationFrame?.(() => {
      if (activeTarget === target) node.classList?.add?.("visible");
    }) || node.classList?.add?.("visible");
  }

  function handleShow(event) {
    const target = closestTooltipTarget(eventTarget(event, doc), doc);
    if (target) showTooltip(target);
  }

  function handleHide(event) {
    if (!activeTarget) return;
    const nextTarget = closestTooltipTarget(event?.relatedTarget || null, doc);
    if (nextTarget === activeTarget) return;
    hideTooltip();
  }

  function handleKeydown(event) {
    if (event?.key === "Escape") hideTooltip();
  }

  function handleViewportChange() {
    if (!activeTarget) return;
    if (typeof doc.contains === "function" && !doc.contains(activeTarget)) {
      hideTooltip();
      return;
    }
    positionTooltip(activeTarget);
  }

  doc.addEventListener?.("pointerover", handleShow, true);
  doc.addEventListener?.("pointerout", handleHide, true);
  doc.addEventListener?.("focusin", handleShow, true);
  doc.addEventListener?.("focusout", handleHide, true);
  doc.addEventListener?.("keydown", handleKeydown, true);
  windowTarget?.addEventListener?.("scroll", handleViewportChange, true);
  windowTarget?.addEventListener?.("resize", handleViewportChange);

  const MutationObserverCtor = windowTarget?.MutationObserver || globalThis.MutationObserver;
  if (MutationObserverCtor) {
    mutationObserver = new MutationObserverCtor(handleViewportChange);
    mutationObserver.observe(doc.body, { childList: true, subtree: true });
  }

  const controller = {
    show: showTooltip,
    hide: hideTooltip,
    destroy() {
      hideTooltip();
      doc.removeEventListener?.("pointerover", handleShow, true);
      doc.removeEventListener?.("pointerout", handleHide, true);
      doc.removeEventListener?.("focusin", handleShow, true);
      doc.removeEventListener?.("focusout", handleHide, true);
      doc.removeEventListener?.("keydown", handleKeydown, true);
      windowTarget?.removeEventListener?.("scroll", handleViewportChange, true);
      windowTarget?.removeEventListener?.("resize", handleViewportChange);
      mutationObserver?.disconnect?.();
      portal?.remove?.();
      portal = null;
      doc[CONTROLLER_KEY] = null;
    },
  };
  doc[CONTROLLER_KEY] = controller;
  return controller;
}
