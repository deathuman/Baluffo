import { presentPopup } from "../shared/ui/popup-presentation.js";
import { createModalCleanup, escapeKeyDismissHandler, focusWithPreventScroll } from "../shared/ui-helpers.js";

function getDocumentTarget() {
  if (typeof document === "undefined" || !document || !document.body) {
    return null;
  }
  return document;
}

function isFocusableElement(value) {
  return Boolean(value && typeof value === "object" && typeof value.focus === "function");
}

function normalizeExistingProfiles(existingProfiles) {
  const seen = new Set();
  return (Array.isArray(existingProfiles) ? existingProfiles : [])
    .map(profile => {
      const name = String(profile?.name || profile?.displayName || "").trim();
      if (!name) return null;
      const normalizedName = name.toLowerCase();
      if (seen.has(normalizedName)) return null;
      seen.add(normalizedName);
      return {
        ...profile,
        name
      };
    })
    .filter(Boolean);
}

function buildProfilePromptLabel(existingProfiles) {
  const profiles = normalizeExistingProfiles(existingProfiles);
  return profiles.length
    ? `Sign in with a local profile. Existing profiles: ${profiles.map(profile => profile?.name).join(", ")}`
    : "Create a local profile to sign in.";
}

function buildModalButtonClassName(variant, ...names) {
  return ["btn", "back-btn", variant, ...names].filter(Boolean).join(" ");
}

function setModeToggleButtonClassName(button, createMode) {
  if (!button) return;
  button.className = buildModalButtonClassName(
    createMode ? "popup-btn-tertiary" : "popup-btn-secondary",
    "local-auth-dialog-secondary"
  );
}

// The four prompts below differ only in their body and in what a dismissal
// resolves to; the overlay, panel, heading, description, teardown and focus
// handoff used to be copied into each one. This is that shared frame, and every
// prompt keeps the `local-auth-dialog-*` handles it shipped with.
function createDialogFrame(doc, { title, description }) {
  const overlay = doc.createElement("div");
  overlay.className = "popup-overlay local-auth-dialog-overlay";
  overlay.dataset.localAuthDialog = "true";

  const panel = doc.createElement("div");
  panel.className = "popup local-auth-dialog";
  panel.setAttribute("role", "dialog");
  panel.setAttribute("aria-modal", "true");
  panel.setAttribute("aria-labelledby", "local-auth-dialog-title");

  const heading = doc.createElement("h2");
  heading.id = "local-auth-dialog-title";
  heading.className = "local-auth-dialog-title";
  heading.textContent = title;

  const descriptionEl = doc.createElement("p");
  descriptionEl.className = "local-auth-dialog-description";
  descriptionEl.textContent = description;

  return { overlay, panel, heading, descriptionEl };
}

function createDialogActions(doc) {
  const actions = doc.createElement("div");
  actions.className = "local-auth-dialog-actions";
  return actions;
}

function createDialogButton(doc, { id, className, variant, text, type = "button" }) {
  const button = doc.createElement("button");
  button.id = id;
  button.className = buildModalButtonClassName(variant, className);
  button.type = type;
  button.textContent = text;
  return button;
}

function createDialogLabel(doc, { htmlFor, text }) {
  const label = doc.createElement("label");
  label.className = "local-auth-dialog-label";
  label.setAttribute("for", htmlFor);
  label.textContent = text;
  return label;
}

function createDialogInput(doc, { required, placeholder, value = "" }) {
  const input = doc.createElement("input");
  input.id = "local-auth-name-input";
  input.className = "local-auth-dialog-input";
  input.name = "profileName";
  input.type = "text";
  input.maxLength = 120;
  input.autocomplete = "off";
  input.required = required;
  input.value = value;
  input.placeholder = placeholder;
  return input;
}

function scheduleFocus(callback) {
  if (typeof window.requestAnimationFrame === "function") {
    window.requestAnimationFrame(callback);
  } else {
    setTimeout(callback, 0);
  }
}

// Mounts a built frame: teardown, Escape/overlay dismissal, popup presentation
// and the deferred focus handoff. `dismissResult` is what a dismissal resolves
// to, which differs by prompt -- `null` for a text answer, `false` for a
// confirmation. Returns the one-shot cleanup the body's own buttons call.
function mountDialog({ doc, frame, body, onDone, dismissResult = null, keyHandler, focusTarget }) {
  let onKeyDown;
  const cleanup = createModalCleanup({
    doc,
    overlay: frame.overlay,
    previousActiveElement: isFocusableElement(doc.activeElement) ? doc.activeElement : null,
    getKeyHandler: () => onKeyDown,
    onDone
  });

  onKeyDown = keyHandler
    ? keyHandler(cleanup)
    : escapeKeyDismissHandler(() => cleanup(dismissResult));

  frame.overlay.addEventListener("click", event => {
    if (event.target === frame.overlay) {
      cleanup(dismissResult);
    }
  });
  doc.addEventListener("keydown", onKeyDown, true);

  frame.panel.append(frame.heading, frame.descriptionEl, ...body);
  frame.overlay.appendChild(frame.panel);
  doc.body.appendChild(frame.overlay);
  presentPopup(frame.overlay, frame.panel, { windowTarget: doc.defaultView });
  if (focusTarget) {
    scheduleFocus(() => focusTarget(cleanup));
  }
  return cleanup;
}

export async function requestTextInputDialog({
  title = "Sign in",
  description = "",
  label = "Value",
  submitLabel = "Continue",
  existingProfiles = [],
  defaultValue = "",
  placeholder = "Enter profile name",
  required = true
} = {}) {
  const doc = getDocumentTarget();
  if (!doc) {
    const fallbackPrompt = typeof globalThis?.window?.prompt === "function" ? globalThis.window.prompt.bind(globalThis.window) : null;
    if (!fallbackPrompt) return null;
    const fallbackLabel = [title, description || buildProfilePromptLabel(existingProfiles)].filter(Boolean).join("\n\n");
    const fallbackValue = fallbackPrompt(fallbackLabel, String(defaultValue || ""));
    return typeof fallbackValue === "string" ? fallbackValue : null;
  }
  return new Promise(resolve => {
    const frame = createDialogFrame(doc, {
      title: String(title || "Sign in"),
      description: String(description || buildProfilePromptLabel(existingProfiles))
    });

    const form = doc.createElement("form");
    form.className = "local-auth-dialog-form";

    const listId = "local-auth-profile-list";
    const profileNames = normalizeExistingProfiles(existingProfiles)
      .map(profile => String(profile?.name || "").trim())
      .filter(Boolean);
    if (profileNames.length) {
      const datalist = doc.createElement("datalist");
      datalist.id = listId;
      profileNames.forEach(name => {
        const option = doc.createElement("option");
        option.value = name;
        datalist.appendChild(option);
      });
      form.appendChild(datalist);
    }

    const input = createDialogInput(doc, {
      required: Boolean(required),
      placeholder: String(placeholder || ""),
      value: String(defaultValue || "")
    });
    if (profileNames.length) {
      input.setAttribute("list", listId);
    }

    const actions = createDialogActions(doc);
    const cancelBtn = createDialogButton(doc, {
      id: "local-auth-cancel-btn",
      className: "local-auth-dialog-cancel",
      variant: "popup-btn-secondary",
      text: "Cancel"
    });
    const submitBtn = createDialogButton(doc, {
      id: "local-auth-submit-btn",
      className: "local-auth-dialog-submit",
      variant: "popup-btn-primary",
      text: String(submitLabel || "Continue"),
      type: "submit"
    });

    actions.append(cancelBtn, submitBtn);
    form.append(
      createDialogLabel(doc, { htmlFor: "local-auth-name-input", text: String(label || "Value") }),
      input,
      actions
    );

    const cleanup = mountDialog({
      doc,
      frame,
      body: [form],
      onDone: resolve,
      focusTarget: () => {
        focusWithPreventScroll(input);
        input.select();
      }
    });

    cancelBtn.addEventListener("click", () => cleanup(null));
    form.addEventListener("submit", event => {
      event.preventDefault();
      cleanup(input.value);
    });
  });
}

export async function requestProfileName({
  title = "Sign in",
  description = "",
  existingProfiles = [],
  defaultValue = ""
} = {}) {
  const profiles = normalizeExistingProfiles(existingProfiles);
  if (!profiles.length) {
    return requestTextInputDialog({
      title,
      description,
      label: "Profile name",
      submitLabel: "Continue",
      existingProfiles,
      defaultValue
    });
  }

  const doc = getDocumentTarget();
  if (!doc) {
    const fallbackPrompt = typeof globalThis?.window?.prompt === "function"
      ? globalThis.window.prompt.bind(globalThis.window)
      : null;
    if (!fallbackPrompt) return null;
    const fallbackLabel = [title, description || buildProfilePromptLabel(profiles)]
      .filter(Boolean)
      .join("\n\n");
    const matchedDefault = profiles.find(
      profile => profile.name.toLowerCase() === String(defaultValue || "").trim().toLowerCase()
    );
    const initialValue = String(
      matchedDefault?.name
      || profiles.find(profile => Boolean(profile?.isCurrent))?.name
      || profiles[0]?.name
      || ""
    );
    const fallbackValue = fallbackPrompt(fallbackLabel, initialValue);
    return typeof fallbackValue === "string" ? fallbackValue : null;
  }

  return new Promise(resolve => {
    const frame = createDialogFrame(doc, {
      title: String(title || "Sign in"),
      description: String(description || buildProfilePromptLabel(profiles))
    });

    const form = doc.createElement("form");
    form.className = "local-auth-dialog-form";

    const helperEl = doc.createElement("p");
    helperEl.className = "local-auth-dialog-helper";

    const stack = doc.createElement("div");
    stack.className = "local-auth-dialog-stack";

    const existingLabel = createDialogLabel(doc, {
      htmlFor: "local-auth-profile-select",
      text: "Choose profile"
    });

    const selectEl = doc.createElement("select");
    selectEl.id = "local-auth-profile-select";
    selectEl.className = "local-auth-dialog-select";
    selectEl.name = "existingProfile";

    profiles.forEach(profile => {
      const option = doc.createElement("option");
      option.value = profile.name;
      option.textContent = profile.name;
      selectEl.appendChild(option);
    });

    const createLabel = createDialogLabel(doc, {
      htmlFor: "local-auth-name-input",
      text: "New profile name"
    });

    const inputEl = createDialogInput(doc, { required: true, placeholder: "Enter new profile name" });

    const actions = createDialogActions(doc);

    const actionCluster = doc.createElement("div");
    actionCluster.className = "local-auth-dialog-action-cluster";

    const createToggleBtn = createDialogButton(doc, {
      id: "local-auth-create-btn",
      className: "local-auth-dialog-secondary",
      variant: "popup-btn-secondary",
      text: ""
    });
    const cancelBtn = createDialogButton(doc, {
      id: "local-auth-cancel-btn",
      className: "local-auth-dialog-cancel",
      variant: "popup-btn-secondary",
      text: "Cancel"
    });
    const submitBtn = createDialogButton(doc, {
      id: "local-auth-submit-btn",
      className: "local-auth-dialog-submit",
      variant: "popup-btn-primary",
      text: "",
      type: "submit"
    });

    const requestedDefault = String(defaultValue || "").trim();
    const matchedDefault = profiles.find(
      profile => profile.name.toLowerCase() === requestedDefault.toLowerCase()
    );
    let createMode = Boolean(requestedDefault) && !matchedDefault;
    let selectedProfileName = String(
      matchedDefault?.name
      || profiles.find(profile => Boolean(profile?.isCurrent))?.name
      || profiles[0]?.name
      || ""
    );
    let newProfileName = createMode ? requestedDefault : "";

    if (selectedProfileName) {
      selectEl.value = selectedProfileName;
    }
    inputEl.value = newProfileName;

    function focusCurrentField() {
      const target = createMode ? inputEl : selectEl;
      focusWithPreventScroll(target);
      if (createMode && typeof inputEl.select === "function") {
        inputEl.select();
      }
    }

    function renderMode() {
      stack.replaceChildren();
      if (createMode) {
        actions.className = "local-auth-dialog-actions local-auth-dialog-actions-create-mode";
        setModeToggleButtonClassName(createToggleBtn, true);
        helperEl.textContent = "Create a new local profile for this device.";
        form.prepend(helperEl);
        stack.append(createLabel, inputEl);
        createToggleBtn.textContent = "Use existing profile";
        submitBtn.textContent = "Create profile";
      } else {
        actions.className = "local-auth-dialog-actions";
        setModeToggleButtonClassName(createToggleBtn, false);
        helperEl.remove();
        stack.append(existingLabel, selectEl);
        createToggleBtn.textContent = "Create new profile";
        submitBtn.textContent = "Continue";
      }
      queueMicrotask(focusCurrentField);
    }

    actionCluster.append(cancelBtn, submitBtn);
    actions.append(createToggleBtn, actionCluster);
    form.append(helperEl, stack, actions);

    const cleanup = mountDialog({ doc, frame, body: [form], onDone: resolve });

    createToggleBtn.addEventListener("click", () => {
      if (createMode) {
        newProfileName = inputEl.value;
      } else {
        selectedProfileName = selectEl.value;
      }
      createMode = !createMode;
      if (createMode) {
        inputEl.value = newProfileName || "";
      } else if (selectedProfileName) {
        selectEl.value = selectedProfileName;
      }
      renderMode();
    });
    cancelBtn.addEventListener("click", () => cleanup(null));
    form.addEventListener("submit", event => {
      event.preventDefault();
      cleanup(createMode ? inputEl.value : selectEl.value);
    });
    renderMode();
  });
}

export async function requestProfileLoadFailureAction({
  title = "Sign in",
  description = ""
} = {}) {
  const fallbackDescription = String(
    description
    || "Could not load existing local profiles. Retry to load them again, create a new local profile, or cancel sign-in."
  );
  const doc = getDocumentTarget();
  if (!doc) {
    const fallbackPrompt = typeof globalThis?.window?.prompt === "function"
      ? globalThis.window.prompt.bind(globalThis.window)
      : null;
    if (!fallbackPrompt) return null;
    const fallbackLabel = [
      title,
      fallbackDescription,
      "Type retry to try again, create to create a new profile, or cancel to abort."
    ].filter(Boolean).join("\n\n");
    const fallbackValue = String(fallbackPrompt(fallbackLabel, "retry") || "").trim().toLowerCase();
    if (fallbackValue === "retry") return "retry";
    if (fallbackValue === "create") return "create";
    return null;
  }
  return new Promise(resolve => {
    const frame = createDialogFrame(doc, {
      title: String(title || "Sign in"),
      description: fallbackDescription
    });

    const actions = createDialogActions(doc);
    const cancelBtn = createDialogButton(doc, {
      id: "local-auth-cancel-btn",
      className: "local-auth-dialog-cancel",
      variant: "popup-btn-secondary",
      text: "Cancel"
    });
    const retryBtn = createDialogButton(doc, {
      id: "local-auth-retry-btn",
      className: "local-auth-dialog-secondary",
      variant: "popup-btn-secondary",
      text: "Retry"
    });
    const createBtn = createDialogButton(doc, {
      id: "local-auth-create-fallback-btn",
      className: "local-auth-dialog-submit",
      variant: "popup-btn-primary",
      text: "Create new profile"
    });
    actions.append(cancelBtn, retryBtn, createBtn);

    const cleanup = mountDialog({
      doc,
      frame,
      body: [actions],
      onDone: resolve,
      focusTarget: () => focusWithPreventScroll(retryBtn)
    });

    cancelBtn.addEventListener("click", () => cleanup(null));
    retryBtn.addEventListener("click", () => cleanup("retry"));
    createBtn.addEventListener("click", () => cleanup("create"));
  });
}

export async function requestConfirmationDialog({
  title = "Confirm",
  description = "",
  confirmLabel = "Confirm",
  cancelLabel = "Cancel"
} = {}) {
  const doc = getDocumentTarget();
  if (!doc) {
    const fallbackConfirm = typeof globalThis?.window?.confirm === "function" ? globalThis.window.confirm.bind(globalThis.window) : null;
    if (!fallbackConfirm) return false;
    const fallbackLabel = [title, description].filter(Boolean).join("\n\n");
    return Boolean(fallbackConfirm(fallbackLabel));
  }
  return new Promise(resolve => {
    const frame = createDialogFrame(doc, {
      title: String(title || "Confirm"),
      description: String(description || "")
    });

    const actions = createDialogActions(doc);
    const cancelBtn = createDialogButton(doc, {
      id: "local-auth-cancel-btn",
      className: "local-auth-dialog-cancel",
      variant: "popup-btn-secondary",
      text: String(cancelLabel || "Cancel")
    });
    const confirmBtn = createDialogButton(doc, {
      id: "local-auth-confirm-btn",
      className: "local-auth-dialog-submit",
      variant: "popup-btn-primary",
      text: String(confirmLabel || "Confirm")
    });
    actions.append(cancelBtn, confirmBtn);

    const cleanup = mountDialog({
      doc,
      frame,
      body: [actions],
      dismissResult: false,
      onDone: result => resolve(Boolean(result)),
      keyHandler: finish => event => {
        if (event.key === "Escape") {
          event.preventDefault();
          finish(false);
          return;
        }
        if (event.key === "Enter") {
          event.preventDefault();
          finish(true);
        }
      },
      focusTarget: () => focusWithPreventScroll(confirmBtn)
    });

    cancelBtn.addEventListener("click", () => cleanup(false));
    confirmBtn.addEventListener("click", () => cleanup(true));
  });
}
