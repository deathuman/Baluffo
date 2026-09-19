import test from "node:test";
import assert from "node:assert/strict";
import { updateJobsPipelineUi } from "../../../frontend/jobs/app/pipeline.js";
import {
  createStyleStub as createStyle,
  createToggleClassList as createClassList
} from "./helpers/dom-test-helpers.mjs";

function createElementMock() {
  const element = {
    dataset: {},
    style: createStyle(),
    className: "",
    hidden: false,
    classList: createClassList(),
    _textContent: "",
    children: [],
    get textContent() {
      return this.children.length
        ? this.children.map(child => String(child?.textContent || "")).join("")
        : String(this._textContent || "");
    },
    set textContent(value) {
      this._textContent = String(value);
      this.children = [];
    },
    appendChild(child) {
      this.children.push(child);
      return child;
    },
    querySelector(selector) {
      const match = /^\[data-ui="([^"]+)"\]$/.exec(String(selector || ""));
      if (!match) return null;
      return this.children.find(child => String(child?.dataset?.ui || "") === match[1]) || null;
    },
    setAttribute(name, value) {
      this[name] = value;
    },
    removeAttribute(name) {
      delete this[name];
    }
  };
  return element;
}

function createCaptionAwareButtonMock() {
  const button = {
    dataset: {},
    style: createStyle(),
    disabled: false,
    classList: createClassList(),
    _textContent: "Update jobs",
    ownerDocument: { createElement: createElementMock },
    children: [],
    get textContent() {
      return this.children.length
        ? this.children.map(child => String(child?.textContent || "")).join("")
        : String(this._textContent || "");
    },
    set textContent(value) {
      this._textContent = String(value);
      this.children = [];
    },
    replaceChildren(...nodes) {
      this.children = nodes;
    },
    querySelector(selector) {
      const match = /^\[data-ui="([^"]+)"\]$/.exec(String(selector || ""));
      if (!match) return null;
      return this.children.find(child => String(child?.dataset?.ui || "") === match[1]) || null;
    },
    setAttribute(name, value) {
      this[name] = value;
    },
    removeAttribute(name) {
      delete this[name];
    }
  };
  const inserted = [];
  const captionUi = "jobs-pipeline-progress-caption";
  button.insertAdjacentElement = (_position, el) => {
    // ponytail: mirror the real toolbar DOM — dynamically created siblings get
    // the button's parent as their parentElement so the status row can carry
    // the running class.
    el.parentElement = button.parentElement;
    inserted.push(el);
  };
  button.parentElement = {
    classList: createClassList(),
    querySelector(selector) {
      if (!String(selector || "").includes(captionUi)) return null;
      return inserted.find(el => String(el?.dataset?.ui || "") === captionUi) || null;
    }
  };
  return { button, inserted };
}

test("updateJobsPipelineUi routes sub-progress to the caption, not the in-button span", () => {
  const { button, inserted } = createCaptionAwareButtonMock();
  const update = (running, opts = {}) => updateJobsPipelineUi({ jobsPipelineRunBtn: button }, {
    running,
    disabled: running,
    buttonLabel: running ? "Fetching job listings... 7m 27s" : "",
    pipelinePayload: { active: running, stage: "fetch" },
    ...opts
  });

  update(true, { progressLabel: "Resolving sources 128/431 · ETA 4m" });

  const caption = inserted.find(el => String(el?.dataset?.ui || "") === "jobs-pipeline-progress-caption");
  assert.ok(caption, "caption element should be created as a sibling");
  assert.equal(caption.textContent, "Resolving sources 128/431 · ETA 4m");
  assert.equal(caption.hidden, false);
  assert.ok(caption.classList.contains("running"));
  assert.ok(button.classList.contains("running"));
  // ponytail: the in-button progress span stays hidden/empty — the button only
  // carries the compact stage + elapsed label and the fill groove.
  assert.equal(button.children[2].dataset.ui, "jobs-pipeline-progress");
  assert.equal(button.children[2].hidden, true);

  update(false);
  assert.equal(caption.textContent, "");
  assert.equal(caption.hidden, true);
  assert.equal(caption.classList.contains("running"), false);
});

test("updateJobsPipelineUi marks the toolbar status row running for the whole active run", () => {
  const { button } = createCaptionAwareButtonMock();
  const statusRow = button.parentElement;

  updateJobsPipelineUi({ jobsPipelineRunBtn: button }, {
    running: true,
    disabled: true,
    buttonLabel: "Checking sources... 13s",
    pipelinePayload: { active: true, stage: "discovery" },
    progressLabel: "Scanning known careers pages"
  });
  assert.equal(statusRow.classList.contains("running"), true);
  // ponytail: the row stays marked running even in silent phases where the
  // caption is empty, so the Last-updated timestamp stays dimmed all run.
  updateJobsPipelineUi({ jobsPipelineRunBtn: button }, {
    running: true,
    disabled: true,
    buttonLabel: "Updating jobs...",
    pipelinePayload: { active: true, stage: "starting" },
    progressLabel: ""
  });
  assert.equal(statusRow.classList.contains("running"), true);

  updateJobsPipelineUi({ jobsPipelineRunBtn: button }, {
    running: false,
    disabled: false,
    buttonLabel: "",
    pipelinePayload: null
  });
  assert.equal(statusRow.classList.contains("running"), false);
});
