#!/usr/bin/env node

const fs = require("node:fs");
const path = require("node:path");

const SERVER_NAME = "baluffo-playwright";
const SERVER_VERSION = "1.0.0";
const DEFAULT_PROTOCOL_VERSION = "2024-11-05";

function loadChromium() {
  const overrideModule = process.env.BALUFFO_MCP_PLAYWRIGHT_MODULE;
  const playwrightModule = overrideModule
    ? require(path.resolve(overrideModule))
    : require("playwright");
  return playwrightModule.chromium;
}

const chromium = loadChromium();

let browser = null;
let context = null;
let activePage = null;

function buildToolDefinitions() {
  return [
    {
      name: "navigate",
      description: "Navigate the active page to a URL and reuse that page for later tool calls.",
      inputSchema: {
        type: "object",
        properties: {
          url: { type: "string", description: "Destination URL." },
          waitUntil: {
            type: "string",
            description: "Navigation wait strategy: load, domcontentloaded, or networkidle.",
            default: "load",
          },
          timeoutMs: {
            type: "number",
            description: "Optional navigation timeout in milliseconds.",
            default: 60000,
          },
        },
        required: ["url"],
      },
    },
    {
      name: "screenshot",
      description: "Capture a screenshot of the active page.",
      inputSchema: {
        type: "object",
        properties: {
          path: {
            type: "string",
            description: "Optional output path. Prefer a `.tmp/...` location when persisting screenshots.",
          },
          fullPage: {
            type: "boolean",
            description: "Capture the full scrollable page.",
            default: false,
          },
        },
      },
    },
    {
      name: "click",
      description: "Click an element on the active page.",
      inputSchema: {
        type: "object",
        properties: {
          selector: { type: "string", description: "CSS selector for the target element." },
        },
        required: ["selector"],
      },
    },
    {
      name: "fill",
      description: "Fill an input or textarea on the active page.",
      inputSchema: {
        type: "object",
        properties: {
          selector: { type: "string", description: "CSS selector for the target element." },
          value: { type: "string", description: "Text value to assign." },
        },
        required: ["selector", "value"],
      },
    },
    {
      name: "evaluate",
      description: "Run a JavaScript expression in the active page context and return the result.",
      inputSchema: {
        type: "object",
        properties: {
          script: { type: "string", description: "JavaScript expression or statements to evaluate." },
        },
        required: ["script"],
      },
    },
    {
      name: "get_html",
      description: "Return HTML from the active page or a matching element.",
      inputSchema: {
        type: "object",
        properties: {
          selector: { type: "string", description: "Optional CSS selector." },
        },
      },
    },
    {
      name: "get_text",
      description: "Return text from the active page body or a matching element.",
      inputSchema: {
        type: "object",
        properties: {
          selector: { type: "string", description: "Optional CSS selector." },
        },
      },
    },
    {
      name: "wait_for_selector",
      description: "Wait until an element is present on the active page.",
      inputSchema: {
        type: "object",
        properties: {
          selector: { type: "string", description: "CSS selector to wait for." },
          timeoutMs: {
            type: "number",
            description: "Optional timeout in milliseconds.",
            default: 30000,
          },
        },
        required: ["selector"],
      },
    },
    {
      name: "reset_session",
      description: "Replace the browser context and clear the active page.",
      inputSchema: {
        type: "object",
        properties: {},
      },
    },
    {
      name: "close",
      description: "Close the browser and release all server resources.",
      inputSchema: {
        type: "object",
        properties: {},
      },
    },
  ];
}

const TOOLS = buildToolDefinitions();

class ToolInputError extends Error {}

async function ensureBrowser() {
  if (!browser) {
    browser = await chromium.launch({
      headless: true,
      args: ["--no-sandbox", "--disable-setuid-sandbox"],
    });
  }
  return browser;
}

async function ensureContext() {
  if (!context) {
    const currentBrowser = await ensureBrowser();
    context = await currentBrowser.newContext({
      viewport: { width: 1280, height: 720 },
      userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    });
  }
  return context;
}

async function closePageIfNeeded() {
  if (activePage && typeof activePage.isClosed === "function" && activePage.isClosed()) {
    activePage = null;
    return;
  }
  if (activePage) {
    await activePage.close().catch(() => {});
    activePage = null;
  }
}

async function resetSession() {
  await closePageIfNeeded();
  if (context) {
    await context.close().catch(() => {});
    context = null;
  }
}

async function closeAll() {
  await resetSession();
  if (browser) {
    await browser.close().catch(() => {});
    browser = null;
  }
}

function requireString(args, key) {
  const value = args?.[key];
  if (typeof value !== "string" || value.length === 0) {
    throw new ToolInputError(`Expected \`${key}\` to be a non-empty string.`);
  }
  return value;
}

function requireActivePage() {
  if (!activePage || (typeof activePage.isClosed === "function" && activePage.isClosed())) {
    activePage = null;
    throw new ToolInputError("No active page. Call `navigate` first.");
  }
  return activePage;
}

function textContent(value) {
  if (value == null) {
    return "";
  }
  return typeof value === "string" ? value : JSON.stringify(value, null, 2);
}

function successText(text, extras = {}) {
  return {
    content: [{ type: "text", text }],
    ...extras,
  };
}

function successWithImage(summary, base64Png) {
  return {
    content: [
      { type: "text", text: summary },
      { type: "image", data: base64Png, mimeType: "image/png" },
    ],
  };
}

function toolFailure(error) {
  return {
    content: [{ type: "text", text: `Error: ${error.message}` }],
    isError: true,
  };
}

async function handleToolCall(name, args = {}) {
  try {
    switch (name) {
      case "navigate": {
        const pageUrl = requireString(args, "url");
        const currentContext = await ensureContext();
        if (!activePage || (typeof activePage.isClosed === "function" && activePage.isClosed())) {
          activePage = await currentContext.newPage();
        }
        const response = await activePage.goto(pageUrl, {
          waitUntil: typeof args.waitUntil === "string" ? args.waitUntil : "load",
          timeout: typeof args.timeoutMs === "number" ? args.timeoutMs : 60000,
        });
        const status = response && typeof response.status === "function" ? response.status() : "no-response";
        return successText(`Navigated to ${pageUrl} (status: ${status})`);
      }

      case "screenshot": {
        const page = requireActivePage();
        const screenshotOptions = {
          fullPage: Boolean(args.fullPage),
        };
        if (typeof args.path === "string" && args.path.length > 0) {
          const targetPath = path.resolve(process.cwd(), args.path);
          fs.mkdirSync(path.dirname(targetPath), { recursive: true });
          screenshotOptions.path = targetPath;
          await page.screenshot(screenshotOptions);
          return successText(`Screenshot saved to ${targetPath}`);
        }
        const buffer = await page.screenshot(screenshotOptions);
        return successWithImage("Captured screenshot from the active page.", buffer.toString("base64"));
      }

      case "click": {
        const page = requireActivePage();
        const selector = requireString(args, "selector");
        await page.click(selector);
        return successText(`Clicked ${selector}`);
      }

      case "fill": {
        const page = requireActivePage();
        const selector = requireString(args, "selector");
        const value = requireString(args, "value");
        await page.fill(selector, value);
        return successText(`Filled ${selector}`);
      }

      case "evaluate": {
        const page = requireActivePage();
        const script = requireString(args, "script");
        const result = await page.evaluate(script);
        return successText(textContent(result));
      }

      case "get_html": {
        const page = requireActivePage();
        const html = typeof args.selector === "string" && args.selector.length > 0
          ? await page.locator(args.selector).innerHTML()
          : await page.content();
        return successText(String(html ?? ""));
      }

      case "get_text": {
        const page = requireActivePage();
        const text = typeof args.selector === "string" && args.selector.length > 0
          ? await page.locator(args.selector).textContent()
          : await page.evaluate(() => {
              const body = document.body;
              if (body && typeof body.innerText === "string" && body.innerText.length > 0) {
                return body.innerText;
              }
              const root = document.documentElement;
              return root && typeof root.innerText === "string" ? root.innerText : "";
            });
        return successText(String(text ?? ""));
      }

      case "wait_for_selector": {
        const page = requireActivePage();
        const selector = requireString(args, "selector");
        await page.waitForSelector(selector, {
          timeout: typeof args.timeoutMs === "number" ? args.timeoutMs : 30000,
        });
        return successText(`Element found: ${selector}`);
      }

      case "reset_session": {
        await resetSession();
        return successText("Browser session reset.");
      }

      case "close": {
        await closeAll();
        return successText("Browser closed.");
      }

      default:
        return toolFailure(new Error(`Unknown tool: ${name}`));
    }
  } catch (error) {
    return toolFailure(error);
  }
}

function buildInitializeResult(params = {}) {
  return {
    protocolVersion:
      typeof params.protocolVersion === "string" && params.protocolVersion.length > 0
        ? params.protocolVersion
        : DEFAULT_PROTOCOL_VERSION,
    serverInfo: {
      name: SERVER_NAME,
      version: SERVER_VERSION,
    },
    capabilities: {
      tools: {},
    },
  };
}

function sendResponse(id, result) {
  sendMessage({
    jsonrpc: "2.0",
    id,
    result,
  });
}

function sendError(id, code, message) {
  sendMessage({
    jsonrpc: "2.0",
    id,
    error: { code, message },
  });
}

function sendMessage(message) {
  const payload = Buffer.from(JSON.stringify(message), "utf8");
  process.stdout.write(`Content-Length: ${payload.length}\r\n\r\n`);
  process.stdout.write(payload);
}

async function handleMessage(message) {
  if (!message || message.jsonrpc !== "2.0") {
    if (message && Object.prototype.hasOwnProperty.call(message, "id")) {
      sendError(message.id, -32600, "Invalid Request");
    }
    return;
  }

  const { id, method, params } = message;
  if (typeof method !== "string") {
    if (Object.prototype.hasOwnProperty.call(message, "id")) {
      sendError(id, -32600, "Invalid Request");
    }
    return;
  }

  if (!Object.prototype.hasOwnProperty.call(message, "id")) {
    if (method === "notifications/initialized") {
      return;
    }
    if (method === "exit") {
      await closeAll();
      process.exit(0);
    }
    return;
  }

  switch (method) {
    case "initialize":
      sendResponse(id, buildInitializeResult(params));
      return;

    case "tools/list":
      sendResponse(id, { tools: TOOLS });
      return;

    case "tools/call": {
      const toolName = params && typeof params.name === "string" ? params.name : "";
      if (!toolName) {
        sendResponse(id, toolFailure(new ToolInputError("Expected `name` to be a non-empty string.")));
        return;
      }
      const result = await handleToolCall(toolName, params.arguments || {});
      sendResponse(id, result);
      return;
    }

    default:
      sendError(id, -32601, `Method not found: ${method}`);
  }
}

let inputBuffer = Buffer.alloc(0);
let requestQueue = Promise.resolve();

function drainInputBuffer() {
  while (true) {
    const headerBoundary = inputBuffer.indexOf("\r\n\r\n");
    if (headerBoundary === -1) {
      return;
    }

    const headerText = inputBuffer.subarray(0, headerBoundary).toString("utf8");
    const match = /^Content-Length:\s*(\d+)$/im.exec(headerText);
    if (!match) {
      inputBuffer = Buffer.alloc(0);
      return;
    }

    const contentLength = Number(match[1]);
    const messageStart = headerBoundary + 4;
    const totalLength = messageStart + contentLength;
    if (inputBuffer.length < totalLength) {
      return;
    }

    const payload = inputBuffer.subarray(messageStart, totalLength).toString("utf8");
    inputBuffer = inputBuffer.subarray(totalLength);

    let message;
    try {
      message = JSON.parse(payload);
    } catch (_error) {
      sendError(null, -32700, "Parse error");
      continue;
    }

    requestQueue = requestQueue
      .then(() => handleMessage(message))
      .catch((error) => {
        if (message && Object.prototype.hasOwnProperty.call(message, "id")) {
          sendError(message.id, -32603, `Internal error: ${error.message}`);
        } else {
          console.error("Unhandled MCP request error:", error);
        }
      });
  }
}

process.stdin.on("data", (chunk) => {
  inputBuffer = Buffer.concat([inputBuffer, chunk]);
  drainInputBuffer();
});

process.stdin.on("end", () => {
  requestQueue = requestQueue.finally(() => closeAll());
});

process.on("SIGINT", () => {
  closeAll()
    .catch(() => {})
    .finally(() => process.exit(0));
});

process.on("SIGTERM", () => {
  closeAll()
    .catch(() => {})
    .finally(() => process.exit(0));
});
