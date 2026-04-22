import test from "node:test";
import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawn } from "node:child_process";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);
const ROOT = path.resolve(__dirname, "..", "..", "..");
const SERVER_PATH = path.join(ROOT, "tools", "mcp", "playwright-server.cjs");

function encodeMessage(message) {
  const payload = Buffer.from(JSON.stringify(message), "utf8");
  return Buffer.concat([
    Buffer.from(`Content-Length: ${payload.length}\r\n\r\n`, "utf8"),
    payload,
  ]);
}

function createProtocolClient(childProcess) {
  let buffer = Buffer.alloc(0);
  let nextId = 1;
  const pending = new Map();

  function resolveMessage(message) {
    const handler = pending.get(message.id);
    if (!handler) {
      return;
    }
    pending.delete(message.id);
    if (message.error) {
      handler.reject(new Error(message.error.message || "MCP request failed"));
      return;
    }
    handler.resolve(message.result);
  }

  childProcess.stdout.on("data", chunk => {
    buffer = Buffer.concat([buffer, chunk]);
    while (true) {
      const headerBoundary = buffer.indexOf("\r\n\r\n");
      if (headerBoundary === -1) {
        return;
      }
      const headerText = buffer.subarray(0, headerBoundary).toString("utf8");
      const match = /^Content-Length:\s*(\d+)$/im.exec(headerText);
      assert.ok(match, `Missing Content-Length header in: ${headerText}`);
      const contentLength = Number(match[1]);
      const bodyStart = headerBoundary + 4;
      const totalLength = bodyStart + contentLength;
      if (buffer.length < totalLength) {
        return;
      }
      const payload = buffer.subarray(bodyStart, totalLength).toString("utf8");
      buffer = buffer.subarray(totalLength);
      resolveMessage(JSON.parse(payload));
    }
  });

  return {
    request(method, params = {}) {
      const id = nextId++;
      return new Promise((resolve, reject) => {
        pending.set(id, { resolve, reject });
        childProcess.stdin.write(
          encodeMessage({
            jsonrpc: "2.0",
            id,
            method,
            params,
          })
        );
      });
    },
    notify(method, params = {}) {
      childProcess.stdin.write(
        encodeMessage({
          jsonrpc: "2.0",
          method,
          params,
        })
      );
    },
  };
}

function buildStubSource() {
  return `
const vm = require("node:vm");

function parseHtmlFromDataUrl(url) {
  const match = /^data:text\\/html(?:;charset=[^,]+)?,([\\s\\S]*)$/i.exec(url);
  if (!match) {
    return "<body></body>";
  }
  return decodeURIComponent(match[1]);
}

function extractBodyHtml(html) {
  const match = /<body[^>]*>([\\s\\S]*)<\\/body>/i.exec(html);
  return match ? match[1] : html;
}

function stripTags(html) {
  return html.replace(/<script[\\s\\S]*?<\\/script>/gi, "").replace(/<[^>]+>/g, " ").replace(/\\s+/g, " ").trim();
}

function createDocument(state) {
  return {
    body: {
      innerText: state.bodyText,
      innerHTML: state.bodyHtml,
      dataset: state.dataset,
    },
    documentElement: {
      innerText: state.bodyText,
    },
  };
}

function createPage() {
  const state = {
    currentUrl: "",
    bodyHtml: "",
    bodyText: "",
    dataset: {},
    globals: {},
    closed: false,
  };

  return {
    isClosed() {
      return state.closed;
    },
    async goto(url) {
      state.currentUrl = url;
      const html = parseHtmlFromDataUrl(url);
      state.bodyHtml = extractBodyHtml(html);
      state.bodyText = stripTags(state.bodyHtml);
      return {
        status() {
          return 200;
        },
      };
    },
    async screenshot(options) {
      const buffer = Buffer.from("fake-png");
      if (options && options.path) {
        require("node:fs").writeFileSync(options.path, buffer);
      }
      return buffer;
    },
    async click() {},
    async fill(selector, value) {
      state.globals.__lastFill = { selector, value };
    },
    locator(selector) {
      return {
        async textContent() {
          if (selector === "body") {
            return state.bodyText;
          }
          return state.globals.__lastFill ? state.globals.__lastFill.value : "";
        },
        async innerHTML() {
          return selector === "body" ? state.bodyHtml : "";
        },
      };
    },
    async evaluate(value) {
      const document = createDocument(state);
      const window = state.globals;
      if (typeof value === "function") {
        const previousDocument = global.document;
        const previousWindow = global.window;
        global.document = document;
        global.window = window;
        try {
          return value();
        } finally {
          global.document = previousDocument;
          global.window = previousWindow;
        }
      }
      return vm.runInNewContext(value, { document, window, JSON });
    },
    async content() {
      return "<html><body>" + state.bodyHtml + "</body></html>";
    },
    async waitForSelector() {},
    async close() {
      state.closed = true;
    },
  };
}

exports.chromium = {
  async launch() {
    return {
      async newContext() {
        return {
          async newPage() {
            return createPage();
          },
          async close() {},
        };
      },
      async close() {},
    };
  },
};
`;
}

async function startServer(testContext) {
  const tmpDir = await mkdtemp(path.join(os.tmpdir(), "baluffo-mcp-server-"));
  await writeFile(path.join(tmpDir, "playwright-stub.cjs"), buildStubSource(), "utf8");
  const childProcess = spawn(
    process.execPath,
    [SERVER_PATH],
    {
      cwd: ROOT,
      env: {
        ...process.env,
        BALUFFO_MCP_PLAYWRIGHT_MODULE: path.join(tmpDir, "playwright-stub.cjs"),
      },
      stdio: ["pipe", "pipe", "pipe"],
    }
  );

  let stderr = "";
  childProcess.stderr.on("data", chunk => {
    stderr += chunk.toString("utf8");
  });

  const client = createProtocolClient(childProcess);
  testContext.after(async () => {
    childProcess.kill();
    await rm(tmpDir, { recursive: true, force: true });
    assert.equal(stderr, "", `Expected silent stderr, got: ${stderr}`);
  });

  const initialize = await client.request("initialize", {
    protocolVersion: "2024-11-05",
    capabilities: {},
    clientInfo: { name: "unit-test", version: "1.0.0" },
  });
  assert.equal(initialize.serverInfo.name, "baluffo-playwright");
  client.notify("notifications/initialized", {});

  return client;
}

test("MCP server handles initialize, tools/list, navigate, get_text, and close", async t => {
  const client = await startServer(t);
  const listResult = await client.request("tools/list", {});
  const toolNames = listResult.tools.map(tool => tool.name);
  assert.deepEqual(toolNames, [
    "navigate",
    "screenshot",
    "click",
    "fill",
    "evaluate",
    "get_html",
    "get_text",
    "wait_for_selector",
    "reset_session",
    "close",
  ]);

  const pageHtml = encodeURIComponent("<html><body>Hello MCP</body></html>");
  const navigateResult = await client.request("tools/call", {
    name: "navigate",
    arguments: { url: `data:text/html,${pageHtml}` },
  });
  assert.match(navigateResult.content[0].text, /Navigated to/);

  const textResult = await client.request("tools/call", {
    name: "get_text",
    arguments: {},
  });
  assert.equal(textResult.content[0].text, "Hello MCP");

  const closeResult = await client.request("tools/call", {
    name: "close",
    arguments: {},
  });
  assert.equal(closeResult.content[0].text, "Browser closed.");
});

test("MCP server reuses the active page across follow-up tool calls", async t => {
  const client = await startServer(t);
  const pageHtml = encodeURIComponent("<html><body>Session test</body></html>");
  await client.request("tools/call", {
    name: "navigate",
    arguments: { url: `data:text/html,${pageHtml}` },
  });

  const setResult = await client.request("tools/call", {
    name: "evaluate",
    arguments: { script: "window.__shared = 'kept'; window.__shared;" },
  });
  assert.equal(setResult.content[0].text, "kept");

  const getResult = await client.request("tools/call", {
    name: "evaluate",
    arguments: { script: "window.__shared;" },
  });
  assert.equal(getResult.content[0].text, "kept");
});

test("MCP server returns a clear tool error before navigate", async t => {
  const client = await startServer(t);
  const clickResult = await client.request("tools/call", {
    name: "click",
    arguments: { selector: "#submit" },
  });

  assert.equal(clickResult.isError, true);
  assert.match(clickResult.content[0].text, /Call `navigate` first/);
});
