#!/usr/bin/env node
/**
 * MCP Autoresponder Hub  v1.1
 * One MCP endpoint (for Claude / ChatGPT) -> up to 10 downstream MCP servers
 * (AWeber, systeme.io, MailerLite, GetResponse ... or several accounts of the same one).
 */
import http from "node:http";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash, timingSafeEqual } from "node:crypto";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { SSEClientTransport } from "@modelcontextprotocol/sdk/client/sse.js";
import { StdioClientTransport } from "@modelcontextprotocol/sdk/client/stdio.js";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { ListToolsRequestSchema, CallToolRequestSchema, McpError, ErrorCode } from "@modelcontextprotocol/sdk/types.js";

const MAX_ACCOUNTS = 10;
const log = (...a) => console.error("[hub]", ...a); // stderr only, so --stdio mode stays clean
const sleepUnref = (ms) => new Promise((r) => setTimeout(r, ms).unref?.());
const shortHash = (s) => createHash("sha1").update(s).digest("hex").slice(0, 4);
const text = (s, isError = false) => ({ content: [{ type: "text", text: typeof s === "string" ? s : JSON.stringify(s, null, 2) }], ...(isError ? { isError: true } : {}) });
const errMsg = (e) => String(e?.message || e);

async function withTimeout(p, ms, what) {
  let t;
  try { return await Promise.race([p, new Promise((_, rej) => { t = setTimeout(() => rej(new Error(`${what} timed out after ${ms}ms`)), ms); })]); }
  finally { clearTimeout(t); }
}
const globToRe = (p) => new RegExp("^" + p.replace(/[.+^${}()|[\]\\]/g, "\\$&").replace(/\*/g, ".*") + "$", "is");

// ---------- .env + config ----------
function loadDotEnv(file = ".env") {
  if (!fs.existsSync(file)) return;
  for (const line of fs.readFileSync(file, "utf8").split(/\r?\n/)) {
    const m = line.match(/^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*?)\s*$/);
    if (!m) continue;
    if (process.env[m[1]] === undefined) process.env[m[1]] = m[2].replace(/^(['"])(.*)\1$/, "$2");
  }
}

function interpolate(v, missing) {
  if (typeof v === "string") return v.replace(/\$\{(\w+)\}/g, (_, k) => { const x = process.env[k]; if (x === undefined || x === "") { missing.add(k); return ""; } return x; });
  if (Array.isArray(v)) return v.map((x) => interpolate(x, missing));
  if (v && typeof v === "object") return Object.fromEntries(Object.entries(v).map(([k, x]) => [k, interpolate(x, missing)]));
  return v;
}

/** Config comes from HUB_CONFIG_JSON (raw JSON or base64; best for serverless) or a file (HUB_CONFIG / ./hub.config.json). */
function loadConfig() {
  let raw;
  if (process.env.HUB_CONFIG_JSON) {
    const s = process.env.HUB_CONFIG_JSON.trim();
    raw = JSON.parse(s.startsWith("{") ? s : Buffer.from(s, "base64").toString("utf8"));
  } else {
    const file = process.env.HUB_CONFIG || path.resolve("hub.config.json");
    if (!fs.existsSync(file)) throw new Error(`Config not found: ${file}. Copy hub.config.example.json, or set HUB_CONFIG_JSON.`);
    raw = JSON.parse(fs.readFileSync(file, "utf8"));
  }
  const num = (k, d, min) => { const v = raw[k] ?? d; if (!Number.isFinite(v) || v < min) throw new Error(`"${k}" must be a number >= ${min}`); return v; };
  const mode = raw.mode ?? "auto";
  if (!["auto", "namespaced", "lazy"].includes(mode)) throw new Error(`"mode" must be auto, namespaced or lazy (got "${mode}")`);
  const list = (raw.accounts || []).filter((a) => a && a.enabled !== false);
  if (!list.length) throw new Error("No enabled accounts configured");
  if (list.length > MAX_ACCOUNTS) throw new Error(`Max ${MAX_ACCOUNTS} accounts (found ${list.length})`);
  const seen = new Set();
  for (const a of list) {
    if (!/^[a-zA-Z0-9-]{1,24}$/.test(a.id || "")) throw new Error(`Invalid account id "${a.id}" (letters, digits, dash; max 24)`);
    if (seen.has(a.id)) throw new Error(`Duplicate account id "${a.id}"`);
    seen.add(a.id);
    if (!!a.url === !!a.command) throw new Error(`Account "${a.id}" needs exactly one of "url" or "command"`);
    if (a.transport && !["http", "sse"].includes(a.transport)) throw new Error(`Account "${a.id}": transport must be http or sse`);
    if (a.url) { let u; try { u = new URL(a.url); } catch { throw new Error(`Account "${a.id}": invalid url`); } if (!/^https?:$/.test(u.protocol)) throw new Error(`Account "${a.id}": url must be http(s)`); }
    for (const k of ["allowTools", "denyTools", "args"]) if (a[k] !== undefined && !(Array.isArray(a[k]) && a[k].every((x) => typeof x === "string"))) throw new Error(`Account "${a.id}": "${k}" must be an array of strings`);
    for (const k of ["headers", "env"]) if (a[k] !== undefined && (typeof a[k] !== "object" || Array.isArray(a[k]))) throw new Error(`Account "${a.id}": "${k}" must be an object`);
  }
  const missing = new Set();
  const accounts = interpolate(list, missing);
  if (missing.size) throw new Error(`Missing environment variable(s) referenced in config: ${[...missing].join(", ")}`);
  return {
    mode, accounts,
    maxToolsForNamespaced: num("maxToolsForNamespaced", 40, 1),
    toolsTtlSeconds: num("toolsTtlSeconds", 300, 5),
    maxResultChars: num("maxResultChars", 60000, 1000),
    listBudgetMs: num("listBudgetMs", 8000, 500),
  };
}

// ---------- one downstream connection ----------
const SAFE_TO_RETRY = /session not found|\b404\b|ECONNREFUSED|ENOTFOUND|not connected|EPIPE/i; // request never reached the tool
const READ_RETRY = /closed|ECONN|fetch failed|terminated|socket|\b5\d\d\b/i;                 // ambiguous: only retry read-only tools

class Account {
  constructor(cfg, opts = {}) {
    this.cfg = cfg;
    this.id = cfg.id;
    this.label = cfg.label || cfg.id;
    this.ttlMs = opts.ttlMs ?? 300000;
    this.allow = (cfg.allowTools || []).map(globToRe);
    this.deny = (cfg.denyTools || []).map(globToRe);
    this.client = null;
    this.tools = [];
    this.status = "disconnected"; // disconnected | connecting | connected | error
    this.error = null;
    this.listedAt = 0;
    this.lastAttempt = 0;
    this._f = {};
  }

  isAllowed(name) {
    if (this.deny.some((r) => r.test(name))) return false;
    return this.allow.length === 0 || this.allow.some((r) => r.test(name));
  }

  _flight(key, fn) { // single-flight: concurrent callers share one operation
    if (this._f[key]) return this._f[key];
    const p = Promise.resolve().then(fn).finally(() => { delete this._f[key]; });
    return (this._f[key] = p);
  }

  async connect() {
    await this.close();
    const c = this.cfg;
    const mk = () => new Client({ name: "mcp-autoresponder-hub", version: "1.1.0" }, { capabilities: {} });
    const attempt = async (transport, ms, label) => {
      const client = mk();
      try { await withTimeout(client.connect(transport), ms, label); return client; }
      catch (e) { try { await client.close(); } catch {} throw e; }
    };
    let client;
    if (c.command) {
      client = await attempt(new StdioClientTransport({ command: c.command, args: c.args || [], env: c.env }), 30000, "stdio start");
    } else {
      const url = new URL(c.url), headers = c.headers || {}, errs = [];
      const sse = () => new SSEClientTransport(url, { requestInit: { headers }, eventSourceInit: { fetch: (u, init) => fetch(u, { ...init, headers: { ...(init?.headers || {}), ...headers } }) } });
      if (c.transport !== "sse") {
        try { client = await attempt(new StreamableHTTPClientTransport(url, { requestInit: { headers } }), 15000, "http connect"); }
        catch (e) { errs.push("http: " + errMsg(e)); if (c.transport === "http") throw new Error(errs[0]); }
      }
      if (!client) {
        try { client = await attempt(sse(), 15000, "sse connect"); }
        catch (e) { errs.push("sse: " + errMsg(e)); throw new Error(errs.join(" | ")); }
      }
    }
    this.client = client;
    client.onclose = () => { if (this.client === client) this.status = "disconnected"; };
  }

  async refreshTools() {
    const all = [];
    let cursor;
    do {
      const r = await withTimeout(this.client.listTools({ cursor }), 20000, "listTools");
      all.push(...r.tools);
      cursor = r.nextCursor;
    } while (cursor);
    this.tools = all.filter((t) => this.isAllowed(t.name));
    this.listedAt = Date.now();
    this.status = "connected";
    this.error = null;
  }

  init() {
    return this._flight("init", async () => {
      this.lastAttempt = Date.now();
      this.status = "connecting";
      try {
        await this.connect();
        await this.refreshTools();
        log(`${this.id}: connected, ${this.tools.length} tools`);
      } catch (e) {
        this.status = "error";
        this.error = errMsg(e);
        log(`${this.id}: FAILED - ${this.error}`);
      } finally { this.settledOnce = true; }
    });
  }

  refreshIfStale() {
    if (this._f.init) return this.settledOnce || this.noWait ? Promise.resolve() : this._f.init; // only the very first load waits
    const now = Date.now();
    if (this.status === "connected") {
      if (now - this.listedAt <= this.ttlMs) return Promise.resolve();
      return this._flight("refresh", async () => {
        try { await this.refreshTools(); } catch (e) { this.status = "disconnected"; this.error = errMsg(e); }
      });
    }
    if (now - this.lastAttempt > 60000) this.init(); // background retry: a dead upstream never stalls a request
    return Promise.resolve();
  }

  async callTool(name, args, retry = true) {
    if (typeof name !== "string" || !name) return text("Tool name must be a non-empty string.", true);
    if (this._f.init) await this._f.init;
    if (this.status === "disconnected" || (this.status === "error" && Date.now() - this.lastAttempt > 5000)) await this.init(); // nothing sent yet, so reconnecting is always safe
    if (this.status !== "connected") return text(`Account "${this.id}" is not connected${this.error ? ": " + this.error : ""}`, true);
    const find = () => this.tools.find((t) => t.name === name); // exact match against the filtered list = deny/allow can't be bypassed by odd strings
    let tool = find();
    if (!tool) { await this._flight("refresh", async () => { try { await this.refreshTools(); } catch {} }); tool = find(); }
    if (!tool) return text(`Tool "${name}" is unknown or blocked on account "${this.id}".`, true);
    const input = args && typeof args === "object" && !Array.isArray(args) ? args : {};
    try {
      return await this.client.callTool({ name, arguments: input }, undefined, { timeout: 120000 });
    } catch (e) {
      const msg = errMsg(e), readOnly = tool.annotations?.readOnlyHint === true;
      if (e instanceof McpError && e.code === ErrorCode.RequestTimeout) return text(`${this.id}/${name} timed out. The action may or may not have completed; check before retrying.`, true);
      if (e instanceof McpError && e.code !== ErrorCode.ConnectionClosed) return text(`${this.id}/${name}: ${msg}`, true); // protocol error from the tool (bad params etc.)
      this.status = "disconnected";
      if (retry && (SAFE_TO_RETRY.test(msg) || (readOnly && READ_RETRY.test(msg)))) {
        await this.init();
        if (this.status === "connected") return this.callTool(name, input, false);
      }
      return text(`Error calling ${this.id}/${name}: ${msg.slice(0, 300)}${readOnly ? "" : ". The outcome is unknown; verify before retrying."}`, true);
    }
  }

  async close() {
    const c = this.client;
    this.client = null;
    if (c) { c.onclose = undefined; try { await c.close(); } catch {} }
  }
}

// ---------- hub ----------
class Hub {
  constructor(cfg) {
    this.cfg = { maxResultChars: 60000, listBudgetMs: 8000, ...cfg };
    this.accounts = new Map(cfg.accounts.map((a) => [a.id, new Account(a, { ttlMs: (cfg.toolsTtlSeconds ?? 300) * 1000 })]));
    this.routes = new Map();
    this._lazy = false;
  }

  async start(budgetMs = this.cfg.listBudgetMs) {
    await Promise.race([Promise.allSettled([...this.accounts.values()].map((a) => a.init())), sleepUnref(budgetMs)]);
  }

  async refresh() { // never blocks longer than listBudgetMs: slow or dead upstreams are served from cache / skipped
    await Promise.race([Promise.allSettled([...this.accounts.values()].map((a) => a.refreshIfStale())), sleepUnref(this.cfg.listBudgetMs)]);
    for (const a of this.accounts.values()) if (a._f.init) a.noWait = true; // slow first connect: keep going in background, don't make every request wait
  }

  totalTools() { return [...this.accounts.values()].reduce((n, a) => n + a.tools.length, 0); }

  mode() { // auto: namespaced until the tool count gets large, then lazy (one-way, so it can't flip-flop)
    if (this.cfg.mode !== "auto") return this.cfg.mode;
    if (this._lazy || this.totalTools() > this.cfg.maxToolsForNamespaced) { this._lazy = true; return "lazy"; }
    return "namespaced";
  }

  buildRoutes() { // exposed name -> {acc, tool}; sanitised, <=64 chars, collisions get a stable hash suffix
    const routes = new Map();
    for (const a of this.accounts.values())
      for (const t of a.tools) {
        const base = `${a.id}__${t.name}`.replace(/[^a-zA-Z0-9_-]/g, "_");
        let name = base.slice(0, 64);
        for (let i = 0; routes.has(name); i++) name = `${base.slice(0, 64 - 5)}_${shortHash(a.id + "/" + t.name + (i || ""))}`;
        routes.set(name, { acc: a, tool: t });
      }
    this.routes = routes;
  }

  summary() {
    return [...this.accounts.values()].map((a) => ({ account: a.id, label: a.label, status: a.status, tools: a.tools.length, ...(a.error ? { error: a.error.slice(0, 200) } : {}) }));
  }

  async listTools() {
    await this.refresh();
    const hubTools = [{
      name: "hub_list_accounts",
      description: "List all connected autoresponder accounts (id, label, status, tool count). Use the account id in other hub tools.",
      inputSchema: { type: "object", properties: {} }, annotations: { readOnlyHint: true },
    }];
    if (this.mode() === "lazy") {
      hubTools.push(
        { name: "hub_list_tools", description: "List the tools (with input schemas) available on one autoresponder account. Optionally filter by keyword.",
          inputSchema: { type: "object", properties: { account: { type: "string", description: "Account id from hub_list_accounts" }, search: { type: "string", description: "Optional keyword to filter tool names/descriptions" } }, required: ["account"] },
          annotations: { readOnlyHint: true } },
        { name: "hub_call_tool", description: "Call a tool on a specific autoresponder account. Get the tool name and schema from hub_list_tools first.",
          inputSchema: { type: "object", properties: { account: { type: "string", description: "Account id" }, tool: { type: "string", description: "Tool name exactly as returned by hub_list_tools" }, arguments: { type: "object", description: "Arguments for the tool", additionalProperties: true } }, required: ["account", "tool"] } });
      return hubTools;
    }
    this.buildRoutes();
    const proxied = [...this.routes].map(([name, { acc, tool: t }]) => ({
      name, description: `[${acc.label}] ${t.description || t.title || t.name}`,
      inputSchema: t.inputSchema || { type: "object", properties: {} }, ...(t.annotations ? { annotations: t.annotations } : {}),
    }));
    return [...hubTools, ...proxied];
  }

  async callTool(name, args = {}) {
    if (name === "hub_list_accounts") return text(this.summary());
    if (name === "hub_list_tools" || name === "hub_call_tool") {
      const a = typeof args.account === "string" ? this.accounts.get(args.account) : undefined;
      if (!a) return text(`Unknown account "${args.account}". Use hub_list_accounts.`, true);
      if (name === "hub_list_tools") {
        await a.refreshIfStale();
        const q = String(args.search || "").toLowerCase();
        return text(a.tools.filter((t) => !q || `${t.name} ${t.description || ""}`.toLowerCase().includes(q)).map((t) => ({ name: t.name, description: t.description, inputSchema: t.inputSchema })));
      }
      return this.pass(await a.callTool(args.tool, args.arguments), a);
    }
    if (!this.routes.has(name)) { await this.refresh(); this.buildRoutes(); } // e.g. tools/call hit a fresh instance before tools/list
    const r = this.routes.get(name);
    if (!r) return text(`Unknown tool "${name}".`, true);
    return this.pass(await r.acc.callTool(r.tool.name, args), r.acc);
  }

  pass(r, acc) { // forward content + isError; cap total text so one big result can't flood the AI's context
    let budget = acc?.cfg.maxResultChars ?? this.cfg.maxResultChars, cut = 0;
    const content = [];
    for (const b of r.content ?? [{ type: "text", text: JSON.stringify(r.toolResult ?? r) }]) {
      if (b.type === "text" && typeof b.text === "string") {
        if (budget <= 0) cut += b.text.length;
        else if (b.text.length > budget) { cut += b.text.length - budget; content.push({ type: "text", text: b.text.slice(0, budget) }); budget = 0; }
        else { budget -= b.text.length; content.push(b); }
      } else content.push(b);
    }
    if (cut) content.push({ type: "text", text: `[hub: output truncated, ${cut} more characters omitted. Re-run with a smaller page size or tighter filters.]` });
    return { content, ...(r.isError ? { isError: true } : {}) };
  }

  async close() { await Promise.allSettled([...this.accounts.values()].map((a) => a.close())); }
}

function buildServer(hub) {
  const server = new Server({ name: "autoresponder-hub", version: "1.1.0" }, { capabilities: { tools: {} } });
  server.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: await hub.listTools() }));
  server.setRequestHandler(CallToolRequestSchema, async (req) => hub.callTool(req.params.name, req.params.arguments || {}));
  return server;
}

// ---------- HTTP front door ----------
const sha = (s) => createHash("sha256").update(String(s)).digest();
const safeEq = (a, b) => timingSafeEqual(sha(a), sha(b));
class HttpError extends Error { constructor(status, code, msg) { super(msg); this.status = status; this.rpcCode = code; } }
const rpcErr = (res, status, code, message, extra = {}) => { res.writeHead(status, { "content-type": "application/json", ...extra }); res.end(JSON.stringify({ jsonrpc: "2.0", error: { code, message }, id: null })); };

function readBody(req, limit = 4 * 1024 * 1024) {
  if (req.body !== undefined && req.body !== null) { // already parsed by the platform (stream is consumed; waiting for 'data' would hang forever)
    let b = req.body;
    if (Buffer.isBuffer(b)) b = b.toString("utf8");
    if (typeof b === "string") { if (b.length > limit) return Promise.reject(new HttpError(413, -32600, "Payload too large")); try { return Promise.resolve(b ? JSON.parse(b) : undefined); } catch { return Promise.reject(new HttpError(400, -32700, "Parse error")); } }
    return Promise.resolve(b);
  }
  return new Promise((resolve, reject) => {
    let size = 0, over = false;
    const chunks = [];
    req.on("data", (c) => { size += c.length; if (over) return; if (size > limit) { over = true; reject(new HttpError(413, -32600, "Payload too large")); } else chunks.push(c); });
    req.on("end", () => { if (over) return; try { resolve(chunks.length ? JSON.parse(Buffer.concat(chunks).toString("utf8")) : undefined); } catch { reject(new HttpError(400, -32700, "Parse error")); } });
    req.on("error", reject);
  });
}

/** Returns a (req,res) handler. Reusable from a Vercel/Node serverless function: `export default makeHandler(hub, tokens)`. */
function makeHandler(hub, tokens, { trustProxy = process.env.TRUST_PROXY === "1" } = {}) {
  const fails = new Map(); // ip -> {n, t}. Only INVALID tokens are throttled; a valid token is never locked out.
  setInterval(() => { const now = Date.now(); for (const [k, v] of fails) if (now - v.t > 60000) fails.delete(k); }, 30000).unref();
  return async (req, res) => {
    try {
      const url = new URL(req.url, "http://localhost");
      if (url.pathname === "/health") { res.writeHead(200, { "content-type": "application/json" }); return res.end('{"ok":true}'); }

      const ip = (trustProxy && String(req.headers["x-forwarded-for"] || "").split(",")[0].trim()) || req.socket.remoteAddress || "?";

      // Auth: /mcp/<TOKEN> (paste-able into Claude.ai / ChatGPT connector URL box) or /mcp + "Authorization: Bearer <TOKEN>"
      let route = url.pathname, presented = "";
      const m = url.pathname.match(/^(?:\/api)?\/mcp\/([^/]+)\/?$/);
      if (m) { try { presented = decodeURIComponent(m[1]); } catch {} route = "/mcp"; }
      else if (/^(?:\/api)?\/mcp\/?$/.test(url.pathname)) { route = "/mcp"; const h = req.headers.authorization || ""; presented = h.startsWith("Bearer ") ? h.slice(7) : ""; }
      if (route !== "/mcp") return rpcErr(res, 404, -32000, "Not found");
      if (!presented || !tokens.some((t) => safeEq(presented, t))) {
        const cur = fails.get(ip), n = cur && Date.now() - cur.t < 60000 ? cur.n + 1 : 1;
        fails.set(ip, { n, t: cur && n > 1 ? cur.t : Date.now() });
        if (n > 20) return rpcErr(res, 429, -32000, "Too many failed attempts", { "retry-after": "60" });
        return rpcErr(res, 401, -32001, "Unauthorized");
      }
      if (req.method !== "POST") return rpcErr(res, 405, -32000, "Method not allowed (stateless server: use POST)", { allow: "POST" });

      const body = await readBody(req);
      const server = buildServer(hub);
      const transport = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined }); // stateless
      res.on("close", () => { transport.close(); server.close(); });
      await server.connect(transport);
      await transport.handleRequest(req, res, body);
    } catch (e) {
      if (res.headersSent) return;
      if (e instanceof HttpError) { rpcErr(res, e.status, e.rpcCode, e.message, { connection: "close" }); res.once("finish", () => req.destroy()); return; }
      log("request error:", errMsg(e));
      rpcErr(res, 500, -32603, "Internal error");
    }
  };
}

function startHttp(hub, tokens, port) {
  const srv = http.createServer(makeHandler(hub, tokens));
  srv.requestTimeout = 150000;
  return new Promise((resolve) => srv.listen(port, () => resolve(srv)));
}

// ---------- main ----------
async function main() {
  loadDotEnv();
  const cfg = loadConfig();
  const hub = new Hub(cfg);
  process.on("unhandledRejection", (e) => log("unhandled rejection:", errMsg(e)));

  let srv;
  const shutdown = async () => { setTimeout(() => process.exit(0), 3000).unref(); srv?.close(); await hub.close(); process.exit(0); };
  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);

  const stdio = process.argv.includes("--stdio");
  const tokens = (process.env.HUB_TOKEN || "").split(",").map((s) => s.trim()).filter(Boolean);
  if (!stdio && (!tokens.length || tokens.some((t) => t.length < 16))) {
    log("HUB_TOKEN is required, min 16 chars (comma-separate several to rotate). Generate: node -e \"console.log(require('crypto').randomBytes(24).toString('hex'))\"");
    process.exit(1);
  }

  const started = hub.start(); // never block the front door on slow upstreams
  started.then(() => log(`mode: ${hub.mode()} (${hub.totalTools()} tools across ${cfg.accounts.length} accounts)`));
  if (stdio) { await started; await buildServer(hub).connect(new StdioServerTransport()); log("serving over stdio"); return; }

  const port = Number(process.env.PORT || 8787);
  srv = await startHttp(hub, tokens, port);
  log(`listening on :${port}`);
  log("connector URL:  https://<your-domain>/mcp/<HUB_TOKEN>");
}

function isMain() { try { return fs.realpathSync(process.argv[1]) === fileURLToPath(import.meta.url); } catch { return false; } }

export { Hub, Account, buildServer, loadConfig, makeHandler, startHttp };
if (isMain()) main().catch((e) => { log("fatal:", errMsg(e)); process.exit(1); });
