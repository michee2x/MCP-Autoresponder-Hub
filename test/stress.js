import http from "node:http";
import net from "node:net";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { spawn, execSync } from "node:child_process";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { Hub, Account, loadConfig } from "../hub.js";
import { startMock } from "./mock-mcp.js";
import { startOdd, startSession } from "./mocks2.js";

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const rec = (lvl, id, msg) => console.log(`${lvl.padEnd(5)} ${id.padEnd(4)} ${msg}`);
const mk = (accounts, extra = {}) => new Hub({ mode: "namespaced", maxToolsForNamespaced: 40, toolsTtlSeconds: 300, ...extra, accounts });
const text = (r) => r?.content?.[0]?.text ?? "";
const TOKEN = "stress-token-1234567890abcdef";
const t0 = Date.now();

await startMock("aweber", 9101); await startMock("systeme", 9102);
await startOdd(9201);
const sess = await startSession(9203);
net.createServer(() => {}).listen(9204); // accepts TCP, never answers = hung upstream

// ---- S5 (slow, run in background): one hung upstream vs. the whole hub
const slowP = (async () => {
  const h = mk([{ id: "hang", url: "http://127.0.0.1:9204/mcp" }, { id: "ok", url: "http://127.0.0.1:9101/mcp" }]);
  await h.accounts.get("ok").init();
  const a = h.accounts.get("hang"); a.status = "error"; a.lastAttempt = 0;
  const s = Date.now(); await h.listTools(); const ms = Date.now() - s;
  rec(ms > 10000 ? "BUG" : "PASS", "S5", `tools/list with ONE hung upstream took ${(ms / 1000).toFixed(1)}s (budget-capped; healthy accounts still listed: ${(await h.listTools()).length} tools)`);
})();

// ---- S1/S2/S3 tool-name collisions + huge result + tool errors
{
  const h = mk([{ id: "odd", url: "http://127.0.0.1:9201/mcp" }]); await h.start();
  const names = (await h.listTools()).map((t) => t.name);
  const dup = names.filter((n, i) => names.indexOf(n) !== i);
  rec(dup.length ? "BUG" : "PASS", "S1", `duplicate exposed tool names: ${dup.length ? dup.join(", ").slice(0, 90) : "none"}  (tools a.b & a_b, and two 60-char names)`);
  const reached = new Set();
  for (const n of names.filter((x) => x.startsWith("odd__") && !/big|fail/.test(x))) reached.add(text(await h.callTool(n)));
  rec(reached.size === 4 ? "PASS" : "BUG", "S1b", `each of the 4 colliding tools reachable via its own exposed name: ${reached.size}/4 distinct upstream tools hit`);
  rec(names.every((n) => n.length <= 64 && /^[a-zA-Z0-9_-]+$/.test(n)) ? "PASS" : "BUG", "S1c", "all exposed names <=64 chars and match ^[a-zA-Z0-9_-]+$");
  const big = await h.callTool("odd__big");
  rec(big.content.reduce((n, b) => n + (b.text?.length || 0), 0) < 70000 && /truncated/.test(big.content.at(-1).text) ? "PASS" : "BUG", "S3", `5,000,000-char upstream result -> ${big.content.reduce((n, b) => n + (b.text?.length || 0), 0)} chars returned, truncation notice present=${/truncated/.test(big.content.at(-1).text)}`);
  const f = await h.callTool("odd__fail");
  rec(f.isError && text(f) === "boom" ? "PASS" : "BUG", "S3b", `tool-level error passthrough: isError=${f.isError} text=${text(f)}`);
  await h.close();
}

// ---- S4 refresh race (thundering herd on a dead account)
{
  const orig = Account.prototype.connect; let n = 0;
  Account.prototype.connect = function () { n++; return orig.call(this); };
  const h = mk([{ id: "dead", url: "http://127.0.0.1:9999/mcp" }]);
  const a = h.accounts.get("dead"); a.status = "error"; a.lastAttempt = 0;
  await Promise.all(Array.from({ length: 30 }, () => h.refresh()));
  Account.prototype.connect = orig;
  rec(n > 1 ? "BUG" : "PASS", "S4", `30 concurrent tools/list => ${n} reconnect attempts to the same dead account (should be 1)`);
}

// ---- S6 downstream session expiry (what serverless recycling / server restarts look like)
{
  const h = mk([{ id: "sess", url: "http://127.0.0.1:9203/mcp" }]); await h.start();
  const first = text(await h.callTool("sess__ping"));
  sess.expire();
  const seq = []; let err0 = "";
  for (let i = 0; i < 4; i++) { const r = await h.callTool("sess__ping"); if (i === 0) err0 = text(r).slice(0, 80); seq.push(r.isError ? "ERR" : "ok"); await sleep(150); }
  const recovered = seq.includes("ok");
  rec(recovered && seq[0] === "ok" ? "PASS" : recovered ? "WARN" : "BUG", "S6", `before=${first}; after session expiry calls: ${seq.join(",")} | first error: ${err0}`);
  await h.close();
}

// ---- S7 stdio child crash + orphan check
{
  const h = mk([{ id: "stdio", command: "node", args: ["test/mock-stdio.js"] }]); await h.start();
  const pid1 = Number(text(await h.callTool("stdio__whoami")));
  process.kill(pid1, "SIGKILL"); await sleep(300);
  const r = await h.callTool("stdio__whoami"); const pid2 = Number(text(r));
  rec(pid2 && pid2 !== pid1 ? "PASS" : "BUG", "S7", `child crashed (pid ${pid1}); next call => ${r.isError ? "ERROR: " + text(r).slice(0, 80) : "recovered on new pid " + pid2}`);
  await h.close(); await sleep(500);
  let alive = true; try { process.kill(pid2 || pid1, 0); } catch { alive = false; }
  rec(alive ? "BUG" : "PASS", "S7b", `stdio child ${alive ? "STILL RUNNING (orphan)" : "terminated"} after hub.close()`);
}

// ---- S8 deny-list edge cases
{
  const a = new Account({ id: "x", url: "http://x", denyTools: ["delete_*"] }, 1000);
  const v = ["delete_subscriber", "DELETE_SUBSCRIBER", "delete_subscriber\n", "delete_subscriber\u200b", " delete_subscriber"].map((n) => [JSON.stringify(n), a.isAllowed(n)]);
  const leaks = v.filter(([, ok]) => ok);
  const hh = mk([{ id: "aweber", url: "http://127.0.0.1:9101/mcp", denyTools: ["delete_*"] }], { mode: "lazy" }); await hh.start();
  const outs = []; for (const t of ["delete_subscriber", "delete_subscriber\n", " delete_subscriber", "DELETE_SUBSCRIBER", ["delete_subscriber"]]) { const r = await hh.callTool("hub_call_tool", { account: "aweber", tool: t, arguments: { email: "a@b.c" } }); outs.push(r.isError && /unknown or blocked|must be a non-empty string/.test(text(r))); }
  rec(outs.every(Boolean) ? "PASS" : "BUG", "S8b", `lazy hub_call_tool: 5 deny-list evasion attempts all refused = ${outs.every(Boolean)}`); await hh.close();
  rec(leaks.length ? "INFO" : "PASS", "S8", `deny "delete_*" lets these through to the upstream: ${leaks.map(([n]) => n).join(" ") || "none"}`);
}

// ---- S11 config validation
{
  const tryCfg = (label, cfg, expectFail) => {
    const f = path.join(os.tmpdir(), "cfg-" + label + ".json"); fs.writeFileSync(f, JSON.stringify(cfg)); process.env.HUB_CONFIG = f;
    let out; try { const c = loadConfig(); out = "accepted"; if (expectFail) rec("BUG", "S11", `${label}: silently ${out}`); else rec("PASS", "S11", `${label}: ok`); } catch (e) { rec(expectFail ? "PASS" : "BUG", "S11", `${label}: rejected (${e.message.slice(0, 60)})`); }
  };
  const one = { id: "a", url: "http://x/mcp" };
  tryCfg("bad-mode-'Lazy'", { mode: "Lazy", accounts: [one] }, true);
  tryCfg("both-url-and-command", { accounts: [{ id: "a", url: "http://x", command: "node" }] }, true);
  tryCfg("bad-url-string", { accounts: [{ id: "a", url: "not a url" }] }, true);
  tryCfg("11-accounts", { accounts: Array.from({ length: 11 }, (_, i) => ({ id: "a" + i, url: "http://x" })) }, true);
  tryCfg("dup-ids", { accounts: [one, one] }, true);
  delete process.env.NOPE_TOKEN;
  tryCfg("missing-env-var", { accounts: [{ id: "a", url: "http://x", headers: { Authorization: "Bearer ${NOPE_TOKEN}" } }] }, true);
}

// ---- process-level tests
function spawnHub(cfg, env = {}, args = []) {
  const f = path.join(os.tmpdir(), "hub-proc-" + Math.random().toString(36).slice(2) + ".json"); fs.writeFileSync(f, JSON.stringify(cfg));
  const p = spawn("node", ["hub.js", ...args], { env: { ...process.env, HUB_CONFIG: f, ...env }, stdio: ["pipe", "pipe", "pipe"] });
  p.errText = ""; p.stderr.on("data", (d) => (p.errText += d));
  p.ready = new Promise((res) => { const iv = setInterval(() => { if (p.errText.includes("listening on") || p.errText.includes("serving over stdio")) { clearInterval(iv); res(); } }, 50); });
  return p;
}
const good = { accounts: [{ id: "aweber", url: "http://127.0.0.1:9101/mcp" }, { id: "systeme", url: "http://127.0.0.1:9102/mcp" }] };
{
  const p = spawnHub(good, { HUB_TOKEN: TOKEN, PORT: "8811" }); await p.ready;
  const base = "http://localhost:8811";
  const H = await (await fetch(base + "/health")).json();
  rec(JSON.stringify(H).includes("aweber") ? "BUG" : "PASS", "S9a", `unauthenticated /health exposes: ${JSON.stringify(H).slice(0, 90)}`);
  const init = JSON.stringify({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-03-26", capabilities: {}, clientInfo: { name: "t", version: "1" } } });
  let r = await fetch(`${base}/mcp/${TOKEN}`, { method: "POST", headers: { "content-type": "application/json", accept: "application/json, text/event-stream" }, body: "{not json" });
  rec(r.status === 400 ? "PASS" : "BUG", "S9b", `malformed JSON body -> HTTP ${r.status} (JSON-RPC spec: 400 / -32700)`);
  r = await fetch(`${base}/mcp/${TOKEN}`, { method: "POST", headers: { "content-type": "application/json" }, body: init });
  rec("INFO", "S9c", `initialize without Accept header -> HTTP ${r.status}`);
  r = await fetch(`${base}/mcp/${TOKEN}`, { method: "GET" }); rec(r.status === 405 ? "PASS" : "WARN", "S9d", `GET /mcp -> ${r.status}`);
  try { r = await fetch(`${base}/mcp/${TOKEN}`, { method: "POST", headers: { "content-type": "application/json" }, body: "x".repeat(5_000_000) }); rec(r.status === 413 ? "PASS" : "BUG", "S9e", `5MB body -> HTTP ${r.status} (should be 413)`); } catch (e) { rec("BUG", "S9e", `5MB body -> connection reset instead of HTTP 413 (${e.cause?.code || e.message})`); }
  const st = Date.now(); let bad = 0; for (let i = 0; i < 300; i++) { const x = await fetch(`${base}/mcp/wrong${i}`, { method: "POST" }); if (x.status === 401) bad++; }
  const ok = await fetch(`${base}/mcp/${TOKEN}`, { method: "POST", headers: { "content-type": "application/json", accept: "application/json, text/event-stream" }, body: init });
  rec(bad === 20 && ok.status === 200 ? "PASS" : "BUG", "S9f", `300 wrong guesses: ${bad} got 401, rest 429 (${Date.now() - st}ms); real token afterwards still works = ${ok.status === 200}`);

  const c = new Client({ name: "t", version: "1" }); await c.connect(new StreamableHTTPClientTransport(new URL(`${base}/mcp/${TOKEN}`)));
  const rss = () => Number(execSync(`ps -o rss= -p ${p.pid}`).toString().trim()) / 1024;
  const r0 = rss(); const s = Date.now(); let fail = 0;
  for (let b = 0; b < 15; b++) await Promise.all(Array.from({ length: 100 }, async () => { try { await c.listTools(); } catch { fail++; } }));
  rec(fail ? "BUG" : "PASS", "S9g", `1500 tools/list (100 concurrent) in ${Date.now() - s}ms, failures=${fail}`);
  fail = 0; const s2 = Date.now();
  for (let b = 0; b < 5; b++) await Promise.all(Array.from({ length: 100 }, async () => { try { const x = await c.callTool({ name: "aweber__get_lists", arguments: {} }); if (!text(x).startsWith("aweber")) fail++; } catch { fail++; } }));
  rec(fail ? "BUG" : "PASS", "S9h", `500 concurrent tool calls through hub in ${Date.now() - s2}ms, failures=${fail}`);
  await sleep(500); const r1 = rss();
  rec(r1 - r0 > 60 ? "WARN" : "PASS", "S9i", `RSS ${r0.toFixed(0)}MB -> ${r1.toFixed(0)}MB after ~2000 requests`);
  for (let b = 0; b < 15; b++) await Promise.all(Array.from({ length: 100 }, async () => { try { await c.listTools(); } catch { fail++; } }));
  await sleep(500); const r2 = rss();
  rec(r2 - r1 > 25 ? "BUG" : "PASS", "S9j", `RSS after a 2nd batch of 1500 requests: ${r1.toFixed(0)}MB -> ${r2.toFixed(0)}MB (plateau = no leak)`);
  await c.close(); p.kill("SIGTERM"); await sleep(300);
}

// ---- S13 upstream auth failure must surface the real reason
{
  await startMock("locked", 9105, "right-token");
  const h = mk([{ id: "locked", url: "http://127.0.0.1:9105/mcp", headers: { Authorization: "Bearer wrong" } }]); await h.start();
  const e = h.accounts.get("locked").error || "";
  rec(/401|Unauthorized/i.test(e) ? "PASS" : "BUG", "S13", `wrong upstream token -> reported error: ${e.slice(0, 110)}`);
}
// ---- S17 hung upstream at process level: front door must open fast and stay usable
{
  const t = Date.now();
  const p = spawnHub({ accounts: [{ id: "hang", url: "http://127.0.0.1:9204/mcp" }, ...good.accounts] }, { HUB_TOKEN: TOKEN, PORT: "8812" }); await p.ready;
  const up = Date.now() - t;
  const c = new Client({ name: "t", version: "1" }); await c.connect(new StreamableHTTPClientTransport(new URL(`http://localhost:8812/mcp/${TOKEN}`)));
  const s = Date.now(); const names = (await c.listTools()).tools.map((x) => x.name); const ms = Date.now() - s;
  const s2 = Date.now(); await c.listTools(); const ms2 = Date.now() - s2;
  rec(up < 3000 && ms < 12000 && ms2 < 1500 && names.includes("aweber__get_lists") ? "PASS" : "BUG", "S17", `hung upstream: port open ${up}ms, 1st tools/list ${ms}ms, 2nd ${ms2}ms, healthy accounts present=${names.includes("aweber__get_lists")}`);
  await c.close(); p.kill("SIGTERM");
}
// ---- S18 config via HUB_CONFIG_JSON (serverless style, base64) + token rotation list
{
  const b64 = Buffer.from(JSON.stringify(good)).toString("base64"); delete process.env.HUB_CONFIG; process.env.HUB_CONFIG_JSON = b64;
  try { const c = loadConfig(); rec(c.accounts.length === 2 ? "PASS" : "BUG", "S18", `HUB_CONFIG_JSON(base64) loaded ${c.accounts.length} accounts`); } catch (e) { rec("BUG", "S18", e.message); }
  delete process.env.HUB_CONFIG_JSON;
}
// ---- S19 Vercel adapter: platform pre-parses body (req.body) and routes /api/mcp/<token>
{
  process.env.HUB_TOKEN = TOKEN; process.env.HUB_CONFIG_JSON = JSON.stringify(good);
  const vercelFn = (await import("../api/_hub.js")).default;
  const srv = http.createServer(async (req, res) => { // emulate @vercel/node helpers: consume stream, set req.body
    const ch = []; for await (const c of req) ch.push(c);
    const raw = Buffer.concat(ch).toString("utf8"); if (raw) { try { req.body = JSON.parse(raw); } catch { req.body = raw; } }
    vercelFn(req, res);
  });
  await new Promise((r) => srv.listen(8813, r));
  const c = new Client({ name: "t", version: "1" }); await c.connect(new StreamableHTTPClientTransport(new URL(`http://localhost:8813/api/mcp/${TOKEN}`)));
  const names = (await c.listTools()).tools.map((x) => x.name); const r = await c.callTool({ name: "systeme__get_lists", arguments: {} });
  rec(names.includes("aweber__get_lists") && text(r).startsWith("systeme") ? "PASS" : "BUG", "S19", `Vercel-style adapter via /api/mcp/<token> with pre-parsed body: ${names.length} tools, call -> ${text(r).slice(0, 30)}`);
  const c2 = new Client({ name: "t", version: "1" }); await c2.connect(new StreamableHTTPClientTransport(new URL("http://localhost:8813/api/mcp"), { requestInit: { headers: { Authorization: "Bearer " + TOKEN } } }));
  rec((await c2.listTools()).tools.length > 3 ? "PASS" : "BUG", "S19b", "Vercel-style adapter via /api/mcp + Authorization: Bearer header");
  await c.close(); await c2.close(); srv.close(); delete process.env.HUB_CONFIG_JSON;
}
// ---- S10 --stdio mode must keep stdout clean JSON
{
  const p = spawnHub(good, {}, ["--stdio"]); await p.ready;
  let out = ""; p.stdout.on("data", (d) => (out += d));
  const send = (o) => p.stdin.write(JSON.stringify(o) + "\n");
  send({ jsonrpc: "2.0", id: 1, method: "initialize", params: { protocolVersion: "2025-03-26", capabilities: {}, clientInfo: { name: "t", version: "1" } } });
  send({ jsonrpc: "2.0", method: "notifications/initialized" }); send({ jsonrpc: "2.0", id: 2, method: "tools/list" });
  await sleep(1500); p.kill("SIGTERM");
  const lines = out.split("\n").filter(Boolean); let clean = lines.length >= 2; for (const l of lines) { try { JSON.parse(l); } catch { clean = false; } }
  rec(clean ? "PASS" : "BUG", "S10", `--stdio mode: ${lines.length} stdout lines, all valid JSON-RPC = ${clean}`);
}

// ---- Static checks
try { execSync("node --check hub.js", { stdio: "pipe" }); rec("PASS", "S12a", "syntax check"); } catch (e) { rec("BUG", "S12a", "syntax error"); }
try { const a = JSON.parse(execSync("npm audit --omit=dev --json || true", { stdio: "pipe" }).toString()); rec(a.metadata?.vulnerabilities?.total ? "WARN" : "PASS", "S12b", `npm audit (prod deps): ${JSON.stringify(a.metadata?.vulnerabilities)}`); } catch { rec("INFO", "S12b", "npm audit unavailable (offline registry?)"); }
await slowP;
console.log(`done in ${((Date.now() - t0) / 1000).toFixed(0)}s`); process.exit(0);
