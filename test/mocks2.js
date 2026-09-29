import http from "node:http";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js";
const S = { type: "object", properties: {} };

// Adversarial tool names / huge results / tool-level errors
export function startOdd(port) {
  const tools = ["a.b", "a_b", "t".repeat(59) + "1", "t".repeat(59) + "2", "big", "fail"].map((name) => ({ name, description: name.slice(0, 8), inputSchema: S }));
  const srv = http.createServer(async (req, res) => {
    const ch = []; for await (const c of req) ch.push(c);
    const body = ch.length ? JSON.parse(Buffer.concat(ch)) : undefined;
    const s = new Server({ name: "odd", version: "1" }, { capabilities: { tools: {} } });
    s.setRequestHandler(ListToolsRequestSchema, async () => ({ tools }));
    s.setRequestHandler(CallToolRequestSchema, async (r) => r.params.name === "big" ? { content: [{ type: "text", text: "x".repeat(5_000_000) }] }
      : r.params.name === "fail" ? { isError: true, content: [{ type: "text", text: "boom" }] } : { content: [{ type: "text", text: "ran:" + r.params.name }] });
    const t = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    res.on("close", () => { t.close(); s.close(); });
    await s.connect(t); await t.handleRequest(req, res, body);
  });
  return new Promise((r) => srv.listen(port, () => r(srv)));
}

// Stateful server: issues Mcp-Session-Id, can "expire" it (simulates restarts / serverless recycling)
export function startSession(port) {
  let sid = null;
  const srv = http.createServer(async (req, res) => {
    const ch = []; for await (const c of req) ch.push(c);
    const body = ch.length ? JSON.parse(Buffer.concat(ch)) : {};
    const send = (id, result, h = {}) => { res.writeHead(200, { "content-type": "application/json", ...h }); res.end(JSON.stringify({ jsonrpc: "2.0", id, result })); };
    if (req.method !== "POST") { res.writeHead(405); return res.end(); }
    if (body.method === "initialize") { sid = "s" + Math.random().toString(36).slice(2); return send(body.id, { protocolVersion: body.params.protocolVersion, capabilities: { tools: {} }, serverInfo: { name: "sess", version: "1" } }, { "mcp-session-id": sid }); }
    if (req.headers["mcp-session-id"] !== sid) { res.writeHead(404, { "content-type": "application/json" }); return res.end(JSON.stringify({ jsonrpc: "2.0", error: { code: -32001, message: "Session not found" }, id: body.id ?? null })); }
    if (body.method === "notifications/initialized") { res.writeHead(202); return res.end(); }
    if (body.method === "tools/list") return send(body.id, { tools: [{ name: "ping", description: "ping", inputSchema: S }] });
    if (body.method === "tools/call") return send(body.id, { content: [{ type: "text", text: "pong" }] });
    res.writeHead(202); res.end();
  });
  return new Promise((r) => srv.listen(port, () => r({ srv, expire: () => { sid = null; } })));
}
