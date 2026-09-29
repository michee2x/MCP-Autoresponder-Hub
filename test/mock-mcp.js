// Tiny fake "autoresponder" MCP server used by the smoke test.
import http from "node:http";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js";

export function startMock(name, port, requiredToken) {
  const tools = [
    { name: "get_lists", description: `Get lists from ${name}`, inputSchema: { type: "object", properties: {} } },
    { name: "add_subscriber", description: "Add a subscriber", inputSchema: { type: "object", properties: { email: { type: "string" } }, required: ["email"] } },
    { name: "delete_subscriber", description: "Delete a subscriber", inputSchema: { type: "object", properties: { email: { type: "string" } }, required: ["email"] } },
  ];
  const srv = http.createServer(async (req, res) => {
    if (requiredToken && req.headers.authorization !== `Bearer ${requiredToken}`) { res.writeHead(401); return res.end(); }
    const chunks = []; for await (const c of req) chunks.push(c);
    const body = chunks.length ? JSON.parse(Buffer.concat(chunks)) : undefined;
    const s = new Server({ name, version: "1" }, { capabilities: { tools: {} } });
    s.setRequestHandler(ListToolsRequestSchema, async () => ({ tools }));
    s.setRequestHandler(CallToolRequestSchema, async (r) => ({ content: [{ type: "text", text: `${name}:${r.params.name}:${JSON.stringify(r.params.arguments)}` }] }));
    const t = new StreamableHTTPServerTransport({ sessionIdGenerator: undefined });
    res.on("close", () => { t.close(); s.close(); });
    await s.connect(t); await t.handleRequest(req, res, body);
  });
  return new Promise((r) => srv.listen(port, () => r(srv)));
}
