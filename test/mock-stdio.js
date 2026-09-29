import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StdioServerTransport } from "@modelcontextprotocol/sdk/server/stdio.js";
import { ListToolsRequestSchema, CallToolRequestSchema } from "@modelcontextprotocol/sdk/types.js";
const s = new Server({ name: "stdio-mock", version: "1" }, { capabilities: { tools: {} } });
s.setRequestHandler(ListToolsRequestSchema, async () => ({ tools: [{ name: "whoami", description: "pid", inputSchema: { type: "object", properties: {} } }] }));
s.setRequestHandler(CallToolRequestSchema, async () => ({ content: [{ type: "text", text: String(process.pid) }] }));
await s.connect(new StdioServerTransport());
