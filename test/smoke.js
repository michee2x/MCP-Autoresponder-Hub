import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import assert from "node:assert/strict";
import { Client } from "@modelcontextprotocol/sdk/client/index.js";
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js";
import { startMock } from "./mock-mcp.js";

const TOKEN = "test-token-1234567890abcdef";
const mocks = [await startMock("aweber", 9101, "secret-a"), await startMock("systeme", 9102)];

async function run(mode) {
  const cfgFile = path.join(os.tmpdir(), `hub-test-${mode}.json`);
  fs.writeFileSync(cfgFile, JSON.stringify({
    mode,
    accounts: [
      { id: "aweber-main", label: "AWeber Main", url: "http://localhost:9101/mcp", headers: { Authorization: "Bearer ${MOCK_A}" }, denyTools: ["delete_*"] },
      { id: "systeme", label: "systeme.io", url: "http://localhost:9102/mcp" },
      { id: "broken", url: "http://localhost:9999/mcp" },
    ],
  }));
  const port = mode === "lazy" ? 8802 : 8801;
  const hub = spawn("node", ["hub.js"], { env: { ...process.env, HUB_CONFIG: cfgFile, HUB_TOKEN: TOKEN, PORT: String(port), MOCK_A: "secret-a" }, stdio: ["ignore", "inherit", "inherit"] });
  await new Promise((r) => setTimeout(r, 3500));
  try {
    // wrong token rejected
    const bad = await fetch(`http://localhost:${port}/mcp/nope`, { method: "POST" });
    assert.equal(bad.status, 401);

    const client = new Client({ name: "test", version: "1" });
    await client.connect(new StreamableHTTPClientTransport(new URL(`http://localhost:${port}/mcp/${TOKEN}`)));
    const names = (await client.listTools()).tools.map((t) => t.name);
    console.log(`[${mode}]`, names.join(", "));

    const accts = JSON.parse((await client.callTool({ name: "hub_list_accounts", arguments: {} })).content[0].text);
    assert.equal(accts.find((a) => a.account === "broken").status, "error"); // one dead account doesn't break the rest
    assert.equal(accts.find((a) => a.account === "aweber-main").tools, 2);   // delete_* filtered

    if (mode === "namespaced") {
      assert.ok(names.includes("aweber-main__add_subscriber") && names.includes("systeme__get_lists"));
      assert.ok(!names.includes("aweber-main__delete_subscriber") && names.includes("systeme__delete_subscriber"));
      const r = await client.callTool({ name: "systeme__add_subscriber", arguments: { email: "a@b.com" } });
      assert.match(r.content[0].text, /^systeme:add_subscriber/);
    } else {
      assert.deepEqual(names.sort(), ["hub_call_tool", "hub_list_accounts", "hub_list_tools"]);
      const lt = JSON.parse((await client.callTool({ name: "hub_list_tools", arguments: { account: "systeme", search: "add" } })).content[0].text);
      assert.equal(lt.length, 1);
      const r = await client.callTool({ name: "hub_call_tool", arguments: { account: "aweber-main", tool: "add_subscriber", arguments: { email: "x@y.com" } } });
      assert.match(r.content[0].text, /^aweber:add_subscriber/);
      const blocked = await client.callTool({ name: "hub_call_tool", arguments: { account: "aweber-main", tool: "delete_subscriber", arguments: { email: "x@y.com" } } });
      assert.equal(blocked.isError, true);
    }
    await client.close();
    console.log(`[${mode}] OK`);
  } finally { hub.kill(); }
}

try { await run("namespaced"); await run("lazy"); console.log("ALL PASSED"); }
catch (e) { console.error("FAILED:", e); process.exitCode = 1; }
finally { mocks.forEach((m) => m.close()); }
