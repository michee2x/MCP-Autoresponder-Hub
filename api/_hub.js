// Shared Vercel entry. Files starting with "_" are not routed by Vercel.
import { Hub, loadConfig, makeHandler } from "../hub.js";
let handler;
function get() {
  if (!handler) {
    const tokens = (process.env.HUB_TOKEN || "").split(",").map((s) => s.trim()).filter(Boolean);
    if (!tokens.length || tokens.some((t) => t.length < 16)) throw new Error("HUB_TOKEN missing or shorter than 16 chars");
    const hub = new Hub(loadConfig()); // config from HUB_CONFIG_JSON env var
    hub.start();                       // connect in background; requests never wait on a dead upstream
    handler = makeHandler(hub, tokens, { trustProxy: true });
  }
  return handler;
}
export default async function (req, res) {
  try { return await get()(req, res); }
  catch (e) { res.statusCode = 500; res.setHeader("content-type", "application/json"); res.end(JSON.stringify({ error: String(e.message || e) })); }
}
