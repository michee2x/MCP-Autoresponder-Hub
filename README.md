# MCP Autoresponder Hub

One MCP endpoint for Claude or ChatGPT that fans out to **up to 10** autoresponder MCP servers
(AWeber, systeme.io, MailerLite, GetResponse... or several accounts of the same platform).

```
Claude / ChatGPT  --(1 connector)-->  Hub  -->  AWeber acct 1
                                            -->  AWeber acct 2
                                            -->  systeme.io
                                            -->  ... (max 10)
```

## Setup (5 minutes)

```bash
npm install
cp hub.config.example.json hub.config.json   # list your MCPs here
cp .env.example .env                         # HUB_TOKEN + credentials
npm start
```

Each account in `hub.config.json` is either:
- **Remote**: `"url"` (+ optional `"headers"` for API key / bearer token). Streamable HTTP, auto-fallback to SSE.
- **Local**: `"command"` + `"args"` + `"env"` (stdio MCP run as a child process).

`${VAR}` values are read from `.env`. Use a different `id` per connection, so two AWeber accounts = two entries.

## Connect your AI account

Deploy somewhere with HTTPS (Railway, Render, Fly.io, a VPS + Caddy, or `docker build -t mcp-hub .`), then add ONE custom connector:

- **Claude.ai** (Settings > Connectors > Add custom connector) and **ChatGPT** (Developer mode > Connectors):
  `https://your-domain.com/mcp/<HUB_TOKEN>`
- Clients that support headers: `https://your-domain.com/mcp` with `Authorization: Bearer <HUB_TOKEN>`
- **Claude Desktop / Claude Code (local)**: run in stdio mode, `node hub.js --stdio`

Treat the URL like a password: it grants access to every connected account.

## How tools are exposed

`"mode": "auto"` (default) picks for you:
- **namespaced** (<= 40 total tools): every tool appears as `accountId__toolName`, e.g. `aweber-main__add_subscriber`.
- **lazy** (> 40 tools): only 3 tools are shown (`hub_list_accounts`, `hub_list_tools`, `hub_call_tool`) and the AI discovers
  tools per account on demand. This keeps the AI's context small when 10 MCPs are connected.

Force either with `"mode": "namespaced"` or `"mode": "lazy"`.

## Safety controls

- `denyTools` / `allowTools` per account, with `*` wildcards (e.g. `["delete_*", "send_broadcast"]`). Blocked tools are hidden and refused.
  Strongly recommended for anything that emails a whole list or deletes subscribers.
- A dead account never breaks the others; it is retried every 60s. Check with `hub_list_accounts`.
- Max 10 accounts is enforced at startup.

## Deploying on Vercel

Included: `api/mcp/index.js`, `api/mcp/[token].js`, `vercel.json` (300s max duration, the ceiling on Hobby with Fluid compute).

1. Push the folder to a Git repo and import it in Vercel (Fluid compute on).
2. `hub.config.json` is git-ignored on purpose, so pass the config as an env var: `HUB_CONFIG_JSON` = the JSON (raw, or base64 of it).
   Also set `HUB_TOKEN` and every `${VAR}` your config references (Project Settings > Environment Variables).
3. Connector URL: `https://<project>.vercel.app/api/mcp/<HUB_TOKEN>`  (or `/api/mcp` + Bearer header).
4. Turn OFF Vercel Deployment Protection for the production domain, or Claude/ChatGPT get a login page instead of your MCP.

Vercel caveats:
- **stdio (`command`) accounts don't work** in serverless. Use remote `url` MCPs only.
- Instances get recycled: the hub reconnects and re-lists tools on a cold start (first request can take a few seconds).
- The token is part of the URL, and Vercel request logs record URL paths. Prefer the Bearer header where the client allows it and rotate
  the token if logs are shared (`HUB_TOKEN` accepts several comma-separated tokens for rotation).
- Not verified on a live Vercel deployment (only against a local emulation of its request handling).

## Limits to know about

- Downstream MCPs that require an **interactive OAuth login** (browser consent) can't be logged into by the hub.
  Give the hub a static API key/token instead, or use a platform's API-key MCP.
- Results larger than `maxResultChars` (default 60,000) are truncated with a notice; image blocks are not capped.
- Tool lists refresh every `toolsTtlSeconds`; upstream `list_changed` notifications are not used.
- Tests: `npm test` (smoke) and `npm run stress` (36 adversarial checks: collisions, hung/dead upstreams, session expiry, crashes, limits).
