# EAST Railway Hub

Pure relay — **not** part of consensus. Sits between the L1 validator
(EASTCHAIN on Vercel) and every connected Light Node.

```
Validator (Vercel, L1)
   │  POST /internal/publish-block  (x-railway-secret header)
   ▼
Railway Hub  ───────────────►  broadcasts block:new to every Light Node (WS)
   ▲
   │  heartbeat / sync_request / ack / tx:submit  (WS, forwarded best-effort)
Light Node (user's phone, browser WS client)
```

## Deploy on Railway
1. Push this folder as its own repo (or a Railway service pointed at this subfolder).
2. Railway auto-detects `package.json` → runs `npm run build` then `npm start`.
3. Set environment variables in Railway:
   - `RAILWAY_VALIDATOR_SECRET` — random long string. Must match the value
     Vercel uses when it POSTs to `/internal/publish-block`.
   - `PORT` — Railway sets this automatically, no need to set manually.
4. Note the public URL Railway gives you (e.g. `https://east-hub-production.up.railway.app`).
   - WS endpoint for Light Nodes: `wss://<that-domain>/`
   - HTTP publish endpoint for Vercel: `https://<that-domain>/internal/publish-block`
   - Debug status: `GET https://<that-domain>/status`

## On the Vercel (Next.js) side
Set these env vars:
- `RAILWAY_PUBLISH_URL=https://<domain>/internal/publish-block`
- `RAILWAY_VALIDATOR_SECRET=<same value as above>`
- `NEXT_PUBLIC_RAILWAY_WS_URL=wss://<domain>/` (public — used by the browser Light Node client)

## Phase 2 — Chain gateway (east-validator proxy)

The Hub can forward read/write HTTP to the **east-validator** sealer so Vercel
and clients stop treating Neon as the ledger source of truth.

### Extra environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `EAST_VALIDATOR_URL` | yes (for proxy) | Base URL of the Go validator, e.g. `https://east-validator-production.up.railway.app` (no trailing slash). Alias: `VALIDATOR_HTTP_URL`. |
| `VALIDATOR_API_SECRET` | yes (for `POST /rpc/tx`) | Same value as the validator's `API_SECRET`. Hub sends it as `X-API-Secret`. Alias: `EAST_VALIDATOR_API_SECRET`. |
| `CHAIN_PROXY_TIMEOUT_MS` | no | Default `12000`. |

Existing vars (`RAILWAY_VALIDATOR_SECRET`, `PORT`, …) are unchanged.

### Public proxy routes

| Hub route | Upstream (validator) |
|-----------|----------------------|
| `GET /rpc/account/:address` | `GET /account/:address` |
| `GET /rpc/block/latest` | `GET /block/latest` |
| `GET /rpc/block/:height` | `GET /block/:height` |
| `GET /rpc/supply` | `GET /supply` |
| `GET /rpc/supply/:bucket` | `GET /supply/:bucket` |
| `GET /rpc/stats` | `GET /stats` |
| `POST /rpc/tx` | `POST /tx` (body = signed tx JSON; hub attaches API secret) |

`GET /health` includes a `chain` object: `{ configured, ok, latencyMs, raw }` from the validator's `/health`.

### Example

```bash
# Account balance on chain (6-decimal subunits)
curl -s "$HUB_URL/rpc/account/0xYourAddress"

# Latest block
curl -s "$HUB_URL/rpc/block/latest"

# Submit signed tx (transfer / stake / claim_mining / …)
curl -s -X POST "$HUB_URL/rpc/tx" \
  -H "Content-Type: application/json" \
  -d @signed-tx.json
```

The Hub does **not** store balances. If `EAST_VALIDATOR_URL` is unset, `/rpc/*` returns `503`.
