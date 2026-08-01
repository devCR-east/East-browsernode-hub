import { createServer, IncomingMessage, ServerResponse } from "http";
import { WebSocketServer, WebSocket } from "ws";
import {
  InboundMessage, BlockHeader, Role, EAST_CHAIN_ID, NodeTier,
} from "./types";
import {
  chainConfigured,
  chainBaseUrl,
  proxyGet,
  proxyPostTx,
  pingValidator,
} from "./chain-proxy";

const PORT = Number(process.env.PORT || process.env.WS_PORT || 8081);
const VALIDATOR_SECRET = process.env.RAILWAY_VALIDATOR_SECRET || "";
const DEBUG_MODE = process.env.DEBUG_MODE === "true";

// ─── Utility: Enhanced Logging with Timestamps ─────────────────
function log(prefix: string, msg: string, data?: unknown) {
  const timestamp = new Date().toISOString();
  if (data) {
    console.log(`[${timestamp}] [${prefix}] ${msg}`, JSON.stringify(data, null, 2));
  } else {
    console.log(`[${timestamp}] [${prefix}] ${msg}`);
  }
}

function logDebug(prefix: string, msg: string, data?: unknown) {
  if (DEBUG_MODE) {
    const timestamp = new Date().toISOString();
    if (data) {
      console.debug(`[${timestamp}] [${prefix}] [DEBUG] ${msg}`, JSON.stringify(data, null, 2));
    } else {
      console.debug(`[${timestamp}] [${prefix}] [DEBUG] ${msg}`);
    }
  }
}

if (!VALIDATOR_SECRET) {
  log("RAILWAY", "⚠️  WARNING: RAILWAY_VALIDATOR_SECRET is not set — anyone could connect as validator!");
}

log("RAILWAY", `🚀 Starting Railway Hub on port ${PORT}`, { DEBUG_MODE });

// Top-N light nodes by score get promoted to "relay"
const GUARDIAN_COUNT = 20;
const BROADCASTER_COUNT = 400; // 20 per guardian
const VISION_COUNT = 8000; // 20 per broadcaster
const ECHO_COUNT = 160000; // 20 per vision
const RELAY_RESCORE_INTERVAL_MS = 60_000;

// Bootstrap: how many peer candidates a freshly-connected node gets handed
// so it can start its own gossip mesh immediately, instead of waiting up to
// RELAY_RESCORE_INTERVAL_MS for a tier:assign parent. This is deliberately
// NOT the same mechanism as tier assignment — no sorting/ranking of the
// whole roster, just a cheap weighted sample. See sampleBootstrapPeers().
const BOOTSTRAP_SAMPLE_SIZE = 8;

// Connection ceiling & per-IP rate limit
const MAX_LIGHT_NODES = Number(process.env.MAX_LIGHT_NODES || 5000);
const IP_RATE_LIMIT_MAX = 10;
const IP_RATE_LIMIT_WINDOW_MS = 60_000;
const connectionAttemptsByIp = new Map<string, number[]>();

function isRateLimited(ip: string): boolean {
  const now = Date.now();
  const recent = (connectionAttemptsByIp.get(ip) || []).filter((t) => now - t < IP_RATE_LIMIT_WINDOW_MS);
  recent.push(now);
  connectionAttemptsByIp.set(ip, recent);
  const limited = recent.length > IP_RATE_LIMIT_MAX;
  if (limited) {
    log("RATE_LIMIT", `⛔ IP ${ip} exceeded rate limit`, { attempts: recent.length, max: IP_RATE_LIMIT_MAX });
  }
  return limited;
}

// Prevents connectionAttemptsByIp from growing forever
setInterval(() => {
  const cutoff = Date.now() - IP_RATE_LIMIT_WINDOW_MS;
  let cleaned = 0;
  connectionAttemptsByIp.forEach((timestamps, ip) => {
    const recent = timestamps.filter((t) => t > cutoff);
    if (recent.length === 0) {
      connectionAttemptsByIp.delete(ip);
      cleaned++;
    } else {
      connectionAttemptsByIp.set(ip, recent);
    }
  });
  if (cleaned > 0) {
    logDebug("RATE_LIMIT", `Cleaned up ${cleaned} IP entries from rate limit map`);
  }
}, IP_RATE_LIMIT_WINDOW_MS);

interface EastSocket extends WebSocket {
  isAlive?: boolean;
  role?: Role;
  nodeId?: string;
  connectedAt?: number;
}

// ─── State ────────────────────────────────────────────────────────
let validatorSocket: EastSocket | null = null;
const lightNodes = new Map<string, EastSocket>();
const fullLightNodes = new Map<string, EastSocket>(); // subset of lightNodes with nodeType:"full" — see HelloMessage.nodeType doc comment in types.ts
let latestHeader: BlockHeader | null = null;
const recentHeaders: BlockHeader[] = []; // rolling buffer, newest last
const BACKFILL_SIZE = 1000; // How many recent headers sync_request can serve — kept in sync with the ring buffer trim below and with client.ts's RAILWAY_BACKFILL_LIMIT

// Node telemetry for /status endpoint
interface NodeTelemetry {
  lastHeartbeat: number;
  lastAckHeight: number;
  connectedAt: number;
  avgLatencyMs: number;
  participationSeconds: number;
  verifiedHeaderCount: number;
  tier: NodeTier;
  parentNodeId: string | null;
  hasFullLedger: boolean;
  nodeType: "light" | "full";
  messagesReceived: number;
  messagesSent: number;
  lastMessageType?: string;
  lastMessageTime?: number;
}
const telemetry = new Map<string, NodeTelemetry>();
let currentFullSyncProviders: string[] = [];

// ─── Message counters for statistics ─────────────────────────────
interface MessageStats {
  [key: string]: number;
}
const messageStats: MessageStats = {};

// ─── RPC balance request/response correlation ──────────────────────
// Vercel's /internal/rpc-get-balance HTTP call picks a full node and waits
// on this promise; the matching "rpc_balance_response" WS message (or a
// timeout) resolves it. Deliberately short-lived and in-memory only — if
// the hub restarts mid-request, Vercel's own timeout/fallback-to-Postgres
// handles it, nothing here needs to survive a restart.
const pendingBalanceRequests = new Map<string, { resolve: (balance: string | null) => void; timeout: NodeJS.Timeout }>();
const RPC_BALANCE_REQUEST_TIMEOUT_MS = 2000;

function requestBalanceFromFullNode(address: string): Promise<string | null> {
  return new Promise((resolve) => {
    // Pick randomly among the top 5 scored full nodes (or however many are
    // connected, if fewer) — "best PoC score, chosen randomly among the
    // good candidates" per the design brief, so load doesn't always funnel
    // through a single top-ranked node.
    const candidates = [...fullLightNodes.entries()]
      .map(([nodeId, socket]) => ({ nodeId, socket, score: score(telemetry.get(nodeId)!) }))
      .filter((c) => telemetry.get(c.nodeId)) // telemetry must exist
      .sort((a, b) => b.score - a.score)
      .slice(0, 5);

    if (candidates.length === 0) {
      resolve(null); // no full node available — caller falls back to Postgres
      return;
    }
    const picked = candidates[Math.floor(Math.random() * candidates.length)];

    const requestId = `${Date.now()}-${Math.random().toString(36).slice(2)}`;
    const timeout = setTimeout(() => {
      pendingBalanceRequests.delete(requestId);
      resolve(null); // timed out — caller falls back to Postgres
    }, RPC_BALANCE_REQUEST_TIMEOUT_MS);
    pendingBalanceRequests.set(requestId, { resolve, timeout });

    send(picked.socket, { type: "rpc_balance_request", requestId, address });
    logDebug("RPC", `Balance request ${requestId} sent to ${picked.nodeId}`, { address, candidateCount: candidates.length });
  });
}

function recordMessageStat(type: string) {
  messageStats[type] = (messageStats[type] || 0) + 1;
}

function recomputeFullSyncProviders() {
  const providers = [...telemetry.entries()]
    .filter(([, t]) => t.hasFullLedger)
    .map(([nodeId]) => nodeId);
  const changed =
    providers.length !== currentFullSyncProviders.length ||
    providers.some((id) => !currentFullSyncProviders.includes(id));
  if (changed) {
    currentFullSyncProviders = providers;
    broadcastToLightNodes({ type: "full_sync_providers", nodeIds: currentFullSyncProviders });
    log("FULL_SYNC", `✅ Full-sync providers updated`, { providers: currentFullSyncProviders, count: providers.length });
  } else {
    logDebug("FULL_SYNC", `Full-sync providers unchanged`, { count: providers.length });
  }
}

function score(t: NodeTelemetry): number {
  const latencyScore = 1000 / Math.max(t.avgLatencyMs, 1);
  return latencyScore * Math.log1p(t.participationSeconds) * Math.log1p(t.verifiedHeaderCount);
}

// Weighted random sample of up to `count` connected light nodes, excluding
// `excludeNodeId` (the requester itself). Weighted by participationSeconds
// (+1 so a brand-new node still has a nonzero, just small, chance) using the
// Efraimidis-Spirakis algorithm: each candidate gets key = random()^(1/weight),
// and the top `count` keys win. This is deliberately NOT pure-random —
// pure-random sampling over all connected sockets would let an attacker who
// opens thousands of throwaway connections dominate the sample a new node
// receives (a classic eclipse-attack setup), since every socket counts
// equally regardless of how long it's actually been a real, active node.
// Biasing toward proven participationSeconds means a Sybil swarm of
// just-connected sockets rarely gets drawn compared to nodes with an actual
// track record. This never touches score() or recomputeTiers() — it's O(N)
// once per bootstrap (rare), not O(N) on a recurring timer.
function sampleBootstrapPeers(excludeNodeId: string, count: number): string[] {
  const keyed: { nodeId: string; key: number }[] = [];
  for (const [nodeId, socket] of lightNodes) {
    if (nodeId === excludeNodeId) continue;
    if (socket.readyState !== socket.OPEN) continue;
    const weight = (telemetry.get(nodeId)?.participationSeconds ?? 0) + 1;
    keyed.push({ nodeId, key: Math.random() ** (1 / weight) });
  }
  keyed.sort((a, b) => b.key - a.key);
  return keyed.slice(0, count).map((c) => c.nodeId);
}

function recomputeTiers() {
  const validNodes = [...telemetry.entries()].filter(([, t]) => t.avgLatencyMs > 0);
  logDebug("TIERS", `Computing tiers from ${validNodes.length} valid nodes`, { totalNodes: telemetry.size });

  const ranked = validNodes
    .sort(([, a], [, b]) => score(b) - score(a))
    .map(([nodeId]) => nodeId);

  const leaderId: string | null = ranked[0] ?? null;
  const guardianIds = ranked.slice(1, 1 + GUARDIAN_COUNT);
  const broadcasterIds = ranked.slice(1 + GUARDIAN_COUNT, 1 + GUARDIAN_COUNT + BROADCASTER_COUNT);
  const visionIds = ranked.slice(
    1 + GUARDIAN_COUNT + BROADCASTER_COUNT,
    1 + GUARDIAN_COUNT + BROADCASTER_COUNT + VISION_COUNT
  );
  const echoIds = ranked.slice(
    1 + GUARDIAN_COUNT + BROADCASTER_COUNT + VISION_COUNT,
    1 + GUARDIAN_COUNT + BROADCASTER_COUNT + VISION_COUNT + ECHO_COUNT
  );

  // Build the new (tier, parent) for every node currently known, whether
  // ranked into a tier or not (unranked nodes explicitly get tier:"none",
  // parent:null — same as an unscored brand-new node, falls back to
  // Railway/archive/validator directly per client.ts's own bootstrap path).
  const assignments = new Map<string, { tier: NodeTier; parentNodeId: string | null }>();
  for (const nodeId of telemetry.keys()) assignments.set(nodeId, { tier: "none", parentNodeId: null });

  if (leaderId) assignments.set(leaderId, { tier: "leader", parentNodeId: null });
  guardianIds.forEach((id) => assignments.set(id, { tier: "guardian", parentNodeId: leaderId }));
  broadcasterIds.forEach((id, i) => {
    const parent = guardianIds.length > 0 ? guardianIds[i % guardianIds.length] : leaderId;
    assignments.set(id, { tier: "broadcaster", parentNodeId: parent });
  });
  visionIds.forEach((id, i) => {
    const parent = broadcasterIds.length > 0 ? broadcasterIds[i % broadcasterIds.length]
      : (guardianIds.length > 0 ? guardianIds[i % guardianIds.length] : leaderId);
    assignments.set(id, { tier: "vision", parentNodeId: parent });
  });
  echoIds.forEach((id, i) => {
    const parent = visionIds.length > 0 ? visionIds[i % visionIds.length]
      : (broadcasterIds.length > 0 ? broadcasterIds[i % broadcasterIds.length]
        : (guardianIds.length > 0 ? guardianIds[i % guardianIds.length] : leaderId));
    assignments.set(id, { tier: "echo", parentNodeId: parent });
  });

  let changedCount = 0;
  for (const [nodeId, next] of assignments) {
    const t = telemetry.get(nodeId);
    if (!t) continue;
    const changed = t.tier !== next.tier || t.parentNodeId !== next.parentNodeId;
    t.tier = next.tier;
    t.parentNodeId = next.parentNodeId;
    if (changed) {
      changedCount++;
      const s = lightNodes.get(nodeId);
      if (s) {
        send(s, { type: "tier:assign", tier: next.tier, parentNodeId: next.parentNodeId });
      }
    }
  }

  log("TIERS", `🌳 Tier hierarchy recomputed`, {
    leader: leaderId,
    guardians: guardianIds.length,
    broadcasters: broadcasterIds.length,
    visions: visionIds.length,
    echoes: echoIds.length,
    unranked: telemetry.size - (leaderId ? 1 : 0) - guardianIds.length - broadcasterIds.length - visionIds.length - echoIds.length,
    reassigned: changedCount,
  });
}


function send(socket: EastSocket, msg: unknown) {
  if (socket.readyState === WebSocket.OPEN) {
    try {
      socket.send(JSON.stringify(msg));
      logDebug("SOCKET", `Sent message to ${socket.nodeId}`, msg);
    } catch (err) {
      log("SOCKET", `❌ Error sending message to ${socket.nodeId}`, err);
    }
  } else {
    logDebug("SOCKET", `Cannot send - socket not open for ${socket.nodeId}`, { state: socket.readyState });
  }
}

function broadcastToFullNodes(msg: unknown) {
  const json = JSON.stringify(msg);
  let sentCount = 0;
  fullLightNodes.forEach((socket) => {
    if (socket.readyState === WebSocket.OPEN) {
      try {
        socket.send(json);
        sentCount++;
      } catch (err) {
        log("BROADCAST", `❌ Error broadcasting balance update to ${socket.nodeId}`, err);
      }
    }
  });
  logDebug("BROADCAST", `Balance update broadcast complete`, { sentTo: sentCount, totalFullNodes: fullLightNodes.size });
}

function broadcastToLightNodes(msg: unknown) {
  const json = JSON.stringify(msg);
  let sentCount = 0;
  lightNodes.forEach((socket) => {
    if (socket.readyState === WebSocket.OPEN) {
      try {
        socket.send(json);
        sentCount++;
      } catch (err) {
        log("BROADCAST", `❌ Error broadcasting to ${socket.nodeId}`, err);
      }
    }
  });
  logDebug("BROADCAST", `Broadcast complete`, {
    messageType: (msg as any).type,
    sentTo: sentCount,
    totalConnected: lightNodes.size,
  });
}

function publishBlock(header: BlockHeader) {
  const oldHeight = latestHeader?.height ?? -1;
  latestHeader = header;
  recentHeaders.push(header);
  if (recentHeaders.length > BACKFILL_SIZE) recentHeaders.shift();

  log("BLOCK", `📦 Publishing block`, {
    height: header.height,
    hash: header.hash.substring(0, 16) + "...",
    previousHeight: oldHeight,
    blockGap: header.height - oldHeight,
    recentHeadersCount: recentHeaders.length,
    lightNodesConnected: lightNodes.size,
    timestamp: header.timestamp,
  });

  recordMessageStat("block:new");
  broadcastToLightNodes({ type: "block:new", header });

  log("BLOCK", `✅ Block relayed to light nodes`, {
    height: header.height,
    targetedNodes: lightNodes.size,
  });
}

// ─── HTTP: validator (Vercel serverless) publishes a block via POST ──
function handleHttp(req: IncomingMessage, res: ServerResponse) {
  const timestamp = new Date().toISOString();

  if (req.method === "GET" && req.url === "/status") {
    const latestBlockHash = latestHeader?.hash ? latestHeader.hash.substring(0, 16) + "..." : "none";
    const status = {
      ok: true,
      timestamp,
      validatorConnected: !!validatorSocket,
      lightNodesConnected: lightNodes.size,
      maxLightNodes: MAX_LIGHT_NODES,
      latestHeight: latestHeader?.height ?? -1,
      latestBlockHash,
      tierCounts: {
        leader: [...telemetry.values()].filter((t) => t.tier === "leader").length,
        guardian: [...telemetry.values()].filter((t) => t.tier === "guardian").length,
        broadcaster: [...telemetry.values()].filter((t) => t.tier === "broadcaster").length,
        vision: [...telemetry.values()].filter((t) => t.tier === "vision").length,
        echo: [...telemetry.values()].filter((t) => t.tier === "echo").length,
        none: [...telemetry.values()].filter((t) => t.tier === "none").length,
      },
      fullSyncProviders: currentFullSyncProviders,
      fullSyncProvidersCount: currentFullSyncProviders.length,
      recentHeadersCount: recentHeaders.length,
      backfillSize: BACKFILL_SIZE,
      connectionAttemptsByIpCount: connectionAttemptsByIp.size,
      messageStats,
      nodes: [...telemetry.entries()].map(([nodeId, t]) => ({
        nodeId,
        tier: t.tier,
        parentNodeId: t.parentNodeId,
        hasFullLedger: t.hasFullLedger,
        connectedAtSeconds: Math.round((Date.now() - t.connectedAt) / 1000),
        lastAckHeight: t.lastAckHeight,
        lastHeartbeatAgo: Math.round((Date.now() - t.lastHeartbeat) / 1000),
        avgLatencyMs: t.avgLatencyMs,
        participationSeconds: t.participationSeconds,
        verifiedHeaderCount: t.verifiedHeaderCount,
        messagesReceived: t.messagesReceived,
        messagesSent: t.messagesSent,
        lastMessageType: t.lastMessageType,
      })),
    };
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(JSON.stringify(status, null, 2));
    log("HTTP", `📊 Status endpoint accessed`, { lightNodesConnected: lightNodes.size });
    return;
  }

  if (req.method === "POST" && req.url === "/internal/publish-block") {
    const authHeader = req.headers["x-railway-secret"];
    const headerValid = VALIDATOR_SECRET && authHeader === VALIDATOR_SECRET;

    if (!headerValid) {
      const reason = !VALIDATOR_SECRET ? "NO_SECRET_CONFIGURED" : "INVALID_SECRET";
      log("HTTP", `🚫 Publish-block: UNAUTHORIZED`, {
        reason,
        headerProvided: !!authHeader,
      });
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "UNAUTHORIZED", reason }));
      return;
    }

    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 1024 * 1024) {
        log("HTTP", "❌ Publish-block: Payload too large", { size: body.length });
        res.writeHead(413, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: "PAYLOAD_TOO_LARGE" }));
      }
    });

    req.on("end", () => {
      try {
        log("HTTP", `📥 Publish-block request received`, { payloadSize: body.length });
        const { header } = JSON.parse(body) as { header: BlockHeader };

        if (!header || typeof header.height !== "number" || !header.hash) {
          log("HTTP", `❌ Publish-block: Invalid header`, { hasHeader: !!header, height: header?.height, hash: !!header?.hash });
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: false, error: "INVALID_HEADER" }));
          return;
        }

        publishBlock(header);
        res.writeHead(200, { "Content-Type": "application/json" });
        const response = { success: true, relayedTo: lightNodes.size, height: header.height };
        res.end(JSON.stringify(response));
        log("HTTP", `✅ Publish-block: Success`, { height: header.height, relayedTo: lightNodes.size });
      } catch (err) {
        log("HTTP", `❌ Publish-block: JSON parse error`, err);
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: "INVALID_JSON" }));
      }
    });
    return;
  }

  if (req.method === "GET" && req.url === "/health") {
    // Async health includes optional chain (east-validator) reachability.
    (async () => {
      const chain = chainConfigured()
        ? await pingValidator()
        : { ok: false, error: "EAST_VALIDATOR_URL not set" };
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({
        ok: true,
        uptime: process.uptime(),
        hub: {
          lightNodesConnected: lightNodes.size,
          latestHeaderHeight: latestHeader?.height ?? -1,
          validatorWsConnected: !!validatorSocket,
        },
        chain: {
          configured: chainConfigured(),
          url: chainConfigured() ? chainBaseUrl() : null,
          ...chain,
        },
      }));
    })().catch((err) => {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ ok: false, error: String(err) }));
    });
    return;
  }

  // ─── HTTP: Vercel pushes a balance change to broadcast to full nodes ──
  if (req.method === "POST" && req.url === "/internal/push-balance-update") {
    const authHeader = req.headers["x-railway-secret"];
    if (!VALIDATOR_SECRET || authHeader !== VALIDATOR_SECRET) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "UNAUTHORIZED" }));
      return;
    }
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 64 * 1024) {
        res.writeHead(413, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: "PAYLOAD_TOO_LARGE" }));
      }
    });
    req.on("end", () => {
      try {
        const { address, balance } = JSON.parse(body) as { address: string; balance: string };
        if (!address || typeof balance !== "string") {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: false, error: "INVALID_PAYLOAD" }));
          return;
        }
        broadcastToFullNodes({ type: "balance:update", address, balance });
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true, relayedTo: fullLightNodes.size }));
      } catch {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: "INVALID_JSON" }));
      }
    });
    return;
  }

  // ─── HTTP: wallet app pushes a VERIFIED signed sync attestation to
  // relay to every connected full node — see types.ts's SyncAttestationMessage
  // doc comment. Railway does not verify the signature itself (it isn't
  // in the trust path either way — the wallet app already verified it
  // before calling this, and any peer receiving the broadcast can redo
  // that verification independently); it only relays.
  if (req.method === "POST" && req.url === "/internal/push-sync-attestation") {
    const authHeader = req.headers["x-railway-secret"];
    if (!VALIDATOR_SECRET || authHeader !== VALIDATOR_SECRET) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "UNAUTHORIZED" }));
      return;
    }
    let body = "";
    req.on("data", (chunk) => {
      body += chunk;
      if (body.length > 64 * 1024) {
        res.writeHead(413, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: "PAYLOAD_TOO_LARGE" }));
      }
    });
    req.on("end", () => {
      try {
        const { walletAddress, nodeId, height, signedAt, signature } = JSON.parse(body) as {
          walletAddress: string; nodeId: string; height: number; signedAt: number; signature: string;
        };
        if (!walletAddress || !nodeId || typeof height !== "number" || typeof signedAt !== "number" || !signature) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: false, error: "INVALID_PAYLOAD" }));
          return;
        }
        broadcastToFullNodes({ type: "sync:attestation", walletAddress, nodeId, height, signedAt, signature });
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true, relayedTo: fullLightNodes.size }));
      } catch {
        res.writeHead(400, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: false, error: "INVALID_JSON" }));
      }
    });
    return;
  }

  // ─── HTTP: Vercel's /api/rpc asks a full node to answer eth_getBalance ──
  // Always resolves (never 5xx on "no full node available" — that's a
  // normal, expected state, not a hub error) so the caller's own timeout
  // logic doesn't need special-casing: { balance: null } means "fall back
  // to Postgres", same as a timeout would.
  if (req.method === "POST" && req.url === "/internal/rpc-get-balance") {
    const authHeader = req.headers["x-railway-secret"];
    if (!VALIDATOR_SECRET || authHeader !== VALIDATOR_SECRET) {
      res.writeHead(401, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ success: false, error: "UNAUTHORIZED" }));
      return;
    }
    let body = "";
    req.on("data", (chunk) => { body += chunk; });
    req.on("end", async () => {
      try {
        const { address } = JSON.parse(body) as { address: string };
        if (!address) {
          res.writeHead(400, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ success: false, error: "INVALID_PAYLOAD" }));
          return;
        }
        const balance = await requestBalanceFromFullNode(address);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ success: true, balance, source: balance !== null ? "full_node" : "none_available" }));
      } catch (err) {
        log("RPC", "❌ rpc-get-balance error", err);
        res.writeHead(200, { "Content-Type": "application/json" }); // still 200 — caller falls back either way
        res.end(JSON.stringify({ success: true, balance: null, source: "error" }));
      }
    });
    return;
  }


  // ─── Phase 2: public RPC proxy → east-validator (chain source of truth) ─
  {
    const urlPath = (req.url || "").split("?")[0];

    if (req.method === "GET" && urlPath === "/rpc/block/latest") {
      void (async () => {
        const result = await proxyGet("/block/latest");
        res.writeHead(result.status, { "Content-Type": result.contentType });
        res.end(result.body);
      })();
      return;
    }

    const blockMatch = urlPath.match(/^\/rpc\/block\/(\d+)$/);
    if (req.method === "GET" && blockMatch) {
      const height = blockMatch[1];
      void (async () => {
        const result = await proxyGet(`/block/${height}`);
        res.writeHead(result.status, { "Content-Type": result.contentType });
        res.end(result.body);
      })();
      return;
    }

    const accountMatch = urlPath.match(/^\/rpc\/account\/([^/]+)$/);
    if (req.method === "GET" && accountMatch) {
      const address = decodeURIComponent(accountMatch[1]);
      void (async () => {
        const result = await proxyGet(`/account/${encodeURIComponent(address)}`);
        res.writeHead(result.status, { "Content-Type": result.contentType });
        res.end(result.body);
      })();
      return;
    }

    if (req.method === "GET" && urlPath === "/rpc/supply") {
      void (async () => {
        const result = await proxyGet("/supply");
        res.writeHead(result.status, { "Content-Type": result.contentType });
        res.end(result.body);
      })();
      return;
    }

    const bucketMatch = urlPath.match(/^\/rpc\/supply\/([^/]+)$/);
    if (req.method === "GET" && bucketMatch) {
      const bucket = decodeURIComponent(bucketMatch[1]);
      void (async () => {
        const result = await proxyGet(`/supply/${encodeURIComponent(bucket)}`);
        res.writeHead(result.status, { "Content-Type": result.contentType });
        res.end(result.body);
      })();
      return;
    }

    if (req.method === "GET" && urlPath === "/rpc/stats") {
      void (async () => {
        const result = await proxyGet("/stats");
        res.writeHead(result.status, { "Content-Type": result.contentType });
        res.end(result.body);
      })();
      return;
    }

    if (req.method === "POST" && urlPath === "/rpc/tx") {
      let body = "";
      req.on("data", (chunk) => { body += chunk; });
      req.on("end", () => {
        void (async () => {
          if (!body || body.length > 1_000_000) {
            res.writeHead(400, { "Content-Type": "application/json" });
            res.end(JSON.stringify({ ok: false, error: "invalid_or_too_large_body" }));
            return;
          }
          log("RPC", "➡️  proxy POST /tx → east-validator", { bytes: body.length });
          const result = await proxyPostTx(body);
          res.writeHead(result.status, { "Content-Type": result.contentType });
          res.end(result.body);
        })();
      });
      return;
    }
  }


  log("HTTP", `⚠️  Unhandled HTTP request`, { method: req.method, url: req.url });
  res.writeHead(404);
  res.end();
}

const httpServer = createServer(handleHttp);
const wss = new WebSocketServer({ server: httpServer, perMessageDeflate: false, maxPayload: 64 * 1024 });

wss.on("connection", (socket: EastSocket, req) => {
  const ip = (req.headers["x-forwarded-for"] as string)?.split(",")[0]?.trim()
    || req.socket.remoteAddress || "unknown";

  log("CONNECTION", `🔗 New connection attempt`, { ip, connectedNodes: lightNodes.size });

  if (isRateLimited(ip)) {
    send(socket, { type: "error", message: "RATE_LIMITED — too many connection attempts, try again shortly" });
    socket.close();
    log("CONNECTION", `❌ Connection rejected: RATE_LIMITED`, { ip });
    return;
  }

  if (lightNodes.size >= MAX_LIGHT_NODES) {
    send(socket, { type: "error", message: "HUB_AT_CAPACITY" });
    socket.close();
    log("CONNECTION", `❌ Connection rejected: HUB_AT_CAPACITY`, { ip, currentNodes: lightNodes.size, max: MAX_LIGHT_NODES });
    return;
  }

  socket.isAlive = true;
  socket.connectedAt = Date.now();

  log("CONNECTION", `✅ Connection accepted`, { ip, totalConnections: lightNodes.size + 1 });

  socket.on("pong", () => {
    socket.isAlive = true;
    logDebug("HEARTBEAT", `Pong received from ${socket.nodeId}`);
  });

  socket.on("message", (raw) => {
    let msg: InboundMessage;
    try {
      msg = JSON.parse(raw.toString());
    } catch (err) {
      log("MESSAGE", `❌ Invalid JSON from ${socket.nodeId}`, { error: String(err) });
      send(socket, { type: "error", message: "Invalid JSON" });
      return;
    }

    recordMessageStat(msg.type);

    const t = telemetry.get(socket.nodeId || "unknown");
    if (t) {
      t.messagesReceived++;
      t.lastMessageType = msg.type;
      t.lastMessageTime = Date.now();
    }

    logDebug("MESSAGE", `Received ${msg.type}`, { nodeId: socket.nodeId, role: socket.role });

    switch (msg.type) {
      case "hello": {
        if (msg.chainId !== undefined && msg.chainId !== EAST_CHAIN_ID) {
          log("AUTH", `❌ WRONG_NETWORK from ${ip}`, { expectedChainId: EAST_CHAIN_ID, receivedChainId: msg.chainId });
          send(socket, { type: "error", message: `WRONG_NETWORK — expected chainId ${EAST_CHAIN_ID}` });
          socket.close();
          return;
        }

        if (msg.role === "validator") {
          if (!VALIDATOR_SECRET || msg.secret !== VALIDATOR_SECRET) {
            log("AUTH", `❌ UNAUTHORIZED validator attempt from ${ip}`);
            send(socket, { type: "error", message: "UNAUTHORIZED" });
            socket.close();
            return;
          }
          validatorSocket = socket;
          socket.role = "validator";
          socket.nodeId = msg.nodeId || "EASTCHAIN-L1";
          log("AUTH", `✅ Validator authenticated and connected`, { nodeId: socket.nodeId });
        } else {
          socket.role = "light-node";
          socket.nodeId = msg.nodeId;
          lightNodes.set(msg.nodeId, socket);
          const nodeType: "light" | "full" = msg.nodeType === "full" ? "full" : "light";
          telemetry.set(msg.nodeId, {
            lastHeartbeat: Date.now(),
            lastAckHeight: -1,
            connectedAt: Date.now(),
            avgLatencyMs: 0,
            participationSeconds: 0,
            verifiedHeaderCount: 0,
            tier: "none",
            parentNodeId: null,
            hasFullLedger: false,
            nodeType,
            messagesReceived: 1,
            messagesSent: 0,
            lastMessageType: "hello",
          });
          if (nodeType === "full") fullLightNodes.set(msg.nodeId, socket);

          log("NODE", `✅ Light node connected`, {
            nodeId: msg.nodeId,
            ip,
            totalLightNodes: lightNodes.size,
            chainId: msg.chainId,
          });

          // Tier assignment isn't sent here — this node has avgLatencyMs:0
          // (no relay_stats reported yet) so recomputeTiers() wouldn't rank
          // it into anything but "none" anyway. It gets its real tier +
          // parent on the next periodic rescore, same as it always waited
          // for the old roster to consider it.
          send(socket, { type: "full_sync_providers", nodeIds: currentFullSyncProviders });

          // Bootstrap sample goes out immediately (not gated on the 60s
          // rescore) so the node can start dialing peers and running its own
          // gossip mesh (PEX + mesh expansion, see webrtc-peer.ts/client.ts)
          // right away instead of sitting with zero peers until tier:assign
          // eventually arrives. tier:assign still comes later and still
          // governs scoring/leader duties — this is purely about not
          // leaving a brand-new node isolated in the meantime.
          const bootstrapPeers = sampleBootstrapPeers(msg.nodeId, BOOTSTRAP_SAMPLE_SIZE);
          send(socket, { type: "bootstrap_peers", nodeIds: bootstrapPeers });
          log("BOOTSTRAP", `🌱 Bootstrap sample sent on connect`, { nodeId: msg.nodeId, sampleSize: bootstrapPeers.length });

          const nodeTelemetry = telemetry.get(msg.nodeId);
          if (nodeTelemetry) {
            nodeTelemetry.messagesSent += 1;
          }
        }

        send(socket, {
          type: "welcome",
          network: "EAST",
          chainId: EAST_CHAIN_ID,
          version: "1.2",
          role: socket.role,
          latestHeight: latestHeader?.height ?? -1,
        });

        if (socket.role === "light-node" && socket.nodeId) {
          const nodeTelemetry = telemetry.get(socket.nodeId);
          if (nodeTelemetry) {
            nodeTelemetry.messagesSent++;
          }
        }

        break;
      }

      case "ping": {
        send(socket, { type: "pong", time: Date.now() });
        logDebug("PING", `Pong sent to ${socket.nodeId}`);
        if (t) t.messagesSent++;
        break;
      }

      // ── Validator → Railway → all light nodes ──────────────────
      case "block:new": {
        if (socket.role !== "validator") {
          log("BLOCK", `❌ FORBIDDEN: Non-validator tried block:new from ${socket.nodeId}`, { role: socket.role });
          send(socket, { type: "error", message: "FORBIDDEN — validator role required" });
          return;
        }
        publishBlock(msg.header);
        break;
      }

      // ── Light node → Railway → validator (best-effort passthrough) ──
      case "heartbeat": {
        if (socket.role !== "light-node" || !socket.nodeId) {
          logDebug("HEARTBEAT", `⚠️  Heartbeat from non-light-node`, { role: socket.role, nodeId: socket.nodeId });
          return;
        }
        const nodeTelemetry = telemetry.get(socket.nodeId);
        if (nodeTelemetry) {
          nodeTelemetry.lastHeartbeat = Date.now();
          nodeTelemetry.messagesSent++;
        }
        if (validatorSocket) {
          send(validatorSocket, msg);
          log("HEARTBEAT", `📍 Heartbeat relayed`, {
            nodeId: socket.nodeId,
            height: msg.height,
            timestamp: msg.timestamp,
          });
        } else {
          log("HEARTBEAT", `⚠️  Heartbeat dropped - validator offline`, { nodeId: socket.nodeId, height: msg.height });
        }
        break;
      }

      case "sync_request": {
        if (socket.role !== "light-node" || !socket.nodeId) {
          logDebug("SYNC", `⚠️  Sync request from non-light-node`, { role: socket.role });
          return;
        }

        const backfill = recentHeaders.slice(-BACKFILL_SIZE);
        log("SYNC", `🔄 Sync request received`, {
          nodeId: socket.nodeId,
          requestFromHeight: msg.fromHeight,
          backfillSize: backfill.length,
          maxBackfillSize: BACKFILL_SIZE,
          availableBlockRange: recentHeaders.length > 0
            ? `${recentHeaders[0]?.height} - ${recentHeaders[recentHeaders.length - 1]?.height}`
            : "empty",
          latestHeight: latestHeader?.height ?? -1,
        });

        if (backfill.length > 0) {
          send(socket, { type: "block:backfill", headers: backfill });
          const nodeTelemetry = telemetry.get(socket.nodeId);
          if (nodeTelemetry) nodeTelemetry.messagesSent++;
          log("SYNC", `📦 Backfill sent`, {
            nodeId: socket.nodeId,
            blocksCount: backfill.length,
            heightRange: `${backfill[0]?.height} - ${backfill[backfill.length - 1]?.height}`,
          });
        } else {
          log("SYNC", `⚠️  No backfill available`, { nodeId: socket.nodeId, recentHeadersCount: recentHeaders.length });
        }

        if (validatorSocket) {
          send(validatorSocket, msg);
          log("SYNC", `📤 Sync request forwarded to validator`, { nodeId: socket.nodeId, fromHeight: msg.fromHeight });
        } else {
          log("SYNC", `⚠️  Sync request not forwarded - validator offline`, { nodeId: socket.nodeId });
        }
        break;
      }

      case "ack": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const nodeTelemetry = telemetry.get(socket.nodeId);
        if (nodeTelemetry) {
          nodeTelemetry.lastAckHeight = msg.height;
          nodeTelemetry.messagesSent++;
        }

        log("ACK", `✅ ACK received and relayed`, {
          nodeId: socket.nodeId,
          ackHeight: msg.height,
          timestamp: msg.timestamp,
          heightGapFromLatest: (latestHeader?.height ?? -1) - msg.height,
        });

        if (validatorSocket) send(validatorSocket, msg);
        break;
      }

      // ── Light node → Railway → validator (tx relay) ──────────────
      case "tx:submit": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        if (!validatorSocket) {
          send(socket, { type: "error", message: "VALIDATOR_OFFLINE" });
          log("TX", `❌ tx:submit dropped - validator offline`, { nodeId: socket.nodeId });
          return;
        }
        log("TX", `💰 Transaction submitted`, {
          nodeId: socket.nodeId,
          hasPayload: !!msg.payload,
        });
        send(validatorSocket, msg);
        const nodeTelemetry = telemetry.get(socket.nodeId);
        if (nodeTelemetry) nodeTelemetry.messagesSent++;
        break;
      }

      // ── Relay scoring: node self-reports, Railway just stores it ────
      // Explicit re-ask — used when a node's peer mesh has fully collapsed
      // (connectedPeerIds hits 0) and it needs a fresh sample to restart
      // its own gossip growth. Same sampling as the one sent on hello;
      // this is just the "ask again later" path, so it stays just as cheap.
      case "bootstrap_request": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const peers = sampleBootstrapPeers(socket.nodeId, BOOTSTRAP_SAMPLE_SIZE);
        send(socket, { type: "bootstrap_peers", nodeIds: peers });
        log("BOOTSTRAP", `🌱 Bootstrap sample sent on request`, { nodeId: socket.nodeId, sampleSize: peers.length });
        break;
      }

      case "relay_stats": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const nodeTelemetry = telemetry.get(socket.nodeId);
        if (nodeTelemetry) {
          nodeTelemetry.avgLatencyMs = msg.avgLatencyMs;
          nodeTelemetry.participationSeconds = msg.participationSeconds;
          nodeTelemetry.verifiedHeaderCount = msg.verifiedHeaderCount;
          const hadFullLedger = nodeTelemetry.hasFullLedger;
          nodeTelemetry.hasFullLedger = msg.hasFullLedger ?? false;

          log("RELAY_STATS", `📊 Relay stats updated`, {
            nodeId: socket.nodeId,
            avgLatencyMs: msg.avgLatencyMs,
            participationSeconds: msg.participationSeconds,
            verifiedHeaderCount: msg.verifiedHeaderCount,
            hasFullLedger: msg.hasFullLedger,
            fullLedgerChanged: hadFullLedger !== (msg.hasFullLedger ?? false),
          });

          if (nodeTelemetry.hasFullLedger !== hadFullLedger) {
            recomputeFullSyncProviders();
          }
        }
        break;
      }

      // A "full" node answering a balance query we (Railway) asked it via
      // requestBalanceFromFullNode() above. Anything unmatched (unknown/
      // expired requestId) is silently dropped — the HTTP caller already
      // timed out and fell back to Postgres by the time a very late reply
      // shows up, so there's nothing to resolve.
      case "rpc_balance_response": {
        const pending = pendingBalanceRequests.get(msg.requestId);
        if (!pending) break;
        clearTimeout(pending.timeout);
        pendingBalanceRequests.delete(msg.requestId);
        pending.resolve(msg.balance);
        break;
      }

      // ── Peer-to-peer full sync
      case "full_sync_request": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const target = lightNodes.get(msg.toNodeId);
        if (!target) {
          send(socket, { type: "error", message: `PEER_OFFLINE — ${msg.toNodeId}` });
          log("FULL_SYNC", `❌ Full sync request failed - peer offline`, {
            fromNodeId: socket.nodeId,
            toNodeId: msg.toNodeId,
            heightRange: `${msg.fromHeight} - ${msg.toHeight}`,
          });
          return;
        }
        log("FULL_SYNC", `🔄 Full sync request relayed`, {
          fromNodeId: socket.nodeId,
          toNodeId: msg.toNodeId,
          heightRange: `${msg.fromHeight} - ${msg.toHeight}`,
          blockCount: msg.toHeight - msg.fromHeight + 1,
        });
        send(target, { ...msg, fromNodeId: socket.nodeId });
        const targetTelemetry = telemetry.get(msg.toNodeId);
        if (targetTelemetry) targetTelemetry.messagesSent++;
        break;
      }

      case "full_sync_response": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const target = lightNodes.get(msg.toNodeId);
        if (!target) {
          log("FULL_SYNC", `⚠️  Full sync response dropped - requester offline`, { fromNodeId: socket.nodeId, toNodeId: msg.toNodeId });
          return;
        }
        log("FULL_SYNC", `📦 Full sync response relayed`, {
          fromNodeId: socket.nodeId,
          toNodeId: msg.toNodeId,
          blocksCount: msg.blocks?.length ?? 0,
        });
        send(target, { ...msg, fromNodeId: socket.nodeId });
        const targetTelemetry = telemetry.get(msg.toNodeId);
        if (targetTelemetry) targetTelemetry.messagesSent++;
        break;
      }

      // ── WebRTC signaling
      case "webrtc_offer":
      case "webrtc_answer":
      case "ice_candidate": {
        if (socket.role !== "light-node" || !socket.nodeId) return;
        const target = lightNodes.get(msg.toNodeId);
        if (!target) {
          send(socket, { type: "error", message: `PEER_OFFLINE — ${msg.toNodeId}` });
          if (msg.type !== "ice_candidate") {
            log("WEBRTC", `❌ ${msg.type} failed - peer offline`, {
              fromNodeId: socket.nodeId,
              toNodeId: msg.toNodeId,
            });
          }
          return;
        }
        if (msg.type !== "ice_candidate") {
          log("WEBRTC", `🔗 ${msg.type} relayed`, {
            fromNodeId: socket.nodeId,
            toNodeId: msg.toNodeId,
            sdpLength: msg.sdp?.length ?? 0,
          });
        }
        send(target, { ...msg, fromNodeId: socket.nodeId });
        const targetTelemetry = telemetry.get(msg.toNodeId);
        if (targetTelemetry) targetTelemetry.messagesSent++;
        break;
      }

      default:
        log("MESSAGE", `⚠️  Unknown message type`, { type: (msg as any).type, nodeId: socket.nodeId });
        send(socket, { type: "error", message: "Unknown message type" });
    }
  });

  socket.on("close", () => {
    const duration = socket.connectedAt ? Math.round((Date.now() - socket.connectedAt) / 1000) : 0;
    const t = telemetry.get(socket.nodeId || "");

    if (socket.role === "validator") {
      validatorSocket = null;
      log("CONNECTION", `❌ Validator disconnected`, { nodeId: socket.nodeId, connectedForSeconds: duration });
    } else if (socket.nodeId) {
      lightNodes.delete(socket.nodeId);
      fullLightNodes.delete(socket.nodeId);
      telemetry.delete(socket.nodeId);

      log("CONNECTION", `❌ Light node disconnected`, {
        nodeId: socket.nodeId,
        ip,
        connectedForSeconds: duration,
        remainingNodes: lightNodes.size,
        messagesReceived: t?.messagesReceived ?? 0,
        messagesSent: t?.messagesSent ?? 0,
        lastAckHeight: t?.lastAckHeight ?? -1,
      });

      // If this node had descendants relying on it (anything but an Echo
      // leaf, which nothing else's parent points to), don't make them wait
      // out the full rescore interval disconnected from the tree — recompute
      // right away. An Echo leaving just quietly drops out of the count.
      if (t && (t.tier === "leader" || t.tier === "guardian" || t.tier === "broadcaster" || t.tier === "vision")) {
        log("TIERS", `🔄 Recomputing tiers early — a ${t.tier} disconnected`, { nodeId: socket.nodeId });
        recomputeTiers();
      }
      if (currentFullSyncProviders.includes(socket.nodeId)) {
        recomputeFullSyncProviders();
      }
    }
  });

  socket.on("error", (err) => {
    log("SOCKET", `❌ Socket error for ${socket.nodeId}`, err);
  });
});

// Dead-connection cleanup
setInterval(() => {
  let terminated = 0;
  wss.clients.forEach((client) => {
    const ws = client as EastSocket;
    if (!ws.isAlive) {
      terminated++;
      ws.terminate();
      log("HEARTBEAT", `🔴 Dead connection terminated for ${ws.nodeId}`);
      return;
    }
    ws.isAlive = false;
    ws.ping();
  });
  if (terminated > 0) {
    log("HEARTBEAT", `⏱️  Heartbeat cleanup`, { terminatedConnections: terminated, activeConnections: wss.clients.size });
  } else {
    logDebug("HEARTBEAT", "Heartbeat check complete", { activeConnections: wss.clients.size });
  }
}, 30000);

// Rescore + re-assign the tier hierarchy periodically
setInterval(() => {
  log("RESCORE", `🔄 Starting tier hierarchy rescore`, { totalNodes: telemetry.size });
  recomputeTiers();
}, RELAY_RESCORE_INTERVAL_MS);

setInterval(() => {
  logDebug("RESCORE", `Starting full-sync providers rescore`);
  recomputeFullSyncProviders();
}, RELAY_RESCORE_INTERVAL_MS);

// Periodic status dump
setInterval(() => {
  const status = {
    timestamp: new Date().toISOString(),
    uptime: Math.round(process.uptime()),
    validatorConnected: !!validatorSocket,
    lightNodesConnected: lightNodes.size,
    latestHeight: latestHeader?.height ?? -1,
    recentHeadersCount: recentHeaders.length,
    guardianCount: [...telemetry.values()].filter((t) => t.tier === "guardian").length,
    broadcasterCount: [...telemetry.values()].filter((t) => t.tier === "broadcaster").length,
    visionCount: [...telemetry.values()].filter((t) => t.tier === "vision").length,
    echoCount: [...telemetry.values()].filter((t) => t.tier === "echo").length,
    fullSyncProvidersCount: currentFullSyncProviders.length,
    memoryUsage: process.memoryUsage(),
    messageStats,
  };
  log("STATUS", `📈 Periodic status dump`, status);
}, 5 * 60 * 1000); // Every 5 minutes

httpServer.listen(PORT, () => {
  log("RAILWAY", `✅ EAST Hub is listening`, {
    port: PORT,
    endpoints: [
      `ws://localhost:${PORT} (WebSocket)`,
      `http://localhost:${PORT}/status (Status)`,
      `http://localhost:${PORT}/health (Health Check)`,
      `http://localhost:${PORT}/internal/publish-block (Block Publish)`,
      `http://localhost:${PORT}/rpc/* (Phase 2 chain proxy → EAST_VALIDATOR_URL)`,
    ],
    chainProxy: {
      configured: chainConfigured(),
      url: chainConfigured() ? chainBaseUrl() : null,
    },
  });
});

// Graceful shutdown
process.on("SIGTERM", () => {
  log("RAILWAY", "📡 SIGTERM received - shutting down gracefully");
  httpServer.close(() => {
    log("RAILWAY", "👋 Server closed");
    process.exit(0);
  });
});

process.on("SIGINT", () => {
  log("RAILWAY", "📡 SIGINT received - shutting down gracefully");
  httpServer.close(() => {
    log("RAILWAY", "👋 Server closed");
    process.exit(0);
  });
});
