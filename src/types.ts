// ─── EAST Railway Hub — shared message protocol ─────────────────────
// Railway is NOT part of consensus. It only relays:
//   Validator (L1, Vercel)  →  Railway  →  all connected Light Nodes
//   Light Node  →  Railway  →  Validator (heartbeat / sync_request / ack / tx)
//   Light Node  ⇄  Railway  ⇄  Light Node (WebRTC signaling passthrough only —
//                                           Railway never inspects SDP/ICE content)
// This file must stay in sync with src/lib/lightnode/protocol.ts in the
// Next.js app — copy changes both ways.

export type Role = "validator" | "light-node";

// Reserved on ChainList/ethereum-lists for EAST's mainnet. Used here purely
// as a network-identity check on `hello` — NOT the same guarantee as EIP-155
// tx replay protection (this hub doesn't sign/relay raw transactions). Keep
// numerically in sync with EAST_CHAIN_ID in src/lib/contracts/registry.ts.
export const EAST_CHAIN_ID = 172026;

export interface BlockHeader {
  height: number;
  hash: string;
  previousHash: string;
  merkleRoot: string;
  validator: string | null;
  timestamp: number;
  epoch: number;
  signature?: string | null; // 0x-prefixed secp256k1 EIP-191 sig, see chain-signing.ts
}

export interface HelloMessage {
  type: "hello";
  role: Role;
  nodeId: string;
  secret?: string; // required when role === "validator"
  chainId?: number; // light nodes should send EAST_CHAIN_ID; missing = pre-upgrade client, allowed through with a warning rather than dropped
  // Opt-in only — a light node declares itself "full" by choice (storage/
  // battery cost of maintaining a balance replica). Separate concept from
  // hasFullLedger (RelayStatsMessage below): hasFullLedger is about serving
  // full BLOCK HISTORY for catch-up sync and is validator-daemon-only;
  // nodeType:"full" is about serving BALANCE queries for the MetaMask-facing
  // RPC endpoint and is something any light node (browser or daemon) can
  // opt into. A malicious/wrong answer from a "full" node is caught the
  // same way everything else here is: the requester (Vercel's /api/rpc)
  // falls back to querying Postgres directly if the full-node's answer
  // looks wrong or doesn't arrive in time — a full-node is a read-latency
  // optimization, never a trusted-without-checking source of truth.
  nodeType?: "light" | "full";
}

export interface WelcomeMessage {
  type: "welcome";
  network: "EAST";
  chainId: number;
  version: string;
  role: Role;
  latestHeight: number;
}

export interface BlockNewMessage {
  type: "block:new";
  header: BlockHeader;
}

export interface BlockBackfillMessage {
  type: "block:backfill";
  headers: BlockHeader[];
}

export interface HeartbeatMessage {
  type: "heartbeat";
  nodeId: string;
  height: number;
  timestamp: number;
}

export interface SyncRequestMessage {
  type: "sync_request";
  nodeId: string;
  fromHeight: number;
}

export interface AckMessage {
  type: "ack";
  nodeId: string;
  height: number;
  timestamp: number;
}

export interface TxSubmitMessage {
  type: "tx:submit";
  nodeId: string;
  payload: unknown;
}

// ── Tiered gossip hierarchy ────────────────────────────────────────────
// Replaces the old flat top-5 "everyone dials the same relays" roster,
// which doesn't scale (RELAY_ROSTER_SIZE=5 × MAX_MESH_PEERS on the client
// meant only ~100 total lightnodes could ever hold a P2P slot — everyone
// past that fell back to hitting Vercel/the validator/the archive
// directly, defeating the point). Instead, ALL scored nodes are ranked by
// the same score() function and sliced into 5 tiers, each node getting
// exactly ONE parent to dial (not a menu of candidates):
//   Leader (rank 0, 1 node)              — parent: null, talks to Railway directly
//   Guardian (rank 1-20, 20)             — parent: the Leader
//   Broadcaster (rank 21-420, 400)       — parent: one of the 20 Guardians
//   Vision (rank 421-8420, 8000)         — parent: one of the 400 Broadcasters
//   Echo (rank 8421-168420, 160000)      — parent: one of the 8000 Visions
//   none (rank 168421+, or not yet scored) — parent: null, falls back to
//     Railway/archive/validator directly same as today, until scored in
//     on a future rescore
// A new block flows Leader → its 20 Guardians → their 400 Broadcasters →
// their 8000 Visions → their 160000 Echoes, 4 WebRTC hops covering up to
// ~168421 nodes instead of however many thousand all hitting
// Railway/Vercel independently. Railway holds the whole tree structure in
// memory (see telemetry map) since it computed it — nodes don't need to
// track their own subtree.
export type NodeTier = "leader" | "guardian" | "broadcaster" | "vision" | "echo" | "none";

// Sent 1:1 whenever a node's tier or parent changes (including on first
// assignment). parentNodeId is null only for "leader" and "none" — both
// of those talk to Railway/Vercel directly instead of a WebRTC parent.
export interface TierAssignMessage {
  type: "tier:assign";
  tier: NodeTier;
  parentNodeId: string | null;
}

// ── Relay scoring & promotion ─────────────────────────────────────────
// A light node self-reports its own measured connection quality; Railway
// never trusts this alone for anything security-relevant (a lying node can
// only make ITSELF look like a better relay candidate — every header it
// ever forwards to a peer is still independently signature-verified
// client-side in verifyHeader(), so a malicious "relay" can at worst be
// useless, never a trusted data source).
export interface RelayStatsMessage {
  type: "relay_stats";
  nodeId: string;
  avgLatencyMs: number;
  participationSeconds: number;
  verifiedHeaderCount: number;
  // Set by validator daemons (full-node-sync.js) once their LOCAL ledger has
  // caught up to the network tip — never set by browser Light Nodes, which
  // don't keep a full historical ledger and so can never serve one. Railway
  // uses this purely to decide who to list in full_sync_providers below; it
  // is NOT trusted for anything else (see the comment above this interface).
  hasFullLedger?: boolean;
}

// ── Peer-to-peer full sync (catch-up) ───────────────────────────────
// Lets a node with a gap ask a PEER for the missing block range before
// ever hitting Vercel's archive API — spreads catch-up load across
// validators/light nodes instead of funneling every gap through
// serverless invocations. Only nodes that reported hasFullLedger:true
// (see RelayStatsMessage) are ever listed here — browser Light Nodes
// never qualify, since they don't retain full history to serve.
//
// Railway's role is strictly a blind relay for the request/response pair
// (same pattern as webrtc_offer/answer below) — it never inspects or
// verifies the block contents. The requester independently verifies every
// block it receives (same verifyHeader()/verifyBlock() check used for
// Railway- and archive-sourced blocks) before trusting any of it.
export interface FullSyncProvidersMessage {
  type: "full_sync_providers";
  nodeIds: string[];
}
export interface FullSyncRequestMessage {
  type: "full_sync_request";
  fromNodeId: string; // set by Railway on relay, ignored if sent by client
  toNodeId: string;
  fromHeight: number;
  toHeight: number;
}
export interface FullSyncResponseMessage {
  type: "full_sync_response";
  fromNodeId: string; // set by Railway on relay, ignored if sent by client
  toNodeId: string;
  blocks: BlockHeader[];
}

// ── WebRTC signaling passthrough ────────────────────────────────────
// Railway just forwards these by toNodeId → socket lookup, unmodified,
// same as it already does for tx:submit. It never parses sdp/candidate.
export interface WebrtcOfferMessage {
  type: "webrtc_offer";
  fromNodeId: string; // set by Railway on relay, ignored if sent by client
  toNodeId: string;
  sdp: string;
}
export interface WebrtcAnswerMessage {
  type: "webrtc_answer";
  fromNodeId: string;
  toNodeId: string;
  sdp: string;
}
export interface IceCandidateMessage {
  type: "ice_candidate";
  fromNodeId: string;
  toNodeId: string;
  candidate: string;
}

// ── Bootstrap discovery ─────────────────────────────────────────────
// Gives a freshly-connected node a handful of peer candidates immediately,
// instead of it sitting with zero peers until the next RELAY_RESCORE_INTERVAL_MS
// tier:assign cycle. Deliberately separate from tier assignment: no ranking
// of the whole roster, just a cheap weighted sample (see
// sampleBootstrapPeers() in server.ts) biased toward nodes with a proven
// participation history, so a swarm of throwaway connections can't dominate
// the sample a new node receives. Railway sends BootstrapPeersMessage
// unprompted right after "hello"; BootstrapRequestMessage is the explicit
// re-ask a node sends if its whole peer mesh has since collapsed to zero.
export interface BootstrapRequestMessage {
  type: "bootstrap_request";
}
export interface BootstrapPeersMessage {
  type: "bootstrap_peers";
  nodeIds: string[];
}

export interface PingMessage { type: "ping"; }
export interface PongMessage { type: "pong"; time: number; }
export interface ErrorMessage { type: "error"; message: string; }

// ── Full Lightnode balance replica (RPC read-path optimization) ────────
// BalanceUpdateMessage: Vercel pushes this to Railway via the authenticated
// /internal/push-balance-update HTTP route (same x-railway-secret pattern
// as /internal/publish-block) whenever a balance actually changes on-chain.
// Railway broadcasts it to every connected nodeType:"full" node so their
// local replicas stay current — Railway itself does NOT persist this
// beyond the in-memory broadcast, same "blind relay" role as everywhere
// else in this file.
export interface BalanceUpdateMessage {
  type: "balance:update";
  address: string;
  balance: string; // decimal string, not a JS number — avoids float precision loss for large EAST amounts
}

// RpcBalanceRequestMessage: Railway → one specific full node, when Vercel's
// /api/rpc asks Railway to resolve an eth_getBalance read via a full node
// instead of Postgres directly. requestId round-trips so Railway can match
// the reply without ambiguity if multiple requests are in flight.
export interface RpcBalanceRequestMessage {
  type: "rpc_balance_request";
  requestId: string;
  address: string;
}
export interface RpcBalanceResponseMessage {
  type: "rpc_balance_response";
  requestId: string;
  address: string;
  balance: string | null; // null = "I don't have this address in my replica"
}

// ── Full node reset-detection: signed sync attestations ────────────────
// Broadcast to every connected full node so a suspicious height regression
// can be caught by ANY peer holding two attestations from the same
// wallet_address, not just by the wallet app's own Postgres bookkeeping.
// Railway relays this — same "blind relay, never the trust anchor" role as
// everywhere else in this file. Signature verification happens at the
// wallet app (before this ever reaches Railway) and, in principle, can
// also be redone by any peer that receives it (ethers.verifyMessage needs
// only the payload + signature, no secret).
export interface SyncAttestationMessage {
  type: "sync:attestation";
  walletAddress: string;
  nodeId: string;
  height: number;
  signedAt: number;
  signature: string;
}


export type InboundMessage =
  | HelloMessage
  | BlockNewMessage
  | HeartbeatMessage
  | SyncRequestMessage
  | AckMessage
  | TxSubmitMessage
  | RelayStatsMessage
  | FullSyncRequestMessage
  | FullSyncResponseMessage
  | WebrtcOfferMessage
  | WebrtcAnswerMessage
  | IceCandidateMessage
  | BootstrapRequestMessage
  | RpcBalanceResponseMessage
  | PingMessage;

