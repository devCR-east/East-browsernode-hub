/**
 * Phase 2 — HTTP proxy from this Hub to east-validator (chain source of truth).
 * Hub remains a blind relay for WS; these routes only forward JSON and never
 * invent balances or mint.
 */

const VALIDATOR_URL = (process.env.EAST_VALIDATOR_URL || process.env.VALIDATOR_HTTP_URL || "")
  .trim()
  .replace(/\/$/, "");
const VALIDATOR_API_SECRET =
  process.env.VALIDATOR_API_SECRET || process.env.EAST_VALIDATOR_API_SECRET || "";
const PROXY_TIMEOUT_MS = Number(process.env.CHAIN_PROXY_TIMEOUT_MS || 12_000);

export function chainConfigured(): boolean {
  return VALIDATOR_URL.length > 0;
}

export function chainBaseUrl(): string {
  return VALIDATOR_URL;
}

type ProxyResult = {
  status: number;
  body: string;
  contentType: string;
};

async function proxyFetch(
  method: string,
  path: string,
  opts?: { body?: string; apiSecret?: boolean },
): Promise<ProxyResult> {
  if (!VALIDATOR_URL) {
    return {
      status: 503,
      body: JSON.stringify({
        ok: false,
        error: "EAST_VALIDATOR_URL not configured on hub",
      }),
      contentType: "application/json",
    };
  }

  const url = `${VALIDATOR_URL}${path.startsWith("/") ? path : `/${path}`}`;
  const headers: Record<string, string> = {
    Accept: "application/json",
  };
  if (opts?.body) {
    headers["Content-Type"] = "application/json";
  }
  if (opts?.apiSecret) {
    if (!VALIDATOR_API_SECRET) {
      return {
        status: 503,
        body: JSON.stringify({
          ok: false,
          error: "VALIDATOR_API_SECRET not configured on hub — cannot forward write routes",
        }),
        contentType: "application/json",
      };
    }
    headers["X-API-Secret"] = VALIDATOR_API_SECRET;
  }

  const ac = new AbortController();
  const timer = setTimeout(() => ac.abort(), PROXY_TIMEOUT_MS);
  try {
    const res = await fetch(url, {
      method,
      headers,
      body: opts?.body,
      signal: ac.signal,
    });
    const text = await res.text();
    const ct = res.headers.get("content-type") || "application/json";
    return { status: res.status, body: text, contentType: ct };
  } catch (err) {
    const message = err instanceof Error ? err.message : String(err);
    return {
      status: 502,
      body: JSON.stringify({
        ok: false,
        error: "validator_unreachable",
        detail: message,
        target: VALIDATOR_URL,
      }),
      contentType: "application/json",
    };
  } finally {
    clearTimeout(timer);
  }
}

export async function proxyGet(path: string): Promise<ProxyResult> {
  return proxyFetch("GET", path);
}

export async function proxyPostTx(body: string): Promise<ProxyResult> {
  return proxyFetch("POST", "/tx", { body, apiSecret: true });
}

/** Lightweight ping used by /health — does not require API secret. */
export async function pingValidator(): Promise<{
  ok: boolean;
  status?: number;
  latencyMs?: number;
  error?: string;
  raw?: unknown;
}> {
  if (!VALIDATOR_URL) {
    return { ok: false, error: "EAST_VALIDATOR_URL not set" };
  }
  const started = Date.now();
  const result = await proxyGet("/health");
  const latencyMs = Date.now() - started;
  if (result.status < 200 || result.status >= 300) {
    return {
      ok: false,
      status: result.status,
      latencyMs,
      error: result.body.slice(0, 500),
    };
  }
  let raw: unknown = result.body;
  try {
    raw = JSON.parse(result.body);
  } catch {
    /* keep string */
  }
  return { ok: true, status: result.status, latencyMs, raw };
}
