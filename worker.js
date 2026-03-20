/**
 * femaproxy — Cloudflare Worker
 * CORS proxy for FEMA OpenFEMA API endpoints used by disasterdata.io
 *
 * Routes:
 *   /PublicAssistanceFundedProjectsDetails  → FEMA PA endpoint
 *   /HazardMitigationAssistanceProjects     → FEMA HMA endpoint  ← NEW
 *
 * All query parameters are forwarded as-is.
 * CORS headers are added so the browser can fetch cross-origin.
 */

const FEMA_BASE = "https://www.fema.gov/api/open/v2";

// Map of allowed proxy route segments → FEMA endpoint paths
const ROUTE_MAP = {
  "/PublicAssistanceFundedProjectsDetails": "/PublicAssistanceFundedProjectsDetails",
  "/HazardMitigationAssistanceProjects":    "/HazardMitigationAssistanceProjects",
};

const CORS_HEADERS = {
  "Access-Control-Allow-Origin":  "*",
  "Access-Control-Allow-Methods": "GET, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type",
};

export default {
  async fetch(request, env, ctx) {
    // Handle CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, { status: 204, headers: CORS_HEADERS });
    }

    // Only allow GET
    if (request.method !== "GET") {
      return new Response("Method not allowed", { status: 405, headers: CORS_HEADERS });
    }

    const url = new URL(request.url);
    const femaPath = ROUTE_MAP[url.pathname];

    if (!femaPath) {
      return new Response(
        JSON.stringify({ error: "Unknown endpoint", path: url.pathname }),
        { status: 404, headers: { ...CORS_HEADERS, "Content-Type": "application/json" } }
      );
    }

    // Build the upstream FEMA URL, forwarding all query params
    const upstream = new URL(FEMA_BASE + femaPath);
    url.searchParams.forEach((value, key) => upstream.searchParams.set(key, value));

    try {
      const femaResp = await fetch(upstream.toString(), {
        headers: {
          "User-Agent": "disasterdata.io/1.0 (Cloudflare Worker proxy)",
          "Accept":     "application/json",
        },
      });

      // Stream the response back with CORS headers merged in
      const respHeaders = new Headers(femaResp.headers);
      Object.entries(CORS_HEADERS).forEach(([k, v]) => respHeaders.set(k, v));

      return new Response(femaResp.body, {
        status:  femaResp.status,
        headers: respHeaders,
      });

    } catch (err) {
      return new Response(
        JSON.stringify({ error: "Upstream fetch failed", detail: err.message }),
        { status: 502, headers: { ...CORS_HEADERS, "Content-Type": "application/json" } }
      );
    }
  },
};
