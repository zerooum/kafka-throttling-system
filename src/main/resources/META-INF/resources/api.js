// api.js — the only network surface. Same-origin fetch wrappers.

// POST /messages with a unique idempotency key. Returns { status, body }.
// Never throws on non-2xx (caller inspects status); only throws on network error.
export async function postMessage(idempotencyKey, payload) {
  const res = await fetch("/messages", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Idempotency-Key": idempotencyKey,
    },
    body: JSON.stringify({ payload }),
  });
  let body = null;
  try {
    body = await res.json();
  } catch (_) {
    /* non-JSON body — leave null */
  }
  return { status: res.status, body };
}

export async function getThrottle(adminToken) {
  const res = await fetch("/admin/throttle", {
    headers: { "X-Admin-Token": adminToken },
  });
  if (!res.ok) throw new Error(`throttle ${res.status}`);
  return res.json();
}

export async function getCbStatus(adminToken) {
  const res = await fetch("/admin/circuit-breaker/status", {
    headers: { "X-Admin-Token": adminToken },
  });
  if (!res.ok) throw new Error(`cb ${res.status}`);
  return res.json();
}

export async function getMetricsText() {
  const res = await fetch("/q/metrics");
  if (!res.ok) throw new Error(`metrics ${res.status}`);
  return res.text();
}
