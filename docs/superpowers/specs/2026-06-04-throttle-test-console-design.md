# Throttle Test Console — Design

**Date:** 2026-06-04
**Status:** Approved

## Purpose

A simple browser frontend (plain JS + HTML + CSS, no build step) to exercise the
Kafka throttling system: publish messages on demand or in bursts, and watch the
consumer drain them at the pace enforced by the rate limiter.

Two jobs:

1. **Publish** — send single messages for manual API testing, or burst-publish
   many messages to flood the system past the rate limit.
2. **Observe** — show, in real time, that consumption is capped at the
   limiter's refill rate while a backlog absorbs the burst.

## Constraints

- Pure static frontend: `index.html`, `app.js`, `style.css`. No bundler, no npm,
  no runtime CDN dependency (works offline).
- No Java/backend changes. Uses only endpoints that already exist.
- Served by Quarkus from `src/main/resources/META-INF/resources/`, reachable at
  `http://localhost:8080/`. Same origin as the API → no CORS configuration.

## Existing endpoints used

| Endpoint | Method | Auth | Used for |
|---|---|---|---|
| `/messages` | POST | header `X-Idempotency-Key` | publish a message |
| `/admin/throttle` | GET | header `X-Admin-Token` | available tokens, capacity, refill rate |
| `/admin/circuit-breaker/status` | GET | header `X-Admin-Token` | circuit-breaker state |
| `/q/metrics` | GET | none | Prometheus counters (ingress + consumed tallies) |

### Request/response shapes

- `POST /messages`
  - Headers: `X-Idempotency-Key: <uuid>`, `Content-Type: application/json`
  - Body: `{ "payload": { ... } }` (`metadata` optional, omitted by default)
  - `202 Accepted` → `{ "messageId", "acceptedAt" }`
  - `409 Conflict` → duplicate idempotency key
  - `400 Bad Request` → missing key or payload
- `GET /admin/throttle` → `{ "capacity", "available", "refillTokens", "refillPeriodMs" }`
- `GET /admin/circuit-breaker/status` → `{ "state" }`

### Relevant Prometheus metric names (`/q/metrics`)

Micrometer maps dots to underscores and appends `_total` to counters:

- `messages_ingress_received_total{outcome="accepted"|"duplicate"|"rejected"}`
- `messages_consumed_total{outcome="success"|"dlq"}`

The frontend parses these lines with a small text parser (regex per line:
`name{tags} value`).

## Layout (single page, top to bottom)

1. **Controls bar**
   - Payload JSON textarea (default sample, e.g. `{"hello":"world"}`).
   - `Send 1` button.
   - `Resend last key` button — re-POST the last payload reusing the previous
     `X-Idempotency-Key`, to deliberately trigger a `409` duplicate. Disabled
     until a first send has happened.
   - Burst count number input (default `500`).
   - `Send burst` button.
   - Admin-token text input (default `dev-admin`).
2. **KPI tiles** — Published, Accepted, Duplicate, Consumed (success / dlq),
   Backlog, Circuit-breaker state.
3. **Live row**
   - Consumption-rate chart: `<canvas>` sparkline, msg/s, last ~60 samples,
     with a horizontal reference line at the refill rate (refillTokens per
     refillPeriod, normalized to per-second) marking the ceiling.
   - Token-bucket gauge: `available / capacity`.
4. **Response log** — scrolling list of recent send outcomes
   (`202 messageId=…`, `409 duplicate`, `error: …`), newest on top, capped length.

## Data flow

### Publishing

- Each message is one `POST /messages` with `X-Idempotency-Key:
  crypto.randomUUID()` and body `{ "payload": <parsed textarea JSON> }`.
- **Send 1**: one request; append result to the response log. Stores the used
  idempotency key + payload as "last".
- **Resend last key**: re-POST the stored "last" payload with the stored "last"
  idempotency key (not a fresh UUID) → expected `409`, increments the Duplicate
  tally.
- **Send burst**: loop N requests with a concurrency cap (~20 in-flight) so the
  browser does not open thousands of sockets at once. Disable the burst button
  while a burst is running; show progress (sent / total).
- The frontend maintains its own `published` counter (incremented per attempted
  POST) — there is no backend metric for "published by this client".

### Polling (every 1000 ms)

A single interval loop:

1. `GET /q/metrics` → parse into an object of counter values.
2. `GET /admin/throttle` → tokens + capacity + refill rate.
3. `GET /admin/circuit-breaker/status` → CB state.

Then update derived values:

- **Consumed total** = `messages_consumed_total{success}` +
  `messages_consumed_total{dlq}`.
- **Backlog** = `published − consumedTotal` (floored at 0).
- **Rate (msg/s)** = `(consumedTotal_now − consumedTotal_prev) / intervalSeconds`,
  pushed into the chart ring buffer.
- Tiles, gauge, and chart redraw from the latest snapshot.

## Module boundaries (`app.js`)

Kept as small, named sections with clear responsibilities:

- **`api`** — fetch wrappers: `postMessage`, `getThrottle`, `getCbStatus`,
  `getMetricsText`. Owns headers and base paths.
- **`metrics`** — `parse(text) -> { [name#tags]: number }` plus accessor
  helpers. Pure, no DOM, no fetch.
- **`chart`** — canvas ring-buffer sparkline: `push(value)`, `draw()`, holds the
  reference-line value. No fetch, no app state.
- **`ui`** — DOM binding: reads controls, renders tiles/gauge/log, wires button
  handlers. The only section touching the DOM.

Each section is independently understandable: `metrics` and `chart` are pure and
testable in isolation; `api` is the only network surface; `ui` is the only DOM
surface.

## Error handling

- **Send failure** (network or non-2xx other than 409): red entry in the
  response log; never throw out of the burst loop (one bad send must not abort
  the rest).
- **Invalid payload JSON**: validated before send; inline error near the
  textarea; no request issued.
- **Metrics/admin poll failure**: affected tiles render `—`; the polling loop
  continues (one failed tick must not stop future ticks). A wrong admin token
  surfaces as `401` → token-dependent tiles show `—` and a hint to check the
  token.

## Testing

Manual acceptance (no automated test harness for static assets in this project):

1. Start Quarkus (`quarkus dev`) with Kafka, Redis, and the legacy stub
   reachable so consumption actually flows.
2. Open `http://localhost:8080/`.
3. **Single send** → response log shows `202` with a messageId. Then
   **Resend last key** → response log shows `409 duplicate` and the Duplicate
   tile increments.
4. **Burst 500** with capacity 100 / refill 100/s → consumption-rate chart rises
   and **flattens at ~100 msg/s** against the reference line; backlog spikes then
   drains; token gauge sits near empty during the burst and refills after.
5. Kill the legacy stub mid-burst → `dlq` consumed tally rises, CB state moves to
   `OPEN`; chart/log keep updating without crashing.

## Out of scope (YAGNI)

- No server-sent events / websocket stream (backend has none; polling is
  sufficient).
- No persistence of history across page reloads.
- No auth beyond the admin-token field.
- No build tooling, framework, or charting library.
