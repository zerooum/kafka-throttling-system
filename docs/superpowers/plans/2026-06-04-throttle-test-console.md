# Throttle Test Console Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a dependency-free browser console, served by Quarkus, to publish messages (single / resend-duplicate / burst) and watch the consumer drain them at the rate-limiter's pace in real time.

**Architecture:** Plain ES-module JS + HTML + CSS dropped into `src/main/resources/META-INF/resources/` so Quarkus serves it same-origin at `http://localhost:8080/` (no CORS, no Java changes). Pure logic (`metrics.js` Prometheus parser, `chart.js` sparkline buffer) lives in standalone modules with an in-browser `tests.html` assertion harness. `api.js` is the only network surface; `app.js` is the only DOM surface and runs the 1 s polling loop.

**Tech Stack:** Browser ES modules, Canvas 2D, Fetch API, `crypto.randomUUID()`. No npm, no bundler, no framework, no CDN. Backend: existing Quarkus endpoints (`/messages`, `/admin/throttle`, `/admin/circuit-breaker/status`, `/q/metrics`).

---

## File Structure

All under `src/main/resources/META-INF/resources/`:

| File | Responsibility |
|---|---|
| `index.html` | Single-page layout: controls, KPI tiles, chart+gauge, response log. |
| `style.css` | Styling for the above. |
| `metrics.js` | Pure: parse Prometheus text → map; `sumByTag` accessor. No DOM, no fetch. |
| `chart.js` | Pure-ish: `Sparkline` fixed-capacity buffer + canvas `draw()`. No fetch, no app state. |
| `api.js` | Fetch wrappers: `postMessage`, `getThrottle`, `getCbStatus`, `getMetricsText`. Only network surface. |
| `app.js` | DOM binding + control handlers + 1 s polling loop. Only DOM surface. |
| `tests.html` + `tests.js` | In-browser assertion harness for the two pure modules. Opened manually at `/tests.html`. |

**Verification model:** `metrics.js` and `chart.js` are verified automatically by `tests.html` (open in browser → all green). `api.js` / `app.js` (fetch + DOM + polling) are verified by the manual acceptance steps in Task 7, which mirror the spec's Testing section. There is no Node/npm harness — that would violate the "no build tooling" constraint.

---

## Task 1: Scaffold + confirm Quarkus serves static files

**Files:**
- Create: `src/main/resources/META-INF/resources/index.html`
- Create: `src/main/resources/META-INF/resources/style.css`

- [ ] **Step 1: Create a minimal index.html**

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Throttle Test Console</title>
  <link rel="stylesheet" href="style.css">
</head>
<body>
  <h1>Throttle Test Console</h1>
  <p id="ping">scaffold ok</p>
</body>
</html>
```

- [ ] **Step 2: Create a minimal style.css**

```css
body {
  font-family: system-ui, sans-serif;
  margin: 1.5rem;
  background: #0f1115;
  color: #e6e6e6;
}
h1 { font-size: 1.25rem; }
```

- [ ] **Step 3: Start Quarkus dev mode**

Run: `./mvnw quarkus:dev` (leave running in a second terminal)
Expected: `Listening on: http://localhost:8080`. Kafka/Redis dev services start.

- [ ] **Step 4: Confirm the files are served same-origin**

Run: `curl -s -o /dev/null -w "%{http_code} %{content_type}\n" http://localhost:8080/`
Expected: `200 text/html` (Quarkus serves `index.html` as the welcome file at `/`).

Run: `curl -s http://localhost:8080/style.css | head -1`
Expected: `body {`

Run: `curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/q/metrics`
Expected: `200` (confirms the metrics endpoint the frontend depends on is live).

- [ ] **Step 5: Commit**

```bash
git add src/main/resources/META-INF/resources/index.html src/main/resources/META-INF/resources/style.css
git commit -m "feat(console): scaffold static frontend served by Quarkus"
```

---

## Task 2: Prometheus parser (`metrics.js`) — TDD via tests.html

**Files:**
- Create: `src/main/resources/META-INF/resources/tests.html`
- Create: `src/main/resources/META-INF/resources/tests.js`
- Create: `src/main/resources/META-INF/resources/metrics.js`

- [ ] **Step 1: Write the test harness page**

`tests.html`:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Throttle Console — self-tests</title>
  <style>
    body { font-family: monospace; margin: 1.5rem; }
    .ok { color: #1a7f37; }
    .fail { color: #c0392b; font-weight: bold; }
  </style>
</head>
<body>
  <h1>Throttle Console — self-tests</h1>
  <ul id="out"></ul>
  <script type="module" src="tests.js"></script>
</body>
</html>
```

- [ ] **Step 2: Write the failing tests for the parser**

`tests.js`:

```js
import { parse, sumByTag } from "./metrics.js";

const out = document.getElementById("out");
let failures = 0;
function check(name, cond) {
  const li = document.createElement("li");
  li.textContent = (cond ? "PASS " : "FAIL ") + name;
  li.className = cond ? "ok" : "fail";
  if (!cond) failures++;
  out.appendChild(li);
}

const sample = `# HELP messages_consumed_total
# TYPE messages_consumed_total counter
messages_consumed_total{outcome="success",} 213.0
messages_consumed_total{outcome="dlq",} 7.0
messages_ingress_received_total{outcome="accepted",} 498.0
messages_ingress_received_total{outcome="duplicate",} 2.0
some_other_metric 5.0`;

const p = parse(sample);
check("success counter = 213", sumByTag(p, "messages_consumed_total", "outcome", "success") === 213);
check("dlq counter = 7", sumByTag(p, "messages_consumed_total", "outcome", "dlq") === 7);
check("sum consumed = 220", sumByTag(p, "messages_consumed_total") === 220);
check("untagged metric parsed", p["some_other_metric"] === 5);
check("missing metric = 0", sumByTag(p, "nope", "outcome", "x") === 0);

const summary = document.createElement("li");
summary.textContent = failures === 0 ? "ALL PASS" : `${failures} FAILURE(S)`;
summary.className = failures === 0 ? "ok" : "fail";
out.appendChild(summary);
```

- [ ] **Step 3: Verify the tests fail**

Open `http://localhost:8080/tests.html` in a browser. Open devtools console.
Expected: a module-load error / blank list because `metrics.js` does not exist yet (404 on the import). This confirms the harness runs the right file.

- [ ] **Step 4: Implement `metrics.js`**

```js
// metrics.js — pure Prometheus text-exposition parser. No DOM, no fetch.

// Parse Prometheus text into a flat map keyed by the full series string
// (e.g. `messages_consumed_total{outcome="success",}`) → numeric value.
export function parse(text) {
  const out = {};
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (line === "" || line.startsWith("#")) continue;
    const sp = line.lastIndexOf(" ");
    if (sp === -1) continue;
    const key = line.slice(0, sp).trim();
    const val = Number(line.slice(sp + 1).trim());
    if (Number.isNaN(val)) continue;
    out[key] = val;
  }
  return out;
}

// Sum every series of `name`. If `tag`/`tagVal` are given, only series whose
// label set contains `tag="tagVal"` are summed. Missing metric → 0.
export function sumByTag(parsed, name, tag, tagVal) {
  let total = 0;
  for (const key of Object.keys(parsed)) {
    const brace = key.indexOf("{");
    const metric = brace === -1 ? key : key.slice(0, brace);
    if (metric !== name) continue;
    if (tag) {
      const labels = brace === -1 ? "" : key.slice(brace);
      if (!labels.includes(`${tag}="${tagVal}"`)) continue;
    }
    total += parsed[key];
  }
  return total;
}
```

- [ ] **Step 5: Verify the tests pass**

Reload `http://localhost:8080/tests.html`.
Expected: all parser lines green, final line `ALL PASS`.

- [ ] **Step 6: Commit**

```bash
git add src/main/resources/META-INF/resources/tests.html src/main/resources/META-INF/resources/tests.js src/main/resources/META-INF/resources/metrics.js
git commit -m "feat(console): add Prometheus metrics parser with browser self-tests"
```

---

## Task 3: Sparkline buffer (`chart.js`) — TDD via tests.html

**Files:**
- Create: `src/main/resources/META-INF/resources/chart.js`
- Modify: `src/main/resources/META-INF/resources/tests.js`

- [ ] **Step 1: Add failing Sparkline tests**

Append to `tests.js`, immediately **before** the `summary` block (so the summary still runs last):

```js
import { Sparkline } from "./chart.js";

const s = new Sparkline(3);
[1, 2, 3, 4].forEach((v) => s.push(v));
check("buffer capped at 3", s.samples().length === 3);
check("keeps newest [2,3,4]", JSON.stringify(s.samples()) === "[2,3,4]");
check("max = 4", s.max() === 4);
check("refLine defaults to 0", s.refLine === 0);
```

Note: ES `import` statements hoist to the top of the module at load time, so placing this `import` mid-file is valid — but for readability move both `import` lines to the top of `tests.js` together. The `check(...)` calls must stay before the `summary` block.

- [ ] **Step 2: Verify the new tests fail**

Reload `http://localhost:8080/tests.html`.
Expected: load error (404 on `chart.js`) — the page fails to import. Confirms the harness targets the missing module.

- [ ] **Step 3: Implement `chart.js`**

```js
// chart.js — fixed-capacity sample buffer + canvas sparkline renderer.
// Pure data logic (push/samples/max) is independently testable; draw() needs a
// canvas 2D context and is exercised by the live UI.
export class Sparkline {
  constructor(capacity = 60) {
    this.capacity = capacity;
    this.refLine = 0; // reference value (msg/s ceiling); set by the caller
    this._data = [];
  }

  push(v) {
    this._data.push(v);
    if (this._data.length > this.capacity) this._data.shift();
  }

  samples() {
    return this._data.slice();
  }

  max() {
    return this._data.length ? Math.max(...this._data) : 0;
  }

  // Draw the series and a dashed reference line into a 2D context.
  draw(ctx, width, height, refLine) {
    ctx.clearRect(0, 0, width, height);
    const data = this._data;
    const peak = Math.max(this.max(), refLine || 0, 1);
    const pad = 4;
    const w = width - pad * 2;
    const h = height - pad * 2;
    const x = (i) => pad + (data.length <= 1 ? 0 : (i / (data.length - 1)) * w);
    const y = (v) => pad + h - (v / peak) * h;

    if (refLine) {
      ctx.strokeStyle = "#c0392b";
      ctx.setLineDash([4, 4]);
      ctx.beginPath();
      ctx.moveTo(pad, y(refLine));
      ctx.lineTo(pad + w, y(refLine));
      ctx.stroke();
      ctx.setLineDash([]);
    }

    ctx.strokeStyle = "#2ecc71";
    ctx.lineWidth = 2;
    ctx.beginPath();
    data.forEach((v, i) => {
      const px = x(i);
      const py = y(v);
      if (i === 0) ctx.moveTo(px, py);
      else ctx.lineTo(px, py);
    });
    ctx.stroke();
  }
}
```

- [ ] **Step 4: Verify all tests pass**

Reload `http://localhost:8080/tests.html`.
Expected: every line green, final line `ALL PASS`.

- [ ] **Step 5: Commit**

```bash
git add src/main/resources/META-INF/resources/chart.js src/main/resources/META-INF/resources/tests.js
git commit -m "feat(console): add sparkline buffer with browser self-tests"
```

---

## Task 4: Network layer (`api.js`)

**Files:**
- Create: `src/main/resources/META-INF/resources/api.js`

`api.js` is fetch-only and verified through the live UI in Task 7 (no isolated test — it has no logic beyond request shaping).

- [ ] **Step 1: Implement `api.js`**

```js
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
```

- [ ] **Step 2: Sanity-check it imports cleanly**

Reload `http://localhost:8080/tests.html` (which does not import `api.js`) — confirm it still shows `ALL PASS` (no syntax error introduced anywhere). `api.js` itself is exercised in Task 7.

- [ ] **Step 3: Commit**

```bash
git add src/main/resources/META-INF/resources/api.js
git commit -m "feat(console): add fetch wrappers for messages and admin endpoints"
```

---

## Task 5: Full layout markup + styling

**Files:**
- Modify: `src/main/resources/META-INF/resources/index.html`
- Modify: `src/main/resources/META-INF/resources/style.css`

- [ ] **Step 1: Replace index.html body with the full layout**

Replace the entire `index.html` with:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Throttle Test Console</title>
  <link rel="stylesheet" href="style.css">
</head>
<body>
  <h1>Throttle Test Console</h1>

  <section class="controls">
    <label class="payload-label">Payload JSON
      <textarea id="payload" rows="3">{"hello":"world"}</textarea>
    </label>
    <div id="payload-error" class="error" hidden></div>
    <div class="control-row">
      <button id="send-one">Send 1</button>
      <button id="resend" disabled>Resend last key</button>
      <label>Burst <input id="burst-count" type="number" value="500" min="1"></label>
      <button id="send-burst">Send burst</button>
      <span id="burst-progress" class="muted"></span>
      <label class="token-label">Admin token
        <input id="admin-token" type="text" value="dev-admin">
      </label>
    </div>
  </section>

  <section class="tiles">
    <div class="tile"><span class="label">Published</span><span id="t-published" class="val">0</span></div>
    <div class="tile"><span class="label">Accepted</span><span id="t-accepted" class="val">—</span></div>
    <div class="tile"><span class="label">Duplicate</span><span id="t-duplicate" class="val">—</span></div>
    <div class="tile"><span class="label">Consumed ok</span><span id="t-success" class="val">—</span></div>
    <div class="tile"><span class="label">Consumed dlq</span><span id="t-dlq" class="val">—</span></div>
    <div class="tile"><span class="label">Backlog</span><span id="t-backlog" class="val">—</span></div>
    <div class="tile"><span class="label">Circuit breaker</span><span id="t-cb" class="val">—</span></div>
  </section>

  <section class="live">
    <div class="chart-box">
      <div class="label">Consumption rate (msg/s) · red dashed = limit ceiling</div>
      <canvas id="chart" width="520" height="160"></canvas>
      <div id="rate-now" class="muted">— msg/s</div>
    </div>
    <div class="gauge-box">
      <div class="label">Token bucket</div>
      <div class="gauge"><div id="gauge-fill" class="gauge-fill"></div></div>
      <div id="gauge-text" class="muted">— / —</div>
    </div>
  </section>

  <section class="log">
    <div class="label">Response log</div>
    <ul id="response-log"></ul>
  </section>

  <script type="module" src="app.js"></script>
</body>
</html>
```

- [ ] **Step 2: Replace style.css with full styling**

```css
body {
  font-family: system-ui, sans-serif;
  margin: 1.5rem;
  background: #0f1115;
  color: #e6e6e6;
}
h1 { font-size: 1.25rem; margin-bottom: 1rem; }
.label { display: block; font-size: 0.72rem; text-transform: uppercase; letter-spacing: 0.04em; color: #8a93a2; }
.muted { color: #8a93a2; font-size: 0.8rem; }
.error { color: #ff6b6b; font-size: 0.85rem; margin: 0.25rem 0; }

.controls { margin-bottom: 1rem; }
.payload-label { font-size: 0.72rem; text-transform: uppercase; color: #8a93a2; }
#payload {
  display: block; width: 100%; max-width: 520px; margin-top: 0.25rem;
  background: #171a21; color: #e6e6e6; border: 1px solid #2a2f3a;
  border-radius: 6px; padding: 0.5rem; font-family: monospace; resize: vertical;
}
.control-row { display: flex; flex-wrap: wrap; gap: 0.6rem; align-items: center; margin-top: 0.6rem; }
.control-row label { font-size: 0.8rem; color: #c4ccd6; }
input[type="number"], input[type="text"] {
  background: #171a21; color: #e6e6e6; border: 1px solid #2a2f3a;
  border-radius: 5px; padding: 0.3rem 0.4rem;
}
input[type="number"] { width: 5rem; }
button {
  background: #2563eb; color: #fff; border: none; border-radius: 6px;
  padding: 0.4rem 0.8rem; cursor: pointer; font-size: 0.85rem;
}
button:disabled { background: #394150; cursor: not-allowed; }

.tiles { display: flex; flex-wrap: wrap; gap: 0.6rem; margin: 1rem 0; }
.tile {
  background: #171a21; border: 1px solid #2a2f3a; border-radius: 8px;
  padding: 0.6rem 0.9rem; min-width: 6.5rem;
}
.tile .val { display: block; font-size: 1.4rem; font-weight: 600; margin-top: 0.2rem; }

.live { display: flex; flex-wrap: wrap; gap: 1.5rem; margin: 1rem 0; align-items: flex-start; }
.chart-box canvas {
  display: block; margin-top: 0.4rem; background: #12151c;
  border: 1px solid #2a2f3a; border-radius: 8px;
}
.gauge-box { min-width: 12rem; }
.gauge {
  width: 100%; height: 1.4rem; margin-top: 0.4rem; background: #12151c;
  border: 1px solid #2a2f3a; border-radius: 8px; overflow: hidden;
}
.gauge-fill { height: 100%; width: 0%; background: #2ecc71; transition: width 0.4s ease; }

.log ul {
  list-style: none; padding: 0; margin: 0.4rem 0 0; max-height: 16rem;
  overflow-y: auto; font-family: monospace; font-size: 0.82rem;
}
.log li { padding: 0.2rem 0; border-bottom: 1px solid #1c2029; }
.log li.ok { color: #2ecc71; }
.log li.warn { color: #f0ad4e; }
.log li.err { color: #ff6b6b; }
```

- [ ] **Step 3: Verify the layout renders (app.js not present yet)**

Open `http://localhost:8080/`. You will see a console error `app.js 404` — expected; markup/styling still render. Confirm: payload textarea, the four buttons, seven tiles, an empty chart canvas, gauge bar, and an empty response log are all visible and styled.

- [ ] **Step 4: Commit**

```bash
git add src/main/resources/META-INF/resources/index.html src/main/resources/META-INF/resources/style.css
git commit -m "feat(console): full single-page layout and styling"
```

---

## Task 6: Controls — single send, resend-duplicate, burst (`app.js` part 1)

**Files:**
- Create: `src/main/resources/META-INF/resources/app.js`

- [ ] **Step 1: Create `app.js` with imports, state, and the send/control logic**

```js
import { postMessage, getThrottle, getCbStatus, getMetricsText } from "./api.js";
import { parse, sumByTag } from "./metrics.js";
import { Sparkline } from "./chart.js";

const POLL_MS = 1000;
const MAX_INFLIGHT = 20;
const LOG_CAP = 50;

const state = {
  published: 0,
  lastKey: null,
  lastPayload: null,
  prevConsumed: null,
  bursting: false,
};

const spark = new Sparkline(60);

const el = (id) => document.getElementById(id);
const payloadEl = el("payload");
const payloadErr = el("payload-error");
const adminTokenEl = el("admin-token");

function readPayload() {
  try {
    const obj = JSON.parse(payloadEl.value);
    payloadErr.hidden = true;
    return obj;
  } catch (e) {
    payloadErr.textContent = "Invalid JSON: " + e.message;
    payloadErr.hidden = false;
    return undefined; // signal: do not send
  }
}

function logEntry(text, kind) {
  const ul = el("response-log");
  const li = document.createElement("li");
  li.textContent = text;
  if (kind) li.className = kind;
  ul.insertBefore(li, ul.firstChild);
  while (ul.children.length > LOG_CAP) ul.removeChild(ul.lastChild);
}

async function sendOne(idempotencyKey, payload) {
  state.published++;
  el("t-published").textContent = state.published;
  try {
    const { status, body } = await postMessage(idempotencyKey, payload);
    if (status === 202) {
      logEntry(`202 messageId=${body && body.messageId}`, "ok");
    } else if (status === 409) {
      logEntry(`409 duplicate (key=${idempotencyKey})`, "warn");
    } else {
      logEntry(`${status} ${body ? JSON.stringify(body) : ""}`, "err");
    }
  } catch (e) {
    logEntry(`error: ${e.message}`, "err");
  }
}

el("send-one").addEventListener("click", async () => {
  const payload = readPayload();
  if (payload === undefined) return;
  const key = crypto.randomUUID();
  state.lastKey = key;
  state.lastPayload = payload;
  el("resend").disabled = false;
  await sendOne(key, payload);
});

el("resend").addEventListener("click", async () => {
  if (!state.lastKey) return;
  await sendOne(state.lastKey, state.lastPayload);
});

el("send-burst").addEventListener("click", async () => {
  if (state.bursting) return;
  const payload = readPayload();
  if (payload === undefined) return;
  const total = Math.max(1, parseInt(el("burst-count").value, 10) || 0);

  state.bursting = true;
  el("send-burst").disabled = true;
  const progress = el("burst-progress");

  let started = 0;
  let done = 0;

  async function worker() {
    while (started < total) {
      started++;
      const key = crypto.randomUUID();
      state.lastKey = key;
      state.lastPayload = payload;
      await sendOne(key, payload);
      done++;
      progress.textContent = `${done}/${total}`;
    }
  }

  const workers = [];
  for (let i = 0; i < Math.min(MAX_INFLIGHT, total); i++) workers.push(worker());
  await Promise.all(workers);

  el("resend").disabled = false;
  el("send-burst").disabled = false;
  state.bursting = false;
  progress.textContent = `done ${total}`;
});
```

- [ ] **Step 2: Verify single send**

Reload `http://localhost:8080/` (Quarkus dev still running). Click **Send 1**.
Expected: response log shows a green `202 messageId=01J...`; the Published tile increments to `1`. The **Resend last key** button becomes enabled.

- [ ] **Step 3: Verify resend produces a duplicate**

Click **Resend last key**.
Expected: response log shows an amber `409 duplicate (key=...)`. Published increments again (frontend counts attempts).

- [ ] **Step 4: Verify invalid JSON is blocked**

Edit the payload textarea to `{bad json`, click **Send 1**.
Expected: red inline error under the textarea (`Invalid JSON: ...`); no new log entry; Published does not increment. Restore payload to `{"hello":"world"}`.

- [ ] **Step 5: Verify burst**

Set burst count to `50`, click **Send burst**.
Expected: button disables, progress text counts up `…/50` then `done 50`; log fills with `202` entries (capped at 50 visible); Published jumps by 50; button re-enables.

- [ ] **Step 6: Commit**

```bash
git add src/main/resources/META-INF/resources/app.js
git commit -m "feat(console): publish controls — single, resend-duplicate, burst"
```

---

## Task 7: Live polling — tiles, gauge, rate chart, error handling (`app.js` part 2)

**Files:**
- Modify: `src/main/resources/META-INF/resources/app.js`

- [ ] **Step 1: Append the polling loop to `app.js`**

Add at the end of `app.js` (after the burst handler):

```js
// ---- live polling ----

function setTilesUnknown() {
  for (const id of ["t-accepted", "t-duplicate", "t-success", "t-dlq", "t-backlog"]) {
    el(id).textContent = "—";
  }
}

async function pollMetrics() {
  try {
    const text = await getMetricsText();
    const p = parse(text);
    const accepted = sumByTag(p, "messages_ingress_received_total", "outcome", "accepted");
    const duplicate = sumByTag(p, "messages_ingress_received_total", "outcome", "duplicate");
    const success = sumByTag(p, "messages_consumed_total", "outcome", "success");
    const dlq = sumByTag(p, "messages_consumed_total", "outcome", "dlq");
    const consumedTotal = success + dlq;

    el("t-accepted").textContent = accepted;
    el("t-duplicate").textContent = duplicate;
    el("t-success").textContent = success;
    el("t-dlq").textContent = dlq;
    el("t-backlog").textContent = Math.max(0, state.published - consumedTotal);

    if (state.prevConsumed !== null) {
      const rate = (consumedTotal - state.prevConsumed) / (POLL_MS / 1000);
      spark.push(Math.max(0, rate));
      el("rate-now").textContent = `${rate.toFixed(0)} msg/s`;
    }
    state.prevConsumed = consumedTotal;
  } catch (e) {
    setTilesUnknown(); // one failed tick must not stop the loop
  }
}

async function pollThrottle() {
  try {
    const t = await getThrottle(adminTokenEl.value);
    const pct = t.capacity ? Math.round((t.available / t.capacity) * 100) : 0;
    el("gauge-fill").style.width = pct + "%";
    el("gauge-text").textContent = `${t.available} / ${t.capacity}`;
    spark.refLine = t.refillPeriodMs ? (t.refillTokens * 1000) / t.refillPeriodMs : 0;
  } catch (e) {
    el("gauge-text").textContent = "— / — (check admin token)";
    el("gauge-fill").style.width = "0%";
  }
}

async function pollCb() {
  try {
    const c = await getCbStatus(adminTokenEl.value);
    el("t-cb").textContent = c.state;
  } catch (e) {
    el("t-cb").textContent = "—";
  }
}

function drawChart() {
  const canvas = el("chart");
  const ctx = canvas.getContext("2d");
  spark.draw(ctx, canvas.width, canvas.height, spark.refLine || 0);
}

async function tick() {
  await Promise.all([pollMetrics(), pollThrottle(), pollCb()]);
  drawChart();
}

setInterval(tick, POLL_MS);
tick();
```

- [ ] **Step 2: Verify idle polling populates tiles and gauge**

Reload `http://localhost:8080/`. Wait ~2 s without sending.
Expected: Accepted/Duplicate/Consumed/dlq tiles show real numbers (reflecting prior sends), Backlog shows a number, Circuit breaker shows `CLOSED`, the token gauge fills toward `100 / 100`, and the rate line reads `0 msg/s`.

- [ ] **Step 3: Verify the rate ceiling under burst (spec acceptance #4)**

Set burst count `500`, click **Send burst**. Watch for ~10 s.
Expected: consumption-rate chart rises and **flattens against the red dashed reference line (~100 msg/s)**; Backlog spikes then drains toward 0; token gauge sits near empty during the burst and refills after; `Consumed ok` climbs steadily.

- [ ] **Step 4: Verify bad-token resilience (spec error handling)**

Change the Admin token field to `wrong`. Wait ~2 s.
Expected: gauge text shows `— / — (check admin token)`, Circuit breaker shows `—`, but the metrics-driven tiles (Accepted/Consumed/Backlog) and the rate chart keep updating — the loop does not crash. Restore the token to `dev-admin` and confirm the gauge recovers.

- [ ] **Step 5: Verify DLQ + circuit-breaker path (spec acceptance #5)**

With Quarkus still running, stop the legacy stub it calls (in `quarkus:dev` the legacy target is the Wiremock/dev resource at `http://localhost:8089`; stop or make it fail), then **Send burst 200**.
Expected: `Consumed dlq` tile rises, Circuit breaker tile moves to `OPEN`, and the chart/log keep updating without errors in the devtools console.

- [ ] **Step 6: Confirm self-tests still pass**

Open `http://localhost:8080/tests.html`.
Expected: `ALL PASS` (pure modules untouched by this task).

- [ ] **Step 7: Commit**

```bash
git add src/main/resources/META-INF/resources/app.js
git commit -m "feat(console): live polling for tiles, token gauge, and rate chart"
```

---

## Task 8: Final acceptance pass + docs note

**Files:**
- Modify: `README.md` (append a short usage note)

- [ ] **Step 1: Append a usage section to README.md**

Add near the end of `README.md`:

```markdown
## Throttle Test Console

A dependency-free browser console for exercising the system is served by Quarkus
at `http://localhost:8080/` when the app is running.

- **Send 1** — publish a single message (fresh idempotency key).
- **Resend last key** — re-send the previous message with the same key to trigger
  a `409` duplicate.
- **Send burst** — flood N messages (default 500) to push past the rate limit.
- The dashboard polls `/q/metrics` and `/admin/*` every second to show published /
  accepted / duplicate / consumed / backlog tallies, the circuit-breaker state, a
  live consumption-rate chart (with the limiter ceiling drawn as a red dashed
  line), and the token-bucket gauge.

Admin endpoints need the token from `admin.token` (default `dev-admin`); set it in
the Admin token field. Open `http://localhost:8080/tests.html` to run the parser /
chart self-tests.
```

- [ ] **Step 2: Run the full acceptance script once more**

With `quarkus:dev`, Kafka, Redis, and the legacy stub all healthy, walk the spec's
Testing section end to end: single send → resend `409` → burst 500 flattening at
the ceiling → bad token resilience → DLQ/CB on legacy failure. Confirm each
behaves as described above.

- [ ] **Step 3: Confirm no Java/config changes were needed**

Run: `git diff --name-only main -- src/main/java src/main/resources/application.properties pom.xml`
Expected: empty output (the console is pure static assets; no backend changes).

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: document the throttle test console"
```

---

## Self-Review Notes

- **Spec coverage:** controls bar (Task 5/6), KPI tiles incl. Published/Backlog/CB (Task 5/7), rate chart with reference line (Task 3/7), token gauge (Task 5/7), response log (Task 5/6), 1 s polling of all three sources (Task 7), client-side published counter & backlog (Task 7), concurrency-capped burst with progress (Task 6), resend-duplicate (Task 6), invalid-JSON guard + poll-failure resilience + bad-token handling (Task 6/7), Quarkus same-origin serving (Task 1). All spec sections map to a task.
- **No placeholders:** every code step contains complete, runnable code; every verification step states the exact URL/command and expected observation.
- **Type/name consistency:** `parse`/`sumByTag` (metrics.js) and `Sparkline` with `push`/`samples`/`max`/`refLine`/`draw` (chart.js) are used identically in `tests.js` and `app.js`. `postMessage` returns `{ status, body }` and is consumed that way in `sendOne`. DOM ids in `index.html` match every `el("...")` lookup in `app.js`.
- **Constraint honored:** no npm/bundler/framework/CDN; verification of pure logic is in-browser (`tests.html`), UI verification is manual per the spec.
```

