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
