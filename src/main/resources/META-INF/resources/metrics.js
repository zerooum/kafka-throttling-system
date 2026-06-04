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
