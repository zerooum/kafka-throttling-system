import { parse, sumByTag } from "./metrics.js";
import { Sparkline } from "./chart.js";

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

const s = new Sparkline(3);
[1, 2, 3, 4].forEach((v) => s.push(v));
check("buffer capped at 3", s.samples().length === 3);
check("keeps newest [2,3,4]", JSON.stringify(s.samples()) === "[2,3,4]");
check("max = 4", s.max() === 4);
check("refLine defaults to 0", s.refLine === 0);

const summary = document.createElement("li");
summary.textContent = failures === 0 ? "ALL PASS" : `${failures} FAILURE(S)`;
summary.className = failures === 0 ? "ok" : "fail";
out.appendChild(summary);
