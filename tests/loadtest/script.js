/*
Copyright 2024 Blnk Finance Authors.
Apache 2.0
*/
import http from "k6/http";
import { check } from "k6";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import { textSummary } from "https://jslib.k6.io/k6-summary/0.0.4/index.js";

const URL = __ENV.URL || "https://YOUR_DOMAIN/transactions";
// baseline | ramp | contention | soak | spike
const SCENARIO = __ENV.SCENARIO || "baseline";
const HOT_DEST = __ENV.HOT_DEST || "@hot-balance-0001";
const HOT_RPS = Number(__ENV.HOT_RPS || 308); // ~70%
const RANDOM_RPS = Number(__ENV.RANDOM_RPS || 132); // ~30%

function dth(scn) {
  var o = {};
  o["http_req_failed{scenario:" + scn + "}"] = ["rate<0.001"];
  o[`http_req_duration{scenario:${scn},expected_response:true}`] = [
    "p(95)<300",
    "p(99)<600",
  ];
  return o;
}

// Simple merge without spread/rest (for Goja compatibility)
function merge() {
  var out = {};
  for (var i = 0; i < arguments.length; i++) {
    var src = arguments[i];
    if (!src) continue;
    for (var k in src) {
      if (Object.prototype.hasOwnProperty.call(src, k)) out[k] = src[k];
    }
  }
  return out;
}

export const options = buildOptions();

function buildOptions() {
  if (SCENARIO === "baseline") {
    return {
      scenarios: {
        baseline: {
          executor: "constant-vus",
          vus: Number(__ENV.VUS || 100),
          duration: __ENV.DURATION || "5m",
          gracefulStop: "30s",
          tags: { scenario: "baseline" },
        },
      },
      thresholds: dth("baseline"),
    };
  }

  if (SCENARIO === "ramp") {
    return {
      scenarios: {
        ramp: {
          executor: "ramping-vus",
          startVUs: Number(__ENV.START_VUS || 100),
          stages: [
            {
              duration: __ENV.STAGE1 || "5m",
              target: Number(__ENV.VUS1 || 300),
            },
            {
              duration: __ENV.STAGE2 || "5m",
              target: Number(__ENV.VUS2 || 600),
            },
            {
              duration: __ENV.STAGE3 || "5m",
              target: Number(__ENV.VUS3 || 1000),
            },
          ],
          gracefulRampDown: "1m",
          tags: { scenario: "ramp" },
        },
      },
      thresholds: dth("ramp"),
    };
  }

  if (SCENARIO === "contention") {
    return {
      scenarios: {
        contention_hot: {
          executor: "constant-arrival-rate",
          rate: HOT_RPS,
          timeUnit: "1s",
          duration: __ENV.DURATION || "5m",
          preAllocatedVUs: Number(__ENV.VUS || 200),
          maxVUs: Number(__ENV.MAX_VUS || 800),
          tags: { scenario: "contention_hot" },
          exec: "hotTraffic",
        },
        contention_random: {
          executor: "constant-arrival-rate",
          rate: RANDOM_RPS,
          timeUnit: "1s",
          duration: __ENV.DURATION || "5m",
          preAllocatedVUs: Number(__ENV.VUS || 200),
          maxVUs: Number(__ENV.MAX_VUS || 800),
          tags: { scenario: "contention_random" },
          exec: "randomTraffic",
        },
      },
      thresholds: merge(dth("contention_hot"), dth("contention_random")),
    };
  }

  if (SCENARIO === "soak") {
    return {
      scenarios: {
        soak: {
          executor: "constant-vus",
          vus: Number(__ENV.VUS || 200),
          duration: __ENV.DURATION || "2h",
          gracefulStop: "1m",
          tags: { scenario: "soak" },
        },
      },
      thresholds: dth("soak"),
    };
  }

  if (SCENARIO === "spike") {
    return {
      scenarios: {
        spike_warm: {
          executor: "constant-arrival-rate",
          rate: Number(__ENV.BASE_RPS || 200),
          timeUnit: "1s",
          duration: __ENV.WARM || "2m",
          preAllocatedVUs: Number(__ENV.PRE_VUS || 300),
          maxVUs: Number(__ENV.MAX_VUS || 1200),
          tags: { scenario: "spike_warm" },
        },
        spike_peak: {
          executor: "constant-arrival-rate",
          rate: Number(__ENV.SPIKE_RPS || 600),
          timeUnit: "1s",
          duration: __ENV.PEAK || "1m",
          preAllocatedVUs: Number(__ENV.PRE_VUS || 300),
          maxVUs: Number(__ENV.MAX_VUS || 1200),
          tags: { scenario: "spike_peak" },
        },
        spike_recover: {
          executor: "constant-arrival-rate",
          rate: Number(__ENV.BASE_RPS || 200),
          timeUnit: "1s",
          duration: __ENV.COOL || "3m",
          preAllocatedVUs: Number(__ENV.PRE_VUS || 300),
          maxVUs: Number(__ENV.MAX_VUS || 1200),
          tags: { scenario: "spike_recover" },
        },
      },
      thresholds: merge(
        dth("spike_warm"),
        dth("spike_peak"),
        dth("spike_recover")
      ),
    };
  }

  throw new Error("Unknown SCENARIO=" + SCENARIO);
}

function postTxn(dest) {
  var amount = Math.floor(Math.random() * (1000 - 100 + 1)) + 100; // 100..1000
  var payload = JSON.stringify({
    amount: amount,
    description: "load test",
    precision: 100,
    allow_overdraft: true,
    reference: uuidv4(),
    currency: "USD",
    source: "@world",
    destination: dest,
  });

  var res = http.post(URL, payload, {
    headers: { "Content-Type": "application/json" },
    timeout: "30s",
    tags: { endpoint: "transactions" },
  });

  check(res, {
    "is status 201": function (r) {
      return r.status === 201;
    },
  });
}

export default function () {
  // baseline, ramp, soak, spike use this
  postTxn("@" + uuidv4());
}

export function hotTraffic() {
  postTxn(HOT_DEST);
}

export function randomTraffic() {
  postTxn("@" + uuidv4());
}

// Optional: saves detailed stats to stdout + JSON file
export function handleSummary(data) {
  return {
    stdout: textSummary(data, { indent: " ", enableColors: true }),
    "summary.json": JSON.stringify(data, null, 2),
  };
}
