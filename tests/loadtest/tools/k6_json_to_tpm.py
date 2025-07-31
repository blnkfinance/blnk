#!/usr/bin/env python3
import argparse, json, math, sys
from datetime import datetime
from collections import defaultdict
import csv

# k6 JSON output is newline-delimited. Each line is a JSON object with fields like:
# {"type":"Point","metric":"http_req_duration","data":{"time":"2025-07-26T07:40:01.234Z","value":222.5},"tags":{...}}
# We bucket by UTC minute and optionally filter by scenario tag.

def floorminute(ts: str) -> str:
    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%dT%H:%M")

parser = argparse.ArgumentParser()
parser.add_argument("infile")
parser.add_argument("outfile")
parser.add_argument("--apdex-threshold", type=float, default=300.0, help="Apdex T in ms")
parser.add_argument("--scenario", help="Only include samples where tags.scenario matches this value")
args = parser.parse_args()

counts = defaultdict(int)        # http_reqs per minute
fail_counts = defaultdict(float) # http_req_failed values summed
latencies = defaultdict(list)    # http_req_duration values (ms) per minute

with open(args.infile, "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        if obj.get("type") != "Point":
            continue
        tags = obj.get("tags", {})
        if args.scenario and tags.get("scenario") != args.scenario:
            continue
        metric = obj.get("metric")
        data = obj.get("data", {})
        ts = data.get("time")
        if not ts:
            continue
        minute = floorminute(ts)
        val = data.get("value")
        if metric == "http_reqs":
            counts[minute] += int(val or 0)
        elif metric == "http_req_failed":
            fail_counts[minute] += float(val or 0.0)
        elif metric == "http_req_duration":
            if val is not None:
                latencies[minute].append(float(val))

# Compute Apdex and percentiles per minute
T = args.apdex_threshold

def pct(values, p):
    if not values:
        return float("nan")
    values = sorted(values)
    k = (len(values) - 1) * (p / 100.0)
    i = int(math.floor(k))
    j = int(math.ceil(k))
    if i == j:
        return values[i]
    return values[i] + (values[j] - values[i]) * (k - i)

minutes = sorted(set(list(counts.keys()) + list(latencies.keys())))

with open(args.outfile, "w", newline="") as out:
    w = csv.writer(out)
    w.writerow(["minute","requests","tpm","success_ratio","p50_ms","p95_ms","p99_ms","apdex"])
    for m in minutes:
        reqs = counts.get(m, 0)
        fails = fail_counts.get(m, 0.0)
        lats = latencies.get(m, [])
        if reqs == 0 and lats:
            # Fallback: count lat samples as requests when http_reqs wasn't emitted
            reqs = len(lats)
        tpm = reqs
        succ_ratio = 1.0 if reqs == 0 else max(0.0, min(1.0, 1.0 - (fails / reqs)))
        p50 = pct(lats, 50)
        p95 = pct(lats, 95)
        p99 = pct(lats, 99)
        if lats:
            satisfied = sum(1 for x in lats if x <= T)
            tolerated = sum(1 for x in lats if x <= 4*T) - satisfied
            apdex = (satisfied + 0.5 * max(0, tolerated)) / len(lats)
        else:
            apdex = float("nan")
        w.writerow([m, reqs, tpm, round(succ_ratio, 6), round(p50, 3), round(p95, 3), round(p99, 3), round(apdex, 6)])

print("Wrote", args.outfile)