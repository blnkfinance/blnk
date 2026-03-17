#!/usr/bin/env python3
from __future__ import annotations

import csv
import hashlib
import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SUPPORTED_SUFFIXES = {".json", ".csv", ".ndjson"}


def discover_runs(root: Path) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for path in sorted(root.rglob("*")):
        if not path.is_file() or path.suffix.lower() not in SUPPORTED_SUFFIXES:
            continue
        if "tools/dashboard" in path.as_posix():
            continue
        parsed = parse_file(path, root)
        if parsed is not None:
            runs.append(parsed)
    runs = merge_related_runs(runs)
    runs.sort(
        key=lambda run: (
            run.get("collectedAt") or "",
            run.get("name") or "",
        ),
        reverse=True,
    )
    return runs


def merge_related_runs(runs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    summaries = [run for run in runs if run.get("kind") == "k6_summary"]
    ndjson_runs = [run for run in runs if run.get("kind") == "k6_ndjson"]
    if not summaries or not ndjson_runs:
        return runs

    summary_by_id = {run["id"]: run for run in summaries if run.get("id")}
    consumed_ndjson_ids: set[str] = set()

    for summary in summaries:
        match = find_matching_ndjson(summary, ndjson_runs, consumed_ndjson_ids)
        if match is None:
            continue
        consumed_ndjson_ids.add(match["id"])
        merged = merge_summary_with_ndjson(summary, match)
        summary_by_id[summary["id"]] = merged

    merged_runs: list[dict[str, Any]] = []
    for run in runs:
        run_id = run.get("id")
        if run_id in consumed_ndjson_ids:
            continue
        if run.get("kind") == "k6_summary" and run_id in summary_by_id:
            merged_runs.append(summary_by_id[run_id])
            continue
        merged_runs.append(run)
    return merged_runs


def find_matching_ndjson(
    summary: dict[str, Any],
    ndjson_runs: list[dict[str, Any]],
    consumed_ndjson_ids: set[str],
) -> dict[str, Any] | None:
    summary_source = summary.get("source") or ""
    summary_parent = Path(summary_source).parent.as_posix()
    summary_time = parse_iso(summary.get("collectedAt"))

    best_match: dict[str, Any] | None = None
    best_score: tuple[float, float] | None = None

    for candidate in ndjson_runs:
        candidate_id = candidate.get("id")
        if not candidate_id or candidate_id in consumed_ndjson_ids:
            continue

        candidate_source = candidate.get("source") or ""
        candidate_parent = Path(candidate_source).parent.as_posix()
        if candidate_parent != summary_parent:
            continue

        candidate_time = parse_iso(candidate.get("collectedAt"))
        time_delta = abs((summary_time - candidate_time).total_seconds())
        if time_delta > 120:
            continue

        # Prefer the closest file in time, then the largest series count.
        score = (time_delta, -len(candidate.get("series") or []))
        if best_score is None or score < best_score:
            best_score = score
            best_match = candidate

    return best_match


def merge_summary_with_ndjson(summary: dict[str, Any], ndjson_run: dict[str, Any]) -> dict[str, Any]:
    merged = dict(summary)
    merged["series"] = ndjson_run.get("series") or []
    merged["source"] = f'{summary.get("source", "")} + {ndjson_run.get("source", "")}'
    merged["mergedSources"] = [summary.get("source"), ndjson_run.get("source")]
    merged["stats"] = build_stats(merged.get("overview", {}), merged["series"])
    return merged


def parse_iso(value: Any) -> datetime:
    if not value:
        return datetime.fromtimestamp(0, tz=timezone.utc)
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return datetime.fromtimestamp(0, tz=timezone.utc)


def parse_file(path: Path, root: Path) -> dict[str, Any] | None:
    try:
        text = path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return None

    parsed = parse_text(path.name, text)
    if parsed is None:
        return None

    stat = path.stat()
    rel_path = path.relative_to(root).as_posix()
    parsed.update(
        {
            "id": rel_path,
            "name": parsed.get("name") or path.stem,
            "source": rel_path,
            "collectedAt": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
            "sizeBytes": stat.st_size,
        }
    )
    if "stats" not in parsed:
        parsed["stats"] = build_stats(parsed.get("overview", {}), parsed.get("series", []))
    return parsed


def parse_text(name: str, text: str) -> dict[str, Any] | None:
    trimmed = text.strip()
    if not trimmed:
        return None

    if looks_like_ndjson(trimmed):
        return parse_ndjson(name, trimmed)

    if trimmed.startswith("{"):
        try:
            obj = json.loads(trimmed)
        except json.JSONDecodeError:
            obj = None
        if isinstance(obj, dict) and "metrics" in obj:
            return parse_summary(name, obj)

    if looks_like_csv(trimmed):
        return parse_csv(name, trimmed)

    return None


def parse_summary(name: str, payload: dict[str, Any]) -> dict[str, Any]:
    metrics = payload.get("metrics", {})
    http_reqs = metric_values(metrics, "http_reqs")
    failed = metric_values(metrics, "http_req_failed")
    checks_metric = metric_values(metrics, "checks")
    duration_metric = (
        metric_values(metrics, "http_req_duration{expected_response:true}")
        or metric_values(metrics, "http_req_duration")
    )
    vus = metric_values(metrics, "vus")
    vus_max = metric_values(metrics, "vus_max")

    request_rate = to_float(http_reqs.get("rate"))
    overview = {
        "requestsTotal": to_float(http_reqs.get("count")),
        "requestRate": request_rate,
        "throughputPerMinute": request_rate * 60 if request_rate is not None else None,
        "successRate": 1 - to_float(failed.get("rate")) if to_float(failed.get("rate")) is not None else None,
        "checkRate": to_float(checks_metric.get("rate")),
        "avgMs": to_float(duration_metric.get("avg")),
        "p50Ms": to_float(duration_metric.get("med")),
        "p95Ms": to_float(duration_metric.get("p(95)")),
        "p99Ms": to_float(duration_metric.get("p(99)")),
        "maxMs": to_float(duration_metric.get("max")),
        "vus": to_float(vus.get("value")),
        "vusMax": to_float(vus_max.get("max") or vus_max.get("value")),
        "dataSentBytes": to_float(metric_values(metrics, "data_sent").get("count")),
        "dataReceivedBytes": to_float(metric_values(metrics, "data_received").get("count")),
    }

    checks = []
    for check in payload.get("root_group", {}).get("checks", []):
        passes = to_float(check.get("passes")) or 0
        fails = to_float(check.get("fails")) or 0
        total = passes + fails
        checks.append(
            {
                "name": check.get("name") or "Unnamed check",
                "passes": passes,
                "fails": fails,
                "rate": (passes / total) if total else None,
            }
        )

    thresholds: list[dict[str, Any]] = []
    for metric_name, definition in metrics.items():
        metric_thresholds = definition.get("thresholds", {})
        for label, result in metric_thresholds.items():
            thresholds.append(
                {
                    "metric": metric_name,
                    "label": f"{metric_name} {label}",
                    "ok": bool(result.get("ok")),
                }
            )

    return {
        "kind": "k6_summary",
        "durationMs": to_float(payload.get("state", {}).get("testRunDurationMs")),
        "overview": overview,
        "thresholds": thresholds,
        "checks": checks,
        "series": [],
        "stats": build_stats(overview, []),
    }


def parse_csv(name: str, text: str) -> dict[str, Any]:
    reader = csv.DictReader(text.splitlines())
    series: list[dict[str, Any]] = []
    total_requests = 0.0
    success_weight = 0.0

    for row in reader:
        requests = to_float(row.get("requests")) or 0.0
        success_rate = clamp_01(to_float(row.get("success_ratio")))
        point = {
            "minute": row.get("minute"),
            "label": row.get("minute"),
            "requests": requests,
            "tpm": to_float(row.get("tpm")) or requests,
            "successRate": success_rate,
            "p50Ms": to_float(row.get("p50_ms")),
            "p95Ms": to_float(row.get("p95_ms")),
            "p99Ms": to_float(row.get("p99_ms")),
            "apdex": clamp_01(to_float(row.get("apdex"))),
        }
        total_requests += requests
        if success_rate is not None:
            success_weight += success_rate * requests
        series.append(point)

    overview = {
        "requestsTotal": total_requests or None,
        "throughputPerMinute": (total_requests / len(series)) if series else None,
        "successRate": (success_weight / total_requests) if total_requests else None,
        "p50Ms": last_defined(series, "p50Ms"),
        "p95Ms": last_defined(series, "p95Ms"),
        "p99Ms": last_defined(series, "p99Ms"),
        "peakTpm": max((point.get("tpm") or 0) for point in series) if series else None,
        "apdex": mean([point.get("apdex") for point in series]),
    }

    return {
        "kind": "minute_csv",
        "durationMs": len(series) * 60 * 1000 if series else None,
        "overview": overview,
        "thresholds": [],
        "checks": [],
        "series": series,
        "stats": build_stats(overview, series),
    }


def parse_ndjson(name: str, text: str) -> dict[str, Any]:
    counts: defaultdict[str, int] = defaultdict(int)
    fail_counts: defaultdict[str, float] = defaultdict(float)
    latencies: defaultdict[str, list[float]] = defaultdict(list)
    all_latencies: list[float] = []

    for line in text.splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        try:
            obj = json.loads(stripped)
        except json.JSONDecodeError:
            continue
        if obj.get("type") != "Point":
            continue
        data = obj.get("data", {})
        timestamp = data.get("time")
        if not timestamp:
            continue
        minute = floor_minute(timestamp)
        value = to_float(data.get("value"))
        if obj.get("metric") == "http_reqs":
            counts[minute] += int(value or 0)
        elif obj.get("metric") == "http_req_failed":
            fail_counts[minute] += value or 0.0
        elif obj.get("metric") == "http_req_duration" and value is not None:
            latencies[minute].append(value)
            all_latencies.append(value)

    minutes = sorted(set(counts.keys()) | set(latencies.keys()))
    series: list[dict[str, Any]] = []
    total_requests = 0
    total_failures = 0.0
    apdex_values: list[float] = []

    for minute in minutes:
        lats = latencies.get(minute, [])
        requests = counts.get(minute, 0) or len(lats)
        failures = fail_counts.get(minute, 0.0)
        stats = percentile_stats(lats)
        apdex = compute_apdex(lats)
        if apdex is not None:
            apdex_values.append(apdex)
        total_requests += requests
        total_failures += failures
        series.append(
            {
                "minute": minute,
                "label": minute[11:],
                "requests": requests,
                "tpm": requests,
                "successRate": clamp_01(1 - (failures / requests)) if requests else None,
                "p50Ms": stats["p50"],
                "p95Ms": stats["p95"],
                "p99Ms": stats["p99"],
                "apdex": apdex,
            }
        )

    latency_stats = percentile_stats(all_latencies)
    overview = {
        "requestsTotal": total_requests or None,
        "throughputPerMinute": (total_requests / len(series)) if series else None,
        "successRate": clamp_01(1 - (total_failures / total_requests)) if total_requests else None,
        "avgMs": latency_stats["avg"],
        "p50Ms": latency_stats["p50"],
        "p95Ms": latency_stats["p95"],
        "p99Ms": latency_stats["p99"],
        "maxMs": latency_stats["max"],
        "peakTpm": max((point.get("tpm") or 0) for point in series) if series else None,
        "apdex": mean(apdex_values),
    }

    return {
        "kind": "k6_ndjson",
        "durationMs": len(series) * 60 * 1000 if series else None,
        "overview": overview,
        "thresholds": [],
        "checks": [],
        "series": series,
        "stats": build_stats(overview, series),
    }


def build_stats(overview: dict[str, Any], series: list[dict[str, Any]]) -> list[dict[str, str]]:
    stats = [
        ("Total requests", format_compact(overview.get("requestsTotal"))),
        ("Request rate", format_rate(overview.get("requestRate"))),
        ("Throughput", format_tpm(overview.get("throughputPerMinute"))),
        ("Peak throughput", format_tpm(overview.get("peakTpm"))),
        ("Success rate", format_percent(overview.get("successRate"))),
        ("Check rate", format_percent(overview.get("checkRate"))),
        ("P50 latency", format_ms(overview.get("p50Ms"))),
        ("P95 latency", format_ms(overview.get("p95Ms"))),
        ("P99 latency", format_ms(overview.get("p99Ms"))),
        ("Average latency", format_ms(overview.get("avgMs"))),
        ("Max latency", format_ms(overview.get("maxMs"))),
        ("Apdex", format_decimal(overview.get("apdex"))),
        ("Virtual users", format_compact(overview.get("vus"))),
        ("Max virtual users", format_compact(overview.get("vusMax"))),
        ("Data sent", format_bytes(overview.get("dataSentBytes"))),
        ("Data received", format_bytes(overview.get("dataReceivedBytes"))),
        ("Series points", str(len(series)) if series else "n/a"),
    ]
    return [{"label": label, "display": value} for label, value in stats]


def metric_values(metrics: dict[str, Any], name: str) -> dict[str, Any]:
    return metrics.get(name, {}).get("values", {})


def looks_like_csv(text: str) -> bool:
    header = text.splitlines()[0].lower()
    return "minute" in header and ("requests" in header or "tpm" in header)


def looks_like_ndjson(text: str) -> bool:
    for line in text.splitlines()[:10]:
        if '"type"' in line and ('"Point"' in line or '"Metric"' in line):
            return True
    return False


def floor_minute(timestamp: str) -> str:
    dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
    return dt.strftime("%Y-%m-%dT%H:%M")


def percentile_stats(values: list[float]) -> dict[str, float | None]:
    if not values:
        return {"p50": None, "p95": None, "p99": None, "avg": None, "max": None}
    ordered = sorted(values)
    return {
        "p50": percentile(ordered, 50),
        "p95": percentile(ordered, 95),
        "p99": percentile(ordered, 99),
        "avg": sum(ordered) / len(ordered),
        "max": ordered[-1],
    }


def percentile(values: list[float], pct: int) -> float | None:
    if not values:
        return None
    k = (len(values) - 1) * (pct / 100.0)
    lower = math.floor(k)
    upper = math.ceil(k)
    if lower == upper:
        return values[lower]
    return values[lower] + (values[upper] - values[lower]) * (k - lower)


def compute_apdex(values: list[float], threshold: float = 300.0) -> float | None:
    if not values:
        return None
    satisfied = sum(1 for value in values if value <= threshold)
    tolerated = sum(1 for value in values if threshold < value <= threshold * 4)
    return (satisfied + 0.5 * tolerated) / len(values)


def last_defined(series: list[dict[str, Any]], key: str) -> float | None:
    for point in reversed(series):
        value = point.get(key)
        if value is not None:
            return value
    return None


def mean(values: list[float | None]) -> float | None:
    actual = [value for value in values if value is not None]
    if not actual:
        return None
    return sum(actual) / len(actual)


def clamp_01(value: float | None) -> float | None:
    if value is None:
        return None
    return min(1.0, max(0.0, value))


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def format_compact(value: float | None) -> str:
    if value is None:
        return "n/a"
    if value >= 1_000_000:
        return f"{value / 1_000_000:.1f}M"
    if value >= 1_000:
        return f"{value / 1_000:.1f}k"
    if value.is_integer():
        return str(int(value))
    return f"{value:.1f}"


def format_rate(value: float | None) -> str:
    return f"{format_compact(value)}/sec" if value is not None else "n/a"


def format_tpm(value: float | None) -> str:
    return f"{format_compact(value)}/min" if value is not None else "n/a"


def format_percent(value: float | None) -> str:
    return f"{value * 100:.1f}%" if value is not None else "n/a"


def format_ms(value: float | None) -> str:
    if value is None:
        return "n/a"
    if value >= 1000:
        return f"{value / 1000:.2f}s"
    if value >= 100:
        return f"{value:.0f}ms"
    return f"{value:.1f}ms"


def format_decimal(value: float | None) -> str:
    return f"{value:.2f}" if value is not None else "n/a"


def format_bytes(value: float | None) -> str:
    if value is None:
        return "n/a"
    units = ["B", "KB", "MB", "GB", "TB"]
    index = 0
    current = value
    while current >= 1024 and index < len(units) - 1:
        current /= 1024
        index += 1
    return f"{current:.1f} {units[index]}"


def payload_etag(payload: Any) -> str:
    body = json.dumps(payload, sort_keys=True).encode("utf-8")
    return hashlib.sha1(body).hexdigest()
