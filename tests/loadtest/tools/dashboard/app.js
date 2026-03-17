const state = {
  runs: [],
  currentRun: null,
  selectedId: null,
  selectedSource: "api",
  compareMode: false,
  compareIds: [],
  compareCache: {},
};

const elements = {
  refreshButton: document.getElementById("refreshButton"),
  compareButton: document.getElementById("compareButton"),
  fileButton: document.getElementById("fileButton"),
  fileInput: document.getElementById("fileInput"),
  dropZone: document.getElementById("dropZone"),
  runCount: document.getElementById("runCount"),
  runList: document.getElementById("runList"),
  runTitle: document.getElementById("runTitle"),
  runKind: document.getElementById("runKind"),
  runMeta: document.getElementById("runMeta"),
  summaryGrid: document.getElementById("summaryGrid"),
  throughputChart: document.getElementById("throughputChart"),
  latencyChart: document.getElementById("latencyChart"),
  reliabilityChart: document.getElementById("reliabilityChart"),
  thresholdHead: document.getElementById("thresholdHead"),
  thresholdTable: document.getElementById("thresholdTable"),
  checksHead: document.getElementById("checksHead"),
  checksTable: document.getElementById("checksTable"),
  metricsHead: document.getElementById("metricsHead"),
  metricsTable: document.getElementById("metricsTable"),
  emptyStateTemplate: document.getElementById("emptyStateTemplate"),
};

async function init() {
  bindEvents();
  await refreshRuns();
}

function bindEvents() {
  elements.refreshButton.addEventListener("click", () => {
    refreshRuns().catch(handleError);
  });

  elements.compareButton.addEventListener("click", () => {
    toggleCompareMode().catch(handleError);
  });

  elements.fileButton.addEventListener("click", () => {
    elements.fileInput.click();
  });

  elements.fileInput.addEventListener("change", async (event) => {
    const [file] = event.target.files || [];
    if (file) {
      await loadUploadedFile(file);
    }
    event.target.value = "";
  });

  ["dragenter", "dragover"].forEach((eventName) => {
    elements.dropZone.addEventListener(eventName, (event) => {
      event.preventDefault();
      elements.dropZone.classList.add("drag-active");
    });
  });

  ["dragleave", "dragend", "drop"].forEach((eventName) => {
    elements.dropZone.addEventListener(eventName, (event) => {
      event.preventDefault();
      elements.dropZone.classList.remove("drag-active");
    });
  });

  elements.dropZone.addEventListener("drop", async (event) => {
    const [file] = event.dataTransfer?.files || [];
    if (file) {
      await loadUploadedFile(file);
    }
  });

  elements.dropZone.addEventListener("click", () => {
    elements.fileInput.click();
  });

  elements.dropZone.addEventListener("keydown", (event) => {
    if (event.key === "Enter" || event.key === " ") {
      event.preventDefault();
      elements.fileInput.click();
    }
  });
}

async function refreshRuns() {
  const response = await fetch("/api/runs", { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to load runs (${response.status})`);
  }

  const payload = await response.json();
  state.runs = Array.isArray(payload.runs) ? payload.runs : [];
  state.compareIds = state.compareIds.filter((id) => state.runs.some((run) => run.id === id));
  renderRunList();
  renderCompareButton();

  if (state.compareMode) {
    await renderCompareMode();
    return;
  }

  if (state.selectedSource === "upload" && state.currentRun) {
    renderDashboard(state.currentRun);
    return;
  }

  if (!state.runs.length) {
    state.currentRun = null;
    state.selectedId = null;
    renderDashboard(null);
    return;
  }

  const nextId = state.selectedId && state.runs.some((run) => run.id === state.selectedId)
    ? state.selectedId
    : state.runs[0].id;

  await selectRun(nextId);
}

async function selectRun(runId) {
  state.selectedId = runId;
  state.selectedSource = "api";
  renderRunList();

  const response = await fetch(`/api/runs/${encodeURIComponent(runId)}`, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to load run ${runId}`);
  }

  state.currentRun = await response.json();
  state.compareCache[runId] = state.currentRun;
  renderDashboard(state.currentRun);
}

async function handleRunSelection(runId) {
  if (!state.compareMode) {
    await selectRun(runId);
    return;
  }

  const existingIndex = state.compareIds.indexOf(runId);
  if (existingIndex > -1) {
    state.compareIds.splice(existingIndex, 1);
  } else if (state.compareIds.length < 2) {
    state.compareIds.push(runId);
  } else {
    state.compareIds = [state.compareIds[1], runId];
  }

  renderCompareButton();
  renderRunList();
  await renderCompareMode();
}

async function toggleCompareMode() {
  state.compareMode = !state.compareMode;

  if (state.compareMode) {
    state.compareIds = state.selectedSource === "api" && state.selectedId ? [state.selectedId] : [];
    renderCompareButton();
    renderRunList();
    await renderCompareMode();
    return;
  }

  state.compareIds = [];
  renderCompareButton();
  renderRunList();

  if (state.selectedSource === "upload" && state.currentRun) {
    renderDashboard(state.currentRun);
    return;
  }

  if (state.selectedId) {
    await selectRun(state.selectedId);
    return;
  }

  renderDashboard(null);
}

function renderCompareButton() {
  elements.compareButton.textContent = state.compareMode
    ? `Exit compare${state.compareIds.length ? ` (${state.compareIds.length}/2)` : ""}`
    : "Compare reports";
  elements.compareButton.classList.toggle("button-active", state.compareMode);
}

async function fetchRunDetail(runId) {
  const response = await fetch(`/api/runs/${encodeURIComponent(runId)}`, { cache: "no-store" });
  if (!response.ok) {
    throw new Error(`Failed to load run ${runId}`);
  }
  return response.json();
}

async function ensureRunDetail(runId) {
  if (state.selectedSource === "api" && state.currentRun?.id === runId) {
    return state.currentRun;
  }
  if (state.compareCache[runId]) {
    return state.compareCache[runId];
  }
  const run = await fetchRunDetail(runId);
  state.compareCache[runId] = run;
  return run;
}

async function renderCompareMode() {
  const compareRuns = await Promise.all(state.compareIds.map((runId) => ensureRunDetail(runId)));

  if (compareRuns.length < 2) {
    renderCompareSelectionState(compareRuns);
    return;
  }

  renderCompareDashboard(compareRuns[0], compareRuns[1]);
}

function renderRunList() {
  elements.runCount.textContent = String(state.runs.length);

  if (!state.runs.length) {
    elements.runList.innerHTML = `<div class="empty-state"><p class="empty-title">No runs found</p><p class="empty-text">Run k6 and keep its output in \`tests/loadtest\`, or upload a load test file above.</p></div>`;
    return;
  }

  elements.runList.innerHTML = state.runs
    .map((run) => {
      const active = state.compareMode
        ? state.compareIds.includes(run.id)
        : state.selectedSource === "api" && run.id === state.selectedId;
      const overview = run.overview || {};
      const compareIndex = state.compareIds.indexOf(run.id);
      return `
        <button class="run-item ${active ? "active" : ""}" data-run-id="${escapeHtml(run.id)}" type="button">
          <div class="run-item-header">
            <p class="run-item-title">${escapeHtml(run.name || "Unnamed run")}</p>
            <div class="run-item-header-actions">
              ${state.compareMode ? `<span class="compare-marker ${compareIndex > -1 ? "compare-marker-active" : ""}">${compareIndex === 0 ? "A" : compareIndex === 1 ? "B" : "+"}</span>` : ""}
              <span class="pill ${active ? "" : "pill-muted"}">${escapeHtml(humanKind(run.kind))}</span>
            </div>
          </div>
          <p class="run-item-source">${escapeHtml(run.source || "")}</p>
          <div class="run-item-metrics">
            <span>${escapeHtml(formatTpm(overview.throughputPerMinute))}</span>
            <span>${escapeHtml(formatMs(overview.p95Ms))}</span>
            <span>${escapeHtml(formatPercent(overview.successRate))}</span>
          </div>
        </button>
      `;
    })
    .join("");

  elements.runList.querySelectorAll("[data-run-id]").forEach((button) => {
    button.addEventListener("click", () => {
      handleRunSelection(button.dataset.runId).catch(handleError);
    });
  });
}

async function loadUploadedFile(file) {
  const text = await file.text();
  const run = parseUploadedFile(file.name, text);
  state.compareMode = false;
  state.compareIds = [];
  state.selectedId = run.id;
  state.selectedSource = "upload";
  state.currentRun = run;
  renderCompareButton();
  renderRunList();
  renderDashboard(run);
}

function parseUploadedFile(fileName, text) {
  const trimmed = text.trim();
  if (!trimmed) {
    throw new Error("The uploaded file is empty.");
  }

  if (looksLikeNdjson(trimmed)) {
    return normalizeNdjsonRun(fileName, trimmed);
  }

  if (trimmed.startsWith("{")) {
    let parsed;
    try {
      parsed = JSON.parse(trimmed);
    } catch (error) {
      throw new Error("That JSON file could not be parsed.");
    }
    if (parsed && parsed.metrics) {
      return normalizeSummaryRun(fileName, parsed);
    }
  }

  if (looksLikeCsv(trimmed)) {
    return normalizeCsvRun(fileName, trimmed);
  }

  throw new Error("Unsupported file. Use a k6 summary JSON, raw NDJSON export, or the minute-bucket CSV output.");
}

function normalizeSummaryRun(fileName, payload) {
  const metrics = payload.metrics || {};
  const httpReqs = metricValues(metrics, "http_reqs");
  const failed = metricValues(metrics, "http_req_failed");
  const checksMetric = metricValues(metrics, "checks");
  const durationMetric =
    metricValues(metrics, "http_req_duration{expected_response:true}") ||
    metricValues(metrics, "http_req_duration");

  const requestRate = toNumber(httpReqs.rate);
  const overview = {
    requestsTotal: toNumber(httpReqs.count),
    requestRate,
    throughputPerMinute: requestRate !== null ? requestRate * 60 : null,
    successRate: toNumber(failed.rate) !== null ? clamp01(1 - toNumber(failed.rate)) : null,
    checkRate: toNumber(checksMetric.rate),
    avgMs: toNumber(durationMetric.avg),
    p50Ms: toNumber(durationMetric.med),
    p95Ms: toNumber(durationMetric["p(95)"]),
    p99Ms: toNumber(durationMetric["p(99)"]),
    maxMs: toNumber(durationMetric.max),
    vus: toNumber(metricValues(metrics, "vus").value),
    vusMax: toNumber(metricValues(metrics, "vus_max").max ?? metricValues(metrics, "vus_max").value),
    dataSentBytes: toNumber(metricValues(metrics, "data_sent").count),
    dataReceivedBytes: toNumber(metricValues(metrics, "data_received").count),
  };

  const checks = (payload.root_group?.checks || []).map((check) => {
    const passes = toNumber(check.passes) || 0;
    const fails = toNumber(check.fails) || 0;
    const total = passes + fails;
    return {
      name: check.name || "Unnamed check",
      passes,
      fails,
      rate: total ? passes / total : null,
    };
  });

  const thresholds = [];
  Object.entries(metrics).forEach(([metricName, definition]) => {
    Object.entries(definition.thresholds || {}).forEach(([label, result]) => {
      thresholds.push({
        metric: metricName,
        label: `${metricName} ${label}`,
        ok: Boolean(result.ok),
      });
    });
  });

  return finalizeRun({
    id: `upload:${fileName}`,
    name: fileName.replace(/\.[^.]+$/, ""),
    source: fileName,
    kind: "k6_summary",
    collectedAt: new Date().toISOString(),
    durationMs: toNumber(payload.state?.testRunDurationMs),
    overview,
    thresholds,
    checks,
    series: [],
  });
}

function normalizeCsvRun(fileName, text) {
  const [headerLine, ...lines] = text.split(/\r?\n/).filter(Boolean);
  const headers = headerLine.split(",");
  const series = [];
  let totalRequests = 0;
  let successWeight = 0;

  lines.forEach((line) => {
    const cols = line.split(",");
    const row = Object.fromEntries(headers.map((header, index) => [header, cols[index] ?? ""]));
    const requests = toNumber(row.requests) || 0;
    const successRate = clamp01(toNumber(row.success_ratio));
    totalRequests += requests;
    if (successRate !== null) {
      successWeight += successRate * requests;
    }
    series.push({
      minute: row.minute,
      label: row.minute,
      requests,
      tpm: toNumber(row.tpm) ?? requests,
      successRate,
      p50Ms: toNumber(row.p50_ms),
      p95Ms: toNumber(row.p95_ms),
      p99Ms: toNumber(row.p99_ms),
      apdex: clamp01(toNumber(row.apdex)),
    });
  });

  return finalizeRun({
    id: `upload:${fileName}`,
    name: fileName.replace(/\.[^.]+$/, ""),
    source: fileName,
    kind: "minute_csv",
    collectedAt: new Date().toISOString(),
    durationMs: series.length ? series.length * 60 * 1000 : null,
    overview: {
      requestsTotal: totalRequests || null,
      throughputPerMinute: series.length ? totalRequests / series.length : null,
      successRate: totalRequests ? successWeight / totalRequests : null,
      p50Ms: lastDefined(series, "p50Ms"),
      p95Ms: lastDefined(series, "p95Ms"),
      p99Ms: lastDefined(series, "p99Ms"),
      peakTpm: series.length ? Math.max(...series.map((point) => point.tpm || 0)) : null,
      apdex: mean(series.map((point) => point.apdex)),
    },
    thresholds: [],
    checks: [],
    series,
  });
}

function normalizeNdjsonRun(fileName, text) {
  const counts = new Map();
  const failures = new Map();
  const latencies = new Map();
  const allLatencies = [];

  text.split(/\r?\n/).forEach((line) => {
    const trimmed = line.trim();
    if (!trimmed) {
      return;
    }
    let obj;
    try {
      obj = JSON.parse(trimmed);
    } catch {
      return;
    }
    if (obj.type !== "Point") {
      return;
    }
    const timestamp = obj.data?.time;
    if (!timestamp) {
      return;
    }
    const bucket = floorMinute(timestamp);
    const value = toNumber(obj.data?.value);
    if (obj.metric === "http_reqs") {
      counts.set(bucket, (counts.get(bucket) || 0) + (value || 0));
    } else if (obj.metric === "http_req_failed") {
      failures.set(bucket, (failures.get(bucket) || 0) + (value || 0));
    } else if (obj.metric === "http_req_duration" && value !== null) {
      const values = latencies.get(bucket) || [];
      values.push(value);
      latencies.set(bucket, values);
      allLatencies.push(value);
    }
  });

  const minutes = [...new Set([...counts.keys(), ...latencies.keys()])].sort();
  const series = [];
  let totalRequests = 0;
  let totalFailures = 0;
  const apdexValues = [];

  minutes.forEach((minute) => {
    const lats = latencies.get(minute) || [];
    const requests = counts.get(minute) || lats.length;
    const failed = failures.get(minute) || 0;
    totalRequests += requests;
    totalFailures += failed;
    const apdex = computeApdex(lats);
    if (apdex !== null) {
      apdexValues.push(apdex);
    }
    series.push({
      minute,
      label: minute.slice(11),
      requests,
      tpm: requests,
      successRate: requests ? clamp01(1 - failed / requests) : null,
      p50Ms: percentile(lats, 50),
      p95Ms: percentile(lats, 95),
      p99Ms: percentile(lats, 99),
      apdex,
    });
  });

  const latencyStats = percentileStats(allLatencies);
  return finalizeRun({
    id: `upload:${fileName}`,
    name: fileName.replace(/\.[^.]+$/, ""),
    source: fileName,
    kind: "k6_ndjson",
    collectedAt: new Date().toISOString(),
    durationMs: series.length ? series.length * 60 * 1000 : null,
    overview: {
      requestsTotal: totalRequests || null,
      throughputPerMinute: series.length ? totalRequests / series.length : null,
      successRate: totalRequests ? clamp01(1 - totalFailures / totalRequests) : null,
      avgMs: latencyStats.avg,
      p50Ms: latencyStats.p50,
      p95Ms: latencyStats.p95,
      p99Ms: latencyStats.p99,
      maxMs: latencyStats.max,
      peakTpm: series.length ? Math.max(...series.map((point) => point.tpm || 0)) : null,
      apdex: mean(apdexValues),
    },
    thresholds: [],
    checks: [],
    series,
  });
}

function finalizeRun(run) {
  return {
    ...run,
    stats: buildStats(run.overview || {}, run.series || []),
  };
}

function renderDashboard(run) {
  if (!run) {
    renderEmptyState();
    return;
  }

  const overview = run.overview || {};
  const points = run.series || [];

  elements.runTitle.textContent = run.name || "Benchmark run";
  elements.runKind.textContent = humanKind(run.kind);
  elements.runMeta.textContent = buildRunMeta(run, points);

  renderSummaryGrid(overview, points, run.durationMs);
  renderThroughputChart(points);
  renderLatencyChart(points, overview);
  renderReliabilityChart(points, overview);
  renderThresholds(run.thresholds || []);
  renderChecks(run.checks || []);
  renderMetricsTable(run.stats || []);
}

function renderCompareSelectionState(compareRuns) {
  const firstRun = compareRuns[0] || null;
  elements.runTitle.textContent = "Compare reports";
  elements.runKind.textContent = `${compareRuns.length}/2 selected`;
  elements.runMeta.textContent = firstRun
    ? `Selected ${firstRun.name}. Choose one more discovered run to compare against it.`
    : "Select two discovered runs from the list to compare them side by side.";

  elements.summaryGrid.innerHTML = [
    summaryCard("Report A", firstRun ? firstRun.name : "Select a report"),
    summaryCard("Report B", compareRuns[1] ? compareRuns[1].name : "Select another"),
    summaryCard("Mode", "Comparison"),
  ].join("");

  elements.throughputChart.innerHTML = emptyBlock("Select two reports to see comparison charts.");
  elements.latencyChart.innerHTML = emptyBlock("Select two reports to compare latency.");
  elements.reliabilityChart.innerHTML = emptyBlock("Select two reports to compare reliability.");
  elements.thresholdHead.innerHTML = `<tr><th>Signal</th><th>Status</th></tr>`;
  elements.thresholdTable.innerHTML = `<tr><td colspan="2" class="empty-table-cell">Threshold comparisons appear after two reports are selected.</td></tr>`;
  elements.checksHead.innerHTML = `<tr><th>Check</th><th>Passes</th><th>Fails</th><th>Rate</th></tr>`;
  elements.checksTable.innerHTML = `<tr><td colspan="4" class="empty-table-cell">Check comparisons appear after two reports are selected.</td></tr>`;
  elements.metricsHead.innerHTML = `<tr><th>Metric</th><th>Value</th></tr>`;
  elements.metricsTable.innerHTML = `<tr><td colspan="2" class="empty-table-cell">Metric comparisons appear after two reports are selected.</td></tr>`;
}

function renderCompareDashboard(leftRun, rightRun) {
  const leftName = leftRun.name || "Report A";
  const rightName = rightRun.name || "Report B";

  elements.runTitle.textContent = `${leftName} vs ${rightName}`;
  elements.runKind.textContent = "Comparison";
  elements.runMeta.textContent = `A: ${leftName} · B: ${rightName}`;

  renderCompareSummaryGrid(leftRun, rightRun);
  renderCompareThroughput(leftRun, rightRun);
  renderCompareLatency(leftRun, rightRun);
  renderCompareReliability(leftRun, rightRun);
  renderCompareThresholds(leftRun, rightRun);
  renderCompareChecks(leftRun, rightRun);
  renderCompareMetrics(leftRun, rightRun);
}

function renderEmptyState() {
  elements.runTitle.textContent = "No load test selected";
  elements.runKind.textContent = "Waiting";
  elements.runMeta.textContent = "Choose a discovered run or upload a load test file.";
  elements.summaryGrid.innerHTML = "";
  elements.summaryGrid.appendChild(elements.emptyStateTemplate.content.cloneNode(true));
  elements.throughputChart.innerHTML = emptyBlock("Run or upload a load test file to see throughput.");
  elements.latencyChart.innerHTML = emptyBlock("Latency appears here once a run is selected.");
  elements.reliabilityChart.innerHTML = emptyBlock("Reliability appears here once a run is selected.");
  elements.thresholdHead.innerHTML = `<tr><th>Signal</th><th>Status</th></tr>`;
  elements.thresholdTable.innerHTML = `<tr><td colspan="2" class="empty-table-cell">Threshold results will appear here.</td></tr>`;
  elements.checksHead.innerHTML = `<tr><th>Check</th><th>Passes</th><th>Fails</th><th>Rate</th></tr>`;
  elements.checksTable.innerHTML = `<tr><td colspan="4" class="empty-table-cell">No checks available.</td></tr>`;
  elements.metricsHead.innerHTML = `<tr><th>Metric</th><th>Value</th></tr>`;
  elements.metricsTable.innerHTML = `<tr><td colspan="2" class="empty-table-cell">No metrics available.</td></tr>`;
}

function renderSummaryGrid(overview, series, durationMs) {
  const cards = [
    summaryCard("Total requests", formatCompact(overview.requestsTotal)),
    summaryCard("Throughput", formatTpm(overview.throughputPerMinute)),
    summaryCard("Success rate", formatPercent(overview.successRate)),
    summaryCard("P95 latency", formatMs(overview.p95Ms)),
    summaryCard("Duration", formatDuration(durationMs)),
    summaryCard("Series points", series.length ? String(series.length) : "aggregate only"),
  ];
  elements.summaryGrid.innerHTML = cards.join("");
}

function renderCompareSummaryGrid(leftRun, rightRun) {
  const cards = [
    compareSummaryCard("Total requests", leftRun.name, leftRun.overview?.requestsTotal, rightRun.name, rightRun.overview?.requestsTotal, formatCompact, describeDelta(leftRun.overview?.requestsTotal, rightRun.overview?.requestsTotal, formatCompact)),
    compareSummaryCard("Throughput", leftRun.name, leftRun.overview?.throughputPerMinute, rightRun.name, rightRun.overview?.throughputPerMinute, formatTpm, describeDelta(leftRun.overview?.throughputPerMinute, rightRun.overview?.throughputPerMinute, formatTpm)),
    compareSummaryCard("Success rate", leftRun.name, leftRun.overview?.successRate, rightRun.name, rightRun.overview?.successRate, formatPercent, describeDelta(leftRun.overview?.successRate, rightRun.overview?.successRate, formatPercent)),
    compareSummaryCard("P95 latency", leftRun.name, leftRun.overview?.p95Ms, rightRun.name, rightRun.overview?.p95Ms, formatMs, describeDelta(leftRun.overview?.p95Ms, rightRun.overview?.p95Ms, formatMs)),
    compareSummaryCard("Duration", leftRun.name, leftRun.durationMs, rightRun.name, rightRun.durationMs, formatDuration, describeDelta(leftRun.durationMs, rightRun.durationMs, formatDuration)),
    compareSummaryCard("Apdex", leftRun.name, leftRun.overview?.apdex, rightRun.name, rightRun.overview?.apdex, formatDecimal, describeDelta(leftRun.overview?.apdex, rightRun.overview?.apdex, formatDecimal)),
  ];
  elements.summaryGrid.innerHTML = cards.join("");
}

function renderThroughputChart(series) {
  if (!series.length) {
    elements.throughputChart.innerHTML = emptyBlock("This run only has aggregate metrics, so there is no throughput timeline.");
    return;
  }
  elements.throughputChart.innerHTML = lineChart({
    series,
    valueKey: "tpm",
    label: "Throughput per minute",
    formatter: formatTpm,
  });
}

function renderCompareThroughput(leftRun, rightRun) {
  if (hasSeries(leftRun, "tpm") && hasSeries(rightRun, "tpm")) {
    elements.throughputChart.innerHTML = compareLineChart({
      runs: [leftRun, rightRun],
      valueKey: "tpm",
      label: "Throughput per minute",
      formatter: formatTpm,
    });
    return;
  }
  elements.throughputChart.innerHTML = compareBarChart("Throughput", leftRun, rightRun, "throughputPerMinute", formatTpm);
}

function renderLatencyChart(series, overview) {
  if (series.length) {
    elements.latencyChart.innerHTML = lineChart({
      series,
      valueKey: "p95Ms",
      label: "P95 latency",
      formatter: formatMs,
    });
    return;
  }

  const bars = [
    ["P50", overview.p50Ms],
    ["P95", overview.p95Ms],
    ["P99", overview.p99Ms],
    ["Max", overview.maxMs],
  ].filter(([, value]) => value !== null && value !== undefined);

  if (!bars.length) {
    elements.latencyChart.innerHTML = emptyBlock("No latency metrics were exported for this run.");
    return;
  }

  const maxValue = Math.max(...bars.map(([, value]) => value), 1);
  elements.latencyChart.innerHTML = `
    <div class="bar-chart">
      ${bars
        .map(
          ([label, value]) => `
            <div class="bar-chart-row">
              <span>${escapeHtml(label)}</span>
              <div class="bar-chart-track"><div class="bar-chart-fill" style="width:${(value / maxValue) * 100}%"></div></div>
              <strong>${escapeHtml(formatMs(value))}</strong>
            </div>
          `
        )
        .join("")}
    </div>
  `;
}

function renderCompareLatency(leftRun, rightRun) {
  if (hasSeries(leftRun, "p95Ms") && hasSeries(rightRun, "p95Ms")) {
    elements.latencyChart.innerHTML = compareLineChart({
      runs: [leftRun, rightRun],
      valueKey: "p95Ms",
      label: "P95 latency",
      formatter: formatMs,
    });
    return;
  }
  elements.latencyChart.innerHTML = compareBarChart("P95 latency", leftRun, rightRun, "p95Ms", formatMs);
}

function renderReliabilityChart(series, overview) {
  const key = series.some((point) => point.apdex !== null && point.apdex !== undefined) ? "apdex" : "successRate";
  const withData = series.filter((point) => point[key] !== null && point[key] !== undefined);
  if (withData.length >= 2) {
    elements.reliabilityChart.innerHTML = lineChart({
      series: withData,
      valueKey: key,
      label: key === "apdex" ? "Apdex" : "Success rate",
      formatter: key === "apdex" ? formatDecimal : formatPercent,
      min: 0,
      max: 1,
    });
    return;
  }

  const single = key === "apdex" ? overview.apdex : overview.successRate;
  const display = key === "apdex" ? formatDecimal(single) : formatPercent(single);
  elements.reliabilityChart.innerHTML = emptyBlock(display === "n/a" ? "No reliability metric was exported for this run." : `Overall ${key === "apdex" ? "Apdex" : "success rate"}: ${display}`);
}

function renderCompareReliability(leftRun, rightRun) {
  const compareKey = hasValue(leftRun.overview?.apdex) && hasValue(rightRun.overview?.apdex) ? "apdex" : "successRate";
  if (hasSeries(leftRun, compareKey) && hasSeries(rightRun, compareKey)) {
    elements.reliabilityChart.innerHTML = compareLineChart({
      runs: [leftRun, rightRun],
      valueKey: compareKey,
      label: compareKey === "apdex" ? "Apdex" : "Success rate",
      formatter: compareKey === "apdex" ? formatDecimal : formatPercent,
      min: 0,
      max: 1,
    });
    return;
  }
  elements.reliabilityChart.innerHTML = compareBarChart(
    compareKey === "apdex" ? "Apdex" : "Success rate",
    leftRun,
    rightRun,
    compareKey,
    compareKey === "apdex" ? formatDecimal : formatPercent
  );
}

function renderThresholds(thresholds) {
  elements.thresholdHead.innerHTML = `<tr><th>Signal</th><th>Status</th></tr>`;
  if (!thresholds.length) {
    elements.thresholdTable.innerHTML = `<tr><td colspan="2" class="empty-table-cell">No thresholds were exported for this run.</td></tr>`;
    return;
  }

  elements.thresholdTable.innerHTML = thresholds
    .map(
      (threshold) => `
        <tr>
          <td>${escapeHtml(humanizeThreshold(threshold))}</td>
          <td><span class="status-pill ${threshold.ok ? "status-pass" : "status-fail"}">${threshold.ok ? "Pass" : "Fail"}</span></td>
        </tr>
      `
    )
    .join("");
}

function renderCompareThresholds(leftRun, rightRun) {
  elements.thresholdHead.innerHTML = `<tr><th>Signal</th><th>A</th><th>B</th></tr>`;
  const merged = mergeByLabel(leftRun.thresholds || [], rightRun.thresholds || [], humanizeThreshold);
  if (!merged.length) {
    elements.thresholdTable.innerHTML = `<tr><td colspan="3" class="empty-table-cell">No thresholds were exported for either run.</td></tr>`;
    return;
  }

  elements.thresholdTable.innerHTML = merged
    .map((row) => {
      const tone = buildCompareTone(row.left?.ok, row.right?.ok, formatThresholdStatus);
      return `
        <tr class="${tone.changed ? "compare-row-changed" : ""}">
          <td>${escapeHtml(row.label)}</td>
          <td class="${tone.leftClass}">${row.left ? `<span class="status-pill ${row.left.ok ? "status-pass" : "status-fail"}">${row.left.ok ? "Pass" : "Fail"}</span>` : `<span class="empty-table-cell">n/a</span>`}</td>
          <td class="${tone.rightClass}">${row.right ? `<span class="status-pill ${row.right.ok ? "status-pass" : "status-fail"}">${row.right.ok ? "Pass" : "Fail"}</span>` : `<span class="empty-table-cell">n/a</span>`}</td>
        </tr>
      `;
    })
    .join("");
}

function renderChecks(checks) {
  elements.checksHead.innerHTML = `<tr><th>Check</th><th>Passes</th><th>Fails</th><th>Rate</th></tr>`;
  if (!checks.length) {
    elements.checksTable.innerHTML = `<tr><td colspan="4" class="empty-table-cell">No checks were exported for this run.</td></tr>`;
    return;
  }

  elements.checksTable.innerHTML = checks
    .map(
      (check) => `
        <tr>
          <td>${escapeHtml(check.name)}</td>
          <td>${escapeHtml(formatCompact(check.passes))}</td>
          <td>${escapeHtml(formatCompact(check.fails))}</td>
          <td>${escapeHtml(formatPercent(check.rate))}</td>
        </tr>
      `
    )
    .join("");
}

function renderCompareChecks(leftRun, rightRun) {
  elements.checksHead.innerHTML = `<tr><th>Check</th><th>A</th><th>B</th><th>Delta</th></tr>`;
  const merged = mergeByLabel(leftRun.checks || [], rightRun.checks || [], (check) => check.name);
  if (!merged.length) {
    elements.checksTable.innerHTML = `<tr><td colspan="4" class="empty-table-cell">No checks were exported for either run.</td></tr>`;
    return;
  }

  elements.checksTable.innerHTML = merged
    .map((row) => {
      const leftRate = row.left?.rate ?? null;
      const rightRate = row.right?.rate ?? null;
      const tone = buildCompareTone(leftRate, rightRate, formatPercent);
      return `
        <tr class="${tone.changed ? "compare-row-changed" : ""}">
          <td>${escapeHtml(row.label)}</td>
          <td class="${tone.leftClass}">${escapeHtml(formatPercent(leftRate))}</td>
          <td class="${tone.rightClass}">${escapeHtml(formatPercent(rightRate))}</td>
          <td><span class="${tone.deltaClass}">${escapeHtml(formatDelta(leftRate, rightRate, formatPercent))}</span></td>
        </tr>
      `;
    })
    .join("");
}

function renderMetricsTable(stats) {
  elements.metricsHead.innerHTML = `<tr><th>Metric</th><th>Value</th></tr>`;
  if (!stats.length) {
    elements.metricsTable.innerHTML = `<tr><td colspan="2" class="empty-table-cell">No metrics available.</td></tr>`;
    return;
  }

  elements.metricsTable.innerHTML = stats
    .map(
      (stat) => `
        <tr>
          <td>${escapeHtml(stat.label)}</td>
          <td>${escapeHtml(stat.display)}</td>
        </tr>
      `
    )
    .join("");
}

function renderCompareMetrics(leftRun, rightRun) {
  const rows = [
    compareMetricRow("Total requests", leftRun.overview?.requestsTotal, rightRun.overview?.requestsTotal, formatCompact),
    compareMetricRow("Request rate", leftRun.overview?.requestRate, rightRun.overview?.requestRate, formatRate),
    compareMetricRow("Throughput", leftRun.overview?.throughputPerMinute, rightRun.overview?.throughputPerMinute, formatTpm),
    compareMetricRow("Success rate", leftRun.overview?.successRate, rightRun.overview?.successRate, formatPercent),
    compareMetricRow("P95 latency", leftRun.overview?.p95Ms, rightRun.overview?.p95Ms, formatMs),
    compareMetricRow("P99 latency", leftRun.overview?.p99Ms, rightRun.overview?.p99Ms, formatMs),
    compareMetricRow("Average latency", leftRun.overview?.avgMs, rightRun.overview?.avgMs, formatMs),
    compareMetricRow("Duration", leftRun.durationMs, rightRun.durationMs, formatDuration),
    compareMetricRow("Apdex", leftRun.overview?.apdex, rightRun.overview?.apdex, formatDecimal),
    compareMetricRow("Data sent", leftRun.overview?.dataSentBytes, rightRun.overview?.dataSentBytes, formatBytes),
    compareMetricRow("Data received", leftRun.overview?.dataReceivedBytes, rightRun.overview?.dataReceivedBytes, formatBytes),
  ];

  elements.metricsHead.innerHTML = `<tr><th>Metric</th><th>A</th><th>B</th><th>Delta</th></tr>`;
  elements.metricsTable.innerHTML = rows
    .map(
      (row) => `
        <tr class="${row.changed ? "compare-row-changed" : ""}">
          <td>${escapeHtml(row.label)}</td>
          <td class="${row.leftClass}">${escapeHtml(row.left)}</td>
          <td class="${row.rightClass}">${escapeHtml(row.right)}</td>
          <td><span class="${row.deltaClass}">${escapeHtml(row.delta)}</span></td>
        </tr>
      `
    )
    .join("");
}

function lineChart({ series, valueKey, label, formatter, min = null, max = null }) {
  const values = series
    .map((point, index) => ({
      index,
      label: point.label || point.minute || String(index + 1),
      value: toNumber(point[valueKey]),
    }))
    .filter((point) => point.value !== null);

  if (values.length < 2) {
    return emptyBlock(`Not enough data to draw ${label.toLowerCase()}.`);
  }

  const width = 620;
  const height = 220;
  const padding = 24;
  const yMin = min !== null ? min : Math.min(...values.map((point) => point.value));
  const yMax = max !== null ? max : Math.max(...values.map((point) => point.value));
  const yRange = Math.max(yMax - yMin, 0.0001);
  const xStep = (width - padding * 2) / Math.max(values.length - 1, 1);

  const points = values.map((point, index) => {
    const x = padding + index * xStep;
    const y = height - padding - ((point.value - yMin) / yRange) * (height - padding * 2);
    return { ...point, x, y };
  });

  const polyline = points.map((point) => `${point.x},${point.y}`).join(" ");

  return `
    <svg class="svg-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(label)}">
      <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" stroke="currentColor" opacity="0.15"></line>
      <polyline points="${polyline}" fill="none" stroke="currentColor" stroke-width="3"></polyline>
      ${points
        .map(
          (point) => `<circle cx="${point.x}" cy="${point.y}" r="3.5" fill="currentColor"><title>${escapeHtml(point.label)}: ${escapeHtml(formatter(point.value))}</title></circle>`
        )
        .join("")}
    </svg>
    <div class="chart-caption">
      <span>${escapeHtml(label)}</span>
      <span>${escapeHtml(formatter(points[0].value))} to ${escapeHtml(formatter(points[points.length - 1].value))}</span>
    </div>
  `;
}

function compareLineChart({ runs, valueKey, label, formatter, min = null, max = null }) {
  const datasets = runs.map((run, index) => {
    const points = (run.series || [])
      .map((point, pointIndex) => ({
        label: point.label || point.minute || String(pointIndex + 1),
        value: toNumber(point[valueKey]),
      }))
      .filter((point) => point.value !== null);
    return {
      name: run.name,
      color: index === 0 ? "#0979c6" : "#8b9bb1",
      points,
    };
  });

  if (datasets.some((dataset) => dataset.points.length < 2)) {
    return emptyBlock(`Both reports need timeline data to compare ${label.toLowerCase()}.`);
  }

  const width = 620;
  const height = 220;
  const padding = 24;
  const allValues = datasets.flatMap((dataset) => dataset.points.map((point) => point.value));
  const yMin = min !== null ? min : Math.min(...allValues);
  const yMax = max !== null ? max : Math.max(...allValues);
  const yRange = Math.max(yMax - yMin, 0.0001);

  const polylines = datasets
    .map((dataset) => {
      const xStep = (width - padding * 2) / Math.max(dataset.points.length - 1, 1);
      const plotted = dataset.points.map((point, index) => {
        const x = padding + index * xStep;
        const y = height - padding - ((point.value - yMin) / yRange) * (height - padding * 2);
        return { ...point, x, y };
      });
      return {
        ...dataset,
        plotted,
        polyline: plotted.map((point) => `${point.x},${point.y}`).join(" "),
      };
    });

  return `
    <svg class="svg-chart" viewBox="0 0 ${width} ${height}" role="img" aria-label="${escapeHtml(label)} comparison">
      <line x1="${padding}" y1="${height - padding}" x2="${width - padding}" y2="${height - padding}" stroke="currentColor" opacity="0.15"></line>
      ${polylines
        .map(
          (dataset) => `
            <polyline points="${dataset.polyline}" fill="none" stroke="${dataset.color}" stroke-width="3"></polyline>
            ${dataset.plotted
              .map(
                (point) => `<circle cx="${point.x}" cy="${point.y}" r="3" fill="${dataset.color}"><title>${escapeHtml(dataset.name)} - ${escapeHtml(point.label)}: ${escapeHtml(formatter(point.value))}</title></circle>`
              )
              .join("")}
          `
        )
        .join("")}
    </svg>
    <div class="compare-legend">
      ${polylines
        .map(
          (dataset) => `
            <span class="compare-legend-item">
              <span class="compare-legend-swatch" style="background:${dataset.color}"></span>
              ${escapeHtml(dataset.name)}
            </span>
          `
        )
        .join("")}
    </div>
  `;
}

function compareBarChart(label, leftRun, rightRun, key, formatter) {
  const tone = buildCompareTone(leftRun.overview?.[key], rightRun.overview?.[key], formatter);
  const values = [
    { label: "A", name: leftRun.name, value: leftRun.overview?.[key] ?? null, color: "#0979c6", toneClass: tone.leftClass },
    { label: "B", name: rightRun.name, value: rightRun.overview?.[key] ?? null, color: "#8b9bb1", toneClass: tone.rightClass },
  ];
  const actual = values.filter((item) => item.value !== null && item.value !== undefined);
  if (!actual.length) {
    return emptyBlock(`Neither report exported ${label.toLowerCase()}.`);
  }

  const maxValue = Math.max(...actual.map((item) => item.value), 1);
  return `
    <div class="bar-chart">
      ${values
        .map(
          (item) => `
            <div class="bar-chart-row ${item.toneClass}">
              <div class="compare-bar-label">
                <strong>${escapeHtml(item.label)}</strong>
                <span>${escapeHtml(item.name)}</span>
              </div>
              <div class="bar-chart-track"><div class="bar-chart-fill" style="width:${item.value ? (item.value / maxValue) * 100 : 0}%; background:${item.color}"></div></div>
              <strong>${escapeHtml(formatter(item.value))}</strong>
            </div>
          `
        )
        .join("")}
    </div>
    <div class="compare-legend">
      ${values
        .map(
          (item) => `
            <span class="compare-legend-item">
              <span class="compare-legend-swatch" style="background:${item.color}"></span>
              ${escapeHtml(item.label)}: ${escapeHtml(item.name)}
            </span>
          `
        )
        .join("")}
    </div>
  `;
}

function buildRunMeta(run, points) {
  const parts = [];
  if (run.source) {
    parts.push(run.source);
  }
  if (run.collectedAt) {
    parts.push(formatDate(run.collectedAt));
  }
  parts.push(points.length ? `${points.length} data points` : "aggregate summary");
  return parts.join(" · ");
}

function hasSeries(run, key) {
  return (run.series || []).filter((point) => point[key] !== null && point[key] !== undefined).length >= 2;
}

function hasValue(value) {
  return value !== null && value !== undefined;
}

function valuesDiffer(leftValue, rightValue, formatter = (value) => String(value)) {
  const leftMissing = !hasValue(leftValue);
  const rightMissing = !hasValue(rightValue);
  if (leftMissing || rightMissing) {
    return leftMissing !== rightMissing;
  }
  return formatter(leftValue) !== formatter(rightValue);
}

function buildCompareTone(leftValue, rightValue, formatter = (value) => String(value)) {
  const changed = valuesDiffer(leftValue, rightValue, formatter);
  let deltaClass = "delta-pill delta-pill-neutral";

  if (changed) {
    if (!hasValue(leftValue) && hasValue(rightValue)) {
      deltaClass = "delta-pill delta-pill-added";
    } else if (hasValue(leftValue) && !hasValue(rightValue)) {
      deltaClass = "delta-pill delta-pill-removed";
    } else if (Number.isFinite(leftValue) && Number.isFinite(rightValue)) {
      deltaClass = rightValue >= leftValue ? "delta-pill delta-pill-added" : "delta-pill delta-pill-removed";
    } else {
      deltaClass = "delta-pill delta-pill-changed";
    }
  }

  return {
    changed,
    leftClass: changed && hasValue(leftValue) ? "diff-removed" : "",
    rightClass: changed && hasValue(rightValue) ? "diff-added" : "",
    deltaClass,
  };
}

function mergeByLabel(leftItems, rightItems, getLabel) {
  const map = new Map();
  leftItems.forEach((item) => {
    const label = getLabel(item);
    map.set(label, { label, left: item, right: null });
  });
  rightItems.forEach((item) => {
    const label = getLabel(item);
    if (map.has(label)) {
      map.get(label).right = item;
    } else {
      map.set(label, { label, left: null, right: item });
    }
  });
  return [...map.values()];
}

function compareMetricRow(label, leftValue, rightValue, formatter) {
  const tone = buildCompareTone(leftValue, rightValue, formatter);
  return {
    label,
    left: formatter(leftValue),
    right: formatter(rightValue),
    delta: formatDelta(leftValue, rightValue, formatter),
    changed: tone.changed,
    leftClass: tone.leftClass,
    rightClass: tone.rightClass,
    deltaClass: tone.deltaClass,
  };
}

function compareSummaryCard(label, leftName, leftRaw, rightName, rightRaw, formatter, delta) {
  const tone = buildCompareTone(leftRaw, rightRaw, formatter);
  const leftValue = formatter(leftRaw);
  const rightValue = formatter(rightRaw);
  return `
    <article class="panel stat-card compare-card ${tone.changed ? "compare-card-changed" : ""}">
      <p class="section-kicker">${escapeHtml(label)}</p>
      <div class="compare-values compare-values-stacked">
        <div class="compare-value-row ${tone.leftClass}">
          <div class="compare-value-meta">
            <span class="compare-run-label">A</span>
            <span class="compare-run-name">${escapeHtml(leftName)}</span>
          </div>
          <strong class="compare-value-strong">${escapeHtml(leftValue)}</strong>
        </div>
        <div class="compare-value-row ${tone.rightClass}">
          <div class="compare-value-meta">
            <span class="compare-run-label">B</span>
            <span class="compare-run-name">${escapeHtml(rightName)}</span>
          </div>
          <strong class="compare-value-strong">${escapeHtml(rightValue)}</strong>
        </div>
      </div>
      <p class="compare-delta"><span class="${tone.deltaClass}">${escapeHtml(delta)}</span></p>
    </article>
  `;
}

function buildStats(overview, series) {
  return [
    ["Total requests", formatCompact(overview.requestsTotal)],
    ["Request rate", formatRate(overview.requestRate)],
    ["Throughput", formatTpm(overview.throughputPerMinute)],
    ["Peak throughput", formatTpm(overview.peakTpm)],
    ["Success rate", formatPercent(overview.successRate)],
    ["Check rate", formatPercent(overview.checkRate)],
    ["P50 latency", formatMs(overview.p50Ms)],
    ["P95 latency", formatMs(overview.p95Ms)],
    ["P99 latency", formatMs(overview.p99Ms)],
    ["Average latency", formatMs(overview.avgMs)],
    ["Max latency", formatMs(overview.maxMs)],
    ["Apdex", formatDecimal(overview.apdex)],
    ["Virtual users", formatCompact(overview.vus)],
    ["Max virtual users", formatCompact(overview.vusMax)],
    ["Data sent", formatBytes(overview.dataSentBytes)],
    ["Data received", formatBytes(overview.dataReceivedBytes)],
    ["Series points", series.length ? String(series.length) : "n/a"],
  ]
    .filter(([, display]) => display !== "n/a")
    .map(([label, display]) => ({ label, display }));
}

function metricValues(metrics, name) {
  return metrics[name]?.values || {};
}

function humanizeThreshold(threshold) {
  const metricName = threshold.metric || "";
  const rawLabel = threshold.label || "";
  const rule = rawLabel.startsWith(`${metricName} `) ? rawLabel.slice(metricName.length + 1) : rawLabel;
  const { baseMetric, tags } = parseMetricName(metricName);
  const metricLabel = humanizeMetricName(baseMetric);
  const ruleLabel = humanizeThresholdRule(rule, baseMetric);
  const tagLabel = humanizeMetricTags(tags);
  const cleanedRule =
    metricLabel.toLowerCase().endsWith("rate") && ruleLabel.startsWith("Rate ")
      ? ruleLabel.slice(5)
      : ruleLabel;
  return [metricLabel, cleanedRule, tagLabel ? `(${tagLabel})` : ""].filter(Boolean).join(" ");
}

function parseMetricName(metricName) {
  const match = metricName.match(/^([^{}]+)(?:\{([^}]+)\})?$/);
  if (!match) {
    return { baseMetric: metricName, tags: {} };
  }

  const [, baseMetric, rawTags] = match;
  const tags = {};
  if (rawTags) {
    rawTags.split(",").forEach((entry) => {
      const [key, value] = entry.split(":");
      if (key && value) {
        tags[key.trim()] = value.trim();
      }
    });
  }
  return { baseMetric, tags };
}

function humanizeMetricName(metricName) {
  const known = {
    http_req_failed: "Failed request rate",
    http_req_duration: "Request duration",
    checks: "Checks",
  };
  if (known[metricName]) {
    return known[metricName];
  }
  return metricName.replaceAll("_", " ");
}

function humanizeThresholdRule(rule, metricName) {
  if (!rule) {
    return "";
  }

  let match = rule.match(/^p\((\d+)\)\s*<\s*([\d.]+)$/);
  if (match) {
    const [, percentileValue, thresholdValue] = match;
    return `P${percentileValue} under ${formatThresholdValue(metricName, Number(thresholdValue))}`;
  }

  match = rule.match(/^(avg|max|min|med|rate)\s*<\s*([\d.]+)$/);
  if (match) {
    const [, stat, thresholdValue] = match;
    const readableStat = {
      avg: "Average",
      max: "Max",
      min: "Min",
      med: "Median",
      rate: "Rate",
    }[stat] || stat;
    return `${readableStat} under ${formatThresholdValue(metricName, Number(thresholdValue))}`;
  }

  return rule.replaceAll("<", " under ").replaceAll(">", " over ");
}

function formatThresholdValue(metricName, value) {
  if (!Number.isFinite(value)) {
    return String(value);
  }
  if (metricName.includes("duration")) {
    return formatMs(value);
  }
  if (metricName.includes("failed") || metricName === "checks") {
    return formatPercent(value);
  }
  return formatCompact(value);
}

function humanizeMetricTags(tags) {
  const parts = [];
  if (tags.scenario) {
    parts.push(tags.scenario);
  }
  if (tags.expected_response === "true") {
    parts.push("successful responses");
  }
  if (tags.expected_response === "false") {
    parts.push("failed responses");
  }
  return parts.join(", ");
}

function percentileStats(values) {
  if (!values.length) {
    return { p50: null, p95: null, p99: null, avg: null, max: null };
  }
  const ordered = [...values].sort((a, b) => a - b);
  return {
    p50: percentile(ordered, 50),
    p95: percentile(ordered, 95),
    p99: percentile(ordered, 99),
    avg: ordered.reduce((sum, value) => sum + value, 0) / ordered.length,
    max: ordered[ordered.length - 1],
  };
}

function percentile(values, pct) {
  if (!values.length) {
    return null;
  }
  const k = (values.length - 1) * (pct / 100);
  const lower = Math.floor(k);
  const upper = Math.ceil(k);
  if (lower === upper) {
    return values[lower];
  }
  return values[lower] + (values[upper] - values[lower]) * (k - lower);
}

function computeApdex(values, threshold = 300) {
  if (!values.length) {
    return null;
  }
  const satisfied = values.filter((value) => value <= threshold).length;
  const tolerated = values.filter((value) => value > threshold && value <= threshold * 4).length;
  return (satisfied + tolerated * 0.5) / values.length;
}

function floorMinute(timestamp) {
  const date = new Date(timestamp);
  if (Number.isNaN(date.getTime())) {
    return timestamp;
  }
  return `${date.getUTCFullYear()}-${String(date.getUTCMonth() + 1).padStart(2, "0")}-${String(date.getUTCDate()).padStart(2, "0")}T${String(date.getUTCHours()).padStart(2, "0")}:${String(date.getUTCMinutes()).padStart(2, "0")}`;
}

function lastDefined(series, key) {
  for (let index = series.length - 1; index >= 0; index -= 1) {
    const value = series[index][key];
    if (value !== null && value !== undefined) {
      return value;
    }
  }
  return null;
}

function mean(values) {
  const actual = values.filter((value) => value !== null && value !== undefined);
  if (!actual.length) {
    return null;
  }
  return actual.reduce((sum, value) => sum + value, 0) / actual.length;
}

function looksLikeCsv(text) {
  const header = text.split(/\r?\n/, 1)[0].toLowerCase();
  return header.includes("minute") && (header.includes("requests") || header.includes("tpm"));
}

function looksLikeNdjson(text) {
  const firstLine = text.split(/\r?\n/, 1)[0];
  return firstLine.includes('"type"') && firstLine.includes('"Point"');
}

function clamp01(value) {
  return value === null ? null : Math.min(1, Math.max(0, value));
}

function toNumber(value) {
  const num = Number(value);
  return Number.isFinite(num) ? num : null;
}

function humanKind(kind) {
  return {
    k6_summary: "k6 summary",
    k6_ndjson: "raw k6 stream",
    minute_csv: "minute bucket csv",
  }[kind] || kind || "load test";
}

function summaryCard(label, value) {
  return `
    <article class="panel stat-card">
      <p class="section-kicker">${escapeHtml(label)}</p>
      <h2>${escapeHtml(value)}</h2>
    </article>
  `;
}

function emptyBlock(text) {
  return `<div class="empty-state"><p class="empty-text">${escapeHtml(text)}</p></div>`;
}

function formatCompact(value) {
  if (value === null || value === undefined) {
    return "n/a";
  }
  return new Intl.NumberFormat(undefined, {
    notation: value >= 10000 ? "compact" : "standard",
    maximumFractionDigits: value >= 100 ? 0 : 2,
  }).format(value);
}

function formatRate(value) {
  return value === null || value === undefined ? "n/a" : `${formatCompact(value)}/sec`;
}

function formatTpm(value) {
  return value === null || value === undefined ? "n/a" : `${formatCompact(value)}/min`;
}

function formatPercent(value) {
  return value === null || value === undefined ? "n/a" : `${(value * 100).toFixed(1)}%`;
}

function formatMs(value) {
  if (value === null || value === undefined) {
    return "n/a";
  }
  if (value >= 1000) {
    return `${(value / 1000).toFixed(2)}s`;
  }
  return value >= 100 ? `${value.toFixed(0)}ms` : `${value.toFixed(1)}ms`;
}

function formatDuration(value) {
  if (value === null || value === undefined) {
    return "n/a";
  }
  const seconds = Math.round(value / 1000);
  if (seconds >= 3600) {
    return `${(seconds / 3600).toFixed(1)}h`;
  }
  if (seconds >= 60) {
    return `${(seconds / 60).toFixed(1)}m`;
  }
  return `${seconds}s`;
}

function formatDecimal(value) {
  return value === null || value === undefined ? "n/a" : value.toFixed(2);
}

function formatBytes(value) {
  if (value === null || value === undefined) {
    return "n/a";
  }
  const units = ["B", "KB", "MB", "GB", "TB"];
  let current = value;
  let index = 0;
  while (current >= 1024 && index < units.length - 1) {
    current /= 1024;
    index += 1;
  }
  return `${current.toFixed(1)} ${units[index]}`;
}

function formatThresholdStatus(value) {
  return value ? "Pass" : "Fail";
}

function formatDelta(leftValue, rightValue, formatter) {
  if (leftValue === null || leftValue === undefined || rightValue === null || rightValue === undefined) {
    return "n/a";
  }
  const diff = rightValue - leftValue;
  if (!Number.isFinite(diff)) {
    return "n/a";
  }
  const prefix = diff > 0 ? "+" : "";
  return `${prefix}${formatter(diff)}`;
}

function describeDelta(leftValue, rightValue, formatter) {
  if (leftValue === null || leftValue === undefined || rightValue === null || rightValue === undefined) {
    return "Delta unavailable";
  }
  const diff = rightValue - leftValue;
  if (!Number.isFinite(diff)) {
    return "Delta unavailable";
  }
  if (diff === 0) {
    return "No change";
  }
  const direction = diff > 0 ? "B higher by" : "B lower by";
  return `${direction} ${formatter(Math.abs(diff))}`;
}

function formatDate(value) {
  const date = new Date(value);
  if (Number.isNaN(date.getTime())) {
    return value;
  }
  return new Intl.DateTimeFormat(undefined, {
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
  }).format(date);
}

function handleError(error) {
  console.error(error);
  elements.runMeta.textContent = error.message || "Something went wrong loading the dashboard.";
}

function escapeHtml(value) {
  return String(value)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");
}

init().catch(handleError);
