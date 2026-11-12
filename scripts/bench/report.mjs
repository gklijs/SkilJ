// Turns the records two commits' benchmark runs wrote into a comparison
// (docs/performance.md, "Comparing two commits"). Pure: compare.mjs runs
// the benchmarks and hands their records here; compare.test.mjs feeds it
// synthetic ones.
//
// A record is one JSON line of skilj_test_support::bench: `scenario`
// lines, then one `complete` line. A *run* is one benchmark binary's
// execution in one trial; a trial runs every benchmark once per side.

/** The record layout this report reads (skilj_test_support::bench::FORMAT). */
export const FORMAT = 1;

/** Trials a side needs before the report calls a difference anything but noise. */
export const MIN_TRIALS_FOR_VERDICT = 3;

/** Parses one run's JSON lines. Blank lines are ignored. */
export function parseRecords(text) {
  return text.split('\n').filter(line => line.trim() !== '').map((line, i) => {
    try {
      return JSON.parse(line);
    } catch (e) {
      throw new Error(`record line ${i + 1} is not JSON: ${e.message}`);
    }
  });
}

/**
 * Checks one run's records against what was asked for. Returns the
 * problems found, an empty list when the run is usable: a run that skipped,
 * crashed before finishing, lost a scenario or completed fewer operations
 * than it meant to is not one a comparison may use.
 */
export function validateRun(records, { benchmark, scenarios, rounds }) {
  const problems = [];
  if (records.length === 0) return [`${benchmark}: no records (did the benchmark skip or crash?)`];
  for (const record of records) {
    if (record.format !== FORMAT) {
      problems.push(`${benchmark}: record format ${record.format}, this report reads ${FORMAT}`);
      return problems;
    }
    if (record.benchmark !== benchmark) {
      problems.push(`${benchmark}: a record of ${record.benchmark}`);
    }
  }
  const versions = new Set(records.map(r => r.benchmark_version));
  if (versions.size !== 1) problems.push(`${benchmark}: mixed benchmark versions ${[...versions]}`);
  const complete = records.filter(r => r.kind === 'complete');
  if (complete.length !== 1) {
    problems.push(`${benchmark}: ${complete.length} completion records, expected 1 (the run did not finish)`);
  }
  const measured = records.filter(r => r.kind === 'scenario');
  if (complete.length === 1 && complete[0].scenarios !== measured.length) {
    problems.push(`${benchmark}: completion counts ${complete[0].scenarios} scenarios, ${measured.length} recorded`);
  }
  for (const name of scenarios) {
    const count = measured.filter(r => r.scenario === name).length;
    if (count !== rounds) problems.push(`${benchmark}: scenario ${name} recorded ${count} times, expected ${rounds}`);
  }
  for (const record of measured) {
    const where = `${benchmark} ${record.scenario} round ${record.round}`;
    if (!scenarios.includes(record.scenario)) problems.push(`${where}: not a requested scenario`);
    if (record.operations !== record.expected_operations) {
      problems.push(`${where}: ${record.operations} of ${record.expected_operations} operations`);
    }
    if (!(record.elapsed_us > 0)) problems.push(`${where}: no elapsed time`);
    if (!Array.isArray(record.samples_us) || record.samples_us.length === 0) {
      problems.push(`${where}: no samples`);
    }
  }
  return problems;
}

export function median(values) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
}

/** The value below which `p` of the samples fall (nearest rank, as the benchmarks print). */
export function percentile(values, p) {
  if (values.length === 0) return null;
  const sorted = [...values].sort((a, b) => a - b);
  return sorted[Math.floor((sorted.length - 1) * p)];
}

/** Coefficient of variation: standard deviation over mean, 0 for a single value. */
export function coefficientOfVariation(values) {
  if (values.length < 2) return 0;
  const mean = values.reduce((a, b) => a + b, 0) / values.length;
  const variance = values.reduce((a, b) => a + (b - mean) ** 2, 0) / (values.length - 1);
  return mean === 0 ? 0 : Math.sqrt(variance) / mean;
}

/**
 * One side's numbers for one scenario across its trials. A trial's
 * throughput is the median of its rounds; its latencies pool the samples
 * of all its rounds.
 */
export function summarizeSide(runs, scenario) {
  const trials = runs.map(run => {
    const rounds = run.records.filter(r => r.kind === 'scenario' && r.scenario === scenario);
    const throughputs = rounds.map(r => r.operations / (r.elapsed_us / 1e6));
    const samples = rounds.flatMap(r => r.samples_us);
    return {
      trial: run.trial,
      throughput: median(throughputs),
      rounds: throughputs,
      p50_us: percentile(samples, 0.5),
      p99_us: percentile(samples, 0.99),
      samples: samples.length,
    };
  });
  const throughputs = trials.map(t => t.throughput);
  const unit = runs[0]?.records.find(r => r.kind === 'scenario' && r.scenario === scenario)?.unit;
  return {
    unit,
    trials,
    throughput: {
      median: median(throughputs),
      min: Math.min(...throughputs),
      max: Math.max(...throughputs),
      cv: coefficientOfVariation(throughputs),
    },
    p50_us: median(trials.map(t => t.p50_us)),
    p99_us: median(trials.map(t => t.p99_us)),
  };
}

/**
 * What a difference amounts to. A single trial, or ranges that overlap,
 * say nothing about a regression; only when every candidate trial is
 * below (or above) every baseline trial, over enough trials, does the
 * report call it slower (or faster). Informational either way.
 */
export function verdict(baseline, candidate) {
  if (Math.min(baseline.trials.length, candidate.trials.length) < MIN_TRIALS_FOR_VERDICT) {
    return 'insufficient trials';
  }
  if (candidate.throughput.max < baseline.throughput.min) return 'slower';
  if (candidate.throughput.min > baseline.throughput.max) return 'faster';
  return 'within noise';
}

function versionOf(runs) {
  return runs[0]?.records.find(r => r.benchmark_version !== undefined)?.benchmark_version;
}

function peakMemory(runs) {
  const values = runs
    .map(run => run.records.find(r => r.kind === 'complete')?.peak_rss_kb)
    .filter(v => typeof v === 'number');
  return values.length ? median(values) : null;
}

/**
 * The comparison of every benchmark. `runs` is
 * `{ baseline: { <benchmark>: [{ trial, records }] }, candidate: {...} }`;
 * `benchmarks` maps each benchmark's name to its requested scenarios.
 */
export function compare(runs, benchmarks, metadata = {}) {
  const results = Object.entries(benchmarks).map(([benchmark, { scenarios }]) => {
    const baselineRuns = runs.baseline[benchmark] ?? [];
    const candidateRuns = runs.candidate[benchmark] ?? [];
    const versions = { baseline: versionOf(baselineRuns), candidate: versionOf(candidateRuns) };
    const comparable = versions.baseline !== undefined && versions.baseline === versions.candidate;
    const environment = {
      baseline: baselineRuns[0]?.records.find(r => r.kind === 'complete')?.environment ?? null,
      candidate: candidateRuns[0]?.records.find(r => r.kind === 'complete')?.environment ?? null,
    };
    return {
      benchmark,
      versions,
      comparable,
      environment,
      peak_rss_kb: { baseline: peakMemory(baselineRuns), candidate: peakMemory(candidateRuns) },
      scenarios: scenarios.map(scenario => {
        const baseline = summarizeSide(baselineRuns, scenario);
        const candidate = summarizeSide(candidateRuns, scenario);
        const delta = (candidate.throughput.median - baseline.throughput.median) / baseline.throughput.median;
        return {
          scenario,
          unit: baseline.unit ?? candidate.unit,
          baseline,
          candidate,
          delta,
          verdict: comparable ? verdict(baseline, candidate) : 'non-comparable',
        };
      }),
    };
  });
  return { format: FORMAT, ...metadata, benchmarks: results };
}

const fmt = n => n === null || n === undefined || Number.isNaN(n) ? 'n/a'
  : n >= 100 ? n.toFixed(0) : n.toFixed(1);
const ms = us => us === null || us === undefined ? 'n/a' : (us / 1000).toFixed(2);
const pct = n => `${n >= 0 ? '+' : ''}${(n * 100).toFixed(1)}%`;
const mib = kb => kb === null || kb === undefined ? 'unavailable' : `${(kb / 1024).toFixed(0)} MiB`;

/** The comparison as Markdown, for the job summary and summary.md. */
export function toMarkdown(report) {
  const lines = [];
  lines.push('## Benchmark comparison', '');
  lines.push(`Baseline \`${report.baseline?.commit ?? '?'}\` → candidate \`${report.candidate?.commit ?? '?'}\`, ` +
    `${report.trials} trial(s) × ${report.rounds} round(s), alternating order.`);
  lines.push('', '_Informational: a delta never fails this job. A verdict needs at least ' +
    `${MIN_TRIALS_FOR_VERDICT} trials and ranges that do not overlap; anything else is reported as noise._`, '');
  if (report.baseline?.env && Object.keys(report.baseline.env).length) {
    lines.push(`Baseline environment overrides: \`${JSON.stringify(report.baseline.env)}\``);
  }
  if (report.candidate?.env && Object.keys(report.candidate.env).length) {
    lines.push(`Candidate environment overrides: \`${JSON.stringify(report.candidate.env)}\``);
  }
  for (const result of report.benchmarks) {
    lines.push('', `### ${result.benchmark} (v${result.versions.baseline ?? '?'}` +
      (result.comparable ? ')' : ` vs v${result.versions.candidate ?? '?'}: non-comparable)`));
    lines.push('', '| scenario | baseline /s (min–max, CV) | candidate /s (min–max, CV) | Δ median | p50 ms | p99 ms | verdict |');
    lines.push('|---|---|---|---|---|---|---|');
    for (const s of result.scenarios) {
      const side = x => `${fmt(x.throughput.median)} (${fmt(x.throughput.min)}–${fmt(x.throughput.max)}, ${(x.throughput.cv * 100).toFixed(1)}%)`;
      lines.push(`| ${s.scenario} (${s.unit ?? '?'}) | ${side(s.baseline)} | ${side(s.candidate)} | ${pct(s.delta)} | ` +
        `${ms(s.baseline.p50_us)} → ${ms(s.candidate.p50_us)} | ${ms(s.baseline.p99_us)} → ${ms(s.candidate.p99_us)} | ` +
        `${s.verdict === 'slower' || s.verdict === 'faster' ? `**${s.verdict}**` : s.verdict} |`);
    }
    lines.push('', `Benchmark process peak memory: ${mib(result.peak_rss_kb.baseline)} → ${mib(result.peak_rss_kb.candidate)}. ` +
      'Database memory: not measured.');
  }
  if (report.environment) {
    const e = report.environment;
    lines.push('', '### Environment', '');
    lines.push(`- Runner: ${e.runner ?? 'local'}, ${e.cpus} × ${e.cpu_model}, ${mib(e.memory_kb)} RAM, ${e.os}`);
    if (report.baseline?.rustc) lines.push(`- rustc: baseline ${report.baseline.rustc}; candidate ${report.candidate.rustc}`);
    const pg = report.benchmarks.find(b => b.environment.candidate?.postgres)?.environment.candidate.postgres;
    if (pg) lines.push(`- Postgres: ${Object.entries(pg).map(([k, v]) => `${k}=${v}`).join(', ')}`);
    if (report.baseline?.build_seconds !== undefined) {
      lines.push(`- Build (not measured time): baseline ${fmt(report.baseline.build_seconds)} s, candidate ${fmt(report.candidate.build_seconds)} s`);
    }
  }
  return lines.join('\n') + '\n';
}
