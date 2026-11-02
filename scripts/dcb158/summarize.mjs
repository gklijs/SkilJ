// Summarize completed experiment logs; never treat skipped/failed runs as data.
// node scripts/dcb158/summarize.mjs [--json] log...
import { readFileSync } from 'node:fs';
import { basename } from 'node:path';

const json = process.argv.includes('--json');
const files = process.argv.slice(2).filter(arg => arg !== '--json');
if (!files.length) throw new Error('provide experiment log files');
const rows = [];
for (const file of files) {
  const log = readFileSync(file, 'utf8');
  if (!log.includes('test result: ok.') || /skipping:|test result: FAILED/.test(log)) {
    throw new Error(`${file}: not a completed, non-skipped benchmark`);
  }
  let round;
  for (const line of log.split('\n')) {
    if (line.startsWith('ROUND ')) round = Number(line.slice(6));
    if (!line.startsWith('RESULT ')) continue;
    const fields = Object.fromEntries(line.slice(7).split(' ').map(field => {
      const [key, value] = field.split('=');
      return [key, /^\d+(\.\d+)?$/.test(value) ? Number(value) : value];
    }));
    rows.push({ file: basename(file), round, ...fields });
  }
}
const median = values => {
  const sorted = values.toSorted((a, b) => a - b);
  const mid = Math.floor(sorted.length / 2);
  return sorted.length % 2 ? sorted[mid] : (sorted[mid - 1] + sorted[mid]) / 2;
};
const groups = Map.groupBy(rows, row => JSON.stringify([
  row.file, row.workload, row.workers, row.mode,
]));
const summary = [...groups.values()].map(group => {
  const first = group[0];
  const metric = name => {
    const values = group.map(row => row[name]);
    return { median: median(values), min: Math.min(...values), max: Math.max(...values) };
  };
  const ratios = group.flatMap(row => {
    const base = rows.find(other => other.file === row.file && other.round === row.round &&
      other.workload === row.workload && other.workers === row.workers && other.mode === 'serial-group');
    return base ? [row.published_per_s / base.published_per_s] : [];
  });
  return {
    file: first.file, workload: first.workload, workers: first.workers, mode: first.mode,
    samples: group.length,
    published_per_s: metric('published_per_s'), ingest_per_s: metric('ingest_per_s'),
    p50_ms: metric('p50_ms'), p99_ms: metric('p99_ms'), stream_lag_p99_ms: metric('stream_lag_p99_ms'),
    ingest_batch_mean: first.ingest_batch_mean === undefined ? null : metric('ingest_batch_mean'),
    retries: metric('retries'),
    ratio_to_grouped_serial: ratios.length ? median(ratios) : null,
  };
});
if (json) {
  console.log(JSON.stringify({ rows, summary }, null, 2));
} else {
  console.log('| log | workload | workers | mode | published/s median (range) | vs grouped | ack p99 ms | stream lag p99 ms |');
  console.log('|---|---|---:|---|---:|---:|---:|---:|');
  for (const row of summary) {
    const rate = row.published_per_s;
    console.log(`| ${row.file} | ${row.workload} | ${row.workers} | ${row.mode} | ${rate.median.toFixed(0)} (${rate.min.toFixed(0)}–${rate.max.toFixed(0)}) | ${row.ratio_to_grouped_serial?.toFixed(2) ?? 'n/a'} | ${row.p99_ms.median.toFixed(1)} | ${row.stream_lag_p99_ms.median.toFixed(1)} |`);
  }
}
