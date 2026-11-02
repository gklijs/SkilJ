// node scripts/dcb158/summarize-tags.mjs <completed tag experiment log>
import { readFileSync } from 'node:fs';
const log = readFileSync(process.argv[2], 'utf8');
if (!log.includes('test result: ok.') || /skipping:|test result: FAILED/.test(log)) {
  throw new Error('a completed, non-skipped tag experiment is required');
}
const rows = log.split('\n').filter(line => line.startsWith('TAGS ')).map(line =>
  Object.fromEntries(line.slice(5).split(' ').map(token => {
    const [key, value] = token.split('=');
    return [key, key === 'kind' ? value : Number(value)];
  })));
if (!rows.length) throw new Error('no tag measurements found');
const median = values => {
  const sorted = values.toSorted((a, b) => a - b);
  const n = sorted.length;
  return n % 2 ? sorted[(n - 1) / 2] : (sorted[n / 2 - 1] + sorted[n / 2]) / 2;
};
const summary = [...Map.groupBy(rows, row => row.keys).values()].map(group => {
  const first = group[0];
  return {
    keys: first.keys, rows: first.rows,
    raw_probe_ms: median(group.filter(r => r.kind === 'raw').map(r => r.probe_ms)),
    interned_probe_ms: median(group.filter(r => r.kind === 'interned').map(r => r.probe_ms)),
    cached_probe_ms: median(group.filter(r => r.kind === 'cached').map(r => r.probe_ms)),
    raw_seed_ms: median(group.map(r => r.raw_seed_ms)),
    intern_seed_ms: median(group.map(r => r.intern_seed_ms)),
    raw_bytes: first.raw_bytes, intern_tag_bytes: first.intern_tag_bytes,
    dictionary_bytes: first.dictionary_bytes, intern_total_bytes: first.intern_total_bytes,
  };
});
console.log(JSON.stringify({ summary, search: log.split('\n').find(line => line.startsWith('SEARCH ')) }, null, 2));
