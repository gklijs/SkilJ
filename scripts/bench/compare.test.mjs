import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { chmodSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { BENCHMARKS, parseArgs } from './compare.mjs';
import { compare, median, percentile, toMarkdown, validateRun, verdict } from './report.mjs';

const HERE = dirname(fileURLToPath(import.meta.url));

// A stand-in for a benchmark binary: writes the records
// skilj_test_support::bench would, for the scenarios compare.mjs asks for,
// with a few percent of trial-to-trial noise and SKILJ_BENCH_SLOWDOWN_US
// added to every operation, as the real benchmarks do. FAKE_* make it
// misbehave the ways a real run can.
const FAKE = `
import { appendFileSync } from 'node:fs';
const [benchmark, scenarios] = [process.env.FAKE_BENCHMARK, process.env.FAKE_SCENARIOS.split(',')];
const env = process.env;
if (env.FAKE_SKIP === benchmark) { console.error('skipping: no test database'); console.log('test result: ok. 1 passed; 0 failed'); process.exit(0); }
if (env.FAKE_FILTERED === benchmark) { console.log('test result: ok. 0 passed; 0 failed; 1 filtered out'); process.exit(0); }
if (env.FAKE_CRASH === benchmark) { console.error('thread panicked'); process.exit(101); }
let seed = [...env.SKILJ_BENCH_RECORD].reduce((h, c) => (h * 31 + c.charCodeAt(0)) >>> 0, 7);
const noise = () => { seed = (seed * 1103515245 + 12345) >>> 0; return 1 + ((seed % 1000) / 1000 - 0.5) * 0.04; };
const slowdown = Number(env.SKILJ_BENCH_SLOWDOWN_US ?? 0);
const version = Number(env.FAKE_VERSION ?? 1);
const rounds = Number(env.SKILJ_BENCH_ROUNDS);
let count = 0;
const write = record => appendFileSync(env.SKILJ_BENCH_RECORD, JSON.stringify({ format: 1, benchmark, benchmark_version: version, ...record }) + '\\n');
for (let round = 1; round <= rounds; round++) {
  for (const scenario of scenarios) {
    if (env.FAKE_DROP === scenario) continue;
    const trial = noise();
    const samples = Array.from({ length: 50 }, () => Math.round(1000 * trial * noise() + slowdown));
    const operations = env.FAKE_SHORT === scenario ? 49 : 50;
    write({ kind: 'scenario', scenario, round, unit: 'command', operations, expected_operations: 50,
      elapsed_us: samples.reduce((a, b) => a + b, 0), samples_us: samples, settings: {}, peak_rss_kb: 40960 });
    count++;
  }
}
if (env.FAKE_INCOMPLETE !== benchmark) {
  write({ kind: 'complete', scenarios: count, peak_rss_kb: 51200,
    environment: { postgres: { fsync: env.FAKE_FSYNC ?? 'on', synchronous_commit: 'on', full_page_writes: 'on', server_version: '17.6' } } });
}
console.log('test result: ok. 1 passed; 0 failed');
`;

function fakeBinaries(t) {
  const dir = mkdtempSync(join(tmpdir(), 'skilj-bench-test-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  writeFileSync(join(dir, 'fake-bench.mjs'), FAKE);
  const binaries = { command_throughput: 'command_throughput', inbound_throughput: 'inbound_throughput',
    projection_catch_up_throughput: 'projection_catch_up_throughput' };
  for (const side of ['baseline', 'candidate']) {
    mkdirSync(join(dir, 'bin', side), { recursive: true });
    for (const [benchmark, binary] of Object.entries(binaries)) {
      const path = join(dir, 'bin', side, binary);
      writeFileSync(path, `#!/usr/bin/env bash
FAKE_BENCHMARK=${benchmark} FAKE_SCENARIOS=${BENCHMARKS[benchmark].scenarios.join(',')} exec node ${join(dir, 'fake-bench.mjs')} "$@"
`);
      chmodSync(path, 0o755);
    }
  }
  return dir;
}

function compareRun(dir, args, env = {}) {
  const out = join(dir, `out-${Math.random().toString(36).slice(2)}`);
  const childEnv = { ...process.env, ...env };
  delete childEnv.GITHUB_STEP_SUMMARY;
  const result = spawnSync('node', [join(HERE, 'compare.mjs'), '--baseline', 'base', '--candidate', 'cand',
    '--binaries', join(dir, 'bin'), '--out', out, ...args], { encoding: 'utf8', env: childEnv });
  const read = name => { try { return readFileSync(join(out, name), 'utf8'); } catch { return null; } };
  return { ...result, out, summary: read('summary.json') && JSON.parse(read('summary.json')), markdown: read('summary.md') };
}

const scenariosOf = report => report.benchmarks.flatMap(b => b.scenarios);

test('a controlled slowdown on the candidate is reported as slower, on every scenario', t => {
  const dir = fakeBinaries(t);
  const result = compareRun(dir, ['--trials', '3', '--candidate-env', 'SKILJ_BENCH_SLOWDOWN_US=500']);
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.summary.status, 'ok');
  for (const s of scenariosOf(result.summary)) {
    assert.equal(s.verdict, 'slower', `${s.scenario}: ${s.verdict}`);
    assert.ok(s.delta < -0.25, `${s.scenario}: ${s.delta}`);
    assert.equal(s.baseline.trials.length, 3);
  }
  assert.match(result.markdown, /\*\*slower\*\*/);
  assert.match(result.markdown, /SKILJ_BENCH_SLOWDOWN_US/);
  assert.match(result.markdown, /Database memory: not measured/);
});

test('the same commit twice compares as noise, not as a regression', t => {
  const dir = fakeBinaries(t);
  const result = compareRun(dir, ['--trials', '3']);
  assert.equal(result.status, 0, result.stderr);
  for (const s of scenariosOf(result.summary)) {
    assert.notEqual(s.verdict, 'slower', s.scenario);
    assert.ok(Math.abs(s.delta) < 0.05, `${s.scenario}: ${s.delta}`);
  }
});

test('a single trial gives no verdict, even for a real slowdown', t => {
  const dir = fakeBinaries(t);
  const result = compareRun(dir, ['--trials', '1', '--candidate-env', 'SKILJ_BENCH_SLOWDOWN_US=500']);
  assert.equal(result.status, 0, result.stderr);
  assert.ok(scenariosOf(result.summary).every(s => s.verdict === 'insufficient trials'));
});

test('trials alternate which commit runs first and keep every run\'s raw records', t => {
  const dir = fakeBinaries(t);
  const result = compareRun(dir, ['--trials', '2', '--benchmarks', 'inbound_throughput']);
  assert.equal(result.status, 0, result.stderr);
  const order = result.stdout.split('\n').filter(l => / scenarios in /.test(l)).map(l => l.split(':')[0]);
  assert.deepEqual(order, ['baseline-inbound_throughput-t1', 'candidate-inbound_throughput-t1',
    'candidate-inbound_throughput-t2', 'baseline-inbound_throughput-t2']);
  const raw = readFileSync(join(result.out, 'raw', 'candidate-inbound_throughput-t2.jsonl'), 'utf8');
  assert.equal(raw.trim().split('\n').length, 2 * 4 + 1);
});

for (const [name, env, message] of [
  ['a skipped benchmark', { FAKE_SKIP: 'inbound_throughput' }, /skipped/],
  ['a crashed benchmark', { FAKE_CRASH: 'projection_catch_up_throughput' }, /exited 101/],
  ['a missing workload', { FAKE_DROP: 'hot/8w' }, /scenario hot\/8w recorded 0 times/],
  ['a short sample count', { FAKE_SHORT: 'trigger/8p' }, /49 of 50 operations/],
  ['a run that did not finish', { FAKE_INCOMPLETE: 'command_throughput' }, /did not finish/],
  ['a binary whose test was filtered out', { FAKE_FILTERED: 'inbound_throughput' }, /test did not run/],
]) {
  test(`${name} fails the comparison`, t => {
    const dir = fakeBinaries(t);
    const result = compareRun(dir, ['--trials', '1'], env);
    assert.equal(result.status, 1, result.stdout);
    assert.match(result.stderr, message);
    assert.equal(result.summary.status, 'failed');
  });
}

test('--require-durable fails a run on a non-durable server', t => {
  const dir = fakeBinaries(t);
  const result = compareRun(dir, ['--trials', '1', '--require-durable'], { FAKE_FSYNC: 'off' });
  assert.equal(result.status, 1);
  assert.match(result.stderr, /fsync=off, --require-durable needs on/);
});

test('different benchmark versions are reported as non-comparable', t => {
  const dir = fakeBinaries(t);
  // Only the candidate's binaries see FAKE_VERSION.
  for (const binary of ['command_throughput', 'inbound_throughput', 'projection_catch_up_throughput']) {
    const path = join(dir, 'bin', 'candidate', binary);
    writeFileSync(path, readFileSync(path, 'utf8').replace('exec node', 'FAKE_VERSION=2 exec node'));
  }
  const result = compareRun(dir, ['--trials', '3']);
  assert.equal(result.status, 2);
  assert.ok(scenariosOf(result.summary).every(s => s.verdict === 'non-comparable'));
  assert.match(result.markdown, /non-comparable/);
});

test('arguments are checked before anything runs', () => {
  assert.throws(() => parseArgs(['--baseline', 'a']), /--candidate are required/);
  assert.throws(() => parseArgs(['--baseline', 'a', '--candidate', 'b', '--benchmarks', 'nope']), /unknown benchmark/);
  assert.throws(() => parseArgs(['--baseline', 'a', '--candidate', 'b', '--trials', '0']), /positive whole number/);
  assert.throws(() => parseArgs(['--baseline', 'a', '--candidate', 'b', '--candidate-env', 'X']), /K=V/);
  const options = parseArgs(['--baseline', 'a', '--candidate', 'b', '--candidate-env', 'A=1=2']);
  assert.deepEqual(options.candidateEnv, { A: '1=2' });
});

test('statistics and verdicts', () => {
  assert.equal(median([3, 1, 2]), 2);
  assert.equal(median([4, 1, 2, 3]), 2.5);
  assert.equal(percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0.5), 5);
  assert.equal(percentile([1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 0.99), 9);
  const side = values => ({ trials: values.map(v => ({ throughput: v })),
    throughput: { min: Math.min(...values), max: Math.max(...values) } });
  assert.equal(verdict(side([100, 101, 102]), side([90, 91, 92])), 'slower');
  assert.equal(verdict(side([100, 101, 102]), side([110, 111, 112])), 'faster');
  assert.equal(verdict(side([100, 101, 102]), side([95, 101, 104])), 'within noise');
  assert.equal(verdict(side([100, 101]), side([90, 91])), 'insufficient trials');
});

test('a run without records is not usable', () => {
  assert.match(validateRun([], { benchmark: 'b', scenarios: ['s'], rounds: 1 })[0], /no records/);
  const report = compare({ baseline: {}, candidate: {} }, { b: { scenarios: [] } });
  assert.equal(report.benchmarks[0].comparable, false);
  assert.match(toMarkdown(report), /non-comparable/);
});
