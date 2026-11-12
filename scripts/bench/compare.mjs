#!/usr/bin/env node
// Compares two commits on a representative set of skilj's `#[ignore]`d
// benchmarks (issue #190, docs/performance.md "Comparing two commits"):
//
//   DATABASE_URL=postgres://... node scripts/bench/compare.mjs \
//     --baseline <rev> --candidate <rev> [--trials 3] [--rounds 2]
//
// Each commit is checked out in a git worktree and its benchmark binaries
// are built in release mode first; build time is recorded, never measured.
// Then every trial runs each benchmark on both commits, alternating which
// commit goes first, each run on the database the benchmark recreates for
// itself. Every run must finish with the scenarios and operation counts it
// was asked for, or the comparison fails; the throughput deltas it reports
// are informational and fail nothing.
//
// Output (default target/bench-compare/<timestamp>): raw/*.jsonl, the
// benchmarks' records; logs/*.log, their output; summary.json and
// summary.md. With GITHUB_STEP_SUMMARY set, summary.md is appended there.

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { appendFileSync, copyFileSync, existsSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { cpus, platform, release, totalmem } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { compare, parseRecords, toMarkdown, validateRun } from './report.mjs';

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..', '..');

/**
 * The representative set: independent and shared hot tags, snapshot-backed
 * decisions, the inbound paths a bridge uses, and projection catch-up.
 * `env` narrows a benchmark to these scenarios; `scenarios` is what each
 * run must then have recorded, once per round.
 */
export const BENCHMARKS = {
  command_throughput: {
    package: 'skilj',
    test: 'command_throughput',
    env: { SKILJ_BENCH_WORKLOADS: 'spread,ind-hot,hot,hot-snap', SKILJ_BENCH_WORKERS: '8' },
    scenarios: ['spread/8w', 'ind-hot/8w', 'hot/8w', 'hot-snap/8w'],
  },
  inbound_throughput: {
    package: 'skilj',
    test: 'inbound_throughput',
    env: {},
    scenarios: ['external/1p', 'external/8p', 'trigger/1p', 'trigger/8p'],
  },
  projection_catch_up_throughput: {
    package: 'skilj-core',
    test: 'async_projection_catch_up_throughput',
    env: {},
    scenarios: ['total', 'per-account', 'both', 'partitioned'],
  },
};
// The test binary is named after its file; the projection benchmark's test
// function is not.
const BINARY = {
  command_throughput: 'command_throughput',
  inbound_throughput: 'inbound_throughput',
  projection_catch_up_throughput: 'projection_catch_up_throughput',
};

const USAGE = `usage: node scripts/bench/compare.mjs --baseline <rev> --candidate <rev> [options]

  --trials <n>              trials per commit (default 3; fewer gives no verdict)
  --rounds <n>              measured rounds per run (SKILJ_BENCH_ROUNDS, default 2)
  --commands <n>            commands per command_throughput scenario (default 800)
  --benchmarks <a,b>        subset of: ${Object.keys(BENCHMARKS).join(', ')}
  --baseline-env K=V        extra environment for the baseline's runs (repeatable)
  --candidate-env K=V       extra environment for the candidate's runs (repeatable),
                            e.g. SKILJ_BENCH_SLOWDOWN_US=1000 for a known slowdown
  --require-durable         SKILJ_BENCH_REQUIRE_DURABLE=1, and fail unless every run
                            reports fsync, synchronous_commit and full_page_writes on
  --allow-embedded          run without DATABASE_URL, on the embedded server
  --run-timeout <minutes>   per benchmark run (default 20)
  --out <dir>               output directory (default target/bench-compare/<timestamp>)
  --target-dir <dir>        Cargo target directory for the builds
                            (default target/bench-compare/build)
  --binaries <dir>          use <dir>/<baseline|candidate>/<binary> instead of building;
                            --baseline/--candidate are then only labels`;

export function parseArgs(argv) {
  const options = {
    trials: 3, rounds: 2, commands: 800, benchmarks: Object.keys(BENCHMARKS),
    baselineEnv: {}, candidateEnv: {}, requireDurable: false, allowEmbedded: false, runTimeout: 20,
  };
  const value = (i, name) => {
    if (i >= argv.length) throw new Error(`${name} needs a value\n\n${USAGE}`);
    return argv[i];
  };
  const env = (text, name) => {
    const eq = text.indexOf('=');
    if (eq < 1) throw new Error(`${name} takes K=V, got ${text}`);
    return [text.slice(0, eq), text.slice(eq + 1)];
  };
  const count = (text, name) => {
    const n = Number(text);
    if (!Number.isInteger(n) || n < 1) throw new Error(`${name} takes a positive whole number, got ${text}`);
    return n;
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    switch (arg) {
      case '--baseline': options.baseline = value(++i, arg); break;
      case '--candidate': options.candidate = value(++i, arg); break;
      case '--trials': options.trials = count(value(++i, arg), arg); break;
      case '--rounds': options.rounds = count(value(++i, arg), arg); break;
      case '--commands': options.commands = count(value(++i, arg), arg); break;
      case '--run-timeout': options.runTimeout = count(value(++i, arg), arg); break;
      case '--benchmarks': {
        options.benchmarks = value(++i, arg).split(',').filter(Boolean);
        const unknown = options.benchmarks.filter(b => !BENCHMARKS[b]);
        if (unknown.length) throw new Error(`unknown benchmark(s) ${unknown.join(', ')}\n\n${USAGE}`);
        break;
      }
      case '--baseline-env': { const [k, v] = env(value(++i, arg), arg); options.baselineEnv[k] = v; break; }
      case '--candidate-env': { const [k, v] = env(value(++i, arg), arg); options.candidateEnv[k] = v; break; }
      case '--require-durable': options.requireDurable = true; break;
      case '--allow-embedded': options.allowEmbedded = true; break;
      case '--out': options.out = resolve(value(++i, arg)); break;
      case '--target-dir': options.targetDir = resolve(value(++i, arg)); break;
      case '--binaries': options.binaries = resolve(value(++i, arg)); break;
      case '-h': case '--help': options.help = true; break;
      default: throw new Error(`unknown argument ${arg}\n\n${USAGE}`);
    }
  }
  if (options.help) return options;
  if (!options.baseline || !options.candidate) throw new Error(`--baseline and --candidate are required\n\n${USAGE}`);
  return options;
}

function run(command, args, opts = {}) {
  const result = spawnSync(command, args, { encoding: 'utf8', maxBuffer: 1 << 28, ...opts });
  if (result.error) throw new Error(`${command} ${args.join(' ')}: ${result.error.message}`);
  if (result.status !== 0) {
    throw new Error(`${command} ${args.join(' ')} exited ${result.status}\n${result.stderr}`);
  }
  return result.stdout.trim();
}

/** Builds `side`'s benchmark binaries at `commit` and copies them into `bin`. */
function build(side, commit, options, work) {
  const tree = join(work, 'src', side);
  rmSync(tree, { recursive: true, force: true });
  run('git', ['-C', ROOT, 'worktree', 'add', '--detach', '--force', tree, commit]);
  // The workspace commits no Cargo.lock. Starting both commits from the
  // same one keeps a dependency release published between their builds
  // from becoming a difference between them.
  const lock = join(ROOT, 'Cargo.lock');
  if (existsSync(lock)) copyFileSync(lock, join(tree, 'Cargo.lock'));
  const started = Date.now();
  const targets = options.benchmarks.map(b => BENCHMARKS[b]);
  const bin = join(work, 'bin', side);
  mkdirSync(bin, { recursive: true });
  for (const pkg of new Set(targets.map(t => t.package))) {
    const tests = options.benchmarks.filter(b => BENCHMARKS[b].package === pkg);
    const out = run('cargo', ['test', '--release', '-p', pkg,
      ...tests.flatMap(b => ['--test', BINARY[b]]), '--no-run', '--message-format=json-render-diagnostics'], {
      cwd: tree, env: { ...process.env, CARGO_TARGET_DIR: options.targetDir }, stdio: ['ignore', 'pipe', 'inherit'],
    });
    for (const line of out.split('\n')) {
      if (!line.startsWith('{')) continue;
      const message = JSON.parse(line);
      const name = message.target?.name;
      if (message.reason === 'compiler-artifact' && message.executable && tests.some(b => BINARY[b] === name)) {
        // Copied out at once: the next commit's build may reuse the path.
        copyFileSync(message.executable, join(bin, name));
      }
    }
  }
  for (const b of options.benchmarks) {
    if (!existsSync(join(bin, BINARY[b]))) throw new Error(`${side}: building ${commit} produced no ${BINARY[b]} binary`);
  }
  return {
    tree,
    bin,
    build_seconds: (Date.now() - started) / 1000,
    rustc: run('rustc', ['-V'], { cwd: tree }),
    cargo_lock_sha256: createHash('sha256').update(readFileSync(join(tree, 'Cargo.lock'))).digest('hex'),
  };
}

const DURABLE = ['fsync', 'synchronous_commit', 'full_page_writes'];

/** One benchmark run of one side in one trial. Returns its records, or throws. */
function runOnce({ side, benchmark, trial, prepared, options, work }) {
  const spec = BENCHMARKS[benchmark];
  const label = `${side}-${benchmark}-t${trial}`;
  const record = join(work, 'raw', `${label}.jsonl`);
  rmSync(record, { force: true });
  const env = {
    ...process.env,
    ...spec.env,
    SKILJ_BENCH_RECORD: record,
    SKILJ_BENCH_ROUNDS: String(options.rounds),
    SKILJ_BENCH_COMMANDS: String(options.commands),
    ...(options.requireDurable ? { SKILJ_BENCH_REQUIRE_DURABLE: '1' } : {}),
    ...(side === 'baseline' ? options.baselineEnv : options.candidateEnv),
  };
  const started = Date.now();
  const result = spawnSync(join(prepared.bin, BINARY[benchmark]),
    ['--ignored', '--exact', spec.test, '--nocapture', '--test-threads=1'], {
      cwd: prepared.tree ? join(prepared.tree, spec.package) : work,
      env, encoding: 'utf8', maxBuffer: 1 << 28, timeout: options.runTimeout * 60_000,
    });
  const log = `${result.stdout ?? ''}\n--- stderr ---\n${result.stderr ?? ''}`;
  writeFileSync(join(work, 'logs', `${label}.log`), log);
  const seconds = ((Date.now() - started) / 1000).toFixed(1);
  if (result.error) throw new Error(`${label}: ${result.error.message} after ${seconds} s (see logs/${label}.log)`);
  if (result.status !== 0) throw new Error(`${label}: exited ${result.status ?? result.signal} (see logs/${label}.log)`);
  if (/^skipping:/m.test(log)) throw new Error(`${label}: the benchmark skipped (see logs/${label}.log)`);
  if (!/^test \S+ \.\.\. ok$/m.test(result.stdout ?? '')) {
    throw new Error(`${label}: the benchmark test did not run (see logs/${label}.log)`);
  }
  const records = existsSync(record) ? parseRecords(readFileSync(record, 'utf8')) : [];
  const problems = validateRun(records, { benchmark, scenarios: spec.scenarios, rounds: options.rounds });
  if (options.requireDurable) {
    const postgres = records.find(r => r.kind === 'complete')?.environment?.postgres ?? {};
    for (const name of DURABLE) {
      if (postgres[name] !== 'on') problems.push(`${benchmark}: ${name}=${postgres[name]}, --require-durable needs on`);
    }
  }
  if (problems.length) throw new Error(`${label}:\n  ${problems.join('\n  ')}`);
  console.log(`${label}: ${records.length - 1} scenarios in ${seconds} s`);
  return records;
}

function environment() {
  return {
    runner: process.env.RUNNER_NAME ? `${process.env.RUNNER_NAME} (${process.env.RUNNER_OS ?? '?'})` : null,
    cpus: cpus().length,
    cpu_model: cpus()[0]?.model ?? 'unknown',
    memory_kb: Math.round(totalmem() / 1024),
    os: `${platform()} ${release()}`,
    node: process.version,
    database: process.env.DATABASE_URL ? redact(process.env.DATABASE_URL) : 'embedded',
    database_memory: 'not measured',
  };
}

function redact(url) {
  try {
    const parsed = new URL(url);
    if (parsed.password) parsed.password = '***';
    return parsed.toString();
  } catch {
    return 'unparseable';
  }
}

export function main(argv = process.argv.slice(2)) {
  const options = parseArgs(argv);
  if (options.help) {
    console.log(USAGE);
    return 0;
  }
  if (!process.env.DATABASE_URL && !options.allowEmbedded && !options.binaries) {
    throw new Error('DATABASE_URL is not set: compare on a dedicated Postgres, or pass --allow-embedded');
  }
  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const work = options.out ?? join(ROOT, 'target', 'bench-compare', stamp);
  options.targetDir ??= join(ROOT, 'target', 'bench-compare', 'build');
  mkdirSync(join(work, 'raw'), { recursive: true });
  mkdirSync(join(work, 'logs'), { recursive: true });

  const sides = {};
  const report = { trials: options.trials, rounds: options.rounds, environment: environment(), status: 'running' };
  const finish = (status, error) => {
    report.status = status;
    if (error) report.error = error;
    writeFileSync(join(work, 'summary.json'), JSON.stringify(report, null, 2) + '\n');
  };
  try {
    for (const side of ['baseline', 'candidate']) {
      const rev = options[side];
      if (options.binaries) {
        sides[side] = { commit: rev, bin: join(options.binaries, side), tree: null };
      } else {
        const commit = run('git', ['-C', ROOT, 'rev-parse', '--verify', `${rev}^{commit}`]);
        const same = side === 'candidate' && commit === sides.baseline.commit;
        console.log(`${side}: ${rev} = ${commit}${same ? ' (same commit as the baseline: reusing its build)' : ''}`);
        sides[side] = { commit, ...(same ? { ...sides.baseline } : build(side, commit, options, work)) };
        sides[side].commit = commit;
      }
      report[side] = {
        commit: sides[side].commit, rev, env: side === 'baseline' ? options.baselineEnv : options.candidateEnv,
        rustc: sides[side].rustc, build_seconds: sides[side].build_seconds,
        cargo_lock_sha256: sides[side].cargo_lock_sha256,
      };
    }

    const runs = { baseline: {}, candidate: {} };
    for (let trial = 1; trial <= options.trials; trial++) {
      // Alternating which commit runs first spreads warm caches, a
      // growing server and the runner's drift over both.
      const order = trial % 2 ? ['baseline', 'candidate'] : ['candidate', 'baseline'];
      for (const side of order) {
        for (const benchmark of options.benchmarks) {
          const records = runOnce({ side, benchmark, trial, prepared: sides[side], options, work });
          (runs[side][benchmark] ??= []).push({ trial, records });
        }
      }
    }
    const benchmarks = Object.fromEntries(options.benchmarks.map(b => [b, BENCHMARKS[b]]));
    Object.assign(report, compare(runs, benchmarks, {}));
    const nonComparable = report.benchmarks.filter(b => !b.comparable).map(b => b.benchmark);
    finish('ok');
    const markdown = toMarkdown(report);
    writeFileSync(join(work, 'summary.md'), markdown);
    if (process.env.GITHUB_STEP_SUMMARY) appendFileSync(process.env.GITHUB_STEP_SUMMARY, markdown);
    console.log(`\n${markdown}\nWritten to ${work}`);
    if (nonComparable.length) {
      console.error(`non-comparable benchmark versions: ${nonComparable.join(', ')}`);
      return 2;
    }
    return 0;
  } catch (e) {
    finish('failed', e.message);
    throw e;
  } finally {
    for (const side of Object.values(sides)) {
      if (side.tree && existsSync(side.tree)) {
        spawnSync('git', ['-C', ROOT, 'worktree', 'remove', '--force', side.tree]);
      }
    }
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    process.exitCode = main();
  } catch (e) {
    console.error(`bench compare failed: ${e.message}`);
    process.exitCode = 1;
  }
}
