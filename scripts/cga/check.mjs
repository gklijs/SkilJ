#!/usr/bin/env node
// Pinned, partial CGA analysis with a per-finding regression baseline.
import { createHash } from 'node:crypto';
import { spawnSync } from 'node:child_process';
import { closeSync, existsSync, mkdirSync, openSync, readFileSync, writeFileSync } from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { isDeepStrictEqual, parseArgs } from 'node:util';

export const HERE = dirname(fileURLToPath(import.meta.url));
export const ROOT = resolve(HERE, '../..');
export const REVISION = '2fc63108f0b674fe7a178191ffa15647a18879f1';
export const NIGHTLY = 'nightly-2026-10-18';
const TARGET = 'x86_64-unknown-linux-gnu';
const PACKAGE = 'skilj-core';
export const digest = path => createHash('sha256').update(readFileSync(path)).digest('hex');

// The installed compiler driver is cached independently of workspace targets.
export const toolCacheKey = (here = HERE) =>
  `cga-tool-v1-${REVISION}-${NIGHTLY}-${TARGET}-${digest(join(here, 'compat.patch'))}`;

function runCommand(command, args, options = {}) {
  const result = spawnSync(command, args, { cwd: ROOT, stdio: 'inherit', ...options });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`${command} failed (${result.signal ?? result.status})`);
  return result;
}

export function install({ root = ROOT, here = HERE, run = runCommand,
  prefix = join(root, 'target/cga-tool') } = {}) {
  const stamp = `${toolCacheKey(here)}\n`;
  // Restored binaries still need their compiler/LLVM runtime libraries.
  run('rustup', ['toolchain', 'install', NIGHTLY, '--profile', 'minimal', '--no-self-update',
    '--component', 'rustc-dev', '--component', 'rust-src', '--component', 'llvm-tools'], { cwd: root });
  const marker = join(prefix, 'skilj-build');
  const binary = join(prefix, 'bin/cargo-cga');
  if (existsSync(binary) && existsSync(marker) && readFileSync(marker, 'utf8') === stamp) return binary;
  const source = join(root, 'target/cga-tool-source');
  // Target caches may retain the source directory while stripping its .git
  // directory or objects. Initialize explicitly: otherwise git -C can find
  // the enclosing SkilJ repository and operate on it instead.
  run('git', ['init', source], { cwd: root });
  run('git', ['-C', source, 'fetch', '--depth', '1',
    'https://github.com/OpenDevicePartnership/cga', REVISION], { cwd: root });
  // This dedicated, ignored checkout contains only the downloaded tool.
  run('git', ['-C', source, 'checkout', '--force', REVISION], { cwd: root });
  run('git', ['-C', source, 'apply', '--check', join(here, 'compat.patch')], { cwd: root });
  run('git', ['-C', source, 'apply', join(here, 'compat.patch')], { cwd: root });
  run('cargo', [`+${NIGHTLY}`, 'install', '--path', '.', '--locked', '--force', '--root', prefix], { cwd: source });
  writeFileSync(marker, stamp);
  return binary;
}

export function fileName(name, root = ROOT) {
  // rustc may report toolchains inside the checkout as relative paths.
  const rustSource = '/lib/rustlib/src/rust/library/';
  if (name.includes(rustSource)) return `<rust>/library/${name.split(rustSource)[1]}`;
  if (name.includes('/registry/src/')) {
    const suffix = name.split('/registry/src/')[1];
    return `<registry>/${suffix.slice(suffix.indexOf('/') + 1)}`;
  }
  if (isAbsolute(name)) {
    const path = relative(root, name);
    if (path === '..' || path.startsWith(`..${sep}`) || isAbsolute(path)) {
      throw new Error(`unrecognized diagnostic path: ${name}`);
    }
    return path;
  }
  return name;
}

// Preserve the existing baseline's Unicode code-point ordering.
function compareText(a, b) {
  const left = Array.from(a, c => c.codePointAt(0));
  const right = Array.from(b, c => c.codePointAt(0));
  for (let i = 0; i < Math.min(left.length, right.length); i++) {
    if (left[i] !== right[i]) return left[i] - right[i];
  }
  return left.length - right.length;
}

function compareTuple(a, b) {
  for (let i = 0; i < a.length; i++) {
    const order = compareText(a[i], b[i]);
    if (order) return order;
  }
  return 0;
}

function increment(counts, key) {
  counts.set(key, (counts.get(key) ?? 0) + 1);
}

export function findings(stdout, stderr, returncode, { root = ROOT, here = HERE } = {}) {
  if (returncode !== 0) {
    throw new Error(`CGA failed (exit ${returncode}); inspect target/cga-report/stderr.log`);
  }
  const counts = new Map();
  let completed = false;
  let coreBuilt = false;
  for (const line of stdout.split(/\r?\n/)) {
    if (!line.trim()) continue;
    const message = JSON.parse(line);
    if (message.reason === 'build-finished') completed = message.success === true;
    else if (message.reason === 'compiler-artifact') coreBuilt ||= message.target?.name === 'skilj_core';
    else if (message.reason === 'compiler-message') {
      const diagnostic = message.message;
      if (['error', 'failure-note'].includes(diagnostic.level)) throw new Error('compiler error in CGA output');
      if (diagnostic.code?.code !== 'cga::panics') continue;
      const spans = diagnostic.spans.filter(span => span.is_primary);
      if (!spans.length) throw new Error('CGA finding has no primary source span');
      for (const span of spans) {
        const source = span.text.map(t => t.text).join(' ').trim().replace(/\s+/gu, ' ');
        if (!source) throw new Error('CGA finding has no source text');
        increment(counts, JSON.stringify([fileName(span.file_name, root), diagnostic.message, source]));
      }
    }
  }
  if (!completed || !coreBuilt) {
    throw new Error('CGA did not finish building skilj-core; refusing an empty/partial baseline');
  }
  const skipped = new Map();
  for (const line of stderr.split(/\r?\n/)) {
    if (line.startsWith('cga-skipped-generic:')) increment(skipped, line.slice('cga-skipped-generic:'.length).trim());
  }
  const entries = [...counts].map(([key, count]) => [JSON.parse(key), count]);
  entries.sort(([a], [b]) => compareTuple(a, b));
  return {
    schema: 1,
    tool: {
      revision: REVISION, nightly: NIGHTLY, target: TARGET, package: PACKAGE,
      patch_sha256: digest(join(here, 'compat.patch')),
      config_sha256: digest(join(root, 'cga.toml')),
      lock_sha256: digest(join(root, 'Cargo.lock')),
    },
    skipped_generic_targets: Object.fromEntries([...skipped].sort(([a], [b]) => compareText(a, b))),
    findings: entries.map(([[file, message, source], count]) => ({ file, message, source, count })),
  };
}

function findingCounts(report) {
  const result = new Map();
  for (const finding of report.findings) {
    const key = JSON.stringify([finding.file, finding.message, finding.source]);
    if (!Number.isSafeInteger(finding.count) || finding.count <= 0 || result.has(key)) {
      throw new Error('invalid or duplicate baseline finding');
    }
    result.set(key, finding.count);
  }
  return result;
}

function difference(left, right) {
  return [...left].filter(([key, count]) => count > (right.get(key) ?? 0))
    .map(([key, count]) => [key, count - (right.get(key) ?? 0)]);
}

export function compare(actual, baseline, log = console.error) {
  if (actual.schema !== baseline.schema || !isDeepStrictEqual(actual.tool, baseline.tool)) {
    throw new Error('CGA tool/config changed; review and re-record the baseline');
  }
  const current = findingCounts(actual);
  const accepted = findingCounts(baseline);
  const added = difference(current, accepted);
  const removed = difference(accepted, current);
  const newSkips = difference(new Map(Object.entries(actual.skipped_generic_targets)),
    new Map(Object.entries(baseline.skipped_generic_targets)));
  if (added.length || newSkips.length) {
    for (const [key, count] of added) {
      const [path, message, source] = JSON.parse(key);
      log(`NEW (${count}) ${path}: ${message}\n  ${source}`);
    }
    for (const [target, count] of newSkips) log(`NEW skipped target (${count}): ${target}`);
    throw new Error('new potential panics or analysis gaps; fix them or explicitly review a baseline update');
  }
  if (removed.length || !isDeepStrictEqual(actual.skipped_generic_targets, baseline.skipped_generic_targets)) {
    throw new Error('findings/analysis gaps decreased; run --record and review the smaller baseline');
  }
}

export function main(args = process.argv.slice(2)) {
  const { values } = parseArgs({ args, options: {
    record: { type: 'boolean' }, 'install-only': { type: 'boolean' },
    'tool-cache-key': { type: 'boolean' }, help: { type: 'boolean' },
  } });
  if (values.help) {
    console.log('Usage: node scripts/cga/check.mjs [--record | --install-only | --tool-cache-key]\nPinned partial CGA analysis and per-finding regression check.');
    return;
  }
  if (values['tool-cache-key']) {
    console.log(toolCacheKey());
    return;
  }
  const lock = join(ROOT, 'Cargo.lock');
  const snapshot = join(HERE, 'workspace.lock');
  if (values.record) {
    if (!existsSync(lock)) throw new Error('--record needs an existing workspace Cargo.lock');
  } else if (!existsSync(lock)) writeFileSync(lock, readFileSync(snapshot));
  else if (!readFileSync(lock).equals(readFileSync(snapshot))) {
    throw new Error('Cargo.lock differs from scripts/cga/workspace.lock; review dependencies and use --record');
  }
  const binary = install({ prefix: process.env.CGA_TOOL_ROOT
    ? resolve(process.env.CGA_TOOL_ROOT) : join(ROOT, 'target/cga-tool') });
  if (values['install-only']) return;
  const output = join(ROOT, 'target/cga-report');
  mkdirSync(output, { recursive: true });
  // Fresh analysis of the primary crate; keep dependencies cached.
  runCommand('cargo', [`+${NIGHTLY}`, 'clean', '-p', PACKAGE, '--profile', 'cga', '--target', TARGET, '--target-dir', 'target/cga']);
  const stdout = openSync(join(output, 'stdout.jsonl'), 'w');
  const stderr = openSync(join(output, 'stderr.log'), 'w');
  let result;
  try {
    result = spawnSync(binary, ['cga', 'check', '-p', PACKAGE, '--profile', 'cga', '--config-path', 'cga.toml',
      '--target', TARGET, '--locked', '--color', 'never'], {
      cwd: ROOT, stdio: ['inherit', stdout, stderr], env: { ...process.env, CARGO_TERM_COLOR: 'never' },
    });
  } finally {
    closeSync(stdout);
    closeSync(stderr);
  }
  if (result.error) throw result.error;
  const actual = findings(readFileSync(join(output, 'stdout.jsonl'), 'utf8'),
    readFileSync(join(output, 'stderr.log'), 'utf8'), result.status);
  const encoded = `${JSON.stringify(actual, null, 2)}\n`;
  writeFileSync(join(output, 'findings.json'), encoded);
  const baseline = join(HERE, 'baseline.json');
  if (values.record) {
    writeFileSync(snapshot, readFileSync(lock));
    writeFileSync(baseline, encoded);
  } else compare(actual, JSON.parse(readFileSync(baseline, 'utf8')));
  console.log(`CGA: ${actual.findings.reduce((sum, f) => sum + f.count, 0)} potential panics; ` +
    `${Object.values(actual.skipped_generic_targets).reduce((a, b) => a + b, 0)} skipped generic targets. ` +
    'Partial skilj-core coverage; see scripts/cga/README.md.');
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try { main(); }
  catch (error) { console.error(`check-cga: ${error.message}`); process.exitCode = 1; }
}
