import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { compare, fileName, findings, HERE, install, NIGHTLY, REVISION, toolCacheKey } from './check.mjs';

function fixture(t) {
  const root = mkdtempSync(join(tmpdir(), 'skilj-cga-test-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const here = join(root, 'scripts/cga');
  mkdirSync(here, { recursive: true });
  writeFileSync(join(root, 'Cargo.lock'), 'fixture dependencies\n');
  writeFileSync(join(root, 'cga.toml'), 'fixture configuration\n');
  writeFileSync(join(here, 'compat.patch'), 'fixture patch\n');
  return { root, here };
}

function report(context, { text = 'panic!("example")', line = 10, skipped = '' } = {}) {
  const messages = [
    { reason: 'compiler-message', message: {
      level: 'warning', code: { code: 'cga::panics' }, message: 'a possible panic',
      spans: [{ file_name: 'skilj-core/src/example.rs', line_start: line,
        is_primary: true, text: [{ text }] }],
    } },
    { reason: 'compiler-artifact', target: { name: 'skilj_core' } },
    { reason: 'build-finished', success: true },
  ];
  return findings(messages.map(m => JSON.stringify(m)).join('\n'), skipped, 0, context);
}

const reject = (actual, baseline) => assert.throws(() => compare(actual, baseline, () => {}));

test('line shifts and indentation do not change finding identity', t => {
  const context = fixture(t);
  compare(report(context), report(context, { text: '   panic!("example")  ', line: 100 }));
});

test('replacing a finding at the same total fails', t => {
  const context = fixture(t);
  reject(report(context, { text: 'panic!("new")' }), report(context));
});

test('additional occurrences fail', t => {
  const context = fixture(t);
  const actual = report(context);
  actual.findings[0].count++;
  reject(actual, report(context));
});

test('removed findings require a smaller baseline', t => {
  const context = fixture(t);
  const actual = report(context);
  actual.findings = [];
  reject(actual, report(context));
});

test('both new and gone findings are listed', t => {
  const context = fixture(t);
  const lines = [];
  assert.throws(() => compare(report(context, { text: 'panic!("new")' }), report(context),
    line => lines.push(line)));
  assert.equal(lines.filter(line => line.startsWith('NEW (1) ')).length, 1);
  assert.equal(lines.filter(line => line.startsWith('GONE (1) ')).length, 1);
});

test('new analysis gaps fail', t => {
  const context = fixture(t);
  reject(report(context, { skipped: 'cga-skipped-generic:some::method\n' }), report(context));
});

test('removed analysis gaps require baseline update', t => {
  const context = fixture(t);
  reject(report(context), report(context, { skipped: 'cga-skipped-generic:some::method\n' }));
});

test('configuration changes require review', t => {
  const context = fixture(t);
  const actual = report(context);
  actual.tool.revision = 'different';
  reject(actual, report(context));
});

test('crashes and incomplete logs cannot be recorded', t => {
  const context = fixture(t);
  for (const [stdout, status] of [['', 0], ['', 101], ['{"reason":"build-finished","success":false}', 0],
    ['{"reason":"build-finished","success":true}', 0], ['unexpected output', 0], ['', null]]) {
    assert.throws(() => findings(stdout, '', status, context));
  }
});

test('error diagnostics cannot be recorded', t => {
  const context = fixture(t);
  const message = { reason: 'compiler-message', message: { level: 'error' } };
  assert.throws(() => findings(JSON.stringify(message), '', 0, context));
});

test('cached binaries still require compiler and LLVM runtime installation', t => {
  const context = fixture(t);
  const prefix = join(context.root, 'target/cga-tool');
  const binary = join(prefix, 'bin/cargo-cga');
  mkdirSync(join(prefix, 'bin'), { recursive: true });
  writeFileSync(binary, '');
  writeFileSync(join(prefix, 'skilj-build'), `${toolCacheKey(context.here)}\n`);
  const commands = [];
  assert.equal(install({ ...context, run: (...args) => commands.push(args) }), binary);
  assert.deepEqual(commands, [['rustup', ['toolchain', 'install', NIGHTLY,
    '--profile', 'minimal', '--no-self-update', '--component', 'rustc-dev',
    '--component', 'rust-src', '--component', 'llvm-tools'], { cwd: context.root }]]);
});

test('toolchains inside checkout are not project source', t => {
  const { root } = fixture(t);
  const path = 'rustup/toolchains/nightly/lib/rustlib/src/rust/library/core/src/lib.rs';
  assert.equal(fileName(join(root, path), root), '<rust>/library/core/src/lib.rs');
  assert.equal(fileName(path, root), '<rust>/library/core/src/lib.rs');
  assert.equal(fileName(join(root, 'cargo/registry/src/index-123/sqlx-0.9.0/src/lib.rs'), root),
    '<registry>/sqlx-0.9.0/src/lib.rs');
  assert.equal(fileName(join(root, 'library/example.rs'), root), 'library/example.rs');
});

test('machine-specific paths are normalized', () => {
  assert.equal(fileName('/a/.rustup/toolchains/nightly/lib/rustlib/src/rust/library/core/src/lib.rs'),
    '<rust>/library/core/src/lib.rs');
  assert.equal(fileName('/a/.cargo/registry/src/index-123/sqlx-0.9.0/src/lib.rs'), '<registry>/sqlx-0.9.0/src/lib.rs');
});

test('unknown external paths are rejected', t => {
  const { root } = fixture(t);
  assert.throws(() => fileName('/another-checkout/src/lib.rs', root), /unrecognized/);
});

test('fresh installation uses pinned nightly and applies compatibility patch', t => {
  const context = fixture(t);
  mkdirSync(join(context.root, 'target/cga-tool'), { recursive: true });
  const commands = [];
  install({ ...context, run: (...args) => commands.push(args) });
  const cargo = commands.find(([command]) => command === 'cargo');
  assert.equal(cargo[1][0], `+${NIGHTLY}`);
  assert.ok(commands.some(([command, args]) => command === 'git' && args.includes('--check')));
  assert.equal(readFileSync(join(context.root, 'target/cga-tool/skilj-build'), 'utf8'),
    `${toolCacheKey(context.here)}\n`);
});

test('a restored source directory is initialized and the pinned commit fetched before checkout', t => {
  const context = fixture(t);
  const source = join(context.root, 'target/cga-tool-source');
  mkdirSync(source, { recursive: true });
  mkdirSync(join(context.root, 'target/cga-tool'), { recursive: true });
  // Simulate a target cache that kept sources but omitted Git metadata.
  writeFileSync(join(source, 'Cargo.toml'), 'cached source');
  assert.equal(spawnSync('git', ['init', context.root]).status, 0);
  const commands = [];
  install({ ...context, run: (command, args, options) => {
    commands.push([command, args]);
    if (command === 'git' && args[0] === 'init') {
      assert.equal(spawnSync(command, args, options).status, 0);
    }
    if (command === 'git' && args.includes('checkout')) {
      const top = spawnSync('git', ['-C', source, 'rev-parse', '--show-toplevel'], { encoding: 'utf8' });
      assert.equal(top.status, 0);
      assert.equal(top.stdout.trim(), source);
    }
  } });
  const init = commands.findIndex(([cmd, args]) => cmd === 'git' && args[0] === 'init');
  const fetch = commands.findIndex(([cmd, args]) => cmd === 'git' && args.includes('fetch'));
  const checkout = commands.findIndex(([cmd, args]) => cmd === 'git' && args.includes('checkout'));
  assert.ok(init >= 0 && init < fetch && fetch < checkout);
  assert.deepEqual(commands[fetch], ['git', ['-C', source, 'fetch', '--depth', '1',
    'https://github.com/OpenDevicePartnership/cga', REVISION]]);
});

test('tool cache key includes all compiler-driver pins and needs no Rust setup', t => {
  const context = fixture(t);
  const before = toolCacheKey(context.here);
  assert.ok(before.includes(REVISION));
  assert.ok(before.includes(NIGHTLY));
  assert.ok(before.includes('x86_64-unknown-linux-gnu'));
  writeFileSync(join(context.here, 'compat.patch'), 'another patch');
  assert.notEqual(toolCacheKey(context.here), before);
  const result = spawnSync(process.execPath, [join(HERE, 'check.mjs'), '--tool-cache-key'], {
    env: { PATH: '' }, encoding: 'utf8',
  });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(result.stdout.trim(), toolCacheKey());
});

test('CLI help runs without Rust or a workspace lockfile', () => {
  const result = spawnSync(process.execPath, [join(HERE, 'check.mjs'), '--help'], {
    env: { PATH: '' }, encoding: 'utf8',
  });
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /Usage:/);
});
