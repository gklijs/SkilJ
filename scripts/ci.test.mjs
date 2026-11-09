import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

const HERE = dirname(fileURLToPath(import.meta.url));
const ROOT = resolve(HERE, '..');
function selection(group) {
  const result = spawnSync('bash', ['-c',
    'HERE="$1"; source "$HERE/ci-test-targets.sh"; ci_test_targets "$2" || exit; printf "%s\\n" "$test_app_docs" "${test_args[@]}"',
    'bash', HERE, group], { encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  const [docs, ...args] = result.stdout.trim().split('\n');
  return { docs: docs === 'true', args };
}
function fixture(t) {
  const dir = mkdtempSync(join(tmpdir(), 'skilj-ci-test-'));
  t.after(() => rmSync(dir, { recursive: true, force: true }));
  const bin = join(dir, 'bin');
  mkdirSync(bin);
  writeFileSync(join(bin, 'cargo'), `#!/usr/bin/env bash
printf '%s\\n' "$*" >> "$CAPTURE"
printf '%s\\n' "target=$CARGO_TARGET_DIR" >> "$CAPTURE"
printf '%s\\n' "\${FAKE_TEST_OUTPUT:-tests passed}"
if [[ "$*" == 'build --bin server' && -n "\${FAKE_SERVER:-}" ]]; then
  mkdir -p "$CARGO_TARGET_DIR/debug"
  cp "$FAKE_SERVER" "$CARGO_TARGET_DIR/debug/server"
fi
if [[ " $* " == *" --doc "* ]]; then exit "\${FAKE_DOC_STATUS:-0}"; fi
exit "\${FAKE_TEST_STATUS:-0}"
`, { mode: 0o755 });
  writeFileSync(join(bin, 'pg_isready'), '#!/usr/bin/env bash\nexit 0\n', { mode: 0o755 });
  writeFileSync(join(bin, 'psql'), `#!/usr/bin/env bash
printf '%s\\n' "psql $*" >> "$CAPTURE"
if [[ "$*" == *'SHOW server_version_num'* ]]; then
  echo "\${FAKE_POSTGRES_VERSION:-150019}"
  exit "\${FAKE_VERSION_STATUS:-0}"
fi
if [[ "$*" == *'CREATE DATABASE'* ]]; then exit "\${FAKE_CREATE_STATUS:-0}"; fi
if [[ "$*" == *'SELECT 1'* && "\${FAKE_POSTGRES_DOWN:-0}" == 1 ]]; then exit 1; fi
echo 1
`, { mode: 0o755 });
  return { dir, env: { ...process.env, PATH: `${bin}:${process.env.PATH}`,
    CAPTURE: join(dir, 'args'), CI_TEST_LOG: join(dir, 'tests.log'), CI_POSTGRES_TIMEOUT: '1' } };
}
function run(script, args, env) {
  return spawnSync('bash', [join(HERE, script), ...args], { cwd: ROOT, env, encoding: 'utf8' });
}

test('test shards cover every Cargo integration target exactly once and preserve doctests', () => {
  const result = spawnSync('cargo', ['metadata', '--no-deps', '--format-version', '1'], { cwd: ROOT, encoding: 'utf8' });
  assert.equal(result.status, 0, result.stderr);
  const metadata = JSON.parse(result.stdout);
  const app = metadata.packages.find(p => p.name === 'skilj');
  const expected = app.targets.filter(t => t.kind.includes('test')).map(t => t.name).sort();
  const appShard = selection('app');
  const business = selection('business');
  const targetNames = args => args.flatMap((arg, i) => arg === '--test' ? [args[i + 1]] : []);
  const selected = [...targetNames(appShard.args), ...targetNames(business.args)].sort();
  assert.deepEqual(selected, expected);
  assert.equal(new Set(selected).size, selected.length);
  assert.ok(appShard.args.includes('--lib') && appShard.args.includes('--bins') && appShard.args.includes('--examples'));
  assert.ok(appShard.args.includes('local') && business.args.includes('local'));
  assert.equal(appShard.docs, true);
  assert.equal(business.docs, false);
  // Every other crate, including future members, stays in the libraries shard.
  assert.deepEqual(selection('libraries').args, ['--workspace', '--exclude', 'skilj']);
  assert.deepEqual(selection('all').args, ['--workspace']);
});

test('building a shard does not probe Postgres and emits Cargo timings', t => {
  const { env } = fixture(t);
  const result = run('ci-test.sh', ['--build-only'], { ...env, CI_TEST_GROUP: 'business' });
  assert.equal(result.status, 0, result.stderr);
  assert.equal(readFileSync(env.CAPTURE, 'utf8').split('\n')[0],
    'test -p skilj --features local --test graphql_business_surfaces --no-run --timings');
  assert.doesNotMatch(result.stdout, /preflight/);
});

test('shards preserve the skip guard and a test failure survives successful doctests', t => {
  const { env } = fixture(t);
  const skipped = run('ci-test.sh', [], { ...env, CI_TEST_GROUP: 'business', FAKE_TEST_OUTPUT: 'skipping: unavailable service' });
  assert.equal(skipped.status, 1, skipped.stdout + skipped.stderr);
  assert.match(skipped.stdout, /one or more tests silently skipped/);
  const failed = run('ci-test.sh', [], { ...env, CI_TEST_GROUP: 'app', FAKE_TEST_STATUS: '7', FAKE_DOC_STATUS: '0' });
  assert.equal(failed.status, 7);
  assert.match(readFileSync(env.CAPTURE, 'utf8'), /test -p skilj --features local --doc --no-fail-fast -- --nocapture/);
  const passed = run('ci-test.sh', [], { ...env, CI_TEST_GROUP: 'app' });
  assert.equal(passed.status, 0, passed.stderr);
});

test('unknown test groups fail before probing services', t => {
  const { env } = fixture(t);
  const result = run('ci-test.sh', [], { ...env, CI_TEST_GROUP: 'typo' });
  assert.equal(result.status, 2);
  assert.match(result.stderr, /unknown CI_TEST_GROUP/);
});

test('Postgres compatibility selects existing whole binaries without brokers or doctests', () => {
  assert.deepEqual(selection('postgres'), { docs: false, args: ['-p', 'skilj',
    '--test', 'startup_bounded_contexts', '--test', 'conformance',
    '--test', 'sync_projections', '--test', 'async_projections',
    '--test', 'decrypt_on_read', '--test', 'blind_tags'] });
});

test('compatibility requires an explicit major even for build-only, before probing services', t => {
  const { env } = fixture(t);
  for (const major of ['', 'bogus', '15; exit 0', '015']) {
    const result = run('ci-test.sh', ['--build-only'], {
      ...env, CI_TEST_GROUP: 'postgres', CI_POSTGRES_MAJOR: major,
    });
    assert.equal(result.status, 2, result.stdout + result.stderr);
    assert.match(result.stdout, /FATAL:.*CI_POSTGRES_MAJOR/);
  }
  assert.throws(() => readFileSync(env.CAPTURE, 'utf8'), /ENOENT/);
});

test('compatibility proves the actual server major on setup and test runs', t => {
  const { env } = fixture(t);
  for (const major of ['15', '18']) {
    for (const args of [['--setup-only'], []]) {
      const result = run('ci-test.sh', args, { ...env, CI_TEST_GROUP: 'postgres',
        CI_POSTGRES_MAJOR: major, FAKE_POSTGRES_VERSION: `${major}0019` });
      assert.equal(result.status, 0, result.stdout + result.stderr);
      assert.match(result.stdout, new RegExp(`verified Postgres ${major}`));
    }
  }
  assert.match(readFileSync(env.CAPTURE, 'utf8'), /test -p skilj --test startup_bounded_contexts.*--nocapture/);
});

test('wrong, unreadable or malformed server versions fail before database writes or tests', t => {
  const { env } = fixture(t);
  for (const settings of [
    { FAKE_POSTGRES_VERSION: '170011' }, { FAKE_POSTGRES_VERSION: 'unknown' },
    { FAKE_POSTGRES_VERSION: '150019\n180006' }, { FAKE_VERSION_STATUS: '3' },
  ]) {
    const result = run('ci-test.sh', [], { ...env, CI_TEST_GROUP: 'postgres',
      CI_POSTGRES_MAJOR: '15', ...settings });
    assert.equal(result.status, 1, result.stdout + result.stderr);
    assert.match(result.stdout, /FATAL:.*(expected Postgres|cannot read)/);
  }
  assert.doesNotMatch(readFileSync(env.CAPTURE, 'utf8'), /CREATE DATABASE|^test /m);
});

test('unavailable compatibility server fails without invoking the local fallback', t => {
  const { env } = fixture(t);
  const result = run('ci-test.sh', ['--setup-only'], { ...env, CI_TEST_GROUP: 'postgres',
    CI_POSTGRES_MAJOR: '15', CI_POSTGRES_HOST: '127.0.0.1', CI_POSTGRES_PORT: '1',
    FAKE_POSTGRES_DOWN: '1' });
  assert.equal(result.status, 1, result.stdout + result.stderr);
  assert.match(result.stdout, /refusing fallback/);
  assert.doesNotMatch(result.stdout, /\[postgres-local\]/);
});

test('compatibility preserves setup, test-exit and no-skip failures', t => {
  const { env } = fixture(t);
  for (const [settings, status] of [
    [{ FAKE_CREATE_STATUS: '4' }, 1],
    [{ FAKE_TEST_STATUS: '7' }, 7],
    [{ FAKE_TEST_OUTPUT: 'test prefix skipping: unavailable Postgres' }, 1],
  ]) {
    const result = run('ci-test.sh', [], { ...env, CI_TEST_GROUP: 'postgres',
      CI_POSTGRES_MAJOR: '15', ...settings });
    assert.equal(result.status, status, result.stdout + result.stderr);
  }
});

test('the required test check includes both compatibility boundary jobs', () => {
  const workflow = readFileSync(join(ROOT, '.github/workflows/ci.yml'), 'utf8');
  assert.match(workflow, /postgres: \[15, 18\]/);
  assert.match(workflow, /image: postgres:\$\{\{ matrix.postgres \}\}-alpine/);
  assert.match(workflow, /CI_POSTGRES_MAJOR: \$\{\{ matrix.postgres \}\}/);
  assert.match(workflow, /needs: \[test-suites, postgres-compatibility\]/);
  assert.match(workflow, /POSTGRES_RESULT: \$\{\{ needs.postgres-compatibility.result \}\}/);
  assert.match(workflow, /test "\$RESULT" = success && test "\$POSTGRES_RESULT" = success/);
});

test('template preparation keeps the cached target and removes stale generated sources', t => {
  const { dir, env } = fixture(t);
  const cache = join(dir, 'template');
  mkdirSync(join(cache, 'build'), { recursive: true });
  writeFileSync(join(cache, 'build', 'keep'), 'cached artifacts');
  let result = run('check-template.sh', ['--prepare-only', '--cache-dir', cache], env);
  assert.equal(result.status, 0, result.stderr);
  writeFileSync(join(cache, 'app', 'src', 'stale.rs'), 'old source');
  result = run('check-template.sh', ['--prepare-only', '--cache-dir', cache], env);
  assert.equal(result.status, 0, result.stderr);
  assert.throws(() => readFileSync(join(cache, 'app', 'src', 'stale.rs')), /ENOENT/);
  assert.equal(readFileSync(join(cache, 'build', 'keep'), 'utf8'), 'cached artifacts');
  assert.match(readFileSync(join(cache, 'app', 'Cargo.toml'), 'utf8'), /name = "template-check"/);
  result = run('check-template.sh', ['--prepared', '--no-run', '--cache-dir', cache], env);
  assert.equal(result.status, 0, result.stderr);
  const calls = readFileSync(env.CAPTURE, 'utf8');
  assert.match(calls, /test --no-default-features --test wallet_fixture/);
  assert.match(calls, /check --all-targets --no-default-features/);
  assert.ok(calls.includes(`target=${join(cache, 'build')}`));
});

test('template domain test failure stops before builds and server startup', t => {
  const { env } = fixture(t);
  const result = run('check-template.sh', ['--no-run'], { ...env, FAKE_TEST_STATUS: '17' });
  assert.equal(result.status, 17, result.stdout + result.stderr);
  const calls = readFileSync(env.CAPTURE, 'utf8');
  assert.match(calls, /test --no-default-features --test wallet_fixture/);
  assert.ok(!calls.includes('check --all-targets') && !calls.includes('build --bin server'));
});

test('Docker cache arguments stay intact and the generated workspace contains only template dependencies', t => {
  const { dir, env } = fixture(t);
  const cargo = spawnSync('bash', ['-c', 'command -v cargo'], { encoding: 'utf8' });
  assert.equal(cargo.status, 0, cargo.stderr);
  const metadataPath = join(dir, 'docker-metadata.json');
  const argsPath = join(dir, 'docker-args');
  writeFileSync(join(dir, 'bin', 'docker'), `#!/usr/bin/env bash
case "$1" in
  buildx)
    printf '%s\\n' "$@" > "$DOCKER_ARGS"
    "$REAL_CARGO" metadata --no-deps --offline --format-version 1 > "$DOCKER_METADATA"
    ;;
  run) echo 'DATABASE_URL is required'; exit 1 ;;
  rmi) exit 0 ;;
  *) echo "unexpected Docker command: $*" >&2; exit 2 ;;
esac
`, { mode: 0o755 });
  const cache = `type=local,dest=${join(dir, 'cache with spaces')},mode=max`;
  const result = run('check-template-docker.sh', ['--cache-to', cache], {
    ...env, REAL_CARGO: cargo.stdout.trim(), DOCKER_ARGS: argsPath, DOCKER_METADATA: metadataPath,
  });
  assert.equal(result.status, 0, result.stdout + result.stderr);
  const args = readFileSync(argsPath, 'utf8').trim().split('\n');
  assert.deepEqual(args.slice(0, 5), ['buildx', 'build', '--load', '--cache-to', cache]);
  const metadata = JSON.parse(readFileSync(metadataPath, 'utf8'));
  const names = metadata.packages.map(p => p.name);
  for (const name of ['template-check', 'skilj', 'skilj-core', 'skilj-graphql', 'skilj-rest', 'skilj-test-fixture']) assert.ok(names.includes(name), name);
  assert.ok(!names.includes('skilj-kafka') && !names.includes('skilj-temporal'));
});

for (const [label, failures, message, expectedAttempts, status] of [
  ['recovers from an HTTP 504 download failure', 1, 'local Postgres: HTTP status server error (504 Gateway Timeout) for url (https://github.com/test)', 2, 0],
  ['stops after three HTTP 504 download failures', 3, 'local Postgres: HTTP status server error (504 Gateway Timeout) for url (https://github.com/test)', 3, 1],
  ['does not retry an application failure', 1, 'registration rejected', 1, 1],
  ['does not retry an HTTP 404 download failure', 1, 'local Postgres: HTTP status client error (404 Not Found) for url (https://github.com/test)', 1, 1],
]) {
  test(`template startup ${label}`, t => {
    const { dir, env } = fixture(t);
    const attempts = join(dir, 'attempts');
    const server = join(dir, 'server');
    writeFileSync(server, `#!/usr/bin/env node
const fs = require('node:fs');
const count = Number(fs.existsSync(process.env.ATTEMPTS) ? fs.readFileSync(process.env.ATTEMPTS, 'utf8') : 0) + 1;
fs.writeFileSync(process.env.ATTEMPTS, String(count));
if (count <= Number(process.env.FAILURES)) {
  console.error('error: ' + process.env.FAILURE_MESSAGE);
  process.exit(1);
}
process.on('SIGINT', () => process.exit(0));
console.log('export DEPOSIT=deposit WITHDRAW=withdraw');
console.log('  curl $DEPOSIT');
console.log('  curl $WITHDRAW');
setInterval(() => {}, 1000);
`, { mode: 0o755 });
    writeFileSync(join(dir, 'bin', 'curl'), `#!/usr/bin/env bash
if [[ "$1" == deposit ]]; then echo '{"accepted":true}'; else echo '{"rejectionKind":"insufficient_funds"}'; fi
`, { mode: 0o755 });
    const result = run('check-template.sh', [], {
      ...env, FAKE_SERVER: server, ATTEMPTS: attempts, FAILURES: String(failures), FAILURE_MESSAGE: message,
    });
    assert.equal(result.status, status, result.stdout + result.stderr);
    assert.equal(Number(readFileSync(attempts, 'utf8')), expectedAttempts);
    if (status === 0) assert.match(result.stdout, /quickstart ran: deposit accepted, overdraft rejected/);
    else assert.match(result.stderr, /the server exited before it was ready/);
  });
}
