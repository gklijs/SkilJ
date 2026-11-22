// node --test scripts/agent-guides/eval/summarize.test.mjs
// The scoreboard (summarize.mjs) and the record's usage parsing (record.mjs).

import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, writeFileSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

import { render, readRecords, replaceScoreboard, main, START, END } from './summarize.mjs';
import { parseClaude, parseCodex, parseUsage, buildRecord, STEPS } from './record.mjs';

function record(overrides) {
  return {
    format: 1,
    runId: 'r',
    agent: 'claude',
    agentVersion: '2.1.300',
    model: 'claude-opus-5-5',
    skiljCommit: '0123456789abcdef',
    dirty: false,
    guidesHash: 'fedcba9876543210',
    task: 'add-command',
    linked: false,
    startedAt: '2026-11-20T10:00:00Z',
    durationSeconds: 60,
    lockWaitSeconds: 0,
    steps: Object.fromEntries(STEPS.map((step) => [step, 'pass'])),
    passed: true,
    failure: null,
    costUsd: 1.5,
    tokens: null,
    ...overrides,
  };
}

function withDir(files, fn) {
  const dir = mkdtempSync(join(tmpdir(), 'skilj-eval-summarize-'));
  try {
    for (const [name, content] of Object.entries(files)) {
      writeFileSync(join(dir, name), typeof content === 'string' ? content : JSON.stringify(content));
    }
    return fn(dir);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test('no records: says so, with the suite command', () => {
  const text = render([]);
  assert.match(text, /No runs recorded yet/);
  assert.match(text, /suite\.sh claude --model/);
  assert.match(text, /summarize\.mjs --write/);
});

test('renders a pass rate per task, agent, model and linked, with the latest run', () => {
  const text = render([
    record({ startedAt: '2026-11-20T10:00:00Z', passed: false }),
    record({ startedAt: '2026-11-21T10:00:00Z', passed: true, skiljCommit: 'aaaaaaaabbbb', dirty: true }),
    record({ linked: true, startedAt: '2026-11-19T10:00:00Z' }),
    record({ agent: 'codex', model: 'gpt-6', task: 'evolve-event', passed: false }),
  ]);
  const lines = text.split('\n');
  assert.equal(lines[0], '| Task | Agent | Model | Linked | Passed | Latest run | skilj commit | Guides |');
  assert.ok(lines.includes('| `add-command` | claude | claude-opus-5-5 | no | 1/2 | 2026-11-21 pass | aaaaaaaa+dirty | fedcba98 |'), text);
  assert.ok(lines.includes('| `add-command` | claude | claude-opus-5-5 | yes | 1/1 | 2026-11-19 pass | 01234567 | fedcba98 |'), text);
  assert.ok(lines.includes('| `evolve-event` | codex | gpt-6 | no | 0/1 | 2026-11-20 fail | 01234567 | fedcba98 |'), text);
  assert.equal(lines.length, 5);
});

test('the harness agents are left out unless --baselines', () => {
  const records = [record({ agent: 'none', model: null, passed: false }), record({ agent: 'reference', model: null })];
  assert.match(render(records), /No runs recorded yet/);
  const text = render(records, { baselines: true });
  assert.match(text, /\| none \| - \| no \| 0\/1 \|/);
  assert.match(text, /\| reference \| - \| no \| 1\/1 \|/);
});

test('a record of an unknown format is refused', () => {
  withDir({ 'a.json': record({ format: 2 }) }, (dir) => {
    assert.throws(() => readRecords(dir), /format 2/);
    assert.equal(main(['--results', dir]), 1);
  });
});

test('--write fills the markers and --check tells a stale scoreboard', () => {
  withDir({ 'a.json': record({}) }, (dir) => {
    const doc = join(dir, 'doc.md');
    writeFileSync(doc, `# Doc\n\n${START}\nold\n${END}\n\nAfter.\n`);
    assert.equal(main(['--results', dir, '--doc', doc, '--check']), 1);
    assert.equal(main(['--results', dir, '--doc', doc, '--write']), 0);
    const written = readFileSync(doc, 'utf8');
    assert.match(written, /\| `add-command` \| claude \|/);
    assert.match(written, /After\.\n$/);
    assert.equal(main(['--results', dir, '--doc', doc, '--check']), 0);
  });
});

test('a doc without the markers is an error', () => {
  assert.throws(() => replaceScoreboard('no markers', 'x'), /scoreboard:start/);
});

test('the repository doc matches the checked-in records', () => {
  assert.equal(main(['--check']), 0);
});

test('claude stream-json: cost and usage from the result event', () => {
  const log = [
    '{"type":"system","subtype":"init"}',
    'not json at all',
    '{"type":"assistant","message":{"usage":{"input_tokens":1}}}',
    '{"type":"result","subtype":"success","total_cost_usd":0.42,"usage":{"input_tokens":10,"output_tokens":20,"cache_read_input_tokens":30,"cache_creation_input_tokens":40}}',
  ].join('\n');
  assert.deepEqual(parseClaude(log), {
    costUsd: 0.42,
    tokens: { input: 10, output: 20, cacheRead: 30, cacheWrite: 40 },
  });
});

test('codex JSONL: usage summed over turns, no cost', () => {
  const log = [
    '{"type":"thread.started","thread_id":"t"}',
    '{"type":"turn.completed","usage":{"input_tokens":5,"cached_input_tokens":1,"output_tokens":2}}',
    '{"type":"turn.completed","usage":{"input_tokens":7,"cached_input_tokens":3,"output_tokens":4}}',
  ].join('\n');
  assert.deepEqual(parseCodex(log), { costUsd: null, tokens: { input: 12, output: 6, cacheRead: 4, cacheWrite: null } });
});

test('a log with no usable line gives nulls, never an error', () => {
  for (const agent of ['claude', 'codex', 'none', 'reference', 'other']) {
    assert.deepEqual(parseUsage(agent, 'Error: rate limited\n{"type":"result"'), { costUsd: null, tokens: null });
    assert.deepEqual(parseUsage(agent, ''), { costUsd: null, tokens: null });
  }
  assert.deepEqual(parseClaude('{"type":"result","total_cost_usd":"lots"}'), { costUsd: null, tokens: null });
});

test('a record lists every step, unrun where the run did not get to it', () => {
  const meta = { runId: 'r', agent: 'claude', task: 't', skiljCommit: 'c', guidesHash: 'g', startedAt: 's', durationSeconds: 1, failure: 'agent: claude exited with 1' };
  const failed = buildRecord(meta, null, '');
  assert.equal(failed.passed, false);
  assert.equal(failed.failure, 'agent: claude exited with 1');
  assert.deepEqual(Object.values(failed.steps), STEPS.map(() => 'unrun'));
  const check = { steps: { ...Object.fromEntries(STEPS.map((step) => [step, 'pass'])), static: 'skipped' }, failure: null };
  const passed = buildRecord({ ...meta, failure: null }, check, '');
  assert.equal(passed.passed, true);
  assert.equal(passed.format, 1);
  const failing = buildRecord({ ...meta, failure: null }, { steps: { ...check.steps, acceptance: 'fail' }, failure: 'acceptance: the acceptance tests fail' }, '');
  assert.equal(failing.passed, false);
  assert.equal(failing.failure, 'check: acceptance: the acceptance tests fail');
});
