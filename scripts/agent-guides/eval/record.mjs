#!/usr/bin/env node
// Writes the result record of one evaluation run (run.sh; docs/agent-guidance.md,
// docs/architecture.md §257): what ran, against what, and how each check
// step went. The agent's cost and token use are read from its log when its
// CLI reports them, and are null otherwise: a log the parser doesn't
// understand never fails the record, and the raw log stays in the run's
// directory.
//
// Usage: node record.mjs --out FILE --meta META.json [--check CHECK.json]
//        [--agent-log LOG]
//
// META.json holds the run's fields (runId, agent, agentVersion, model,
// skiljCommit, dirty, guidesHash, task, linked, startedAt, durationSeconds,
// lockWaitSeconds, failure); CHECK.json is what check.sh --result wrote.
// A third agent needs a parser here only if its CLI reports usage; the
// record's format stays the same.

import { readFileSync, writeFileSync, existsSync } from 'node:fs';
import { pathToFileURL } from 'node:url';

export const FORMAT = 1;
export const STEPS = ['own-tests', 'tests-added', 'build-local', 'build-no-local', 'static', 'acceptance', 'run'];

function jsonLines(text) {
  const values = [];
  for (const line of text.split('\n')) {
    const trimmed = line.trim();
    if (!trimmed.startsWith('{')) continue;
    try {
      values.push(JSON.parse(trimmed));
    } catch {
      // Not JSON: the CLI's own output between events.
    }
  }
  return values;
}

const number = (value) => (typeof value === 'number' && Number.isFinite(value) ? value : null);

// Claude Code's --output-format stream-json ends with a `result` event that
// carries the session's cost and usage.
export function parseClaude(text) {
  const results = jsonLines(text).filter((event) => event && event.type === 'result');
  const last = results.at(-1);
  if (!last) return { costUsd: null, tokens: null };
  const usage = last.usage && typeof last.usage === 'object' ? last.usage : null;
  const tokens = usage
    ? {
        input: number(usage.input_tokens),
        output: number(usage.output_tokens),
        cacheRead: number(usage.cache_read_input_tokens),
        cacheWrite: number(usage.cache_creation_input_tokens),
      }
    : null;
  const known = tokens && Object.values(tokens).some((value) => value !== null);
  return { costUsd: number(last.total_cost_usd), tokens: known ? tokens : null };
}

// Codex's exec --json reports usage per turn (`turn.completed`), and no cost.
export function parseCodex(text) {
  const turns = jsonLines(text).filter(
    (event) => event && event.type === 'turn.completed' && event.usage && typeof event.usage === 'object',
  );
  if (turns.length === 0) return { costUsd: null, tokens: null };
  const sum = (field) => {
    const values = turns.map((turn) => number(turn.usage[field])).filter((value) => value !== null);
    return values.length ? values.reduce((a, b) => a + b, 0) : null;
  };
  const tokens = { input: sum('input_tokens'), output: sum('output_tokens'), cacheRead: sum('cached_input_tokens'), cacheWrite: null };
  const known = Object.values(tokens).some((value) => value !== null);
  return { costUsd: null, tokens: known ? tokens : null };
}

// skilj-agent's `task --json` ends with one record line
// ("record":"skilj-agent-task") holding the tokens its model server reported.
// A local model has no cost.
export function parseSkiljAgent(text) {
  const last = jsonLines(text)
    .filter((event) => event && event.record === 'skilj-agent-task')
    .at(-1);
  if (!last || !last.tokens || typeof last.tokens !== 'object') return { costUsd: null, tokens: null };
  const tokens = { input: number(last.tokens.input), output: number(last.tokens.output), cacheRead: null, cacheWrite: null };
  const known = tokens.input !== null || tokens.output !== null;
  return { costUsd: null, tokens: known ? tokens : null };
}

const PARSERS = { claude: parseClaude, codex: parseCodex, 'skilj-agent': parseSkiljAgent };

export function parseUsage(agent, text) {
  const parse = PARSERS[agent];
  if (!parse || typeof text !== 'string') return { costUsd: null, tokens: null };
  try {
    return parse(text);
  } catch {
    return { costUsd: null, tokens: null };
  }
}

// The record: every step is listed, `unrun` where the run didn't get to it.
export function buildRecord(meta, check, agentLog) {
  const steps = Object.fromEntries(STEPS.map((step) => [step, check?.steps?.[step] ?? 'unrun']));
  const failure = meta.failure ?? (check?.failure ? `check: ${check.failure}` : null);
  const passed = failure === null && check !== null && !Object.values(steps).includes('fail');
  const { costUsd, tokens } = parseUsage(meta.agent, agentLog);
  return {
    format: FORMAT,
    runId: meta.runId,
    agent: meta.agent,
    agentVersion: meta.agentVersion ?? null,
    model: meta.model ?? null,
    skiljCommit: meta.skiljCommit,
    dirty: Boolean(meta.dirty),
    guidesHash: meta.guidesHash,
    task: meta.task,
    linked: Boolean(meta.linked),
    startedAt: meta.startedAt,
    durationSeconds: meta.durationSeconds,
    lockWaitSeconds: meta.lockWaitSeconds ?? 0,
    steps,
    passed,
    failure,
    costUsd,
    tokens,
  };
}

function main(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i += 2) {
    if (!argv[i].startsWith('--') || argv[i + 1] === undefined) {
      console.error('record: usage: record.mjs --out FILE --meta META.json [--check CHECK.json] [--agent-log LOG]');
      return 2;
    }
    args[argv[i].slice(2)] = argv[i + 1];
  }
  if (!args.out || !args.meta) {
    console.error('record: --out and --meta are required');
    return 2;
  }
  const meta = JSON.parse(readFileSync(args.meta, 'utf8'));
  let check = null;
  if (args.check && existsSync(args.check)) {
    try {
      check = JSON.parse(readFileSync(args.check, 'utf8'));
    } catch {
      check = null;
    }
  }
  const log = args['agent-log'] && existsSync(args['agent-log']) ? readFileSync(args['agent-log'], 'utf8') : '';
  const record = buildRecord(meta, check, log);
  writeFileSync(args.out, `${JSON.stringify(record, null, 2)}\n`);
  return 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exitCode = main(process.argv.slice(2));
}
