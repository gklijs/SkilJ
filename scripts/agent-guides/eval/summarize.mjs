#!/usr/bin/env node
// Summarizes the evaluation records in scripts/agent-guides/eval/results/
// (run.sh, suite.sh) into the scoreboard of docs/agent-guidance.md: per
// task, agent, model and linked or not, the pass rate and the latest run's
// date, skilj commit and guides hash (docs/architecture.md §257).
//
// The harness agents `none` and `reference` are left out unless
// --baselines: they test the pipeline, not the guides.
//
// Usage: node scripts/agent-guides/eval/summarize.mjs [--results DIR] [--baselines]
//        [--write | --check] [--doc FILE]
//        prints the scoreboard; --write puts it between the doc's
//        <!-- scoreboard:start --> and <!-- scoreboard:end --> markers;
//        --check exits 1 when the doc's scoreboard isn't what the records give.

import { readdirSync, readFileSync, writeFileSync, existsSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
const root = resolve(here, '../../..');
export const FORMATS = [1];
export const HARNESS_AGENTS = ['none', 'reference'];
export const START = '<!-- scoreboard:start -->';
export const END = '<!-- scoreboard:end -->';

export function readRecords(dir) {
  if (!existsSync(dir)) return [];
  return readdirSync(dir)
    .filter((name) => name.endsWith('.json'))
    .sort()
    .map((name) => {
      const record = JSON.parse(readFileSync(join(dir, name), 'utf8'));
      if (!FORMATS.includes(record.format)) {
        throw new Error(`${name}: record format ${JSON.stringify(record.format)} is not one this script reads (${FORMATS.join(', ')})`);
      }
      return record;
    });
}

export function render(records, { baselines = false } = {}) {
  const counted = records.filter((record) => baselines || !HARNESS_AGENTS.includes(record.agent));
  if (counted.length === 0) {
    return [
      'No runs recorded yet. Run the suite for an agent and model, then regenerate this',
      'section:',
      '',
      '```sh',
      'scripts/agent-guides/eval/suite.sh claude --model <model> [--link]',
      'node scripts/agent-guides/eval/summarize.mjs --write',
      '```',
    ].join('\n');
  }
  const groups = new Map();
  for (const record of counted) {
    const key = [record.task, record.agent, record.model ?? '', record.linked ? 'yes' : 'no'].join('\u0000');
    if (!groups.has(key)) groups.set(key, []);
    groups.get(key).push(record);
  }
  const rows = [...groups.values()].map((runs) => {
    runs.sort((a, b) => String(a.startedAt).localeCompare(String(b.startedAt)));
    const latest = runs.at(-1);
    const passed = runs.filter((run) => run.passed).length;
    return {
      task: latest.task,
      agent: latest.agent,
      model: latest.model ?? '-',
      linked: latest.linked ? 'yes' : 'no',
      rate: `${passed}/${runs.length}`,
      latest: `${String(latest.startedAt).slice(0, 10)} ${latest.passed ? 'pass' : 'fail'}`,
      commit: `${String(latest.skiljCommit).slice(0, 8)}${latest.dirty ? '+dirty' : ''}`,
      guides: String(latest.guidesHash).slice(0, 8),
    };
  });
  rows.sort((a, b) =>
    [a.task, a.agent, a.model, a.linked].join('\u0000').localeCompare([b.task, b.agent, b.model, b.linked].join('\u0000')),
  );
  const lines = [
    '| Task | Agent | Model | Linked | Passed | Latest run | skilj commit | Guides |',
    '|---|---|---|---|---|---|---|---|',
    ...rows.map((row) => `| \`${row.task}\` | ${row.agent} | ${row.model} | ${row.linked} | ${row.rate} | ${row.latest} | ${row.commit} | ${row.guides} |`),
  ];
  return lines.join('\n');
}

export function replaceScoreboard(doc, scoreboard) {
  const start = doc.indexOf(START);
  const end = doc.indexOf(END);
  if (start < 0 || end < start) throw new Error(`the doc has no ${START} ... ${END} section`);
  return `${doc.slice(0, start + START.length)}\n${scoreboard}\n${doc.slice(end)}`;
}

export function main(argv) {
  let results = process.env.SKILJ_EVAL_RESULTS || join(here, 'results');
  let doc = join(root, 'docs/agent-guidance.md');
  let mode = 'print';
  let baselines = false;
  for (let i = 0; i < argv.length; i += 1) {
    const arg = argv[i];
    if (arg === '--results' && argv[i + 1]) results = argv[++i];
    else if (arg === '--doc' && argv[i + 1]) doc = argv[++i];
    else if (arg === '--write') mode = 'write';
    else if (arg === '--check') mode = 'check';
    else if (arg === '--baselines') baselines = true;
    else {
      console.error(`summarize: unknown argument ${arg}`);
      return 2;
    }
  }
  let scoreboard;
  try {
    scoreboard = render(readRecords(results), { baselines });
  } catch (error) {
    console.error(`summarize: ${error.message}`);
    return 1;
  }
  if (mode === 'print') {
    console.log(scoreboard);
    return 0;
  }
  const current = readFileSync(doc, 'utf8');
  let next;
  try {
    next = replaceScoreboard(current, scoreboard);
  } catch (error) {
    console.error(`summarize: ${doc}: ${error.message}`);
    return 1;
  }
  if (mode === 'write') {
    writeFileSync(doc, next);
    return 0;
  }
  if (next !== current) {
    console.error(`summarize: ${doc}'s scoreboard isn't what the records in ${results} give: run node scripts/agent-guides/eval/summarize.mjs --write`);
    return 1;
  }
  return 0;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  process.exitCode = main(process.argv.slice(2));
}
