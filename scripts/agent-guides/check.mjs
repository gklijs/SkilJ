#!/usr/bin/env node
// Checks skilj/agent-guides against the workspace it ships with, so a guide
// can't go on describing an API that changed (docs/architecture.md §248):
//
//   layout   every guide is a skill both agents can load: SKILL.md with a
//            `name` equal to its directory and a `description`, linked from
//            .claude/skills and .agents/skills, and listed in README.md and
//            in the template's AGENTS.md, which is checked like a guide
//   links    every relative markdown link, and its #anchor, resolves
//   paths    every repository path a guide names exists
//   traits   a `pub trait X { .. }` block lists exactly the items X has
//   names    every Rust path, snake_case, CamelCase or camelCase name a
//            guide puts in backticks occurs in the workspace's code, or is
//            listed in illustrative-names.txt as an example's own name
//
// Usage: node scripts/agent-guides/check.mjs
import { spawnSync } from 'node:child_process';
import { existsSync, lstatSync, readdirSync, readFileSync, readlinkSync } from 'node:fs';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const HERE = dirname(fileURLToPath(import.meta.url));
export const ROOT = resolve(HERE, '../..');
export const GUIDES = 'skilj/agent-guides';
export const TEMPLATE = 'templates/skilj-template';
// Where each agent looks for a repository's skills.
export const ENTRY_POINTS = ['.claude/skills', '.agents/skills'];

const walk = dir => readdirSync(dir, { withFileTypes: true }).flatMap(entry =>
  entry.isDirectory() ? walk(join(dir, entry.name)) : [join(dir, entry.name)]);

/** Splits markdown into prose and fenced code, both keeping line numbers. */
export function splitMarkdown(text) {
  const prose = [];
  const blocks = [];
  let block = null;
  text.split('\n').forEach((line, index) => {
    const fence = /^\s*```(\w*)/.exec(line);
    if (fence && !block) block = { lang: fence[1], line: index + 2, lines: [] };
    else if (fence) { blocks.push({ ...block, code: block.lines.join('\n') }); block = null; }
    else if (block) block.lines.push(line);
    else prose.push({ line: index + 1, text: line });
  });
  return { prose, blocks };
}

export function frontmatter(text) {
  const match = /^---\n([\s\S]*?)\n---\n/.exec(text);
  if (!match) return null;
  const fields = {};
  let key = null;
  for (const line of match[1].split('\n')) {
    const field = /^([a-z-]+):\s*(.*)$/.exec(line);
    if (field) { key = field[1]; fields[key] = field[2].replace(/^[>|]-?$/, ''); }
    else if (key) fields[key] += ` ${line.trim()}`;
  }
  for (const name of Object.keys(fields)) fields[name] = fields[name].trim();
  return fields;
}

/** GitHub's heading anchor: lowercased, punctuation dropped, spaces to dashes. */
export const slug = heading => heading.trim().toLowerCase()
  .replace(/[^\p{L}\p{N}\s_-]/gu, '').replace(/\s/g, '-');

export function anchors(text) {
  const found = new Set();
  for (const match of text.matchAll(/^#{1,6}\s+(.*)$/gm)) found.add(slug(match[1]));
  for (const match of text.matchAll(/<a\s+(?:id|name)="([^"]+)"/g)) found.add(match[1]);
  return found;
}

/** The item heads of a trait's body: `fn a() -> B`, `const C: D`, `type E: F`. */
export function traitItems(source, name) {
  const start = new RegExp(`pub trait ${name}\\b[^{]*\\{`).exec(source);
  if (!start) return null;
  const items = new Map();
  let depth = 1;
  let head = '';
  let hidden = false;
  let position = start.index + start[0].length;
  const body = source.slice(position);
  for (const raw of body.split('\n')) {
    const line = raw.replace(/\/\/.*$/, '').trim();
    if (depth === 1 && !head) {
      if (/^#\[(doc\(hidden\)|deprecated)/.test(line)) hidden = true;
      if (/^(async\s+)?(fn|const|type)\s/.test(line)) head = ' ';
    }
    if (head) {
      head += `${line} `;
      const end = /^(.*?)\s*(\{|;|=(?!=)|$)/.exec(head.trim());
      const complete = /[{;]/.test(line) || /=(?!=)/.test(line);
      if (complete) {
        const text = normalize(end[1]);
        const item = /^(?:async )?(?:fn|const|type) (\w+)/.exec(text);
        if (item && !hidden) items.set(item[1], text);
        head = '';
        hidden = false;
      }
    }
    for (const char of line) {
      if (char === '{') depth += 1;
      else if (char === '}') depth -= 1;
      if (depth === 0) return items;
    }
    position += raw.length + 1;
  }
  return items;
}

// `crate::blob::BlobField` in the source is `BlobField` in a guide.
const normalize = head => head.replace(/\s+/g, ' ').replace(/\b(?:[a-z_]+::)+/g, '')
  .replace(/\s*([(),<>:&])\s*/g, '$1').replace(/,\)/g, ')').replace(/\b_(?=[a-z])/g, '').trim();

// Words every Rust or markdown reader knows, and that mean nothing here.
const COMMON = new Set(`std core alloc self Self crate super String Vec Option Some None Ok Err Result
  Box Arc str bool i64 u64 u8 u32 usize serde serde_json schemars chrono tokio axum async await
  json true false null`.split(/\s+/));

// What a Rust example calls on a value of the standard library's or serde's.
const STD_METHODS = new Set(`await clone into to_string iter fold map last first unwrap expect is_empty
  len as_str push collect filter any all contains get insert to_owned ok_or unwrap_or parse try_into map_err is_some is_none`.split(/\s+/));

/** Names in a line's backticks that claim to exist in skilj's code. */
export function claimedNames(line) {
  const names = new Set();
  for (const [, code] of line.matchAll(/`([^`]+)`/g)) {
    // A path, a call, a macro or an attribute; not a file, a sentence, a
    // shell command or a JSON fragment.
    if (code.includes('/')) continue;
    if (/\s(?![^<(]*[>)])/.test(code.replace(/^#\[|\]$/g, '')) && !/^[\w:<>&' ]+$/.test(code)) continue;
    for (const [word] of code.matchAll(/[A-Za-z_][A-Za-z0-9_]*/g)) {
      if (COMMON.has(word) || word.length < 4) continue;
      const snake = /^[a-z0-9]+(_[a-z0-9]+)+$/.test(word);
      const camel = /^[A-Z][a-z0-9]+([A-Z][a-z0-9]*)+$/.test(word);
      const lowerCamel = /^[a-z]+([A-Z][a-z0-9]*)+$/.test(word);
      const constant = /^[A-Z0-9]+(_[A-Z0-9]+)+$/.test(word);
      if (snake || camel || lowerCamel || constant) names.add(word);
    }
  }
  return names;
}

function tracked(root) {
  const result = spawnSync('git', ['ls-files', '-z'], { cwd: root, encoding: 'utf8', maxBuffer: 1 << 28 });
  if (result.status !== 0) throw new Error(`git ls-files failed: ${result.stderr}`);
  return result.stdout.split('\0').filter(Boolean);
}

/** `.method(` and `SkiljBuilder::method` in a line's backticks: calls that must exist. */
export function claimedMethods(line) {
  const methods = new Set();
  for (const [, code] of line.matchAll(/`([^`]+)`/g)) {
    for (const [, name] of code.matchAll(/\.([a-z_][a-z0-9_]*)(?:::<[^>]*>)?\(/g)) methods.add(name);
    for (const [, name] of code.matchAll(/\bSkiljBuilder::([a-z_][a-z0-9_]*)/g)) methods.add(name);
  }
  return methods;
}

/** Every `fn` the workspace's own crates declare. */
export function functions(root = ROOT, files = tracked(root)) {
  const names = new Set();
  for (const file of files) {
    if (!/^skilj[a-z-]*\/src\/.*\.rs$/.test(file) || !existsSync(join(root, file))) continue;
    for (const [, name] of readFileSync(join(root, file), 'utf8').matchAll(/\bfn ([a-z_][a-z0-9_]*)/g)) names.add(name);
  }
  return names;
}

/** Every word in the workspace's code: what a guide's names are checked against. */
export function corpus(root = ROOT, files = tracked(root)) {
  const words = new Set();
  const code = /\.(rs|toml|sql|graphql|transcript|hcl|allium|json)$/;
  for (const file of files) {
    if (!code.test(file) || file.startsWith(`${GUIDES}/`) || file.startsWith('scripts/')) continue;
    if (!existsSync(join(root, file))) continue;
    for (const [word] of readFileSync(join(root, file), 'utf8').matchAll(/[A-Za-z_][A-Za-z0-9_]*/g)) {
      words.add(word);
    }
  }
  return words;
}

const readList = path => new Set(readFileSync(path, 'utf8').split('\n')
  .map(line => line.replace(/#.*$/, '').trim()).filter(Boolean));

export function check({ root = ROOT, files = tracked(root), words = corpus(root, files),
  fns = functions(root, files),
  illustrative = readList(join(HERE, 'illustrative-names.txt')) } = {}) {
  const problems = [];
  const guides = join(root, GUIDES);
  const names = readdirSync(guides, { withFileTypes: true })
    .filter(entry => entry.isDirectory()).map(entry => entry.name).sort();
  const index = existsSync(join(guides, 'README.md')) ? readFileSync(join(guides, 'README.md'), 'utf8') : '';
  if (!index) problems.push(`${GUIDES}/README.md: missing - it is the index a generated project's AGENTS.md points at`);

  const agentsPath = join(root, TEMPLATE, 'AGENTS.md');
  const agents = existsSync(agentsPath) ? readFileSync(agentsPath, 'utf8') : '';
  if (!agents) problems.push(`${TEMPLATE}/AGENTS.md: missing - it is how an agent in a generated project finds the guides`);

  // layout
  for (const name of names) {
    const skill = join(guides, name, 'SKILL.md');
    const at = `${GUIDES}/${name}/SKILL.md`;
    if (!existsSync(skill)) { problems.push(`${at}: missing`); continue; }
    const fields = frontmatter(readFileSync(skill, 'utf8'));
    if (!fields) problems.push(`${at}: no frontmatter`);
    else {
      if (fields.name !== name) problems.push(`${at}: name '${fields.name}' is not its directory's, '${name}'`);
      if (!fields.description) problems.push(`${at}: no description, so no agent knows when to load it`);
    }
    if (!index.includes(`${name}/SKILL.md`)) problems.push(`${GUIDES}/README.md: doesn't list ${name}/SKILL.md`);
    if (!agents.includes(`${name}/SKILL.md`)) problems.push(`${TEMPLATE}/AGENTS.md: doesn't list ${name}/SKILL.md`);
    for (const entry of ENTRY_POINTS) {
      const link = join(root, entry, name);
      const target = `../../${GUIDES}/${name}`;
      const stat = lstatSync(link, { throwIfNoEntry: false });
      if (!stat?.isSymbolicLink() || readlinkSync(link) !== target) {
        problems.push(`${entry}/${name}: should be a symlink to ${target}`);
      }
    }
  }
  for (const entry of ENTRY_POINTS) {
    if (!existsSync(join(root, entry))) continue;
    for (const item of readdirSync(join(root, entry), { withFileTypes: true })) {
      if (item.isSymbolicLink() && !names.includes(item.name)) {
        problems.push(`${entry}/${item.name}: links to a guide that no longer exists`);
      }
    }
  }

  const sources = new Map();
  const traitSource = name => {
    for (const file of files) {
      if (!/^skilj[a-z-]*\/src\/.*\.rs$/.test(file)) continue;
      if (!sources.has(file)) sources.set(file, readFileSync(join(root, file), 'utf8'));
      const items = traitItems(sources.get(file), name);
      if (items) return { file, items };
    }
    return null;
  };

  // The template's AGENTS.md makes the same kind of claims as a guide.
  const documents = walk(guides).filter(file => file.endsWith('.md')).sort();
  if (agents) documents.push(agentsPath);
  for (const path of documents) {
    const at = relative(root, path);
    const text = readFileSync(path, 'utf8');
    const { prose, blocks } = splitMarkdown(text);

    for (const { line, text: content } of prose) {
      // links
      for (const [, target] of content.matchAll(/\]\(([^)\s]+)\)/g)) {
        if (/^[a-z]+:/.test(target)) continue;
        const [file, anchor] = target.split('#');
        const resolved = file ? resolve(dirname(path), file) : path;
        if (!existsSync(resolved)) problems.push(`${at}:${line}: link to ${target}, which doesn't exist`);
        else if (anchor && !anchors(readFileSync(resolved, 'utf8')).has(anchor)) {
          problems.push(`${at}:${line}: link to ${target}, but ${relative(root, resolved)} has no such heading`);
        }
      }
      // paths: of the repository, or of a project generated from the template
      for (const [named] of content.replace(/\]\([^)]*\)/g, ']').matchAll(
        /(?<![\w./-])(?:skilj[a-z-]*|docs|templates|scripts|specs|src|tests)\/[\w./-]*\w\.(?:rs|md|toml|hcl|sh|mjs|allium)\b/g)) {
        if (![root, join(root, TEMPLATE), guides].some(base => existsSync(join(base, named)))) {
          problems.push(`${at}:${line}: names ${named}, which doesn't exist`);
        }
      }
      for (const name of claimedMethods(content)) {
        if (!fns.has(name) && !STD_METHODS.has(name) && !illustrative.has(name)) {
          problems.push(`${at}:${line}: \`.${name}()\` is a method no crate declares`);
        }
      }
      // names
      for (const name of claimedNames(content)) {
        if (!words.has(name) && !illustrative.has(name)) {
          problems.push(`${at}:${line}: \`${name}\` occurs nowhere in the workspace's code`);
        }
      }
    }

    // traits, and the calls of a Rust example
    for (const block of blocks) {
      if (block.lang === 'rust') {
        block.code.split('\n').forEach((code, offset) => {
          for (const name of claimedMethods(`\`${code.replace(/\/\/.*$/, '').replaceAll('`', '')}\``)) {
            if (!fns.has(name) && !STD_METHODS.has(name) && !illustrative.has(name)) {
              problems.push(`${at}:${block.line + offset}: \`.${name}()\` is a method no crate declares`);
            }
          }
        });
      }
      for (const [, name] of block.code.matchAll(/^pub trait (\w+)/gm)) {
        const shown = traitItems(block.code, name);
        const real = traitSource(name);
        if (!real) { problems.push(`${at}:${block.line}: shows trait ${name}, which no crate declares`); continue; }
        for (const [item, head] of shown) {
          if (!real.items.has(item)) problems.push(`${at}:${block.line}: ${name} has no \`${item}\` (${real.file})`);
          else if (real.items.get(item) !== head) {
            problems.push(`${at}:${block.line}: ${name}'s \`${head}\` is now \`${real.items.get(item)}\` (${real.file})`);
          }
        }
        for (const item of real.items.keys()) {
          if (!shown.has(item)) problems.push(`${at}:${block.line}: ${name}'s \`${item}\` is missing from the block (${real.file})`);
        }
      }
    }
  }
  return problems;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const problems = check();
  if (problems.length > 0) {
    console.error(`check-agent-guides: ${problems.length} problem(s):\n`);
    for (const problem of problems) console.error(`  ${problem}`);
    console.error('\nA name that only an example uses goes in scripts/agent-guides/illustrative-names.txt.');
    process.exit(1);
  }
  console.log('check-agent-guides: OK - the guides match the workspace.');
}
