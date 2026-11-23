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
//   codes    skilj/src/error_codes.md, the one table of codes, lists exactly
//            the codes the non-test sources produce (docs/architecture.md
//            §258), with the hint and guide skilj-codegen's CODES and
//            skilj-core's GUIDANCE give them; every guide pointer names a
//            skill and an anchor its SKILL.md has; and a code-shaped name a
//            guide puts in backticks (or a SKILL.md's frontmatter in double
//            quotes) is a live code, an instance of a pattern row, or listed
//            in non-code-names.txt or illustrative-names.txt. A retired code
//            in a guide fails wherever it stands; a one-word one only in
//            backticks or quotes, so an ordinary word doesn't trip it.
//
//            The producer forms, and where each one's code sits:
//              fn code(&self) arms           the arm's literal; `x.code()`
//                                            delegates; `kind` in
//                                            CommandRejected is a business
//                                            rejection; `code` in BatchFailed
//                                            is copied
//              BatchFailed { code: X, .. }   `"lit".to_string()`, or a copied
//                                            `x.code().to_string()`
//              skilj-rest fn parts(self)     the tuple's second element
//              coded(X, ..), budget_error(X, ..), set_code(_, X, ..)
//                                            (skilj-graphql) the literal;
//                                            the helpers' own parameter
//              not_found(X, ..), create_type_token_field!'s fifth argument
//                                            an instance of <entity>_not_found
//              batch_item_error's unwrap_or_else(|| "lit".to_string())
//              problem(X, ..), problems.push(X, ..), Diagnostic::new(X, ..)
//                                            (skilj-codegen) the literal
//              tracing::warn!/error!(code = X) (skilj/src/lib.rs)
//                                            `%"lit"`, or a delegating
//                                            `%e.code()`
//              Rejected/CommandRejected { kind: "lit".to_string(), .. },
//              and every snake_case literal of a fn returning a StanceRefusal
//                                            (skilj-core) a rejection kind
//                                            skilj itself gives, listed in
//                                            its own section of the table
//            Any other expression in one of those places, and a direct
//            `.set("code", ..)` outside set_code, fails as unclassified.
//            Gap: a row deleted instead of moved to "Retired codes" is only
//            caught where a guide still names it as a code-shaped token.
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
// The one table of codes, and the two Rust tables of hints and guides.
export const CODE_TABLE = 'skilj/src/error_codes.md';
export const CODEGEN_CODES = 'skilj-codegen/src/codes.rs';
export const CORE_GUIDANCE = 'skilj-core/src/error.rs';

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
  Box Arc str bool i64 u64 u8 u32 usize serde serde_json schemars jiff tokio axum async await
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


// --- codes: the table, the Rust tables and the producers (docs/architecture.md §258) ---

/**
 * Rust source with comments and the contents of string and char literals
 * blanked to spaces, newlines and quote delimiters kept, so brackets can be
 * matched and forms found without a literal or a comment fooling either.
 */
export function maskRust(src) {
  const out = src.split('');
  const blank = (from, to) => { for (let k = from; k < to; k++) if (out[k] !== '\n') out[k] = ' '; };
  let i = 0;
  while (i < src.length) {
    const c = src[i];
    const next = src[i + 1];
    if (c === '/' && next === '/') {
      const end = src.indexOf('\n', i);
      const stop = end < 0 ? src.length : end;
      blank(i, stop);
      i = stop;
      continue;
    }
    if (c === '/' && next === '*') {
      let depth = 1;
      let j = i + 2;
      while (j < src.length && depth > 0) {
        if (src[j] === '/' && src[j + 1] === '*') { depth += 1; j += 2; }
        else if (src[j] === '*' && src[j + 1] === '/') { depth -= 1; j += 2; }
        else j += 1;
      }
      blank(i, j);
      i = j;
      continue;
    }
    const raw = /^b?r(#*)"/.exec(src.slice(i, i + 16));
    if (raw && !/[\w]/.test(src[i - 1] ?? '')) {
      const close = `"${raw[1]}`;
      const start = i + raw[0].length;
      const end = src.indexOf(close, start);
      const stop = end < 0 ? src.length : end;
      blank(start, stop);
      i = stop + close.length;
      continue;
    }
    if (c === '"') {
      let j = i + 1;
      while (j < src.length && src[j] !== '"') j += src[j] === '\\' ? 2 : 1;
      blank(i + 1, j);
      i = j + 1;
      continue;
    }
    if (c === "'") {
      const char = /^'(?:\\(?:x[0-9a-fA-F]{2}|u\{[0-9a-fA-F]+\}|.)|[^\\'\n])'/.exec(src.slice(i, i + 14));
      if (char) { blank(i + 1, i + char[0].length - 1); i += char[0].length; continue; }
    }
    i += 1;
  }
  return out.join('');
}

const OPEN = { '(': ')', '[': ']', '{': '}' };

/** The index of the bracket closing the one at `open`, in masked source. */
export function closing(masked, open) {
  const stack = [];
  for (let i = open; i < masked.length; i++) {
    const c = masked[i];
    if (OPEN[c]) stack.push(OPEN[c]);
    else if (c === ')' || c === ']' || c === '}') {
      stack.pop();
      if (stack.length === 0) return i;
    }
  }
  return masked.length;
}

/** `[start, end)` of each comma-separated part between `from` and `to`. */
function splitTop(masked, from, to) {
  const parts = [];
  let depth = 0;
  let start = from;
  for (let i = from; i < to; i++) {
    const c = masked[i];
    if (OPEN[c]) depth += 1;
    else if (c === ')' || c === ']' || c === '}') depth -= 1;
    else if (c === ',' && depth === 0) { parts.push([start, i]); start = i + 1; }
  }
  if (masked.slice(start, to).trim()) parts.push([start, to]);
  return parts;
}

/** A part of masked source, trimmed, with its offset. */
function trimmed(masked, [start, end]) {
  const text = masked.slice(start, end);
  const lead = text.length - text.trimStart().length;
  return { text: text.trim(), at: start + lead };
}

/** The literal whose opening quote is at `at`, or null. */
function literalAt(masked, original, at) {
  if (masked[at] !== '"') return null;
  const end = masked.indexOf('"', at + 1);
  return original.slice(at + 1, end);
}

/** The arms of the match block opening at `open`: pattern and result. */
function matchArms(masked, open) {
  const end = closing(masked, open);
  const arms = [];
  let depth = 0;
  let armStart = open + 1;
  for (let i = open + 1; i < end; i++) {
    const c = masked[i];
    if (OPEN[c]) { depth += 1; continue; }
    if (c === ')' || c === ']' || c === '}') { depth -= 1; continue; }
    if (depth !== 0 || c !== '=' || masked[i + 1] !== '>') continue;
    const pattern = masked.slice(armStart, i);
    let j = i + 2;
    while (/\s/.test(masked[j])) j += 1;
    let exprEnd;
    if (masked[j] === '{') {
      exprEnd = closing(masked, j) + 1;
    } else {
      let d = 0;
      exprEnd = j;
      while (exprEnd < end) {
        const e = masked[exprEnd];
        if (OPEN[e]) d += 1;
        else if (e === ')' || e === ']' || e === '}') d -= 1;
        else if (e === ',' && d === 0) break;
        exprEnd += 1;
      }
    }
    arms.push({ pattern, start: j, end: exprEnd });
    i = exprEnd;
    while (i < end && /[\s,]/.test(masked[i])) i += 1;
    armStart = i;
    i -= 1;
  }
  return arms;
}

/** An arm's result: a braced block's last expression, or the expression. */
function armResult(masked, { start, end }) {
  if (masked[start] !== '{') return trimmed(masked, [start, end]);
  const inner = masked.slice(start + 1, end - 1);
  let depth = 0;
  let last = 0;
  for (let i = 0; i < inner.length; i++) {
    const c = inner[i];
    if (OPEN[c]) depth += 1;
    else if (c === ')' || c === ']' || c === '}') {
      depth -= 1;
      if (depth === 0 && c === '}' && /^\s*$/.test(inner.slice(i + 1).split('\n')[0] ?? '')) last = i + 1;
    } else if (c === ';' && depth === 0) last = i + 1;
  }
  return trimmed(masked, [start + 1 + last, end - 1]);
}

/** The name of the `fn` enclosing `at`. */
function enclosingFn(masked, at) {
  let name = null;
  for (const match of masked.slice(0, at).matchAll(/\bfn\s+(\w+)/g)) name = match[1];
  return name;
}

/**
 * The source with its test code blanked: an inline `#[cfg(test)] mod` up to
 * its closing brace. Returns the masked source and the files a
 * `#[cfg(test)] mod name;` declares, which aren't scanned at all.
 */
export function withoutTests(masked, file) {
  const out = masked.split('');
  const testFiles = [];
  for (const match of masked.matchAll(/#\[cfg\(test\)\]((?:\s*#\[[^\]]*\])*)\s*(?:pub(?:\([^)]*\))?\s+)?mod\s+(\w+)\s*([{;])/g)) {
    const name = match[2];
    if (match[3] === ';') {
      const dir = dirname(file);
      const base = /\/(mod|lib|main)\.rs$/.test(file) ? dir : file.replace(/\.rs$/, '');
      testFiles.push(`${base}/${name}.rs`, `${base}/${name}/mod.rs`);
      continue;
    }
    const open = match.index + match[0].length - 1;
    const end = closing(masked, open);
    for (let k = match.index; k <= end; k++) if (out[k] !== '\n') out[k] = ' ';
  }
  return { masked: out.join(''), testFiles };
}

/** The sources the producer inventory reads. */
export const producerSource = file =>
  /^(skilj-core|skilj-rest|skilj-graphql|skilj-codegen)\/src\/.*\.rs$/.test(file) || file === 'skilj/src/lib.rs';

/**
 * Every code the non-test sources produce, from the forms in this file's
 * header comment: `codes` (code -> files), `instances` (the entities of
 * `<entity>_not_found`), and `unclassified`, each a site in a producer
 * form whose code expression isn't one the forms accept.
 */
export function producers(root = ROOT, files = tracked(root)) {
  const codes = new Map();
  const kinds = new Map();
  const instances = new Set();
  const unclassified = [];
  const scanned = files.filter(producerSource).filter(file => existsSync(join(root, file)));
  const sources = scanned.map(file => {
    const original = readFileSync(join(root, file), 'utf8');
    return { file, original, ...withoutTests(maskRust(original), file) };
  });
  const excluded = new Set(sources.flatMap(source => source.testFiles));
  const lineOf = (text, at) => text.slice(0, at).split('\n').length;
  for (const { file, original, masked } of sources) {
    if (excluded.has(file)) continue;
    const produce = code => { if (!codes.has(code)) codes.set(code, new Set()); codes.get(code).add(file); };
    const unknown = (at, what) => unclassified.push(`${file}:${lineOf(original, at)}: ${what}`);
    const literal = ({ text, at }) => (/^"\s*"$/.test(text) ? literalAt(masked, original, at) : null);
    const callArgs = open => splitTop(masked, open + 1, closing(masked, open)).map(part => trimmed(masked, part));
    const isDefinition = at => /\bfn\s+$/.test(masked.slice(Math.max(0, at - 8), at)) ||
      /macro_rules!\s*$/.test(masked.slice(Math.max(0, at - 16), at));
    const crate = file.split('/')[0];

    if (['skilj-core', 'skilj-rest', 'skilj-graphql'].includes(crate)) {
      // fn code(&self)
      for (const match of masked.matchAll(/\bfn code\(&self\)[^{;]*\{/g)) {
        const body = match.index + match[0].length - 1;
        const bodyEnd = closing(masked, body);
        const inner = masked.slice(body + 1, bodyEnd);
        const matchSelf = /\bmatch\s+self\s*\{/.exec(inner);
        if (!matchSelf) {
          const expression = inner.trim();
          if (!/^(?:[\w:]+::code\(self\)|\w+\.code\(\))$/.test(expression)) unknown(body, `fn code returns \`${expression}\``);
          continue;
        }
        const open = body + 1 + matchSelf.index + matchSelf[0].length - 1;
        for (const arm of matchArms(masked, open)) {
          const result = armResult(masked, arm);
          const code = literal(result);
          if (code !== null) produce(code);
          else if (/^\w+\.code\(\)$/.test(result.text)) { /* delegation */ }
          else if (result.text === 'kind' && /CommandRejected/.test(arm.pattern)) { /* business */ }
          else if (result.text === 'code' && /BatchFailed/.test(arm.pattern)) { /* copied */ }
          else unknown(result.at, `fn code arm \`${result.text}\``);
        }
      }
    }
    if (crate === 'skilj-core') {
      // Rejection kinds skilj's own decisions give: a Rejected/CommandRejected
      // construction's literal `kind`, and every snake_case literal of a
      // function that returns a StanceRefusal `(reason, kind)`.
      const produceKind = kind => { if (!kinds.has(kind)) kinds.set(kind, new Set()); kinds.get(kind).add(file); };
      for (const match of masked.matchAll(/\b(?:Rejected|CommandRejected)\s*\{/g)) {
        const open = match.index + match[0].length - 1;
        for (const part of splitTop(masked, open + 1, closing(masked, open))) {
          const field = trimmed(masked, part);
          const kind = /^kind\s*:\s*/.exec(field.text);
          if (!kind) continue;
          const value = { text: field.text.slice(kind[0].length), at: field.at + kind[0].length };
          const lit = /^"\s*"\.(?:to_string|into|to_owned)\(\)$/.test(value.text) ? literalAt(masked, original, value.at) : null;
          if (lit !== null) produceKind(lit);
          else if (!/^\w+(?:\.(?:to_string|into|to_owned|clone)\(\))?$/.test(value.text)) unknown(value.at, `rejection kind \`${value.text}\``);
        }
      }
      for (const match of masked.matchAll(/\bfn\s+\w+\s*\(/g)) {
        const open = masked.indexOf('{', match.index);
        if (open < 0 || !/\bStanceRefusal\b/.test(masked.slice(match.index, open))) continue;
        const end = closing(masked, open);
        for (let at = masked.indexOf('"', open); at >= 0 && at < end; at = masked.indexOf('"', masked.indexOf('"', at + 1) + 1)) {
          const lit = literalAt(masked, original, at);
          if (/^[a-z][a-z0-9]*(_[a-z0-9]+)+$/.test(lit)) produceKind(lit);
        }
      }
      for (const match of masked.matchAll(/\bBatchFailed\s*\{\s*code\s*:/g)) {
        const start = match.index + match[0].length;
        const end = splitTop(masked, start, closing(masked, masked.indexOf('{', match.index)))[0][1];
        const value = trimmed(masked, [start, end]);
        const lit = /^"\s*"\.to_string\(\)$/.test(value.text) ? literalAt(masked, original, value.at) : null;
        if (value.text === 'String') continue; // the variant's own declaration
        if (lit !== null) produce(lit);
        else if (!/^\w+\.code\(\)\.to_string\(\)$/.test(value.text)) unknown(value.at, `BatchFailed code \`${value.text}\``);
      }
    }
    if (file === 'skilj-rest/src/error.rs') {
      const parts = /\bfn parts\(self\)[^{]*\{/.exec(masked);
      if (parts) {
        const body = parts.index + parts[0].length - 1;
        const matchSelf = /\bmatch\s+self\s*\{/.exec(masked.slice(body, closing(masked, body)));
        const open = body + matchSelf.index + matchSelf[0].length - 1;
        for (const arm of matchArms(masked, open)) {
          const result = armResult(masked, arm);
          const tuple = result.text.startsWith('(') ? callArgs(result.at) : [];
          const second = tuple[1];
          if (!second) { unknown(result.at, `fn parts arm \`${result.text}\``); continue; }
          const lit = /^"\s*"\.to_string\(\)$/.test(second.text) ? literalAt(masked, original, second.at) : null;
          if (lit !== null) produce(lit);
          else if (!/^\w+\.code\(\)\.to_string\(\)$/.test(second.text)) unknown(second.at, `fn parts code \`${second.text}\``);
        }
      }
    }
    if (crate === 'skilj-graphql') {
      const helpers = new Set(['to_graphql_error', 'coded', 'budget_error', 'not_found', 'set_code']);
      for (const match of masked.matchAll(/\b(coded|budget_error|not_found|set_code)\s*\(/g)) {
        if (isDefinition(match.index)) continue;
        const args = callArgs(match.index + match[0].length - 1);
        const position = match[1] === 'set_code' ? 1 : 0;
        const arg = args[position];
        const lit = arg ? literal(arg) : null;
        const inHelper = helpers.has(enclosingFn(masked, match.index));
        if (lit !== null) {
          if (match[1] === 'not_found') instances.add(lit);
          else produce(lit);
        } else if (match[1] === 'not_found' && arg?.text === '$type_label') { /* the macro's own */ }
        else if (!inHelper) unknown(match.index, `${match[1]}(..) with code \`${arg?.text}\``);
      }
      for (const match of masked.matchAll(/\bcreate_type_token_field!\s*\(/g)) {
        const label = callArgs(match.index + match[0].length - 1)[4];
        const lit = label ? literal(label) : null;
        if (lit !== null) instances.add(lit);
        else unknown(match.index, 'create_type_token_field! without a literal type label');
      }
      for (const match of masked.matchAll(/\.set\(\s*"/g)) {
        const at = match.index + match[0].length - 1;
        if (literalAt(masked, original, at) === 'code' && enclosingFn(masked, match.index) !== 'set_code') {
          unknown(match.index, 'sets "code" directly; use coded(..) or set_code(..)');
        }
      }
      const batch = /\bfn batch_item_error\b[^{]*\{/.exec(masked);
      if (batch) {
        const body = masked.slice(batch.index, closing(masked, batch.index + batch[0].length - 1));
        for (const match of body.matchAll(/unwrap_or_else\(\|\|\s*"/g)) {
          produce(literalAt(masked, original, batch.index + match.index + match[0].length - 1));
        }
      }
    }
    if (crate === 'skilj-codegen') {
      for (const match of masked.matchAll(/\b(problem|problems\.push|Diagnostic::new)\s*\(/g)) {
        if (isDefinition(match.index)) continue;
        const arg = callArgs(match.index + match[0].length - 1)[0];
        // A vector's push of a problem(..), which is the form itself.
        if (match[1] === 'problems.push' && /^(?:crate::)?problem\(/.test(arg?.text ?? '')) continue;
        const lit = arg ? literal(arg) : null;
        if (lit !== null) produce(lit);
        else if (!(arg?.text === 'code' && ['push', 'problem', 'new'].includes(enclosingFn(masked, match.index)))) {
          unknown(match.index, `${match[1]}(..) with code \`${arg?.text}\``);
        }
      }
    }
    if (file === 'skilj/src/lib.rs') {
      for (const match of masked.matchAll(/\btracing::(?:warn|error)!\s*\(/g)) {
        const open = match.index + match[0].length - 1;
        for (const part of splitTop(masked, open + 1, closing(masked, open))) {
          const field = trimmed(masked, part);
          const code = /^code\s*=\s*(%?)\s*/.exec(field.text);
          if (!code) continue;
          const value = { text: field.text.slice(code[0].length), at: field.at + code[0].length };
          const lit = literal(value);
          if (lit !== null) produce(lit);
          else if (!/^%\w+\.code\(\)$/.test(field.text.replace(/^code\s*=\s*/, ''))) unknown(field.at, `log field \`${field.text}\``);
        }
      }
    }
  }
  return { codes, kinds, instances, unclassified };
}

const rustString = text => text.replace(/\\(.)/g, '$1');

/** skilj-codegen's CODES: code -> { category, hint, guide }. */
export function codegenCodes(source) {
  const codes = new Map();
  const string = '"((?:[^"\\\\]|\\\\.)*)"';
  for (const match of source.matchAll(new RegExp(`\\bcode\\(${string},\\s*${string}\\)`, 'g'))) {
    codes.set(match[1], { category: match[2], hint: null, guide: null });
  }
  for (const match of source.matchAll(new RegExp(`\\bguided\\(${string},\\s*${string},\\s*${string},\\s*${string}\\)`, 'g'))) {
    codes.set(match[1], { category: match[2], hint: rustString(match[3]), guide: rustString(match[4]) });
  }
  return codes;
}

/** skilj-core's GUIDANCE: code -> { hint, guide }. */
export function coreGuidance(source) {
  const guidance = new Map();
  const string = '"((?:[^"\\\\]|\\\\.)*)"';
  for (const match of source.matchAll(new RegExp(`\\bentry\\(\\s*${string},\\s*${string},\\s*${string},?\\s*\\)`, 'g'))) {
    guidance.set(match[1], { hint: rustString(match[2]), guide: rustString(match[3]) });
  }
  return guidance;
}

/**
 * The rows of skilj/src/error_codes.md: `| Code | Where | Hint | Guide |`,
 * each cell a code span or empty, under its section's heading. A pattern
 * row's Code cell lists its instances after the pattern.
 */
export function codeTable(text) {
  const rows = [];
  let section = null;
  for (const line of text.split('\n')) {
    const heading = /^##\s+(.*)$/.exec(line);
    if (heading) { section = heading[1].trim(); continue; }
    if (!section || !line.startsWith('|')) continue;
    const cells = line.split('|').slice(1, -1).map(cell => cell.trim());
    const spans = [...(cells[0] ?? '').matchAll(/`([^`]+)`/g)].map(match => match[1]);
    if (spans.length === 0) continue;
    const span = cell => { const match = /^`([^`]*)`$/.exec(cell ?? ''); return match ? match[1] : null; };
    rows.push({ section, code: spans[0], instances: spans.slice(1), where: cells[1] ?? '',
      hint: span(cells[2]), guide: span(cells[3]) });
  }
  return rows;
}

const CODE_SHAPED = /^[A-Za-z][A-Za-z0-9]*(_[A-Za-z0-9]+)+$/;
export const codeShaped = token => CODE_SHAPED.test(token) && token !== token.toUpperCase();

/** The code-shaped names a guide line puts in backticks, or (frontmatter) in double quotes. */
export function namedCodes(line, inFrontmatter = false) {
  const names = [...line.matchAll(/`([^`]+)`/g)].map(match => match[1].trim());
  if (inFrontmatter) names.push(...[...line.matchAll(/"([^"\s]+)"/g)].map(match => match[1]));
  return names.filter(codeShaped);
}

/** The problems of the codes check; see this file's header comment. */
export function checkCodes({ root, files, documents, guides, nonCodeNames, illustrative,
  inventory = producers(root, files) }) {
  const problems = [];
  const read = file => (existsSync(join(root, file)) ? readFileSync(join(root, file), 'utf8') : null);
  const tableText = read(CODE_TABLE);
  if (tableText === null) return [`${CODE_TABLE}: missing - it is the one list of codes (docs/architecture.md §258)`];
  const rows = codeTable(tableText);
  const retired = new Set(rows.filter(row => /^Retired/i.test(row.section)).map(row => row.code));
  const live = rows.filter(row => !/^Retired/i.test(row.section));
  const pattern = '<entity>_not_found';
  const liveCodes = new Set(live.map(row => row.code));
  const codegen = codegenCodes(read(CODEGEN_CODES) ?? '');
  const core = coreGuidance(read(CORE_GUIDANCE) ?? '');

  for (const site of inventory.unclassified) problems.push(`${site}: unclassified code producer`);
  // 1 and 2: the table lists exactly what the sources produce.
  // Rejection kinds skilj's own decisions give have a section of their own,
  // held to the kinds the sources produce as the others are to the codes.
  const isKinds = row => /^Rejection kinds/i.test(row.section);
  const kindRows = new Set(live.filter(isKinds).map(row => row.code));
  const codeRows = new Set(live.filter(row => !isKinds(row)).map(row => row.code));
  for (const row of live) {
    if (row.code === pattern) continue;
    const produced = isKinds(row) ? inventory.kinds ?? new Map() : inventory.codes;
    if (!produced.has(row.code)) {
      problems.push(`${CODE_TABLE}: \`${row.code}\` is a row no code path produces - move it to "Retired codes"`);
    }
  }
  for (const [code, where] of inventory.codes) {
    if (!codeRows.has(code)) problems.push(`${[...where][0]}: produces \`${code}\`, which ${CODE_TABLE} doesn't list`);
  }
  for (const [kind, where] of inventory.kinds ?? new Map()) {
    if (!kindRows.has(kind)) {
      problems.push(`${[...where][0]}: gives the rejection kind \`${kind}\`, which ${CODE_TABLE}'s "Rejection kinds skilj itself gives" doesn't list`);
    }
  }
  const patternRow = live.find(row => row.code === pattern);
  if (inventory.instances.size > 0 || patternRow) {
    const listed = new Set(patternRow?.instances ?? []);
    for (const entity of inventory.instances) {
      if (!listed.has(entity)) problems.push(`${CODE_TABLE}: \`${pattern}\` doesn't list the instance \`${entity}\``);
    }
    for (const entity of listed) {
      if (!inventory.instances.has(entity)) problems.push(`${CODE_TABLE}: \`${pattern}\` lists \`${entity}\`, which no not_found call produces`);
    }
  }
  for (const code of retired) {
    if (liveCodes.has(code)) problems.push(`${CODE_TABLE}: \`${code}\` is both live and retired`);
  }
  // 3: what guides name as codes are live codes.
  const knownInstance = token => {
    const match = /^(.*)_not_found$/.exec(token);
    return match && inventory.instances.has(match[1]);
  };
  for (const path of documents) {
    const at = relative(root, path);
    const text = readFileSync(path, 'utf8');
    const fm = /^---\n[\s\S]*?\n---\n/.exec(text);
    const fmLines = fm ? fm[0].split('\n').length - 1 : 0;
    for (const { line, text: content } of splitMarkdown(text).prose) {
      for (const token of namedCodes(content, line <= fmLines)) {
        if (retired.has(token)) continue;
        if (liveCodes.has(token) || knownInstance(token) || nonCodeNames.has(token) || illustrative.has(token)) continue;
        problems.push(`${at}:${line}: \`${token}\` is no code in ${CODE_TABLE}; a name that isn't a code goes in non-code-names.txt`);
      }
    }
    // 4: a retired code anywhere.
    for (const code of retired) {
      const escaped = code.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
      const found = code.includes('_')
        ? new RegExp(`(?<![A-Za-z0-9_])${escaped}(?![A-Za-z0-9_])`)
        : new RegExp(`[\`"']${escaped}[\`"']`);
      text.split('\n').forEach((content, index) => {
        if (found.test(content)) problems.push(`${at}:${index + 1}: names \`${code}\`, a retired code`);
      });
    }
  }
  // 5: every guide pointer is a skill and an anchor of its SKILL.md.
  const pointers = [
    ...live.filter(row => row.guide).map(row => [CODE_TABLE, row.code, row.guide]),
    ...[...codegen].filter(([, entry]) => entry.guide).map(([code, entry]) => [CODEGEN_CODES, code, entry.guide]),
    ...[...core].map(([code, entry]) => [CORE_GUIDANCE, code, entry.guide]),
  ];
  for (const [where, code, guide] of pointers) {
    const [skill, anchor, ...rest] = guide.split('#');
    const skillFile = join(guides, skill, 'SKILL.md');
    if (rest.length > 0 || skill.includes('/') || !skill) {
      problems.push(`${where}: \`${code}\`'s guide \`${guide}\` isn't <skill> or <skill>#<anchor>`);
    } else if (!existsSync(skillFile)) {
      problems.push(`${where}: \`${code}\`'s guide names \`${skill}\`, which is no skill in ${GUIDES}`);
    } else if (anchor && !anchors(readFileSync(skillFile, 'utf8')).has(anchor)) {
      problems.push(`${where}: \`${code}\`'s guide \`${guide}\` names an anchor ${skill}/SKILL.md doesn't have`);
    }
  }
  // 6: the rows say what the Rust tables say.
  for (const [code, entry] of core) {
    const other = codegen.get(code);
    if (other && (other.hint !== entry.hint || other.guide !== entry.guide)) {
      problems.push(`${CODEGEN_CODES}, ${CORE_GUIDANCE}: \`${code}\` has a different hint or guide in each`);
    }
  }
  for (const code of new Set([...codegen.keys(), ...core.keys()])) {
    if (!liveCodes.has(code)) problems.push(`${CODE_TABLE}: has no row for \`${code}\`, which ${codegen.has(code) ? CODEGEN_CODES : CORE_GUIDANCE} lists`);
  }
  for (const row of live) {
    const expected = codegen.get(row.code) ?? core.get(row.code) ?? { hint: null, guide: null };
    if (row.hint !== (expected.hint ?? null) || row.guide !== (expected.guide ?? null)) {
      problems.push(`${CODE_TABLE}: \`${row.code}\`'s hint or guide isn't the one the Rust table gives`);
    }
  }
  return problems;
}

const readList = path => new Set(readFileSync(path, 'utf8').split('\n')
  .map(line => line.replace(/#.*$/, '').trim()).filter(Boolean));

export function check({ root = ROOT, files = tracked(root), words = corpus(root, files),
  fns = functions(root, files),
  illustrative = readList(join(HERE, 'illustrative-names.txt')),
  nonCodeNames = readList(join(HERE, 'non-code-names.txt')) } = {}) {
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
  problems.push(...checkCodes({ root, files, documents, guides, nonCodeNames, illustrative }));
  return problems;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const problems = check();
  if (problems.length > 0) {
    console.error(`check-agent-guides: ${problems.length} problem(s):\n`);
    for (const problem of problems) console.error(`  ${problem}`);
    console.error('\nA name that only an example uses goes in scripts/agent-guides/illustrative-names.txt,');
    console.error('one that is in the code but no code in skilj/src/error_codes.md in non-code-names.txt.');
    process.exit(1);
  }
  console.log('check-agent-guides: OK - the guides match the workspace.');
}
