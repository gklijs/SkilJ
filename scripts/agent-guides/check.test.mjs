import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { anchors, check, claimedMethods, claimedNames, corpus, frontmatter, functions,
  GUIDES, splitMarkdown, TEMPLATE, traitItems } from './check.mjs';

const TRAIT = `pub trait Thing {
    /// Its name.
    const NAME: &'static str;
    fn tags() -> Vec<crate::shared::Tag> {
        Vec::new()
    }
    #[doc(hidden)]
    fn internal() {}
    fn decide(
        _payload: &Self::Payload,
        _events: &[Self::Event],
    ) -> Decision {
        if true { Decision::No } else { Decision::Yes }
    }
}
pub fn make_thing() {}
`;

// A repository in miniature: one crate, one guide, the template's AGENTS.md.
function repository(t, { skill, source = TRAIT } = {}) {
  const root = mkdtempSync(join(tmpdir(), 'skilj-guides-test-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const files = {
    'skilj-core/src/lib.rs': source,
    [`${GUIDES}/README.md`]: 'The guide is [thing/SKILL.md](thing/SKILL.md).\n',
    [`${GUIDES}/thing/SKILL.md`]: `---\nname: thing\ndescription: >\n  When to read it.\n---\n\n${skill}`,
    [`${TEMPLATE}/AGENTS.md`]: 'Read `thing/SKILL.md`.\n',
  };
  for (const [file, text] of Object.entries(files)) {
    mkdirSync(dirname(join(root, file)), { recursive: true });
    writeFileSync(join(root, file), text);
  }
  for (const entry of ['.claude/skills', '.agents/skills']) {
    mkdirSync(join(root, entry), { recursive: true });
    symlinkSync(`../../${GUIDES}/thing`, join(root, entry, 'thing'));
  }
  const tracked = Object.keys(files);
  return { root, problems: () => check({ root, files: tracked, words: corpus(root, tracked),
    fns: functions(root, tracked), illustrative: new Set(['MyThing']) }) };
}

const GOOD = [
  '# Thing', '', 'Implement `Thing`, as `MyThing` does, and call `make_thing()`.',
  'See `skilj-core/src/lib.rs` and [the index](../README.md).', '',
  '```rust', 'pub trait Thing {', "    const NAME: &'static str;",
  '    fn tags() -> Vec<Tag> { Vec::new() }',
  '    fn decide(payload: &Self::Payload, events: &[Self::Event]) -> Decision { /* .. */ }',
  '}', '```', '',
].join('\n');

test('a guide that matches the workspace passes', t => {
  assert.deepEqual(repository(t, { skill: GOOD }).problems(), []);
});

test('a trait block is held to the trait: missing, removed and changed items', t => {
  const fewer = TRAIT.replace(/    fn tags[\s\S]*?\n    }\n/, '    fn labels() {}\n');
  const problems = repository(t, { skill: GOOD, source: fewer }).problems();
  assert.equal(problems.length, 2, problems.join('\n'));
  assert.match(problems[0], /Thing has no `tags`/);
  assert.match(problems[1], /`labels` is missing from the block/);
  const retyped = TRAIT.replace('-> Decision {', '-> Outcome {');
  assert.match(repository(t, { skill: GOOD, source: retyped }).problems().join('\n'),
    /is now `fn decide\(payload:&Self::Payload,events:&\[Self::Event\]\)->Outcome`/);
});

test('a name, a method, a path and a link that no longer exist are each reported', t => {
  const skill = `${GOOD}\nUse \`OldThing\`, \`.old_call()\`, \`skilj-core/src/gone.rs\` and [this](gone.md#x).\n`;
  const problems = repository(t, { skill }).problems().join('\n');
  assert.match(problems, /`OldThing` occurs nowhere/);
  assert.match(problems, /`\.old_call\(\)` is a method no crate declares/);
  assert.match(problems, /names skilj-core\/src\/gone\.rs/);
  assert.match(problems, /link to gone\.md#x/);
});

test('a call in a Rust example has to exist; a standard one does not', t => {
  const skill = `${GOOD}\n\`\`\`rust\nlet x = make_thing().clone().renamed_call(1);\n\`\`\`\n`;
  const problems = repository(t, { skill }).problems();
  assert.equal(problems.length, 1, problems.join('\n'));
  assert.match(problems[0], /`\.renamed_call\(\)`/);
});

test('a guide needs both entry points, the index and the template to name it', t => {
  const { root, problems } = repository(t, { skill: GOOD });
  rmSync(join(root, '.agents/skills/thing'));
  writeFileSync(join(root, GUIDES, 'README.md'), 'Nothing.\n');
  writeFileSync(join(root, TEMPLATE, 'AGENTS.md'), 'Nothing.\n');
  const found = problems().join('\n');
  assert.match(found, /\.agents\/skills\/thing: should be a symlink/);
  assert.match(found, /README\.md: doesn't list thing\/SKILL\.md/);
  assert.match(found, /AGENTS\.md: doesn't list thing\/SKILL\.md/);
});

test('trait items skip hidden ones and ignore parameter underscores and paths', () => {
  const items = traitItems(TRAIT, 'Thing');
  assert.deepEqual([...items.keys()], ['NAME', 'tags', 'decide']);
  assert.equal(items.get('tags'), 'fn tags()->Vec<Tag>');
  assert.equal(traitItems(TRAIT, 'Other'), null);
});

test('only code-like backticks claim a name', () => {
  assert.deepEqual([...claimedNames('`RoleAccessMapping` and `owner_tag_key`, not `cargo test --all` or `src/wallet_fixture.rs`')],
    ['RoleAccessMapping', 'owner_tag_key']);
  assert.deepEqual([...claimedMethods('`.key_provider(...)` and `SkiljBuilder::payload_limits`, not `a.b`')],
    ['key_provider', 'payload_limits']);
});

test('markdown helpers', () => {
  assert.equal(frontmatter('---\nname: a\ndescription: >\n  One\n  two.\n---\nbody').description, 'One two.');
  assert.equal(frontmatter('no frontmatter'), null);
  const { prose, blocks } = splitMarkdown('a\n```rust\nlet x;\n```\nb');
  assert.deepEqual(prose.map(line => line.text), ['a', 'b']);
  assert.deepEqual(blocks.map(block => [block.lang, block.line, block.code]), [['rust', 3, 'let x;']]);
  assert.ok(anchors('## 6. Commands that need approval\n<a id="x"></a>').has('6-commands-that-need-approval'));
});
