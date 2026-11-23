import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { anchors, check, claimedMethods, claimedNames, CODE_TABLE, CODEGEN_CODES, codeTable,
  CORE_GUIDANCE, corpus, frontmatter, functions, GUIDES, maskRust, namedCodes, producers,
  splitMarkdown, TEMPLATE, traitItems } from './check.mjs';

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

// skilj/src/error_codes.md in miniature: rows of [code, where, hint, guide].
function table({ codegen = [], wire = [], startup = [], kinds = [], retired = [] } = {}) {
  const cell = value => (value ? `\`${value}\`` : '');
  const rows = list => list.map(([code, where, hint, guide]) =>
    `| \`${code}\` | ${where} | ${cell(hint)} | ${cell(guide)} |`).join('\n');
  const head = '| Code | Where | Hint | Guide |\n| --- | --- | --- | --- |';
  return `Intro naming \`<skill>#<anchor>\`.\n\n## Codegen diagnostics\n\n${head}\n${rows(codegen)}\n\n` +
    `## GraphQL and REST error codes\n\n${head}\n${rows(wire)}\n\n## Startup log codes\n\n${head}\n${rows(startup)}\n\n` +
    `## Rejection kinds skilj itself gives\n\n${head}\n${rows(kinds)}\n\n` +
    `## Retired codes\n\n${head}\n${rows(retired)}\n`;
}

// A repository in miniature: one crate, one guide, the template's AGENTS.md,
// and the table of codes (empty, as nothing here produces a code).
function repository(t, { skill, source = TRAIT, extra = {}, codes = table(), frontmatterExtra = '',
  nonCodeNames = [] } = {}) {
  const root = mkdtempSync(join(tmpdir(), 'skilj-guides-test-'));
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const files = {
    'skilj-core/src/lib.rs': source,
    [`${GUIDES}/README.md`]: 'The guide is [thing/SKILL.md](thing/SKILL.md).\n',
    [`${GUIDES}/thing/SKILL.md`]: `---\nname: thing\ndescription: >\n  When to read it.${frontmatterExtra}\n---\n\n${skill}`,
    [`${TEMPLATE}/AGENTS.md`]: 'Read `thing/SKILL.md`.\n',
    [CODE_TABLE]: codes,
    ...extra,
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
    fns: functions(root, tracked), illustrative: new Set(['MyThing', 'occurred_at']),
    nonCodeNames: new Set(nonCodeNames) }) };
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

// --- codes ---

const inventory = (t, sources) => {
  const { root } = repository(t, { skill: GOOD, extra: sources });
  return producers(root, Object.keys(sources));
};
const codesOf = found => [...found.codes.keys()].sort();

test('masking keeps quotes and lines, and blanks literals and comments', () => {
  const masked = maskRust('let a = "x // }"; // { "y"\nlet b = \'}\'; /* { */ r#"}"#;');
  assert.equal(masked.length, 'let a = "x // }"; // { "y"\nlet b = \'}\'; /* { */ r#"}"#;'.length);
  assert.ok(!/[{}]/.test(masked), masked);
  assert.equal(masked.split('\n').length, 2);
});

test('fn code arms: literals, braced literals, delegation, business and copied codes', t => {
  const found = inventory(t, { 'skilj-core/src/error.rs': `
impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::Inner(e) => e.code(),
            Error::Long { .. } => {
                "a_long_code"
            }
            Error::Plain => "plain_code",
            Error::CommandRejected { kind, .. } => kind,
            Error::BatchFailed { code, .. } => code,
        }
    }
}
impl crate::error::SkiljRejection for Other {
    fn code(&self) -> &str {
        Other::code(self)
    }
}` });
  assert.deepEqual(codesOf(found), ['a_long_code', 'plain_code']);
  assert.deepEqual(found.unclassified, []);
});

test('an fn code arm of another shape is unclassified', t => {
  const found = inventory(t, { 'skilj-core/src/error.rs': `
impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::Plain => "plain_code",
            Error::Made(name) => name.as_str(),
        }
    }
}` });
  assert.equal(found.unclassified.length, 1, found.unclassified.join('\n'));
  assert.match(found.unclassified[0], /error\.rs:6: fn code arm `name\.as_str\(\)`/);
});

test('BatchFailed: a literal produces, a copied code delegates, the declaration is skipped', t => {
  const found = inventory(t, { 'skilj-core/src/error.rs': `
pub enum Error {
    BatchFailed {
        /// The original error's code.
        code: String,
        message: String,
    },
}
pub fn batch_failed(cause: &Error) -> Error {
    Error::BatchFailed { code: cause.code().to_string(), message: cause.message() }
}
pub fn batch_failed_msg(message: impl Into<String>) -> Error {
    Error::BatchFailed {
        code: "database_error".to_string(),
        message: message.into(),
    }
}` });
  assert.deepEqual(codesOf(found), ['database_error']);
  assert.deepEqual(found.unclassified, []);
});

test("REST's fn parts: the tuple's second element", t => {
  const found = inventory(t, { 'skilj-rest/src/error.rs': `
impl RestError {
    fn parts(self) -> (StatusCode, String, String) {
        match self {
            RestError::Core(e) => {
                if let Some(detail) = e.internal_detail() {
                    tracing::error!(code = %e.code(), "x");
                }
                (status_for(&e), e.code().to_string(), e.message())
            }
            RestError::Missing => (
                StatusCode::UNAUTHORIZED,
                "missing_credential".to_string(),
                "missing (Authorization) header".to_string(),
            ),
        }
    }
}` });
  assert.deepEqual(codesOf(found), ['missing_credential']);
  assert.deepEqual(found.unclassified, []);
});

test('GraphQL: coded reads the code, not the message; set_code its second argument; the helpers are exempt', t => {
  const found = inventory(t, { 'skilj-graphql/src/error.rs': `
pub(crate) fn set_code(ext: &mut E, code: &str, hint: Option<&str>, guide: Option<&str>) {
    ext.set("code", code);
}
pub(crate) fn coded(code: &'static str, message: impl Into<String>) -> Error {
    Error::new(message).extend_with(|_, ext| set_code(ext, code, None, None))
}
fn budget_error(code: &str, message: String) -> ServerError {
    set_code(&mut extensions, code, None, None);
}
fn lagged() -> Error {
    crate::error::coded(
        "subscription_lagged",
        "not_a_code: the message",
    )
}
fn limit() -> ServerError {
    budget_error("query_too_complex", format!("too complex"))
}
fn manual(ext: &mut E) {
    set_code(ext, "manual_code", None, None);
}` });
  assert.deepEqual(codesOf(found), ['manual_code', 'query_too_complex', 'subscription_lagged']);
  assert.deepEqual(found.unclassified, []);
});

test('GraphQL: setting "code" outside set_code is unclassified', t => {
  const found = inventory(t, { 'skilj-graphql/src/limits.rs': `
fn budget(code: &str) {
    let mut extensions = Default::default();
    extensions.set("code", code);
}` });
  assert.equal(found.unclassified.length, 1);
  assert.match(found.unclassified[0], /limits\.rs:4: sets "code" directly/);
});

test("GraphQL: not_found's entities, split over lines and through the macro's fifth argument", t => {
  const found = inventory(t, { 'skilj-graphql/src/resolvers/mod.rs': `
pub fn not_found(entity: &str, key: &str) -> Error {
    let code = format!("{entity}_not_found");
    Error::new("x").extend_with(|_, ext| crate::error::set_code(ext, &code, None, None))
}
macro_rules! create_type_token_field {
    ($n:expr, $type_label:literal) => {
        $crate::resolvers::not_found($type_label, &type_name)
    };
}
fn one() { not_found("Role", &id); }
fn two() {
    not_found(
        "Projection type",
        &name,
    );
}
fn three() {
    create_type_token_field!(
        n,
        "createExternalEventToken",
        "ExternalEventToken",
        "eventTypeName",
        "EventType",
        get,
        create,
        insert
    )
}
fn four(state: &State) -> BatchItemError {
    batch_item_error(e)
}
fn batch_item_error(e: Error) -> BatchItemError {
    let code = extension("code").unwrap_or_else(|| "error".to_string());
}` });
  assert.deepEqual([...found.instances].sort(), ['EventType', 'Projection type', 'Role']);
  assert.deepEqual(codesOf(found), ['error']);
  assert.deepEqual(found.unclassified, []);
});

test('codegen: problem, problems.push and Diagnostic::new; the forwarding From is no form', t => {
  const found = inventory(t, {
    'skilj-codegen/src/spec.rs': `
impl Problems {
    fn push(&mut self, code: &'static str, message: String) {
        self.items.push(crate::problem(code, self.context.clone(), message, None));
    }
}
fn validate(problems: &mut Problems) {
    problems.push("duplicate_field", format!("x"));
    let mut list = Vec::new();
    list.push(crate::problem("unknown_field_type", String::new(), String::new(), None));
}`,
    'skilj-codegen/src/main.rs': `
impl Diagnostic {
    fn new(code: &'static str, path: &Path, message: impl ToString) -> Self { todo!() }
}
impl From<(&Path, &ValidationProblem)> for Diagnostic {
    fn from((path, problem): (&Path, &ValidationProblem)) -> Self {
        Self { code: problem.code, category: problem.category() }
    }
}
fn io() { Diagnostic::new("io", path, error); }`,
  });
  assert.deepEqual(codesOf(found), ['duplicate_field', 'io', 'unknown_field_type']);
  assert.deepEqual(found.unclassified, []);
});

test('startup: a literal code field produces, a delegating one does not', t => {
  const found = inventory(t, { 'skilj/src/lib.rs': `
fn log(e: &Error) {
    tracing::warn!(code = %"skipped_no_access", registration = %key, "skipped");
    tracing::error!(code = %e.code(), error = %e.message(), "refused");
}` });
  assert.deepEqual(codesOf(found), ['skipped_no_access']);
  assert.deepEqual(found.unclassified, []);
});

test('test code produces nothing: an inline test module and an external one, but a test hook stays', t => {
  const found = inventory(t, {
    'skilj-core/src/encryption/service.rs': `
impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self { Error::A => "live_code" }
    }
}
#[cfg(test)]
pub(crate) fn hook() -> Error { Error::BatchFailed { code: "hook_code".to_string(), message: String::new() } }
#[cfg(test)]
mod tests {
    fn x() -> Error { Error::BatchFailed { code: "inline_test_code".to_string(), message: String::new() } }
}
#[cfg(test)]
mod more;`,
    'skilj-core/src/encryption/service/more.rs': `
fn x() -> Error { Error::BatchFailed { code: "external_test_code".to_string(), message: String::new() } }`,
  });
  assert.deepEqual(codesOf(found), ['hook_code', 'live_code']);
});

test("rejection kinds skilj gives: a Rejected construction's literal kind, a StanceRefusal fn's literals", t => {
  const found = inventory(t, {
    'skilj-core/src/plugin/mod.rs': `
fn decide() -> CommandDecision {
    CommandDecision::Rejected {
        reason: "not implemented".to_string(),
        kind: "decide_not_implemented".to_string(),
    }
}
fn pass(reason: String, kind: &str) -> Outcome {
    Outcome::Rejected { reason, kind: kind.to_string() }
}`,
    'skilj-core/src/db/commands.rs': `
type StanceRefusal = (String, &'static str);
fn of(spec: &Spec) -> Option<Result<Stance, StanceRefusal>> {
    Some(Err((format!("not a retraction: {}", "x y"), "invalid_retraction")))
}
async fn resolve(conn: &mut Conn) -> Result<Result<Tags, StanceRefusal>> {
    let _ = sqlx::query("SELECT a FROM b WHERE c = $1");
    return Ok(Err((
        format!("position {position} is retracted"),
        if retraction { "already_retracted" } else { "doubt_target_retracted" },
    )));
}
fn unrelated() -> &'static str { "not_a_kind" }`,
  });
  assert.deepEqual([...found.kinds.keys()].sort(),
    ['already_retracted', 'decide_not_implemented', 'doubt_target_retracted', 'invalid_retraction']);
  assert.deepEqual(codesOf(found), []);
  assert.deepEqual(found.unclassified, []);
});

// A small workspace with one producer of each kind and the table for it.
const CORE = `
impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::Payload => "payload_too_large",
            Error::Plain => "plain_code",
            Error::Reserved => "reserved_event_type_name",
        }
    }
}
#[rustfmt::skip]
pub static GUIDANCE: &[Guidance] = &[
    entry("payload_too_large", "move the field to a blob", "thing#codes"),
    entry("reserved_event_type_name", "rename it", "thing#codes"),
];`;
const CODEGEN = `
pub(crate) const CODES: &[Code] = &[
    code("duplicate_field", "semantic"),
    guided("reserved_event_type_name", "semantic", "rename it", "thing#codes"),
];`;
const SPEC = `
fn validate(problems: &mut Problems) {
    problems.push("duplicate_field", format!("x"));
    problems.push("reserved_event_type_name", format!("y"));
}`;
const GRAPHQL = `
fn one() { not_found("Projection", &id); }
fn two() { not_found("RoleAccessMapping", &id); }`;
const ROWS = {
  codegen: [['duplicate_field', 'codegen'],
    ['reserved_event_type_name', 'codegen, graphql, rest', 'rename it', 'thing#codes']],
  wire: [['payload_too_large', 'graphql, rest', 'move the field to a blob', 'thing#codes'],
    ['plain_code', 'graphql, rest']],
};
const PATTERN = '| `<entity>_not_found` (`Projection`, `RoleAccessMapping`) | graphql |  |  |\n';
const withPattern = codes => codes.replace('\n\n## Startup', `\n${PATTERN}\n## Startup`);
const SKILL_WITH_CODES = `${GOOD}\n## Codes\n\nSee \`payload_too_large\` and \`Projection_not_found\`, \`RoleAccessMapping_not_found\`.\n`;

// The codes rule's problems, without the names rule's about the same tokens.
const codeProblems = problems => problems.filter(problem => !/occurs nowhere in the workspace/.test(problem));

function codesRepository(t, { core = CORE, codegen = CODEGEN, spec = SPEC, rows = ROWS, skill = SKILL_WITH_CODES,
  extra = {}, frontmatterExtra = '', nonCodeNames = [] } = {}) {
  const built = repository(t, {
    skill, frontmatterExtra, nonCodeNames, codes: withPattern(table(rows)),
    extra: { [CORE_GUIDANCE]: core, [CODEGEN_CODES]: codegen, 'skilj-codegen/src/spec.rs': spec,
      'skilj-graphql/src/resolvers/mod.rs': GRAPHQL, ...extra },
  });
  return { ...built, problems: () => codeProblems(built.problems()) };
}

test('the codes of a small workspace pass', t => {
  assert.deepEqual(codesRepository(t).problems(), []);
});

test('rule 1: a renamed producer fails, though a comparison literal keeps the old name in the code', t => {
  const core = CORE.replace('=> "plain_code"', '=> "plain_renamed"') +
    '\nfn compare(code: &str) -> bool { code == "plain_code" }';
  const problems = codesRepository(t, { core }).problems().join('\n');
  assert.match(problems, /`plain_code` is a row no code path produces/);
  assert.match(problems, /produces `plain_renamed`, which skilj\/src\/error_codes\.md doesn't list/);
});

test('rule 1: a deleted emitter fails, though its CODES entry stays', t => {
  const spec = SPEC.replace('    problems.push("duplicate_field", format!("x"));\n', '');
  const problems = codesRepository(t, { spec }).problems();
  assert.equal(problems.length, 1, problems.join('\n'));
  assert.match(problems[0], /`duplicate_field` is a row no code path produces/);
});

test('rules 1 and 2: a changed BatchFailed literal', t => {
  const rows = { ...ROWS, wire: [...ROWS.wire, ['database_error', 'graphql, rest']] };
  const batch = name => `\nfn batch_failed_msg() -> Error { Error::BatchFailed { code: "${name}".to_string(), message: String::new() } }`;
  assert.deepEqual(codesRepository(t, { rows, core: CORE + batch('database_error') }).problems(), []);
  const problems = codesRepository(t, { rows, core: CORE + batch('db_error') }).problems().join('\n');
  assert.match(problems, /`database_error` is a row no code path produces/);
  assert.match(problems, /produces `db_error`/);
});

test('an unclassified producer fails', t => {
  const core = CORE.replace('Error::Plain => "plain_code",', 'Error::Plain => "plain_code",\n            Error::Made(name) => name,');
  assert.match(codesRepository(t, { core }).problems().join('\n'), /unclassified code producer/);
});

test('rule 1: a producer only in an external test module keeps no row alive', t => {
  const core = CORE.replace('            Error::Plain => "plain_code",\n', '') + '\n#[cfg(test)]\nmod tests;';
  const extra = { 'skilj-core/src/error/tests.rs': 'fn x() -> E { Error::BatchFailed { code: "plain_code".to_string(), message: String::new() } }' };
  const problems = codesRepository(t, { core, extra }).problems();
  assert.equal(problems.length, 1, problems.join('\n'));
  assert.match(problems[0], /`plain_code` is a row no code path produces/);
});

test('rule 3: a misspelled code or pattern instance fails, whatever its casing', t => {
  const skill = `${SKILL_WITH_CODES}\nNot \`Projection_not_fond\`, \`Projeciton_not_found\` or \`paylod_too_large\`; \`occurred_at\` is an example's.\n`;
  const problems = codesRepository(t, { skill }).problems();
  assert.deepEqual(problems.map(problem => /`([^`]+)` is no code/.exec(problem)?.[1]),
    ['Projection_not_fond', 'Projeciton_not_found', 'paylod_too_large']);
});

test('rule 3: a code-shaped name in frontmatter quotes is checked too', t => {
  const problems = codesRepository(t, { frontmatterExtra: ' Triggers on "payload_too_large", "plain_cod".' }).problems();
  assert.equal(problems.length, 1, problems.join('\n'));
  assert.match(problems[0], /SKILL\.md:4: `plain_cod` is no code/);
});

test('rule 3: a name listed as no code passes; a deleted row still in a guide fails though the string lives on', t => {
  const skill = `${SKILL_WITH_CODES}\nSet \`subject_tag\`. Retry on \`plain_code\`.\n`;
  assert.deepEqual(codesRepository(t, { skill, nonCodeNames: ['subject_tag'] }).problems(), []);
  const rows = { ...ROWS, wire: ROWS.wire.filter(([code]) => code !== 'plain_code') };
  const core = CORE.replace('            Error::Plain => "plain_code",\n', '');
  const extra = { 'skilj-retry/src/lib.rs': 'pub const RETRY: &[&str] = &["plain_code"];' };
  const problems = codesRepository(t, { skill, rows, core, extra, nonCodeNames: ['subject_tag'] }).problems();
  assert.equal(problems.length, 1, problems.join('\n'));
  assert.match(problems[0], /`plain_code` is no code/);
});

test('a renamed code whose variant stays fails until the table and then the guide follow', t => {
  const skill = `${SKILL_WITH_CODES}\n**\`Plain\`** (\`plain_code\`) - what it means.\n`;
  const core = CORE.replace('=> "plain_code"', '=> "plain_renamed"');
  let problems = codesRepository(t, { skill, core }).problems().join('\n');
  assert.match(problems, /`plain_code` is a row no code path produces/);
  assert.match(problems, /produces `plain_renamed`/);
  const rows = { ...ROWS, wire: [ROWS.wire[0], ['plain_renamed', 'graphql, rest']] };
  problems = codesRepository(t, { skill, core, rows }).problems();
  assert.equal(problems.length, 1, problems.join('\n'));
  assert.match(problems[0], /`plain_code` is no code/);
  const retired = { ...rows, retired: [['plain_code', 'graphql, rest']] };
  problems = codesRepository(t, { skill, core, rows: retired }).problems();
  assert.equal(problems.length, 1, problems.join('\n'));
  assert.match(problems[0], /names `plain_code`, a retired code/);
});

test('rule 4: a retired code fails in frontmatter and prose; a one-word one only quoted', t => {
  const rows = { ...ROWS, retired: [['old_code', 'graphql, rest'], ['unauthenticated', 'graphql']] };
  const skill = `${SKILL_WITH_CODES}\nThe caller is unauthenticated there, and old_code is gone.\nAnswers \`unauthenticated\`.\n`;
  const { root, problems } = codesRepository(t, { rows, skill, frontmatterExtra: ' Triggers on "old_code".' });
  const lines = readFileSync(join(root, GUIDES, 'thing/SKILL.md'), 'utf8').split('\n');
  const lineOf = text => lines.findIndex(line => line.includes(text)) + 1;
  assert.deepEqual(problems().map(problem => problem.replace(/^.*SKILL\.md:(\d+): names `(\w+)`.*$/, '$1 $2')).sort(),
    [`${lineOf('"old_code"')} old_code`, `${lineOf('old_code is gone')} old_code`,
      `${lineOf('Answers `unauthenticated`')} unauthenticated`].sort());
});

test('rule 4: a code both live and retired fails', t => {
  const rows = { ...ROWS, retired: [['plain_code', 'graphql, rest']] };
  assert.match(codesRepository(t, { rows }).problems().join('\n'), /`plain_code` is both live and retired/);
});

test('rule 5: a guide naming a reference file, a missing skill or a missing anchor fails', t => {
  for (const [guide, expected] of [['thing/references/x.md', /isn't <skill> or <skill>#<anchor>/],
    ['nothing#codes', /names `nothing`, which is no skill/], ['thing#nowhere', /names an anchor thing\/SKILL\.md doesn't have/]]) {
    const core = CORE.replace('"move the field to a blob", "thing#codes"', `"move the field to a blob", "${guide}"`);
    const rows = { ...ROWS, wire: [['payload_too_large', 'graphql, rest', 'move the field to a blob', guide], ROWS.wire[1]] };
    assert.match(codesRepository(t, { core, rows }).problems().join('\n'), expected, guide);
  }
});

test('rule 6: a row unlike its Rust table, a Rust code without a row, a shared code told two ways', t => {
  const rows = { ...ROWS, wire: [['payload_too_large', 'graphql, rest', 'raise the limit', 'thing#codes'], ROWS.wire[1]] };
  assert.match(codesRepository(t, { rows }).problems().join('\n'), /`payload_too_large`'s hint or guide isn't the one/);
  const core = `${CORE.replace('];', '    entry("extra_code", "do it", "thing#codes"),\n];')}`;
  assert.match(codesRepository(t, { core }).problems().join('\n'), /has no row for `extra_code`/);
  const codegen = CODEGEN.replace('"rename it"', '"call it something else"');
  assert.match(codesRepository(t, { codegen }).problems().join('\n'), /`reserved_event_type_name` has a different hint or guide in each/);
});

test('a missing table fails', t => {
  const { root, problems } = repository(t, { skill: GOOD });
  rmSync(join(root, CODE_TABLE));
  assert.match(problems().join('\n'), /error_codes\.md: missing/);
});

test('table and guide helpers', () => {
  const rows = codeTable('## Codegen diagnostics\n\n| Code | Where | Hint | Guide |\n| --- | --- | --- | --- |\n| `a_b` | codegen | `do it` | `thing#x` |\n| `<entity>_not_found` (`Role`) | graphql |  |  |\n');
  assert.deepEqual(rows.map(row => [row.code, row.instances, row.hint, row.guide]),
    [['a_b', [], 'do it', 'thing#x'], ['<entity>_not_found', ['Role'], null, null]]);
  assert.deepEqual(namedCodes('`a_b`, `Role_not_found`, `TOO_LOUD`, `a b_c`, "q_r"', false), ['a_b', 'Role_not_found']);
  assert.deepEqual(namedCodes('"q_r" and "single"', true), ['q_r']);
});

test('the real tree: every producer classified and the table equal to the inventory', () => {
  const found = producers();
  assert.deepEqual(found.unclassified, []);
  assert.ok(found.codes.size > 150, `${found.codes.size}`);
  assert.ok(found.instances.has('Projection type') && found.instances.has('RoleAccessMapping'));
  assert.ok(found.kinds.has('already_retracted') && found.kinds.has('decide_not_implemented'));
  assert.deepEqual(check().filter(problem => /code/.test(problem)), []);
});

test('rejection kinds: listed in their own section, produced, and accepted in guides', t => {
  const kinds = `
fn decide() -> CommandDecision {
    CommandDecision::Rejected { reason: "x".to_string(), kind: "decide_not_implemented".to_string() }
}`;
  const rows = { ...ROWS, kinds: [['decide_not_implemented', 'graphql, rest']] };
  const skill = `${SKILL_WITH_CODES}\nThe default answers \`decide_not_implemented\`.\n`;
  const extra = { 'skilj-core/src/plugin/mod.rs': kinds };
  assert.deepEqual(codesRepository(t, { rows, skill, extra }).problems(), []);
  // Given but not listed, and listed but not given.
  let problems = codesRepository(t, { extra }).problems().join('\n');
  assert.match(problems, /gives the rejection kind `decide_not_implemented`, which .* doesn't list/);
  problems = codesRepository(t, { rows }).problems().join('\n');
  assert.match(problems, /`decide_not_implemented` is a row no code path produces/);
  // A kind listed among the error codes is no error code.
  const misplaced = { ...ROWS, wire: [...ROWS.wire, ['decide_not_implemented', 'graphql, rest']] };
  problems = codesRepository(t, { rows: misplaced, extra }).problems().join('\n');
  assert.match(problems, /`decide_not_implemented` is a row no code path produces/);
});
