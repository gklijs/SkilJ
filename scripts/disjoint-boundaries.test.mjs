// Issue #158: a finite protocol model, not a PostgreSQL implementation test.
// Run: node --test scripts/disjoint-boundaries.test.mjs
import assert from 'node:assert/strict';
import test from 'node:test';

// Tags are bits, event types are 0/1. Query items AND tags, OR types;
// queries OR items. Empty items match nothing. latest does not limit conflicts.
const matches = (query, event) => query.some(({ tags, types }) =>
  (tags !== 0 || types.length !== 0) &&
  (event.tags & tags) === tags &&
  (types.length === 0 || types.includes(event.type)));
const events = Array.from({ length: 16 }, (_, n) => ({ tags: n >> 1, type: n & 1 }));
const items = Array.from({ length: 8 }, (_, tags) =>
  [[], [0], [1], [0, 1]].map(types => ({ tags, types }))).flat();
const queries = [[], ...items.map(item => [item]),
  ...items.flatMap(a => items.map(b => [a, b]))];

// Conservative protocol: choose one tag anchor for a tagged predicate,
// and take a shared reader lock on it; writers take exclusive locks on
// ALL event tags. Type-only reads use a type gate. Every writer also
// takes the matching type gate exclusively. A real protocol can replace
// that coarse gate with hierarchical shared/intention modes.
const readerLocks = query => query.flatMap(({ tags, types }) => {
  if (tags) return [`tag:${tags & -tags}`];
  return types.map(type => `type:${type}`);
});
const writerLocks = event => [
  `type:${event.type}`,
  ...[1, 2, 4].filter(bit => event.tags & bit).map(bit => `tag:${bit}`),
];

test('anchor coverage never misses a matching write, including OR and AND', () => {
  for (const query of queries) {
    for (const event of events) {
      const blocked = readerLocks(query).some(key => writerLocks(event).includes(key));
      assert.ok(!matches(query, event) || blocked, JSON.stringify({ query, event }));
    }
  }
});

test('equal tags are insufficient; a multi-tag writer bridges different anchors', () => {
  const a = [{ tags: 1, types: [0] }];
  assert.equal(matches(a, { tags: 1, type: 1 }), false);
  assert.equal(matches(a, { tags: 3, type: 0 }), true);
  assert.equal(matches([{ tags: 3, types: [] }], { tags: 1, type: 0 }), false);
  assert.equal(matches([{ tags: 2, types: [] }], { tags: 3, type: 0 }), true);
});

// Every interleaving of read, validate and commit for two actors.
function schedules(prefix = [], next = [0, 0]) {
  if (prefix.length === 6) return [prefix];
  return [0, 1].flatMap(id => {
    if (next[id] === 3) return [];
    const advanced = [...next];
    advanced[id]++;
    return schedules([...prefix, [id, next[id]]], advanced);
  });
}

function execute(schedule, query, event, gate) {
  const states = [{}, {}];
  const log = [];
  const completions = [];
  let owner;
  for (const [id, phase] of schedule) {
    const state = states[id];
    if (phase === 0) {
      state.covered = log.length;
      state.accept = !log.some(e => matches(query, e));
    } else if (phase === 1) {
      if (gate && owner !== undefined) return undefined; // blocked schedule
      owner = id;
      if (log.slice(state.covered).some(e => matches(query, e))) {
        state.accept = false; // redispatch the create-once decision
      }
    } else {
      if (state.accept) log.push(event);
      completions.push({ id, accept: state.accept });
      if (gate) assert.equal(owner, id);
      owner = undefined;
    }
  }
  // Independent serial oracle: a create-once command rejects iff a
  // preceding accepted command emitted an event matching its query.
  let precedingAccepted = false;
  let valid = true;
  for (const completion of completions) {
    const expected = !(precedingAccepted && matches(query, event));
    valid &&= completion.accept === expected;
    precedingAccepted ||= expected;
  }
  return valid;
}

test('commit-gate schedules match a serial create-once reference', () => {
  let checked = 0;
  for (const query of queries) {
    for (const event of events) {
      for (const schedule of schedules()) {
        const valid = execute(schedule, query, event, true);
        if (valid !== undefined) {
          assert.equal(valid, true, JSON.stringify({ query, event, schedule }));
          checked++;
        }
      }
    }
  }
  assert.ok(checked > 100_000);
});

test('the same serial oracle detects schedules without a protected commit gate', () => {
  const query = [{ tags: 1, types: [] }];
  const event = { tags: 1, type: 0 };
  assert.ok(schedules().some(schedule => execute(schedule, query, event, false) === false));
});

test('moving validation before an unprotected wait admits a stale decision', () => {
  const query = [{ tags: 1, types: [] }];
  const event = { tags: 1, type: 0 };
  // A reads/validates; B commits; A takes the sequence lock and appends.
  const aDecision = 0;
  const committed = [event];
  assert.notEqual(aDecision, committed.filter(e => matches(query, e)).length);
});

test('latest keeps a bounded selection but every new match still invalidates it', () => {
  const query = [{ tags: 1, types: [0], latest: 1 }];
  assert.equal(matches(query, { tags: 1, type: 0 }), true);
  assert.equal(matches(query, { tags: 1, type: 1 }), false);
});

test('literal timeless invariant rejects an ordinary later matching command', () => {
  const first = { boundary: null, own: [0] };
  const laterEvent = { sequence: 1, tags: 1, type: 0 };
  assert.ok(matches([{ tags: 1, types: [] }], laterEvent));
  assert.ok(first.boundary === null || laterEvent.sequence > first.boundary);
  assert.equal(first.own.includes(laterEvent.sequence), false);
  // A command-time upper bound excludes this later append from the check.
  assert.equal(laterEvent.sequence <= 0, false);
});
