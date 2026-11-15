# Bringing a model from an Event Modeling tool

When the team already models in a tool, don't start the discovery conversation
from nothing: import what the model says into a `.skilj.toml`, then ask the
questions a model can't answer (tags it doesn't mark, rejections without a
kind, `reads_nothing`, sensitive fields). `.skilj.toml` is the one format every
tool imports to; `skilj-codegen import` writes it and nothing is written back
to the tool. `docs/modeling-tools.md` has each tool's naming convention.

## Pick the source from what is there

Never ask a new user which tool they use. Look, in this order, and take the
first that fits:

| What you find | Source | Import |
|---|---|---|
| `.em.hcl` files, or an `.emhcl.yml` | Event Modeling HCL | `emhcl export <model dir> -o model.json`, then `--from emhcl`; follow [emhcl.md](emhcl.md) |
| A JSON file with a top-level `"slices"` array whose elements have `sliceType` (an export of Nebulit's Event Modeling app for Miro, or of eventmodelers.ai) | Event Modeling JSON | `--from em-json` |
| An `.evml` file, or a Markdown code block marked `evml` or `mermaid` that starts with `eventmodeling` | event-modeling-tools text | `--from evml` (copy a code block's body to an `.evml` file first) |
| Tools named `mcp__proophboard__*` in this session, or a `.proophboard/workspace.json` | prooph board | Write Event Modeling JSON from the board (below), then `--from em-json` |
| None of these | | The discovery conversation in `SKILL.md` |

If two fit, take the one the user mentioned, else the first in the table, and
say which you used. A Qlerify model has no import: Qlerify publishes no schema
for its JSON export, so read it with the user and use the conversation instead.

## Import

From the project, with the bounded context in lowercase `snake_case`:

```sh
cargo install skilj-codegen
skilj-codegen import --from em-json model.json banking --output src/banking.skilj.toml
skilj-codegen check src/banking.skilj.toml
```

The notes on stderr are the work left for you, one line each, and they also
head the file as comments:

- a type left out, with why (a list, nested fields, an optional field, a
  decimal): hand-write it in Rust, the `skilj` guide shows how;
- a field that is personal data: a `sensitive` entry, or a `[[blind_tag]]` if
  a rule matches on it (the `skilj-uniqueness` guide);
- a type without tags: ask question 3 of `SKILL.md` for it.

Above each command the file lists its rejection kinds (`# Rejected as: ...`):
the kinds its `decide_<name>` function must return. Then go through the
summary of `SKILL.md` with the user as usual, starting from what was imported.

## prooph board

prooph board has an MCP server, not an export file. Read the board over it
and write one Event Modeling JSON document per chapter, then import that.
Find the tools with ToolSearch (`+proophboard`), list the chapters, and read
each with its chapter tool: lanes, slices and elements, each element with its
name and details.

| On the board | In the JSON |
|---|---|
| The chapter | One document: `{"slices": [...]}` |
| A slice | A slice: `id`, `title`, `index`; `sliceType` `STATE_CHANGE` for one with a command, `STATE_VIEW` otherwise; `commands`, `events`, `readmodels`, `screens`, `processors`, `tables`, `specifications` as arrays |
| The `system` lane an event is in | The event's `modelContext`: name the lane after the bounded context (`Banking`) |
| An element | `id`, `title` (its name), `type` (`COMMAND`, `EVENT`, `READMODEL`), `fields`, `dependencies` |
| A command and the events in its slice | On the command, a dependency `{"id": <event>, "type": "OUTBOUND", "title": <name>, "elementType": "EVENT"}` per event |
| A property of an element's JSON Schema | A field: `name`; `type` `String` (`format: uuid`: `UUID`, `date`: `Date`, `date-time`: `DateTime`), `Int` for `integer`, `Boolean`, `Double` for `number`; `cardinality: "List"` for an array; `optional: true` when not `required` |
| `"x-skilj-tag": true` on a property | `idAttribute: true` |
| `"x-skilj-pii": true` on a property | `pii: true` |
| A command whose details name `POST /v1/commands/trigger` | `apiEndpoint` on the command, that endpoint |
| A line `- <kind>: <reason>` under a `Rejections` heading in a command's details | A specification: `when` a `SPEC_COMMAND` step linked to the command, `then` a `SPEC_ERROR` step titled `<kind>: <reason>` |

Leave out what has no row (UI, automations, hot spots). Only read the board:
this import never changes it. Ask before adding `x-skilj-tag` or a
`Rejections` heading to the board; without them the import still works, with
notes for each type without tags. `skilj-codegen/tests/fixtures/banking.prooph-board.em.json`
is the banking chapter written this way.
