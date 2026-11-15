# Bring your modeling tool

If you design with [Event Modeling](https://eventmodeling.org) in a tool, you
don't have to write the model a second time. `skilj-codegen import` turns one
bounded context of the model into a `.skilj.toml`, the declarative file
`skilj-codegen` generates event and command types from. `.skilj.toml` is the
one format every tool imports to, so each tool needs an importer and nothing
else. Command decisions, projections and tests stay hand-written Rust
(docs/architecture.md §253).

```sh
cargo install skilj-codegen
skilj-codegen import --from <source> <model> <bounded_context> --output src/<bounded_context>.skilj.toml
skilj-codegen check src/<bounded_context>.skilj.toml
```

Nothing is ever written back to the tool. Nothing here is needed for the
quickstart either: a project generated from the template works without any
tool or account.

| Tool | How it is read | `--from` | Level |
|---|---|---|---|
| [Event Modeling HCL](https://event-modeling-hcl.github.io) | `emhcl export` JSON | `emhcl` | Supported: see [its own page](event-modeling-hcl.md) |
| [Nebulit's Event Modeling app](https://miro.com/marketplace/eventmodeling/) for Miro, and [eventmodelers.ai](https://app.eventmodelers.ai) | Its JSON export, in the published [Event Modeling Specification schema](https://github.com/dilgerma/event-modeling-spec) | `em-json` | Experimental |
| [prooph board](https://prooph-board.com) | Its MCP server, through the `skilj-event-modeling` guide, which writes Event Modeling JSON from a board | `em-json` | Experimental |
| The text language of [event-modeling-tools](https://github.com/lgazo/event-modeling-tools) (also Mermaid's `eventmodeling` diagram) | The `.evml` model file | `evml` | Experimental |
| [Qlerify](https://www.qlerify.com) | Not imported | | Dropped: Qlerify publishes no schema for its JSON export, so an importer would be reverse-engineered from exports and break unannounced. Its MCP server could become a route like prooph board's. |

**Experimental** means the importer is tested against skilj-demo's banking
context written in that format (`skilj-codegen/tests/fixtures/`) and checked
against real exports and the format's own definition when it was written
(November 2026), but the format is the tool's, not skilj's, and nobody has
used the import on a real project yet. Report what doesn't import.

With a coding agent, you don't have to pick: the `skilj-event-modeling` guide
looks for a model in the project (an `.em.hcl` folder, a JSON export, an
`.evml` file, prooph board's MCP tools) and runs the right import.

## What every import does

Whatever the source, the import:

- names each event and command type in PascalCase, and takes a bounded
  context's events and the commands that emit them, leaving out external
  events;
- maps `String`, `UUID`, `Date` and `DateTime` to `string`, `Int` and `Long`
  to `i64`, and `Boolean` to `bool`;
- makes a field marked as an id a DCB tag, with the field name as its key;
- leaves out, with a note, a type with a field `.skilj.toml` can't express (a
  list, nested fields, an optional field, a `Double`, `Decimal`, `Number` or
  `Custom`): hand-write that one;
- keeps a field marked as personal data, with a note to declare it sensitive,
  or blind when a rule matches on it;
- gives a command `rest_trigger_allowed = true` when its issuer is the endpoint
  `POST /v1/commands/trigger`;
- lists each command's rejection kinds in a comment above it, from errors
  written as `<kind>: <reason>`; an error without a kind is the generic kind
  `rejected`;
- notes a type with no id field, which has no tags.

The notes go to stderr, one line each (`--diagnostic-format json` makes them
one JSON object), and head the file as comments. The file is validated before
it is written, so an import either generates or names its problems. Add what
no model records afterwards: `reads_nothing`, consistency queries, sensitive
fields and blind tags.

## Event Modeling JSON (`em-json`)

The [Event Modeling Specification](https://github.com/dilgerma/event-modeling-spec)
is a JSON schema for a model as slices of commands, events, read models and
screens, with given/when/then specifications. Nebulit's app for Miro and
eventmodelers.ai export it; `emhcl export` writes it too, which is why the two
importers share their reader. Element ids are the tool's own, so names come
from titles:

1. **Bounded context.** An event's `modelContext`, or else its slice's
   `context`, in `snake_case` is the bounded context: `Banking` and `Todo List`
   are `banking` and `todo_list`.
2. **Names.** An element's title in PascalCase is its `NAME`: `Money
   Deposited` is `MoneyDeposited`. Name events in the past tense and commands
   in the imperative.
3. **Fields.** A field name in `snake_case` is the payload field:
   `accountId` is `account_id`.
4. **Tags.** Mark each field a decision matches on with `idAttribute`. Use the
   same field name for the same thing in every event and command.
5. **Rejections.** Title each refused specification's error step
   `<kind>: <reason>`: `insufficient_funds: withdrawal exceeds the balance`.
6. **REST triggering.** Give the issuing screen, or the command itself, the
   `apiEndpoint` `POST /v1/commands/trigger`.

A copy of an element (`elementCopy`, pointing at its original with `linkedId`)
is the original. `skilj-codegen/tests/fixtures/banking.em.json` is the banking
context in this format.

## prooph board

prooph board keeps a model on its servers and has an MCP server for agents,
not an export file. The `skilj-event-modeling` guide reads a chapter over it and writes it
as Event Modeling JSON, which `--from em-json` imports. Its
`references/modeling-tools.md` has the whole mapping; in short:

1. **Bounded context.** Name each `system` lane after a bounded context.
2. **Names and fields.** An element's name is its title; each property of the
   JSON Schema in its details is a field.
3. **Tags and personal data.** Add `"x-skilj-tag": true` to a property a
   decision matches on, and `"x-skilj-pii": true` to personal data.
4. **Rejections.** List them under a `Rejections` heading in the command's
   details, one `- <kind>: <reason>` line each.
5. **REST triggering.** Name `POST /v1/commands/trigger` in the command's
   details.

The board is only read. The [cinema sample for Axon 5](https://github.com/MateuszNaKodach/Cinema.EventSourcing.VerticalSlice.DCB.Kotlin.Axon5.Spring)
reads and changes boards over the same server, for a DCB project as well.
`skilj-codegen/tests/fixtures/banking.prooph-board.em.json` is what the
guide writes for the banking chapter. This route is the least tested: the
import half is, the reading of a board isn't, since that needs an account.

## event-modeling-tools text (`evml`)

[event-modeling-tools](https://github.com/lgazo/event-modeling-tools) defines a
text language for Event Modeling (`.evml` files, or `evml` code blocks in
Markdown), with a VS Code extension, an Obsidian plugin and a CLI that renders
diagrams; Mermaid draws the same language as its `eventmodeling` diagram. Its
data blocks are free text, so the convention puts the field types there:

```text
eventmodeling

tf 01 ui api.WithdrawMoneyRequest
tf 02 cmd WithdrawMoney [[WithdrawMoney]]
tf 03 evt banking.MoneyWithdrawn [[MoneyWithdrawn]]

data WithdrawMoney {
  account_id: string tag
  amount: integer
}

data MoneyWithdrawn {
  account_id: string tag
  amount: integer
}

note 02 {
  reject insufficient_funds: withdrawal exceeds the balance
}
```

1. **Bounded context.** An event's namespace in `snake_case` is its bounded
   context: `evt banking.MoneyDeposited`. Events of other namespaces, or
   without one, are left out.
2. **Names.** The entity name is the `NAME`: `MoneyDeposited`.
3. **Commands.** A command belongs to the context of an event it leads to:
   put the event's frame right after the command's, or name the command's
   frame with `->>`.
4. **Fields.** In the frame's data, inline or a `data` block named with
   `[[...]]`, one `name: type` per line or comma: `string`, `uuid`, `date`,
   `datetime`, `integer`, `int`, `long`, `boolean` or `bool`. An example value
   works too (`'acc-1'`, `100`, `true`). `name?` is optional.
5. **Tags and personal data.** `tag` after the type marks a DCB tag, `pii`
   personal data: `email: string pii`.
6. **REST triggering.** A `ui` frame in the `api` namespace that leads to a
   command makes it REST-triggered.
7. **Rejections.** A `note` on the command's frame with a line
   `reject <kind>: <reason>` per rejection; given/when/then has no way to say a
   command was refused.

`skilj-codegen/tests/fixtures/banking.evml` is the banking context in this
format; event-modeling-tools' own parser (`event-modeling-language` 1.0.0)
accepts it without a diagnostic.

## Rules in Allium

The model says which commands, events and read models exist. What each
decision requires can be kept in [Allium](https://juxt.github.io/allium), as
an `.allium` file next to the `.skilj.toml`: a rule per command, a
`skilj-test-fixture` test per case. It is optional; the
`skilj-event-modeling` guide's `references/allium.md` shows how the two fit.
