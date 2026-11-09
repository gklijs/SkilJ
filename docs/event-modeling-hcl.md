# Designing with Event Modeling HCL

[Event Modeling HCL](https://event-modeling-hcl.github.io) is a language for
writing an [Event Modeling](https://eventmodeling.org) model as `.em.hcl` files,
with a validator and diagram renderer (`emhcl`) and a
[plugin](https://event-modeling-hcl.github.io/plugin.html) that teaches Claude,
Codex and ChatGPT the method and the language, and changes a team's model in
Git through merge requests. It is a design tool, with no runtime part. This
page shows how a model maps onto skilj, the naming convention that makes the
mapping exact, and how to turn a model into a `.skilj.toml`
(docs/architecture.md §243).

Written against emhcl v0.17.0 (specification v0.8.0) and plugin v0.2.0.

## How the concepts map

| Event Modeling HCL | skilj |
|---|---|
| `bounded_context` and its `event` blocks | A bounded context and its `EventType`s |
| `state_change`: screen → `command` → event | A `CommandType` and its `decide()` |
| `then { event = ... }` / `then { error = ... }` in a scenario | `CommandDecision::Accepted` / `Rejected` |
| A scenario's given/when/then | `skilj-test-fixture`: `command::GivenEvents` with `when`, then `then_accepted`/`then_rejected`; `projection::GivenEvents` with `then_state` |
| `state_view`: events → `readmodel` | A `Projection` |
| A `screen` with `kind = "api"` | The REST and GraphQL surfaces |
| A field with `id_attribute = true` | A DCB tag (see below) |
| A field with `pii = true` | A sensitive field, or a blind tag when a rule matches on it |
| A field with `deduplication_key = true` | An idempotency key |
| An `external = true` bounded context, and the `automation` that translates its events | External event ingestion, or a Kafka, AMQP or NATS bridge |
| An `automation` from an event of one context to a command | A `CrossContextRoute` (its `Source` and `Target` aren't required to be in different contexts) |
| An `automation` started by time ("every night", "after 30 minutes") | A `ScheduleDeadline`, or a recurring schedule |
| An `automation` that is a long, multi-step process | `skilj-temporal` (docs/temporal-integration.md) |
| `aggregate`, `lifecycle` | Not needed; a lifecycle's states are what a `decide()` derives from the matching events |

skilj concepts the language has no place for: blind tags, consistency queries
(`latest`), `reads_nothing`, retractions and doubts, command approval, and
payload limits and blobs. Record them in a `description` or a hotspot, and add
them to the imported `.skilj.toml` or the Rust.

## The naming convention

Follow these rules in the model, and the import below needs no manual mapping:

1. **Bounded context.** The emhcl label is the skilj bounded context name:
   `bounded_context "banking"`. Keep it to 40 characters.
2. **Event and command names.** The emhcl label, in PascalCase, is the skilj
   `NAME`: `event "money_deposited"` is `MoneyDeposited`, `command
   "withdraw_money"` is `WithdrawMoney`. Name events in the past tense and
   commands in the imperative. Titles are only for people; nothing reads them.
3. **DCB tags.** Mark each field a decision needs to match on with
   `id_attribute = true`. Its tag key is the field name without a trailing
   `_id`: `account_id` becomes `account = "account_id"`. Use the same field name
   for the same thing everywhere (one `field_type` per id), so events and
   commands share tag keys. A command that checks two things at once, such as
   a student and a course, has both id fields; it needs no owning aggregate.
4. **Rejections.** Write each refused scenario's error as
   `"<kind>: <reason>"`, with a stable `snake_case` kind:
   `error = "insufficient_funds: withdrawal exceeds the balance"`. The kind is
   the `kind` of `CommandDecision::Rejected` and what `then_rejected` asserts.
5. **REST triggering.** A command callers may send over REST has an API screen
   as its issuer: `kind = "api"`, `api_endpoint = "POST /v1/commands/trigger"`.
6. **Aggregates are optional.** Leave them out, or keep them for the diagram;
   the import doesn't read them. Don't express a uniqueness or cross-entity
   rule as an aggregate boundary; give the command the id fields it needs.

`skilj-codegen/tests/fixtures/banking.em.hcl` is skilj-demo's banking context
written this way. A part of it:

```hcl
bounded_context "banking" {
  field_type "account_id" {
    type         = "String"
    id_attribute = true
  }

  field_type "amount" {
    type = "Int"
  }

  event "money_withdrawn" {
    fields = [field_type.account_id, field_type.amount]
  }
}

state_change "withdraw_money" {
  screen "withdraw_money_request" {
    kind         = "api"
    api_endpoint = "POST /v1/commands/trigger"
    actor        = actor.account_holder
    to           = [command.withdraw_money]
  }

  command "withdraw_money" {
    fields = [field_type.banking.account_id, field_type.banking.amount]
    to     = [event.banking.money_withdrawn]
  }

  scenario "withdrawal_over_balance" {
    case = "limit"

    given {
      event    = event.banking.money_deposited
      examples = [{ account_id = "acc-1", amount = 100 }]
    }

    when {
      command  = command.withdraw_money
      examples = [{ account_id = "acc-1", amount = 150 }]
    }

    then {
      error = "insufficient_funds: withdrawal exceeds the balance"
    }
  }
}
```

That scenario is this test with `skilj-test-fixture`:

```rust,ignore
use skilj_test_fixture::command::GivenEvents;

GivenEvents::<WithdrawMoney>::new()
    .event(BankingEvent::MoneyDeposited(MoneyDepositedPayload {
        account_id: "acc-1".into(),
        amount: 100,
    }))
    .when(WithdrawMoneyPayload { account_id: "acc-1".into(), amount: 150 })
    .then_rejected("insufficient_funds");
```

## From a model to a `.skilj.toml`

Validate and export the model with `emhcl`, then import one bounded context:

```sh
emhcl validate --profile strict model/
emhcl export model/ -o model.json
cargo run -p skilj-codegen --example emhcl_import -- model.json banking > src/banking.skilj.toml
```

Or call `skilj_codegen::emhcl::import(&json, "banking")` from your own tool or
test. The result is a `.skilj.toml` for `skilj_codegen::generate`, with a
comment naming the model element each type came from and the rejection kinds
each command's scenarios use. The import:

- takes the events whose id is in that bounded context, and the commands that
  emit one of them; events of `external = true` contexts are left out;
- maps `String`, `UUID`, `Date` and `DateTime` to `string`, `Int` and `Long`
  to `i64`, and `Boolean` to `bool`;
- leaves out, with a note, a type that has a list, nested fields, an optional
  field, or a `Double`, `Decimal` or `Custom` field: hand-write that one;
- keeps a `pii = true` field, with a note to declare it sensitive or blind;
- notes an event or command without an `id_attribute` field, which has no
  tags, and a scenario error that doesn't follow `<kind>: <reason>`.

It doesn't write projections, automations, tests or `decide()` bodies; those
stay hand-written Rust. After importing, add what the model can't say:
`reads_nothing`, consistency queries, sensitive fields and blind tags. The
banking model imports to `skilj-demo/src/banking.skilj.toml` except for
`reads_nothing` on `DepositMoney`.

## Using the plugin with skilj

The plugin's method skills discover and review the business model; skilj's own
`skilj-event-modeling` skill asks the DCB tag questions the method doesn't. They
work well in that order: model the workflow with the plugin, then go over each
command's id fields with `skilj-event-modeling`, following the convention above.
Install the plugin in Claude Code with:

```sh
claude plugin marketplace add event-modeling-hcl/event-modeling-hcl-plugin
claude plugin install event-modeling-hcl-plugin@event-modeling-hcl
```
