# Writing the summary as an Event Modeling HCL model

When the domain expert or team uses [Event Modeling HCL](https://event-modeling-hcl.github.io)
(they have `.em.hcl` files, `emhcl`, or the event-modeling-hcl plugin), write
the settled summary as an `.em.hcl` model too, or instead. It then gets the
`emhcl` validator and diagrams, and imports straight into a `.skilj.toml`. The
naming convention is in `docs/event-modeling-hcl.md`; this is how the
summary's parts map onto it.

| Summary | `.em.hcl` |
|---|---|
| The bounded context | `bounded_context "<snake_case name>"`, the skilj bounded context name |
| An event, `MoneyDeposited` | `event "money_deposited"` in that context; the label in PascalCase is the `NAME` |
| A payload sketch | `field` blocks, or one `field_type` per field shared across events and commands |
| A tag candidate on `account_id` | `id_attribute = true` on the `account_id` field; the field name is the tag key |
| A blind tag | `pii = true` on the field, and the blind tag decision (normalization, erasure) in its `description` |
| A command, `WithdrawMoney` | `command "withdraw_money"` in a `state_change`, with `to = [event.<context>.<event>]` |
| "Accepted when ..." | A scenario with `then { event = ... }` |
| "Rejected when ..." with label `insufficient_funds` | A scenario with `then { error = "insufficient_funds: <reason>" }` |
| REST-triggered | An issuing `screen` with `kind = "api"` and `api_endpoint = "POST /v1/commands/trigger"` |
| "Happens automatically, on a schedule" | An `automation`; say "on a schedule" in its `description` |
| Open questions | `hotspot` blocks, `status = "open"` |
| Deferred | Leave out of the board, or a hotspot with `kind = "request"` |

Keep these to the convention, because the import reads nothing else:

- Labels are lowercase `snake_case`; titles are for people only.
- Use the same field name for the same thing everywhere, so tag keys match.
- A command that needs two histories has both id fields. Don't invent an
  aggregate to "own" it; aggregates are optional and the import ignores them.
- Don't add a scenario error without a stable kind in front.

Validate before handing it over, with `emhcl validate --profile strict` where
`emhcl` is installed. To import: `emhcl export model/ -o model.json`, then
`cargo run -p skilj-codegen --example emhcl_import -- model.json <context>`.
`skilj-codegen/tests/fixtures/banking.em.hcl` is a complete example, the
banking summary of `output-format.md` as a model.
