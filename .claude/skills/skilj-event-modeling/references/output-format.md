# The structured summary

Write this to a plain markdown file (e.g. `<bounded-context-name>-event-model.md`,
next to where the bounded context's own Rust module will eventually
live) once a meaningful chunk of the domain has been worked through -
don't wait until everything is decided. It's a reviewable checkpoint,
not a final deliverable: show it to the domain expert and expect
corrections.

Deliberately plain data, not Rust and not tied to any future codegen
format - useful on its own as a design document even if nothing ever
reads it back mechanically.

## Shape

```markdown
# <Bounded context name> - event model

One-sentence description of what this bounded context is responsible for.

## Events

### <EventName, past tense>

- **Payload sketch**: field: rough type, field: rough type, ...
- **Triggered by**: <CommandName> (see below) | scheduled | external
- **Tag candidates**: <tag key>: <payload field>, ...

(repeat per event)

## Commands

### <CommandName, imperative>

- **Payload sketch**: field: rough type, ...
- **Tag candidates**: <tag key>: <payload field>, ... (should match the
  tags on whichever event(s) this command's own decision needs to see)
- **Accepted when**: plain-language condition -> emits <EventName>
- **Rejected when**: plain-language condition (repeat per distinct
  rejection reason) - name a short, stable label for each alongside the
  human-readable reason, e.g. `insufficient_funds`

(repeat per command)

## Open questions

- Anything genuinely undecided, one per line, in the domain expert's
  own terms - not "should X be Option<T>", but "does a cancelled order
  ever get reopened, or is cancellation final?"

## Deferred

- Anything mentioned but deliberately not modeled yet (e.g. "reporting
  needs a whole separate view, revisit later") - named so it isn't lost,
  not designed now.
```

## A filled-in example (from `skilj-demo/src/banking.rs`, retrofitted -
## real code already exists here, but this is what the document would
## have looked like before it did)

```markdown
# Banking - event model

Tracks account balances via deposits and withdrawals; no separate
"open account" step - the first deposit for an account id brings it
into existence.

## Events

### MoneyDeposited

- **Payload sketch**: account_id: string, amount: integer
- **Triggered by**: DepositMoney
- **Tag candidates**: account: account_id

### MoneyWithdrawn

- **Payload sketch**: account_id: string, amount: integer
- **Triggered by**: WithdrawMoney
- **Tag candidates**: account: account_id

## Commands

### DepositMoney

- **Payload sketch**: account_id: string, amount: integer
- **Tag candidates**: account: account_id
- **Accepted when**: amount is positive -> emits MoneyDeposited
- **Rejected when**: amount is zero or negative (`invalid_amount`)

### WithdrawMoney

- **Payload sketch**: account_id: string, amount: integer
- **Tag candidates**: account: account_id
- **Accepted when**: amount is positive and no more than the account's
  current balance -> emits MoneyWithdrawn
- **Rejected when**: amount is zero or negative (`invalid_amount`);
  amount exceeds the current balance (`insufficient_funds`)

## Open questions

- (none for this simple example - a real session would leave genuine
  ones here)

## Deferred

- Interest accrual, overdraft limits - out of scope for a first cut.
```

## Handing this off

Once reviewed and settled, this document is the input to the `skilj`
skill's own job: turning each event/command into a real
`EventType`/`CommandType` impl, the payload sketches into real Rust
structs (`#[derive(JsonSchema)]`), and the tag candidates into
`tag_mappings()`. Nothing about that step is automatic - a human (or an
agent using the `skilj` skill) still writes the Rust, using this
document as the brief rather than re-deriving the domain shape from
scratch.
