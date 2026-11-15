# Keeping an application's rules in Allium

[Allium](https://juxt.github.io/allium) is a language for writing down what a
system must do, as `.allium` files next to the code. It is optional for a
skilj application: the template, its quickstart and every guide work without
it. Use it when the user already does, or asks for a place to keep the rules
that a domain expert can read and a checker can check. skilj keeps its own
specification this way (`specs/skilj.allium`).

## Which file says what

The model and the Allium file describe one bounded context from two sides.
Keep each fact in one of them:

| Fact | Where it lives |
|---|---|
| Which commands, events and read models exist; their fields, tags, sensitive fields | The model: the Event Modeling tool's file, and the `.skilj.toml` imported from it (or the Rust, when there is no model) |
| What a command requires before it is accepted, and what it then records | The Allium file: one `rule` per command |
| The cases that show a rule | The Allium file names them; `skilj-test-fixture` tests run them |

The direction is from the model to Allium: a rule refers to the model's names
and never declares a command's or an event's fields, so renaming a field
happens in the model and the rule follows. When the two disagree about which
commands or events exist, the model is right and the rule is out of date.
When they disagree about a requirement, ask the user: that is the decision the
Allium file is for.

Put the file next to the model, one file per bounded context: in `src/`,
`banking.allium` beside `banking.skilj.toml` (or `banking.rs`).

## A rule is a `decide_*` function

```allium
-- allium: 3
-- banking.allium: the rules of the banking bounded context.
-- Commands, events and their fields: src/banking.skilj.toml.

rule WithdrawMoney {
    when: WithdrawMoney(account_id, amount)
    -- Rejected as invalid_amount.
    requires: amount > 0
    -- Rejected as insufficient_funds. The balance is folded from the
    -- MoneyDeposited and MoneyWithdrawn events tagged with account_id.
    requires: balance(account_id) >= amount
    ensures: MoneyWithdrawn(account_id: account_id, amount: amount)
}
```

| In the rule | In Rust |
|---|---|
| `rule WithdrawMoney`, `when: WithdrawMoney(...)` | `decide_withdraw_money`, or `WithdrawMoney`'s `decide()` |
| Each `requires:` | One `CommandDecision::Rejected`; the comment names its `kind`, the one the command's `# Rejected as:` line lists |
| Each `ensures:` of an event | An event in `CommandDecision::Accepted` |
| What a `requires:` reads (`balance(account_id)`) | A fold over the events the command's tags select |

The last row is where skilj differs from other event stores: everything a
`requires:` reads must come from events that carry one of the command's tags,
or a concurrent command can change it unnoticed. When a rule reads something
no tag reaches, that is question 3 of `SKILL.md` again, before it is code.

## A case is a test

Write each case as a `skilj-test-fixture` test in `tests/`, named after the
rule and the case, so a reader can go from one to the other:

```rust,ignore
use skilj_test_fixture::command::GivenEvents;

#[test]
fn withdraw_money_over_the_balance_is_insufficient_funds() {
    GivenEvents::<WithdrawMoney>::new()
        .event(BankingEvent::MoneyDeposited(MoneyDepositedPayload {
            account_id: "acc-1".into(),
            amount: 100,
        }))
        .when(WithdrawMoneyPayload { account_id: "acc-1".into(), amount: 150 })
        .then_rejected("insufficient_funds");
}
```

A `requires:` gets at least the case that fails it and the case at its edge
(here: withdrawing exactly the balance is accepted). The fixture hands
`decide()` the events you list, so these tests check the rule, not which
events the tags select; run the server for that.

## Tools

The Allium plugin writes and reviews `.allium` files with the user (its
`elicit` skill asks the same kind of questions `SKILL.md` does, for rules):

```sh
claude plugin marketplace add juxt/claude-plugins
claude plugin install allium@juxt-plugins
```

In Codex: `codex plugin marketplace add juxt/claude-plugins`, then
`codex plugin add allium@juxt-plugins`. The checker is `cargo install
allium-cli`; `allium check` on the file reports structural problems; its note that no
surface provides a rule's trigger is expected, since skilj's REST and GraphQL
surfaces do.
Both are recommended, never required: without them, keep the file anyway, and
it is still the place the rules are written down.
