# SklilJ

`skilj` is a Rust library for building event-sourced applications backed by Postgres. You define
your domain as **events** (facts that happened) and **commands** (requests to make something
happen), and `skilj` handles storage, consistency checking, and exposing it all over GraphQL and
REST — so you can focus on the domain logic itself.

## Why not classic aggregates?

Most event-sourced libraries make you pick one fixed "aggregate" boundary up front (an `Account`,
an `Order`) and every command for that aggregate replays its entire history. `skilj` uses a
**Dynamic Consistency Boundary (DCB)** instead: events and commands carry tags (e.g. `wallet:
"w1"`), and a command's consistency check spans exactly the tagged events it needs — no more, no
less. A command that touches two things at once (say, enrolling a student in a course) can check
both their histories in one atomic decision, without either one having to "own" the other.

## A quick look

This is trimmed from [`templates/skilj-template/src/wallet.rs`](templates/skilj-template/src/wallet.rs)
— see that file for the complete, running version:

```rust
// An event: something that happened, tagged by which wallet it belongs to.
#[derive(Serialize, Deserialize, JsonSchema)]
struct WithdrawnPayload { wallet_id: String, amount: i64 }

struct Withdrawn;

#[auto_register(BOUNDED_CONTEXT)]
impl EventType for Withdrawn {
    type Payload = WithdrawnPayload;
    const NAME: &'static str = "Withdrawn";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![TagMapping { key: "wallet".into(), field: "wallet_id".into() }]
    }
}

// A command: a request, decided against only the events sharing its tags -
// here, every Deposited/Withdrawn event for this one wallet, nothing else.
struct Withdraw;

#[auto_register(BOUNDED_CONTEXT)]
impl CommandType for Withdraw {
    type Payload = WithdrawPayload;
    type Event = WalletEvent;
    const NAME: &'static str = "Withdraw";
    fn tag_mappings() -> Vec<TagMapping> { /* same as above */ }

    fn decide(payload: &Self::Payload, matching_events: &[Self::Event]) -> CommandDecision {
        let balance = balance_of(matching_events);
        if payload.amount > balance {
            return CommandDecision::Rejected {
                reason: format!("insufficient funds: balance is {balance}"),
                kind: "insufficient_funds".into(),
            };
        }
        CommandDecision::Accepted {
            events: vec![EventSpec {
                event_type: "Withdrawn".into(),
                payload: serde_json::json!({ "wallet_id": payload.wallet_id, "amount": payload.amount }),
            }],
        }
    }
}
```

That's it — no separate storage layer to wire up, no aggregate repository to implement. Add a
`Projection` (also shown in `wallet.rs`) when you need a read-optimised view instead of replaying
events on every query.

## Getting started

The fastest way to try it is to generate a small, working project:

```sh
cargo generate --git https://codeberg.org/gklijs/SklilJ.git templates/skilj-template
```

That gives you a runnable server with one bounded context (the wallet example above) already
wired up — see its own README for how to run it against Postgres.

To add `skilj` to an existing project instead:

```toml
[dependencies]
skilj = "0.0"
```

[`skilj-demo`](skilj-demo) in this repository is a larger worked example (two bounded contexts:
banking and courses) if you want to see more before committing.

## Crates in this workspace

| Crate | What it is |
|---|---|
| [`skilj`](skilj) | The main library - start here. A thin facade over `skilj-core`/`skilj-graphql`/`skilj-rest`. |
| [`skilj-core`](skilj-core) | The domain engine: events, commands, projections, and persistence. No web framework dependency. |
| [`skilj-graphql`](skilj-graphql) | The GraphQL surface - usable on its own if you don't need REST. |
| [`skilj-rest`](skilj-rest) | The REST surface - authenticated routes for agents and automated callers. |
| [`skilj-codegen`](skilj-codegen) | Optional: generate event/command boilerplate from a declarative `.skilj.toml` file instead of hand-writing it. |
| [`skilj-tui`](skilj-tui) | `cargo install skilj-tui` - a terminal console (GraphQL client) for browsing and operating a running deployment. |
| [`skilj-inspector`](skilj-inspector) | `cargo install skilj-inspector` - a terminal console that reads straight from Postgres, for when the GraphQL server isn't running. |
| [`skilj-temporal`](skilj-temporal) | Optional: a bridge from skilj's own event stream to [Temporal](https://temporal.io) - start or signal a workflow execution correlated by a DCB tag. See [`docs/architecture.md` §34](docs/architecture.md#skilj-temporal-plan). |
| [`skilj-kafka`](skilj-kafka) | Optional: a bridge between skilj's own event stream and Kafka, both directions - produce mapped events to a topic, consume a topic into `ExternalEventIngestion`/`CommandTrigger`. See [`docs/architecture.md` §40](docs/architecture.md#skilj-kafka-bridge). |
| [`skilj-amqp`](skilj-amqp) | Optional: `skilj-kafka`'s own sibling for any AMQP 1.0 broker - Solace PubSub+, Azure Service Bus, ActiveMQ Artemis. See [`docs/architecture.md` §41](docs/architecture.md#skilj-amqp-bridge). |
| [`skilj-nats`](skilj-nats) | Optional: a third bridge sibling, for NATS JetStream. See [`docs/architecture.md` §42](docs/architecture.md#skilj-nats-bridge). |
| [`skilj-macros`](skilj-macros) | Internal proc-macros, re-exported through `skilj-core`/`skilj` (not uniformly - each macro picks whichever crate it applies to) - you won't normally add this directly. |

## Documentation

- [`specs/skilj.allium`](specs/skilj.allium) - the behavioural specification: what the system
  guarantees, independent of the Rust code.
- [`docs/architecture.md`](docs/architecture.md) - how that's actually built in Rust, including
  the reasoning behind each design decision. It's written as a running design log, so later
  sections assume you've read the earlier ones.
- [`docs/rest-event-reading.md`](docs/rest-event-reading.md) - the three ways to consume the REST
  event stream and when to use each.
- [`docs/temporal-integration.md`](docs/temporal-integration.md) - pairing skilj with
  [Temporal](https://temporal.io) for long-running, cross-system processes.

## Contributing

Bug reports and pull requests are welcome - see [`CONTRIBUTING.md`](CONTRIBUTING.md) for how to
build, test, and submit changes, and [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) for community
expectations. Found a security issue? See [`SECURITY.md`](SECURITY.md) instead of opening a
public issue.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or
  http://www.apache.org/licenses/LICENSE-2.0)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or http://opensource.org/licenses/MIT)

at your option.

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion
in this project, as defined in the Apache-2.0 license, shall be dual-licensed as above, without
any additional terms or conditions.
