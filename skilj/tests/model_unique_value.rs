//! Generated command histories of the unique-email domain, each checked step
//! by step against a model of it (issue #189, `unique_crm::model`).
//!
//! A history is a sequence of registrations, corrections, releases, release
//! retractions and raw retractions, from two callers, half of them under one
//! of three idempotency keys, over three customers and four addresses in
//! three spellings each. Every step's outcome is compared with the model's:
//! accepted with the same events, rejected with the same kind, or
//! deduplicated to the same sequences. Every appended event's tags are
//! checked too: its customer, its retraction target, and for a blind tag that
//! one address always has one hash and no plaintext is stored.
//!
//! Each history runs in a bounded context of its own, created for it and
//! hard-deleted after it, so a history and every attempt to shrink one start
//! from nothing. Both the blind and the plain email tag are tested.
//!
//! What it doesn't cover: interleavings; idempotency-key retention, which sweeps every bounded context
//! and has a binary of its own; and a raw retraction of a release, which can
//! leave a customer's own read showing a claim another customer holds and is
//! documented as the operator's escape hatch rather than modelled.
//!
//! Bounds: `PROPTEST_CASES` histories per variant (32 by default) of at most
//! `SKILJ_MODEL_MAX_OPS` operations (20 by default). A failure is shrunk and
//! printed as the operations that reproduce it, which `replay` takes as they
//! are, and its seed is kept in `model_unique_value.regressions` next to this
//! file, which runs first next time.

mod unique_crm;

use std::collections::HashMap;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{FileFailurePersistence, TestRng, TestRunner};
use skilj::SkiljBuilder;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::Event;
use unique_crm::model::{
    customer_id, Addr, Call, Expected, Kind, Logged, Model, Op, Position, Target, Value, Variant,
    BASES, CLIENTS, CUSTOMERS, KEYS, SPELLINGS,
};
use unique_crm::{
    bounded_context, runtime, Blind, EmailTagging, Plain, IGNORES_CLAIMS_SINCE, IGNORES_HELD,
    IGNORES_RELEASES, NO_DEFECT,
};

static TEST_DB: tokio::sync::OnceCell<Option<(String, Pool)>> = tokio::sync::OnceCell::const_new();

async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(|| async {
            let database_url = skilj_test_support::database_url("skilj_model_unique_value").await?;
            let pool = db::connect(&database_url).await.unwrap();
            db::migrate(&pool).await.unwrap();
            Some((database_url, pool))
        })
        .await
        .clone()
}

fn env_number(name: &str, default: u32) -> u32 {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

// ---- Running a history -----------------------------------------------------

/// Runs `ops` against a fresh bounded context, step by step against the
/// model. `Err` says which step disagreed and how; `Ok` also when there is no
/// database, after `skipping:` was printed for CI's guard.
async fn run_history(variant: Variant, ops: &[Op]) -> Result<(), String> {
    run_planted(variant, ops, Plant::NONE).await
}

/// A defect planted for a run, to check that the comparison catches it.
#[derive(Clone, Copy, Debug)]
struct Plant {
    /// One of `unique_crm`'s defects, planted in the command it names.
    defect: u8,
    /// Submit without the idempotency key, as a retry layer losing it would.
    drop_keys: bool,
}

impl Plant {
    const NONE: Plant = Plant {
        defect: NO_DEFECT,
        drop_keys: false,
    };
}

async fn run_planted(variant: Variant, ops: &[Op], plant: Plant) -> Result<(), String> {
    match variant {
        Variant::Blind => run_history_as::<Blind>(variant, ops, plant).await,
        Variant::Plain => run_history_as::<Plain>(variant, ops, plant).await,
    }
}

async fn run_history_as<V: EmailTagging>(
    variant: Variant,
    ops: &[Op],
    plant: Plant,
) -> Result<(), String> {
    let Some((database_url, pool)) = test_db().await else {
        return Ok(());
    };
    let (bc, subject) = bounded_context(&pool).await;
    let builder: SkiljBuilder =
        unique_crm::builder_with_defect::<V>(&database_url, &bc, &subject, plant.defect);
    let (skilj, _) = builder.retractions().build().await.unwrap();
    let cache = EventCache::new(1000);
    let target = unique_crm::Target {
        skilj: &skilj,
        pool: &pool,
        bc: &bc,
        cache: &cache,
    };
    let mut checker = Checker::new(variant, &pool, &bc);
    checker.drop_keys = plant.drop_keys;
    let result = checker.run(target, ops).await;
    skilj.shutdown(std::time::Duration::from_secs(10)).await;
    db::hard_delete_bounded_context(&pool, &bc).await.unwrap();
    result
}

struct Checker<'a> {
    variant: Variant,
    pool: &'a Pool,
    bc: &'a str,
    model: Model,
    /// The store's sequence of each event of the model's log.
    sequences: Vec<i64>,
    /// The hash a blind tag gave each address.
    hashes: HashMap<Value, String>,
    /// A planted defect: submit every command without its key.
    drop_keys: bool,
}

impl<'a> Checker<'a> {
    fn new(variant: Variant, pool: &'a Pool, bc: &'a str) -> Self {
        Checker {
            variant,
            pool,
            bc,
            model: Model::new(variant),
            sequences: Vec::new(),
            hashes: HashMap::new(),
            drop_keys: false,
        }
    }

    async fn run(mut self, target: unique_crm::Target<'_>, ops: &[Op]) -> Result<(), String> {
        for (index, op) in ops.iter().enumerate() {
            self.step(target, *op).await.map_err(|error| {
                format!(
                    "step {index} of {} ({:?} tag): {op:?}\n{error}\nthe history:\n{}",
                    ops.len(),
                    self.variant,
                    trace(ops)
                )
            })?;
        }
        Ok(())
    }

    fn position(&self, position: Position) -> i64 {
        match position {
            Position::Index(index) => self.sequences[index],
            Position::PastEnd => self.sequences.last().copied().unwrap_or(0) + 1_000_000,
        }
    }

    async fn step(&mut self, target: unique_crm::Target<'_>, op: Op) -> Result<(), String> {
        let step = self.model.apply(op);
        let payload = match op {
            Op::Register { .. } | Op::Correct { .. } | Op::Release { .. } => {
                let (customer, addr) = step.args.expect("resolved");
                serde_json::json!({ "customer_id": customer_id(customer), "email": addr.text() })
            }
            Op::RetractRelease { .. } => {
                let (customer, addr) = step.args.expect("resolved");
                serde_json::json!({
                    "customer_id": customer_id(customer),
                    "email": addr.text(),
                    "position": self.position(step.target.expect("resolved")),
                })
            }
            Op::RetractAny { .. } => {
                serde_json::json!({ "position": self.position(step.target.expect("resolved")) })
            }
        };
        let call = op.call();
        let outcome = target
            .submit(
                op.command_type(),
                payload.clone(),
                &call.client_id(),
                call.key().filter(|_| !self.drop_keys).as_deref(),
            )
            .await;
        let mismatch = |why: String| {
            Err(format!(
                "payload {payload}\nexpected {:?}\ngot {}\n{why}",
                step.expected,
                describe(&outcome)
            ))
        };
        match (&step.expected, &outcome) {
            (Expected::Accepted(logged), SubmitCommandOutcome::Accepted { events, .. }) => {
                if events.len() != logged.len() {
                    return mismatch(format!("{} events appended", events.len()));
                }
                for (event, logged) in events.iter().zip(logged) {
                    if let Err(why) = self.check_event(event, logged) {
                        return mismatch(why);
                    }
                    self.sequences.push(event.sequence);
                }
            }
            (Expected::Rejected(expected), SubmitCommandOutcome::Rejected { kind, .. })
                if kind == expected => {}
            (
                Expected::Deduplicated(indices),
                SubmitCommandOutcome::Deduplicated {
                    triggered_event_sequences,
                },
            ) => {
                let expected: Vec<i64> =
                    indices.iter().map(|index| self.sequences[*index]).collect();
                if &expected != triggered_event_sequences {
                    return mismatch(format!("expected the sequences {expected:?}"));
                }
            }
            _ => return mismatch(String::new()),
        }
        let stored = db::list_events_for_bounded_context(self.pool, self.bc)
            .await
            .unwrap();
        if stored.len() != self.model.log.len() {
            return mismatch(format!(
                "{} events stored, the model has {}",
                stored.len(),
                self.model.log.len()
            ));
        }
        if self.variant == Variant::Blind {
            for event in &stored {
                let stored_text = format!("{:?} {}", event.tags, event.payload).to_lowercase();
                if stored_text.contains("@example.test") {
                    return mismatch(format!(
                        "an address is stored in the clear at {}: {stored_text}",
                        event.sequence
                    ));
                }
            }
        }
        Ok(())
    }

    /// Whether `event` is the one the model logged: its type, its customer,
    /// its address tag, and for a retraction its target.
    fn check_event(&mut self, event: &Event, logged: &Logged) -> Result<(), String> {
        let tag = |key: &str| {
            event
                .tags
                .iter()
                .find(|tag| tag.key == key)
                .and_then(|tag| tag.value.clone())
        };
        let event_type = match logged.kind {
            Kind::Claimed => "EmailClaimed",
            Kind::Released => "EmailReleased",
            Kind::Retraction(_) => "EventRetracted",
        };
        if event.event_type.name != event_type {
            return Err(format!(
                "event {} is an {}, the model has an {event_type}",
                event.sequence, event.event_type.name
            ));
        }
        if tag("customer") != Some(customer_id(logged.customer)) {
            return Err(format!(
                "event {} has customer {:?}, the model {}",
                event.sequence,
                tag("customer"),
                customer_id(logged.customer)
            ));
        }
        if let Kind::Retraction(target) = logged.kind {
            let expected = self.sequences[target].to_string();
            if tag("skilj_retracts").as_deref() != Some(expected.as_str()) {
                return Err(format!(
                    "retraction {} names {:?}, the model {expected}",
                    event.sequence,
                    tag("skilj_retracts")
                ));
            }
        }
        let Some(email) = tag("email") else {
            return Err(format!("event {} has no email tag", event.sequence));
        };
        match self.variant {
            Variant::Plain if email != logged.value.text() => Err(format!(
                "event {} has email {email:?}, the model {:?}",
                event.sequence,
                logged.value.text()
            )),
            Variant::Plain => Ok(()),
            Variant::Blind => {
                let known = self
                    .hashes
                    .entry(logged.value)
                    .or_insert_with(|| email.clone());
                if *known != email {
                    return Err(format!(
                        "event {} hashes {:?} differently from before",
                        event.sequence, logged.value
                    ));
                }
                match self
                    .hashes
                    .iter()
                    .find(|(value, hash)| **value != logged.value && **hash == email)
                {
                    Some((other, _)) => Err(format!(
                        "event {} hashes {:?} as {other:?} was",
                        event.sequence, logged.value
                    )),
                    None => Ok(()),
                }
            }
        }
    }
}

fn describe(outcome: &SubmitCommandOutcome) -> String {
    match outcome {
        SubmitCommandOutcome::Accepted { events, .. } => format!(
            "accepted: {:?}",
            events
                .iter()
                .map(|event| (event.sequence, event.event_type.name.as_str()))
                .collect::<Vec<_>>()
        ),
        SubmitCommandOutcome::Rejected { kind, reason, .. } => format!("rejected {kind}: {reason}"),
        SubmitCommandOutcome::Deduplicated {
            triggered_event_sequences,
        } => format!("deduplicated: {triggered_event_sequences:?}"),
        other => format!("{other:?}"),
    }
}

/// A history as Rust, to paste into a `replay` test.
fn trace(ops: &[Op]) -> String {
    let mut out = String::from("vec![\n");
    for op in ops {
        out.push_str(&format!("    {op:?},\n"));
    }
    out.push(']');
    out
}

/// Runs `ops` and fails with the step that disagreed.
fn replay(variant: Variant, ops: Vec<Op>) {
    if let Err(error) = runtime().block_on(run_history(variant, &ops)) {
        panic!("{error}");
    }
}

// ---- Generating histories --------------------------------------------------

/// Mostly the canonical spelling, so a plain tag's histories meet the same
/// address often enough to contend for it; the others a third of the time.
fn addr() -> impl Strategy<Value = Addr> {
    let spelling = prop_oneof![4 => Just(0), 1 => Just(1), 1 => Just(SPELLINGS - 1)];
    (0..BASES, spelling).prop_map(|(base, spelling)| Addr { base, spelling })
}

fn call() -> impl Strategy<Value = Call> {
    let key = prop_oneof![1 => Just(None), 1 => (0..KEYS).prop_map(Some)];
    (0..CLIENTS, key).prop_map(|(client, key)| Call { client, key })
}

fn op() -> impl Strategy<Value = Op> {
    let customer = 0..CUSTOMERS;
    prop_oneof![
        4 => (customer.clone(), prop_oneof![1 => addr().prop_map(Some), 1 => Just(None)], call())
            .prop_map(|(customer, addr, call)| Op::Register { customer, addr, call }),
        2 => (customer.clone(), addr(), call())
            .prop_map(|(customer, addr, call)| Op::Correct { customer, addr, call }),
        3 => (customer.clone(), prop_oneof![1 => addr().prop_map(Some), 3 => Just(None)], call())
            .prop_map(|(customer, addr, call)| Op::Release { customer, addr, call }),
        3 => (
            prop_oneof![3 => Just(None), 1 => customer.clone().prop_map(Some)],
            prop_oneof![3 => Just(None), 1 => addr().prop_map(Some)],
            prop_oneof![
                6 => any::<u16>().prop_map(Target::Release),
                1 => any::<u16>().prop_map(Target::NotARelease),
                1 => Just(Target::PastEnd),
            ],
            call(),
        )
            .prop_map(|(customer, addr, target, call)| Op::RetractRelease {
                customer,
                addr,
                target,
                call,
            }),
        2 => (
            prop_oneof![5 => any::<u16>().prop_map(Target::NotARelease), 1 => Just(Target::PastEnd)],
            call(),
        )
            .prop_map(|(target, call)| Op::RetractAny { target, call }),
    ]
}

fn history() -> impl Strategy<Value = Vec<Op>> {
    proptest::collection::vec(op(), 1..=env_number("SKILJ_MODEL_MAX_OPS", 20) as usize)
}

fn config() -> ProptestConfig {
    ProptestConfig {
        cases: env_number("PROPTEST_CASES", 32),
        max_shrink_iters: 256,
        failure_persistence: Some(Box::new(FileFailurePersistence::WithSource("regressions"))),
        ..ProptestConfig::default()
    }
}

proptest! {
    #![proptest_config(config())]

    #[test]
    fn blind_histories_match_the_model(ops in history()) {
        let result = runtime().block_on(run_history(Variant::Blind, &ops));
        prop_assert!(result.is_ok(), "{}", result.unwrap_err());
    }

    #[test]
    fn plain_histories_match_the_model(ops in history()) {
        let result = runtime().block_on(run_history(Variant::Plain, &ops));
        prop_assert!(result.is_ok(), "{}", result.unwrap_err());
    }
}

// ---- The model on its own, without a database ------------------------------

const UNKEYED: Call = Call {
    client: 0,
    key: None,
};

fn at(base: u8, spelling: u8) -> Addr {
    Addr { base, spelling }
}

fn register(customer: u8, addr: Addr) -> Op {
    Op::Register {
        customer,
        addr: Some(addr),
        call: UNKEYED,
    }
}

fn correct(customer: u8, addr: Addr) -> Op {
    Op::Correct {
        customer,
        addr,
        call: UNKEYED,
    }
}

fn release(customer: u8, addr: Addr) -> Op {
    Op::Release {
        customer,
        addr: Some(addr),
        call: UNKEYED,
    }
}

/// The `nth` release in the history, retracted by its own customer under its
/// own address.
fn retract_release(nth: u16) -> Op {
    Op::RetractRelease {
        customer: None,
        addr: None,
        target: Target::Release(nth),
        call: UNKEYED,
    }
}

fn retract_any(target: Target) -> Op {
    Op::RetractAny {
        target,
        call: UNKEYED,
    }
}

fn keyed(op: Op, client: u8, key: u8) -> Op {
    let call = Call {
        client,
        key: Some(key),
    };
    match op {
        Op::Register { customer, addr, .. } => Op::Register {
            customer,
            addr,
            call,
        },
        Op::Correct { customer, addr, .. } => Op::Correct {
            customer,
            addr,
            call,
        },
        Op::Release { customer, addr, .. } => Op::Release {
            customer,
            addr,
            call,
        },
        Op::RetractRelease {
            customer,
            addr,
            target,
            ..
        } => Op::RetractRelease {
            customer,
            addr,
            target,
            call,
        },
        Op::RetractAny { target, .. } => Op::RetractAny { target, call },
    }
}

/// What the model answers each of `ops`, as short labels.
fn answers(variant: Variant, ops: &[Op]) -> Vec<String> {
    let mut model = Model::new(variant);
    ops.iter()
        .map(|op| match model.apply(*op).expected {
            Expected::Accepted(_) => "accepted".to_string(),
            Expected::Rejected(kind) => kind.to_string(),
            Expected::Deduplicated(indices) => format!("deduplicated {indices:?}"),
        })
        .collect()
}

#[test]
fn the_model_holds_an_address_once_normalized_and_a_customer_to_one() {
    let ops = [
        register(0, at(0, 0)),
        register(1, at(0, 1)),
        register(0, at(1, 0)),
        correct(0, at(0, 2)),
        correct(1, at(0, 0)),
        release(1, at(0, 0)),
        release(0, at(0, 1)),
        register(1, at(0, 0)),
    ];
    assert_eq!(
        answers(Variant::Blind, &ops),
        [
            "accepted",
            "email_taken",
            "already_holds_a_value",
            "accepted",
            "email_taken",
            "not_held",
            "accepted",
            "accepted",
        ]
    );
    assert_eq!(
        answers(Variant::Plain, &ops),
        [
            "accepted",
            "accepted",
            "already_holds_a_value",
            "accepted",
            "accepted",
            "accepted",
            "not_held",
            "accepted",
        ],
        "a plain tag doesn't normalize, so each spelling is an address"
    );
}

#[test]
fn the_model_leaves_the_current_holder_after_a_late_retraction() {
    let ops = [
        register(0, at(0, 0)),
        release(0, at(0, 0)),
        register(1, at(0, 0)),
        // Customer 0's claim, the first of the two events that aren't releases.
        retract_any(Target::NotARelease(0)),
        register(2, at(0, 0)),
        register(0, at(1, 0)),
    ];
    assert_eq!(
        answers(Variant::Blind, &ops),
        [
            "accepted",
            "accepted",
            "accepted",
            "accepted",
            "email_taken",
            "accepted"
        ]
    );
}

#[test]
fn the_model_retracts_a_release_only_while_nobody_else_claimed_since() {
    let ops = [
        register(0, at(0, 0)),
        release(0, at(0, 0)),
        retract_release(0),
        register(1, at(0, 0)),
        release(0, at(0, 0)),
        register(1, at(0, 1)),
        release(1, at(0, 0)),
        // The releases still standing: customer 0's second, which customer 1
        // claimed the address after, and customer 1's.
        retract_release(0),
        register(1, at(1, 0)),
        retract_release(1),
    ];
    assert_eq!(
        answers(Variant::Blind, &ops),
        [
            "accepted",
            "accepted",
            "accepted",
            "email_taken",
            "accepted",
            "accepted",
            "accepted",
            "value_taken_since",
            "accepted",
            "already_holds_a_value",
        ]
    );
}

#[test]
fn the_model_refuses_a_release_retraction_naming_another_address() {
    let ops = [
        register(0, at(0, 0)),
        release(0, at(0, 0)),
        Op::RetractRelease {
            customer: None,
            addr: Some(at(1, 0)),
            target: Target::Release(0),
            call: UNKEYED,
        },
        Op::RetractRelease {
            customer: Some(1),
            addr: None,
            target: Target::Release(0),
            call: UNKEYED,
        },
        Op::RetractRelease {
            customer: None,
            addr: Some(at(0, 1)),
            target: Target::Release(0),
            call: UNKEYED,
        },
    ];
    assert_eq!(
        answers(Variant::Blind, &ops),
        [
            "accepted",
            "accepted",
            "retraction_target_mismatch",
            "not_a_release",
            "accepted",
        ],
        "another spelling of the same address is the same address"
    );
}

#[test]
fn the_model_answers_a_raw_retraction_as_the_store_checks_it() {
    let ops = [
        retract_any(Target::PastEnd),
        register(0, at(0, 0)),
        correct(0, at(1, 0)),
        // Events: 0 the claim, 1 its retraction, 2 the new claim.
        retract_any(Target::NotARelease(0)),
        retract_any(Target::NotARelease(1)),
        retract_any(Target::NotARelease(2)),
        register(1, at(1, 0)),
    ];
    assert_eq!(
        answers(Variant::Plain, &ops),
        [
            "retraction_target_not_found",
            "accepted",
            "accepted",
            "already_retracted",
            "retraction_of_retraction",
            "accepted",
            "accepted",
        ]
    );
}

#[test]
fn the_model_answers_an_accepted_key_with_what_it_appended() {
    let ops = [
        keyed(register(0, at(0, 0)), 0, 0),
        keyed(register(1, at(1, 0)), 0, 0),
        keyed(register(1, at(1, 0)), 1, 0),
        keyed(release(2, at(2, 0)), 0, 1),
        keyed(register(2, at(2, 0)), 0, 1),
        keyed(release(2, at(2, 0)), 0, 1),
        keyed(correct(0, at(3, 0)), 0, 0),
        keyed(correct(0, at(0, 0)), 0, 0),
    ];
    assert_eq!(
        answers(Variant::Blind, &ops),
        [
            "accepted",
            "deduplicated [0]",
            "accepted",
            "not_held",
            "accepted",
            "accepted",
            "accepted",
            "deduplicated [4, 5]",
        ],
        "a key is per command type and caller, whatever the payload, and a \
         rejection isn't remembered"
    );
}

// ---- Fixed histories against Postgres --------------------------------------

#[test]
fn a_fixed_history_of_every_operation_matches_the_model() {
    let ops = vec![
        keyed(register(0, at(0, 1)), 0, 0),
        register(1, at(0, 2)),
        keyed(register(1, at(1, 0)), 0, 0),
        register(1, at(1, 0)),
        correct(1, at(2, 1)),
        release(0, at(0, 0)),
        retract_release(0),
        release(0, at(0, 2)),
        register(2, at(0, 0)),
        retract_release(0),
        Op::RetractRelease {
            customer: None,
            addr: Some(at(3, 0)),
            target: Target::Release(0),
            call: UNKEYED,
        },
        retract_any(Target::NotARelease(0)),
        retract_any(Target::PastEnd),
        keyed(correct(2, at(3, 0)), 1, 2),
        keyed(correct(2, at(3, 0)), 1, 2),
    ];
    replay(Variant::Blind, ops.clone());
    replay(Variant::Plain, ops);
}

// ---- Whether the property can fail -----------------------------------------

/// `count` histories as the property generates them, the same every run.
fn sample_histories(count: usize) -> Vec<Vec<Op>> {
    let mut runner = TestRunner::new_with_rng(
        ProptestConfig::default(),
        TestRng::deterministic_rng(proptest::test_runner::RngAlgorithm::ChaCha),
    );
    let strategy = history();
    (0..count)
        .map(|_| strategy.new_tree(&mut runner).unwrap().current())
        .collect()
}

/// The generated histories reach every answer the domain and the store can
/// give, each a few times, so the property is never about the easy cases only.
#[test]
fn generated_histories_reach_every_outcome() {
    for variant in [Variant::Blind, Variant::Plain] {
        let mut seen = std::collections::BTreeMap::<String, usize>::new();
        for ops in sample_histories(300) {
            for answer in answers(variant, &ops) {
                *seen
                    .entry(answer.split(' ').next().unwrap().to_string())
                    .or_default() += 1;
            }
        }
        eprintln!("{variant:?}: {seen:?}");
        for expected in [
            "accepted",
            "deduplicated",
            "email_taken",
            "already_holds_a_value",
            "nothing_to_correct",
            "not_held",
            "not_a_release",
            "value_taken_since",
            "retraction_target_mismatch",
            "retraction_target_not_found",
            "retraction_of_retraction",
            "already_retracted",
        ] {
            let count = seen.get(expected).copied().unwrap_or(0);
            assert!(
                count >= 5,
                "{variant:?}: generated histories answer {expected} {count} times; seen {seen:?}"
            );
        }
    }
}

/// Runs the property itself with `plant`, and says whether it failed within
/// the PR budget of histories. Shrinking is skipped: finding it is the point.
fn property_catches(plant: Plant) -> bool {
    let config = ProptestConfig {
        cases: 32,
        max_shrink_iters: 0,
        failure_persistence: None,
        ..ProptestConfig::default()
    };
    let mut runner = TestRunner::new_with_rng(
        config.clone(),
        TestRng::deterministic_rng(config.rng_algorithm),
    );
    // No database: `skipping:` was printed, which CI fails on.
    if runtime().block_on(test_db()).is_none() {
        return true;
    }
    runner
        .run(&history(), |ops| {
            let result = runtime().block_on(run_planted(Variant::Blind, &ops, plant));
            prop_assert!(result.is_ok(), "{}", result.unwrap_err());
            Ok(())
        })
        .is_err()
}

#[test]
fn the_property_catches_a_registration_ignoring_what_the_customer_holds() {
    assert!(property_catches(Plant {
        defect: IGNORES_HELD,
        drop_keys: false,
    }));
}

#[test]
fn the_property_catches_a_registration_query_missing_releases() {
    assert!(property_catches(Plant {
        defect: IGNORES_RELEASES,
        drop_keys: false,
    }));
}

#[test]
fn the_property_catches_a_release_retraction_ignoring_later_claims() {
    assert!(property_catches(Plant {
        defect: IGNORES_CLAIMS_SINCE,
        drop_keys: false,
    }));
}

#[test]
fn the_property_catches_a_retry_that_drops_its_key() {
    assert!(property_catches(Plant {
        defect: NO_DEFECT,
        drop_keys: true,
    }));
}

/// Found by the model: a release retracted after its customer claimed the
/// address again left two standing claims of it, and a correction retracting
/// the newer one left the customer holding the old address and the new.
#[test]
fn a_release_retracted_after_its_customer_reclaimed_the_address() {
    let ops = vec![
        register(2, at(0, 0)),
        release(2, at(0, 0)),
        register(2, at(0, 0)),
        retract_release(0),
        correct(2, at(2, 0)),
        register(0, at(0, 0)),
    ];
    assert_eq!(
        answers(Variant::Blind, &ops),
        [
            "accepted",
            "accepted",
            "accepted",
            "value_taken_since",
            "accepted",
            "accepted",
        ]
    );
    replay(Variant::Blind, ops.clone());
    replay(Variant::Plain, ops);
}
