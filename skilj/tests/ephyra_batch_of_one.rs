//! A command submitted on its own and the same command submitted as a batch of
//! one answer the same, a first property written with Ephyra
//! (<https://github.com/datadog-labs/ephyra>), an agentic property-based
//! testing library.
//!
//! A generated history of the unique-email domain (`unique_crm`, issue #189)
//! runs step by step in two fresh bounded contexts at once: in one every
//! command goes through `db::decide_and_submit_command`, the direct path, in
//! the other through `CommandBatcher::prepare_command` and
//! `CommandBatcher::submit_prepared`, alone in its batch. After every step the
//! two outcomes must be the same: accepted with the same events at the same
//! sequences, rejected with the same kind and reason, or deduplicated to the
//! same sequences. After the history, both stored logs must be the same too.
//!
//! This is the property that has to keep holding while the direct path
//! becomes a batch of one, so the two paths can be one: it doesn't compare
//! either path with a model, which `model_unique_value.rs` already does for
//! the direct path and for shared batches.
//!
//! A blind tag's hash and an encrypted field differ between the two bounded
//! contexts, each having keys of its own, so a hash is compared as which
//! distinct hash it is, and payloads only for the plain tag.
//!
//! Bounds: `EPHYRA_TESTS` histories per variant (16 by default) of at most
//! `SKILJ_MODEL_MAX_OPS` operations (20 by default). A failure is shrunk and
//! printed with an `EPHYRA_REPLAY=<seed>:<iteration>` line that reproduces
//! it, and remembered under `.ephyra/regressions/`, git-ignored for the same
//! reason as `model_unique_value.regressions`.

mod unique_crm;

use std::collections::HashMap;

use ephyra::{check, gens, Config, Source};
use skilj::SkiljBuilder;
use skilj_core::command_batcher::{CommandBatchItem, CommandBatcher};
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{Event, EventBroadcaster};
use unique_crm::model::{
    customer_id, Addr, Call, Model, Op, Position, Resolved, Target, Variant, BASES, CLIENTS,
    CUSTOMERS, KEYS, SPELLINGS,
};
use unique_crm::{bounded_context, runtime, Blind, EmailTagging, Plain};

static TEST_DB: tokio::sync::OnceCell<Option<(String, Pool)>> = tokio::sync::OnceCell::const_new();

async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(|| async {
            let database_url =
                skilj_test_support::database_url("skilj_ephyra_batch_of_one").await?;
            let pool = db::connect(&database_url).await.unwrap();
            db::migrate(&pool).await.unwrap();
            Some((database_url, pool))
        })
        .await
        .clone()
}

fn env_number(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

// ---- Generating histories --------------------------------------------------

/// Mostly the canonical spelling, so histories meet one address often enough
/// to contend for it.
fn addr(g: &mut Source) -> Addr {
    let spelling = *g.weighted(&[(4, 0), (1, 1), (1, SPELLINGS - 1)]);
    Addr {
        base: g.int(0..BASES),
        spelling,
    }
}

fn call(g: &mut Source) -> Call {
    Call {
        client: g.int(0..CLIENTS),
        key: g.bool().then(|| g.int(0..KEYS)),
    }
}

fn maybe<T>(g: &mut Source, p: f64, value: impl FnOnce(&mut Source) -> T) -> Option<T> {
    if g.bool_with(p) {
        Some(value(g))
    } else {
        None
    }
}

/// One operation, in the proportions `model_unique_value.rs` draws them.
fn op(g: &mut Source) -> Op {
    let customer = |g: &mut Source| g.int(0..CUSTOMERS);
    match *g.weighted(&[(4, 0), (2, 1), (3, 2), (3, 3), (2, 4)]) {
        0 => Op::Register {
            customer: customer(g),
            addr: maybe(g, 0.5, addr),
            call: call(g),
        },
        1 => Op::Correct {
            customer: customer(g),
            addr: addr(g),
            call: call(g),
        },
        2 => Op::Release {
            customer: customer(g),
            addr: maybe(g, 0.25, addr),
            call: call(g),
        },
        3 => Op::RetractRelease {
            customer: maybe(g, 0.25, customer),
            addr: maybe(g, 0.25, addr),
            target: match *g.weighted(&[(6, 0), (1, 1), (1, 2)]) {
                0 => Target::Release(g.int(0..=u16::MAX)),
                1 => Target::NotARelease(g.int(0..=u16::MAX)),
                _ => Target::PastEnd,
            },
            call: call(g),
        },
        _ => Op::RetractAny {
            target: if g.bool_with(5.0 / 6.0) {
                Target::NotARelease(g.int(0..=u16::MAX))
            } else {
                Target::PastEnd
            },
            call: call(g),
        },
    }
}

// ---- Running a history on both paths ---------------------------------------

/// One bounded context and the deployment serving it.
struct Side {
    skilj: skilj::Skilj,
    bc: String,
    cache: EventCache,
    /// Which distinct blind hash each hash seen here is, in order of first
    /// sight, so the two sides' hashes can be compared.
    hashes: HashMap<String, usize>,
}

impl Side {
    async fn new<V: EmailTagging>(database_url: &str, pool: &Pool) -> Side {
        let (bc, subject) = bounded_context(pool).await;
        let builder: SkiljBuilder = unique_crm::builder::<V>(database_url, &bc, &subject);
        let (skilj, _) = builder.retractions().build().await.unwrap();
        Side {
            skilj,
            bc,
            cache: EventCache::new(1000),
            hashes: HashMap::new(),
        }
    }

    async fn close(self, pool: &Pool) {
        self.skilj
            .shutdown(std::time::Duration::from_secs(10))
            .await;
        db::hard_delete_bounded_context(pool, &self.bc)
            .await
            .unwrap();
    }

    fn target<'a>(&'a self, pool: &'a Pool) -> unique_crm::Target<'a> {
        unique_crm::Target {
            skilj: &self.skilj,
            pool,
            bc: &self.bc,
            cache: &self.cache,
        }
    }

    /// The direct path: `decide()` then `db::submit_command`.
    async fn submit_direct(
        &self,
        pool: &Pool,
        op: Op,
        payload: &serde_json::Value,
    ) -> SubmitCommandOutcome {
        self.target(pool)
            .submit(
                op.command_type(),
                payload.clone(),
                &op.call().client_id(),
                op.call().key().as_deref(),
            )
            .await
    }

    /// The batched path, with nothing else in the batch.
    async fn submit_batch_of_one(
        &self,
        pool: &Pool,
        op: Op,
        payload: &serde_json::Value,
    ) -> SubmitCommandOutcome {
        let command_type = db::get_command_type(pool, &self.bc, op.command_type())
            .await
            .unwrap()
            .unwrap();
        let batcher = CommandBatcher::new();
        let prepared = batcher
            .prepare_command(
                pool,
                &*self.skilj.command_dispatcher(),
                &*self.skilj.snapshot_dispatcher(),
                &self.cache,
                CommandBatchItem {
                    command_type,
                    payload: payload.to_string(),
                    client_id: op.call().client_id(),
                    correlation_id: None,
                    causation_id: None,
                    idempotency_key: op.call().key(),
                },
                Some(&unique_crm::master_key()),
                unique_crm::now(),
            )
            .await
            .unwrap();
        batcher
            .submit_prepared(
                pool,
                &self.skilj.command_dispatcher(),
                &self.skilj.projection_dispatcher(),
                &EventBroadcaster::new(16),
                &self.cache,
                Some(&unique_crm::master_key()),
                prepared,
            )
            .await
            .unwrap()
    }

    /// `event` as both sides must agree on it: its sequence, its type, its
    /// tags with a blind hash as which distinct hash it is, and for the plain
    /// tag its payload.
    fn describe_event(&mut self, variant: Variant, event: &Event) -> String {
        let mut tags: Vec<String> = event
            .tags
            .iter()
            .map(|tag| {
                let value = match (&tag.value, variant, tag.key.as_str()) {
                    (Some(hash), Variant::Blind, "email") => {
                        let next = self.hashes.len();
                        format!("hash#{}", self.hashes.entry(hash.clone()).or_insert(next))
                    }
                    (value, _, _) => format!("{value:?}"),
                };
                format!("{}={value}", tag.key)
            })
            .collect();
        tags.sort();
        let payload = match variant {
            Variant::Plain => event.payload.to_string(),
            Variant::Blind => "(encrypted)".to_string(),
        };
        format!(
            "{} {} [{}] {payload}",
            event.sequence,
            event.event_type.name,
            tags.join(", ")
        )
    }

    fn describe(&mut self, variant: Variant, outcome: &SubmitCommandOutcome) -> String {
        match outcome {
            SubmitCommandOutcome::Accepted { events, .. } => format!(
                "accepted: {:?}",
                events
                    .iter()
                    .map(|event| self.describe_event(variant, event))
                    .collect::<Vec<_>>()
            ),
            SubmitCommandOutcome::Rejected { kind, reason, .. } => {
                format!("rejected {kind}: {reason}")
            }
            SubmitCommandOutcome::Deduplicated {
                triggered_event_sequences,
            } => format!("deduplicated: {triggered_event_sequences:?}"),
            other => format!("{other:?}"),
        }
    }

    async fn stored(&mut self, pool: &Pool, variant: Variant) -> Vec<String> {
        let events = db::list_events_for_bounded_context(pool, &self.bc)
            .await
            .unwrap();
        events
            .iter()
            .map(|event| self.describe_event(variant, event))
            .collect()
    }
}

/// The payload `op` sends, resolved against the history so far, as
/// `model_unique_value.rs` builds it.
fn payload(op: Op, resolved: Resolved, sequences: &[i64]) -> serde_json::Value {
    let position = |position: Position| match position {
        Position::Index(index) => sequences[index],
        Position::PastEnd => sequences.last().copied().unwrap_or(0) + 1_000_000,
    };
    match op {
        Op::Register { .. } | Op::Correct { .. } | Op::Release { .. } => {
            let (customer, addr) = resolved.args.expect("resolved");
            serde_json::json!({ "customer_id": customer_id(customer), "email": addr.text() })
        }
        Op::RetractRelease { .. } => {
            let (customer, addr) = resolved.args.expect("resolved");
            serde_json::json!({
                "customer_id": customer_id(customer),
                "email": addr.text(),
                "position": position(resolved.target.expect("resolved")),
            })
        }
        Op::RetractAny { .. } => {
            serde_json::json!({ "position": position(resolved.target.expect("resolved")) })
        }
    }
}

/// Runs `ops` on both paths, `Err` naming the first step where they differ.
async fn both_paths_agree(variant: Variant, ops: &[Op]) -> Result<(), String> {
    match variant {
        Variant::Blind => both_paths_agree_as::<Blind>(variant, ops).await,
        Variant::Plain => both_paths_agree_as::<Plain>(variant, ops).await,
    }
}

async fn both_paths_agree_as<V: EmailTagging>(variant: Variant, ops: &[Op]) -> Result<(), String> {
    let Some((database_url, pool)) = test_db().await else {
        return Ok(());
    };
    let mut direct = Side::new::<V>(&database_url, &pool).await;
    let mut batched = Side::new::<V>(&database_url, &pool).await;
    let result = run_both(variant, ops, &pool, &mut direct, &mut batched).await;
    direct.close(&pool).await;
    batched.close(&pool).await;
    result
}

async fn run_both(
    variant: Variant,
    ops: &[Op],
    pool: &Pool,
    direct: &mut Side,
    batched: &mut Side,
) -> Result<(), String> {
    // The model only resolves each op's defaults and targets against the
    // history so far, so both sides get the same payload. What either side
    // answers is never compared with it.
    let mut model = Model::new(variant);
    let mut sequences = Vec::new();
    for (index, op) in ops.iter().enumerate() {
        let resolved = model.resolve_op(*op);
        let payload = payload(*op, resolved, &sequences);
        let one = direct.submit_direct(pool, *op, &payload).await;
        let other = batched.submit_batch_of_one(pool, *op, &payload).await;
        let (one_text, other_text) = (
            direct.describe(variant, &one),
            batched.describe(variant, &other),
        );
        if one_text != other_text {
            return Err(format!(
                "step {index} ({variant:?} tag): {op:?}\npayload {payload}\n\
                 direct:      {one_text}\nbatch of one: {other_text}"
            ));
        }
        if let SubmitCommandOutcome::Accepted { events, .. } = &one {
            sequences.extend(events.iter().map(|event| event.sequence));
        }
        model.apply_resolved(*op, resolved);
    }
    let (one, other) = (
        direct.stored(pool, variant).await,
        batched.stored(pool, variant).await,
    );
    if one != other {
        return Err(format!(
            "after the history ({variant:?} tag), the stored logs differ\n\
             direct:\n  {}\nbatch of one:\n  {}",
            one.join("\n  "),
            other.join("\n  ")
        ));
    }
    Ok(())
}

fn histories() -> impl ephyra::Gen<Value = Vec<Op>> {
    gens::vec(op as fn(&mut Source) -> Op).len(1..=env_number("SKILJ_MODEL_MAX_OPS", 20))
}

fn config(name: &str) -> Config {
    Config::default().tests(16).name(name)
}

fn property(variant: Variant, name: &str) {
    if runtime().block_on(test_db()).is_none() {
        return;
    }
    check(&config(name), histories(), |ops: Vec<Op>| {
        runtime().block_on(both_paths_agree(variant, &ops))
    })
    .assert_passed();
}

#[test]
fn a_batch_of_one_answers_as_a_single_submit_with_a_blind_tag() {
    property(Variant::Blind, "batch_of_one_blind");
}

#[test]
fn a_batch_of_one_answers_as_a_single_submit_with_a_plain_tag() {
    property(Variant::Plain, "batch_of_one_plain");
}
