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
//! A history can also submit two or three ops contending for one address at
//! once, after a sequential part; some serial order of them must answer what
//! the store answered (`concurrent_histories_match_some_serial_order`). A
//! group races freely, or is prepared on one state and committed one by one
//! or in shared batches, which forces the store to re-decide every command
//! an earlier one overtook. A trace layer counts those re-decisions, so the
//! tests show the commands really overlapped (`Mode`, `redecides`).
//!
//! What it doesn't cover: interleavings beyond that; idempotency-key
//! retention, which sweeps every bounded context and has a binary of its own;
//! doubts; and a raw retraction of a release, which can leave a customer's own
//! read showing a claim another customer holds and is documented as the
//! operator's escape hatch rather than modelled (CONTRIBUTING.md, "What the
//! tests do and don't verify").
//!
//! Bounds: `PROPTEST_CASES` histories per variant (32 by default) of at most
//! `SKILJ_MODEL_MAX_OPS` operations (20 by default), and
//! `SKILJ_MODEL_CONCURRENT_CASES` concurrent groups (16), about a minute and a
//! half in all. A failure is shrunk and printed as the operations that
//! reproduce it, which `replay` and `replay_together` take as they are, and
//! its seed is kept in `model_unique_value.regressions` next to this file,
//! which runs first next time. `.github/workflows/model-tests.yml` runs it at
//! depth weekly.

mod unique_crm;

use std::collections::HashMap;

use proptest::prelude::*;
use proptest::strategy::ValueTree;
use proptest::test_runner::{FileFailurePersistence, TestRng, TestRunner};
use skilj::SkiljBuilder;
use skilj_core::command_batcher::{CommandBatchItem, CommandBatcher, PreparedCommand};
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{Event, EventBroadcaster};
use unique_crm::model::{
    customer_id, Addr, Call, Expected, Kind, Logged, Model, Op, Position, Resolved, Target, Value,
    Variant, BASES, CLIENTS, CUSTOMERS, KEYS, SPELLINGS,
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

// ---- Counting re-decisions -------------------------------------------------

/// How often the store re-decided a command under the lock, per bounded
/// context: a command whose read was overtaken by an event committed after
/// it, which is what two commands overlapping in the store looks like. The
/// store says so in one trace event, `command decide under lock`.
static REDECIDES: std::sync::Mutex<std::collections::BTreeMap<String, usize>> =
    std::sync::Mutex::new(std::collections::BTreeMap::new());

struct RedecideCounter;

#[derive(Default)]
struct RedecideFields {
    message: String,
    bounded_context: String,
}

impl tracing::field::Visit for RedecideFields {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        match field.name() {
            "message" => self.message = format!("{value:?}"),
            "bounded_context" => self.bounded_context = format!("{value:?}"),
            _ => {}
        }
    }
}

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for RedecideCounter {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _context: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut fields = RedecideFields::default();
        event.record(&mut fields);
        if fields.message == "command decide under lock" {
            *REDECIDES
                .lock()
                .unwrap()
                .entry(fields.bounded_context)
                .or_default() += 1;
        }
    }
}

/// The re-decisions in `bc` so far.
fn redecides(bc: &str) -> usize {
    static INSTALLED: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    INSTALLED.get_or_init(|| {
        use tracing_subscriber::layer::SubscriberExt;
        tracing::subscriber::set_global_default(
            tracing_subscriber::registry().with(RedecideCounter),
        )
        .expect("this binary sets no other subscriber");
    });
    REDECIDES.lock().unwrap().get(bc).copied().unwrap_or(0)
}

// ---- Running a history -----------------------------------------------------

/// How a group of commands reaches the store.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Mode {
    /// Submitted at once, each reading and deciding as it goes: whatever
    /// overlap the run produces.
    #[default]
    Racing,
    /// Each prepared against the state before the group, then committed one
    /// after another in the group's order: every later command whose read the
    /// earlier ones overtook is re-decided, deterministically.
    Stale,
    /// Prepared like [`Mode::Stale`], then submitted at once to one batcher,
    /// so commands share batches and are re-decided against events earlier in
    /// their own batch.
    Batched,
}

/// A history: operations one after another, then a group of them submitted
/// together, then more one after another.
#[derive(Clone, Debug, Default)]
struct Script {
    before: Vec<Op>,
    together: Vec<Op>,
    mode: Mode,
    after: Vec<Op>,
}

/// What a run saw beyond agreeing with the model.
#[derive(Clone, Copy, Debug, Default)]
struct Report {
    /// Commands the store re-decided under the lock in the group.
    redecides: usize,
}

impl Script {
    fn sequential(ops: &[Op]) -> Self {
        Script {
            before: ops.to_vec(),
            ..Script::default()
        }
    }

    /// As Rust, to paste into a `replay` test.
    fn trace(&self) -> String {
        format!(
            "before: {}\ntogether ({:?}): {}\nafter: {}",
            trace(&self.before),
            self.mode,
            trace(&self.together),
            trace(&self.after)
        )
    }
}

/// Runs `ops` against a fresh bounded context, step by step against the
/// model. `Err` says which step disagreed and how; `Ok` also when there is no
/// database, after `skipping:` was printed for CI's guard.
async fn run_history(variant: Variant, ops: &[Op]) -> Result<(), String> {
    run_planted(variant, &Script::sequential(ops), Plant::NONE)
        .await
        .map(|_| ())
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

async fn run_planted(variant: Variant, script: &Script, plant: Plant) -> Result<Report, String> {
    match variant {
        Variant::Blind => run_history_as::<Blind>(variant, script, plant).await,
        Variant::Plain => run_history_as::<Plain>(variant, script, plant).await,
    }
}

async fn run_history_as<V: EmailTagging>(
    variant: Variant,
    script: &Script,
    plant: Plant,
) -> Result<Report, String> {
    let Some((database_url, pool)) = test_db().await else {
        return Ok(Report::default());
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
    let result = checker.run(target, script).await;
    skilj.shutdown(std::time::Duration::from_secs(10)).await;
    db::hard_delete_bounded_context(&pool, &bc).await.unwrap();
    result
}

#[derive(Clone)]
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
    report: Report,
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
            report: Report::default(),
        }
    }

    async fn run(
        mut self,
        target: unique_crm::Target<'_>,
        script: &Script,
    ) -> Result<Report, String> {
        let variant = self.variant;
        let context = |part: &str, index: usize, op: &dyn std::fmt::Debug, error: String| {
            format!(
                "{part} {index} ({variant:?} tag): {op:?}\n{error}\nthe history:\n{}",
                script.trace()
            )
        };
        for (index, op) in script.before.iter().enumerate() {
            if let Err(error) = self.step(target, *op).await {
                return Err(context("step before", index, op, error));
            }
        }
        if !script.together.is_empty() {
            if let Err(error) = self.together(target, &script.together, script.mode).await {
                return Err(context("the group of", script.together.len(), &"", error));
            }
        }
        for (index, op) in script.after.iter().enumerate() {
            if let Err(error) = self.step(target, *op).await {
                return Err(context("step after", index, op, error));
            }
        }
        Ok(self.report)
    }

    fn position(&self, position: Position) -> i64 {
        match position {
            Position::Index(index) => self.sequences[index],
            Position::PastEnd => self.sequences.last().copied().unwrap_or(0) + 1_000_000,
        }
    }

    fn payload(&self, op: Op, resolved: Resolved) -> serde_json::Value {
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
                    "position": self.position(resolved.target.expect("resolved")),
                })
            }
            Op::RetractAny { .. } => {
                serde_json::json!({ "position": self.position(resolved.target.expect("resolved")) })
            }
        }
    }

    async fn submit(
        &self,
        target: unique_crm::Target<'_>,
        op: Op,
        payload: serde_json::Value,
    ) -> SubmitCommandOutcome {
        target
            .submit(
                op.command_type(),
                payload,
                &op.call().client_id(),
                self.idempotency_key(op).as_deref(),
            )
            .await
    }

    fn idempotency_key(&self, op: Op) -> Option<String> {
        op.call().key().filter(|_| !self.drop_keys)
    }

    /// `op`, decided on what the store holds now, not yet committed.
    async fn prepare(
        &self,
        target: unique_crm::Target<'_>,
        batcher: &CommandBatcher,
        op: Op,
        payload: &serde_json::Value,
    ) -> PreparedCommand {
        let command_type = db::get_command_type(target.pool, target.bc, op.command_type())
            .await
            .unwrap()
            .unwrap();
        batcher
            .prepare_command(
                target.pool,
                &*target.skilj.command_dispatcher(),
                &*target.skilj.snapshot_dispatcher(),
                target.cache,
                CommandBatchItem {
                    command_type,
                    payload: payload.to_string(),
                    client_id: op.call().client_id(),
                    correlation_id: None,
                    causation_id: None,
                    idempotency_key: self.idempotency_key(op),
                },
                Some(&unique_crm::master_key()),
                unique_crm::now(),
            )
            .await
            .unwrap()
    }

    async fn submit_prepared(
        &self,
        target: unique_crm::Target<'_>,
        batcher: &CommandBatcher,
        prepared: PreparedCommand,
    ) -> SubmitCommandOutcome {
        batcher
            .submit_prepared(
                target.pool,
                &target.skilj.command_dispatcher(),
                &target.skilj.projection_dispatcher(),
                &EventBroadcaster::new(16),
                target.cache,
                Some(&unique_crm::master_key()),
                prepared,
            )
            .await
            .unwrap()
    }

    async fn step(&mut self, target: unique_crm::Target<'_>, op: Op) -> Result<(), String> {
        let resolved = self.model.resolve_op(op);
        let payload = self.payload(op, resolved);
        let outcome = self.submit(target, op, payload.clone()).await;
        let step = self.model.apply_resolved(op, resolved);
        self.compare(&step.expected, &outcome)
            .and(self.check_store().await)
            .map_err(|why| {
                format!(
                    "payload {payload}\nexpected {:?}\ngot {}\n{why}",
                    step.expected,
                    describe(&outcome)
                )
            })
    }

    /// `ops` submitted together as `mode` says, each resolved against the
    /// state before them, as each caller saw it. Some serial order of them
    /// must answer what the store answered, appending its events in that
    /// order: for [`Mode::Stale`], the group's own order.
    async fn together(
        &mut self,
        target: unique_crm::Target<'_>,
        ops: &[Op],
        mode: Mode,
    ) -> Result<(), String> {
        let resolved: Vec<Resolved> = ops.iter().map(|op| self.model.resolve_op(*op)).collect();
        let payloads: Vec<serde_json::Value> = ops
            .iter()
            .zip(&resolved)
            .map(|(op, resolved)| self.payload(*op, *resolved))
            .collect();
        let before = redecides(self.bc);
        let batcher = CommandBatcher::new();
        let outcomes = match mode {
            Mode::Racing => {
                futures_util::future::join_all(
                    ops.iter()
                        .zip(&payloads)
                        .map(|(op, payload)| self.submit(target, *op, payload.clone())),
                )
                .await
            }
            Mode::Stale | Mode::Batched => {
                let mut prepared = Vec::new();
                for (op, payload) in ops.iter().zip(&payloads) {
                    prepared.push(self.prepare(target, &batcher, *op, payload).await);
                }
                if mode == Mode::Stale {
                    let mut outcomes = Vec::new();
                    for prepared in prepared {
                        outcomes.push(self.submit_prepared(target, &batcher, prepared).await);
                    }
                    outcomes
                } else {
                    futures_util::future::join_all(
                        prepared
                            .into_iter()
                            .map(|prepared| self.submit_prepared(target, &batcher, prepared)),
                    )
                    .await
                }
            }
        };
        self.report.redecides += redecides(self.bc) - before;
        let candidates = if mode == Mode::Stale {
            vec![(0..ops.len()).collect()]
        } else {
            orders(ops.len())
        };
        let mut tried = Vec::new();
        for order in candidates {
            let mut trial = self.clone();
            match trial.serially(ops, &resolved, &outcomes, &order) {
                Ok(()) => {
                    *self = trial;
                    return self.check_store().await;
                }
                Err(why) => tried.push(format!("  in the order {order:?}: {why}")),
            }
        }
        let answered: Vec<String> = outcomes.iter().map(describe).collect();
        Err(format!(
            "no serial order answers as the store did\npayloads {payloads:?}\ngot {answered:#?}\n{}",
            tried.join("\n")
        ))
    }

    /// Whether applying `ops` to the model in `order` gives `outcomes`, with
    /// the accepted ones' events appended in that order.
    fn serially(
        &mut self,
        ops: &[Op],
        resolved: &[Resolved],
        outcomes: &[SubmitCommandOutcome],
        order: &[usize],
    ) -> Result<(), String> {
        let mut last = i64::MIN;
        for &index in order {
            let step = self.model.apply_resolved(ops[index], resolved[index]);
            if let SubmitCommandOutcome::Accepted { events, .. } = &outcomes[index] {
                let first = events.first().map_or(last, |event| event.sequence);
                if first < last {
                    return Err(format!(
                        "op {index} appended before an op ordered ahead of it"
                    ));
                }
                last = events.last().map_or(last, |event| event.sequence);
            }
            self.compare(&step.expected, &outcomes[index])
                .map_err(|why| format!("op {index} expected {:?}: {why}", step.expected))?;
        }
        Ok(())
    }

    /// Whether `outcome` is `expected`, recording the sequences it appended.
    fn compare(
        &mut self,
        expected: &Expected,
        outcome: &SubmitCommandOutcome,
    ) -> Result<(), String> {
        match (expected, outcome) {
            (Expected::Accepted(logged), SubmitCommandOutcome::Accepted { events, .. }) => {
                if events.len() != logged.len() {
                    return Err(format!("{} events appended", events.len()));
                }
                for (event, logged) in events.iter().zip(logged) {
                    self.check_event(event, logged)?;
                    self.sequences.push(event.sequence);
                }
                Ok(())
            }
            (Expected::Rejected(expected), SubmitCommandOutcome::Rejected { kind, .. })
                if kind == expected =>
            {
                Ok(())
            }
            (
                Expected::Deduplicated(indices),
                SubmitCommandOutcome::Deduplicated {
                    triggered_event_sequences,
                },
            ) => {
                let expected: Vec<i64> =
                    indices.iter().map(|index| self.sequences[*index]).collect();
                if &expected == triggered_event_sequences {
                    Ok(())
                } else {
                    Err(format!("expected the sequences {expected:?}"))
                }
            }
            _ => Err(format!("got {}", describe(outcome))),
        }
    }

    /// What is stored, as a whole: as many events as the model has, and for
    /// a blind tag no address in the clear.
    async fn check_store(&self) -> Result<(), String> {
        let stored = db::list_events_for_bounded_context(self.pool, self.bc)
            .await
            .unwrap();
        if stored.len() != self.model.log.len() {
            return Err(format!(
                "{} events stored, the model has {}",
                stored.len(),
                self.model.log.len()
            ));
        }
        if self.variant == Variant::Blind {
            for event in &stored {
                let stored_text = format!("{:?} {}", event.tags, event.payload).to_lowercase();
                if stored_text.contains("@example.test") {
                    return Err(format!(
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

/// Every order of `0..count`.
fn orders(count: usize) -> Vec<Vec<usize>> {
    if count == 0 {
        return vec![Vec::new()];
    }
    let mut all = Vec::new();
    for rest in orders(count - 1) {
        for at in 0..=rest.len() {
            let mut order = rest.clone();
            order.insert(at, count - 1);
            all.push(order);
        }
    }
    all
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

/// An op contending for address `base` with the others of its group:
/// claiming it, correcting to it, releasing what the customer holds, or
/// retracting a release, under one key shared by the group half the time.
fn contending(base: u8) -> BoxedStrategy<Op> {
    let at = prop_oneof![4 => Just(0), 1 => Just(1), 1 => Just(SPELLINGS - 1)]
        .prop_map(move |spelling| Addr { base, spelling })
        .boxed();
    let call = prop_oneof![
        1 => Just(Call { client: 0, key: None }),
        2 => (0..CLIENTS).prop_map(|client| Call { client, key: Some(0) }),
    ]
    .boxed();
    let customer = 0..CUSTOMERS;
    prop_oneof![
        3 => (customer.clone(), at.clone(), call.clone()).prop_map(|(customer, addr, call)| {
            Op::Register { customer, addr: Some(addr), call }
        }),
        2 => (customer.clone(), at, call.clone())
            .prop_map(|(customer, addr, call)| Op::Correct { customer, addr, call }),
        2 => (customer, call.clone())
            .prop_map(|(customer, call)| Op::Release { customer, addr: None, call }),
        2 => (any::<u16>(), call).prop_map(|(nth, call)| Op::RetractRelease {
            customer: None,
            addr: None,
            target: Target::Release(nth),
            call,
        }),
    ]
    .boxed()
}

/// A short history, two or three ops contending for one address submitted
/// at once, and a few more ops after, which see the state the group left.
fn script() -> impl Strategy<Value = Script> {
    (
        proptest::collection::vec(op(), 0..=8),
        (0..BASES).prop_flat_map(|base| proptest::collection::vec(contending(base), 2..=3)),
        prop_oneof![Just(Mode::Racing), Just(Mode::Stale), Just(Mode::Batched)],
        proptest::collection::vec(op(), 0..=4),
    )
        .prop_map(|(before, together, mode, after)| Script {
            before,
            together,
            mode,
            after,
        })
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

fn concurrent_config() -> ProptestConfig {
    ProptestConfig {
        cases: env_number("SKILJ_MODEL_CONCURRENT_CASES", 16),
        ..config()
    }
}

proptest! {
    #![proptest_config(concurrent_config())]

    /// Linearizability of a small group: the store answers concurrent
    /// commands as it would have answered them in some serial order.
    #[test]
    fn concurrent_histories_match_some_serial_order(script in script(), plain in any::<bool>()) {
        let variant = if plain { Variant::Plain } else { Variant::Blind };
        let result = runtime().block_on(run_planted(variant, &script, Plant::NONE));
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
            let result = runtime().block_on(run_planted(
                Variant::Blind,
                &Script::sequential(&ops),
                plant,
            ));
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

// ---- Fixed concurrent histories --------------------------------------------

fn replay_together(variant: Variant, script: Script) -> Report {
    match runtime().block_on(run_planted(variant, &script, Plant::NONE)) {
        Ok(report) => report,
        Err(error) => panic!("{error}"),
    }
}

#[test]
fn orders_are_every_permutation() {
    let mut all = orders(3);
    all.sort();
    assert_eq!(
        all,
        [
            [0, 1, 2],
            [0, 2, 1],
            [1, 0, 2],
            [1, 2, 0],
            [2, 0, 1],
            [2, 1, 0]
        ]
    );
}

const MODES: [Mode; 3] = [Mode::Racing, Mode::Stale, Mode::Batched];

/// Two customers claiming one address at once: one gets it. Prepared on the
/// same state, the second and third are each re-decided under the lock.
#[test]
fn two_claims_of_one_address_at_once_leave_one_holder() {
    for variant in [Variant::Blind, Variant::Plain] {
        for mode in MODES {
            let report = replay_together(
                variant,
                Script {
                    together: vec![
                        register(0, at(0, 0)),
                        register(1, at(0, 0)),
                        register(2, at(0, 0)),
                    ],
                    mode,
                    after: vec![register(0, at(1, 0)), register(1, at(1, 0))],
                    ..Script::default()
                },
            );
            if mode != Mode::Racing && test_db_available() {
                assert_eq!(report.redecides, 2, "{variant:?} {mode:?}");
            }
        }
    }
}

/// One caller sending one key twice at once: one decision, and the other
/// answered with its events.
#[test]
fn one_key_twice_at_once_is_decided_once() {
    for variant in [Variant::Blind, Variant::Plain] {
        for mode in MODES {
            replay_together(
                variant,
                Script {
                    together: vec![
                        keyed(register(0, at(0, 0)), 0, 0),
                        keyed(register(1, at(1, 0)), 0, 0),
                    ],
                    mode,
                    after: vec![register(2, at(1, 0))],
                    ..Script::default()
                },
            );
        }
    }
}

/// A claim of a released address and a retraction of that release at once:
/// the retraction can't give the address back once the claim is in. Prepared
/// on the same state, the retraction is re-decided and sees the claim.
#[test]
fn a_release_retraction_racing_a_claim_never_leaves_two_holders() {
    for variant in [Variant::Blind, Variant::Plain] {
        for mode in MODES {
            let report = replay_together(
                variant,
                Script {
                    before: vec![register(0, at(0, 0)), release(0, at(0, 0))],
                    together: vec![register(1, at(0, 0)), retract_release(0)],
                    mode,
                    after: vec![
                        register(2, at(0, 0)),
                        correct(0, at(2, 0)),
                        register(0, at(3, 0)),
                    ],
                },
            );
            if mode != Mode::Racing && test_db_available() {
                assert_eq!(report.redecides, 1, "{variant:?} {mode:?}");
            }
        }
    }
}

/// How often commands submitted at once, each reading as it goes, really
/// overlap in the store. How often depends on the machine, so only none at
/// all fails: then [`Mode::Racing`] would be sequential in all but name. The
/// forced modes above assert their exact count.
#[test]
fn racing_groups_overlap_in_the_store() {
    let groups = env_number("SKILJ_MODEL_RACING_GROUPS", 20) as usize;
    let mut overlapped = 0;
    for _ in 0..groups {
        let report = replay_together(
            Variant::Blind,
            Script {
                together: vec![
                    register(0, at(0, 0)),
                    register(1, at(0, 0)),
                    register(2, at(0, 0)),
                ],
                mode: Mode::Racing,
                ..Script::default()
            },
        );
        if report.redecides > 0 {
            overlapped += 1;
        }
    }
    eprintln!("racing groups re-decided in the store: {overlapped} of {groups}");
    if test_db_available() {
        assert!(overlapped > 0, "no racing group overlapped in the store");
    }
}

fn test_db_available() -> bool {
    runtime().block_on(test_db()).is_some()
}
