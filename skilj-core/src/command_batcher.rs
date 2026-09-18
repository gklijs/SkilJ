//! Group-commit batching for `db::submit_command` (Codeberg issue #32,
//! round two). The first pass at this issue (docs/architecture.md §57)
//! shortened how long `submit_command`'s own bounded-context lock is
//! held per command; a real load test against `skilj-helpdesk` still
//! found the ceiling this exists to raise, because shortening one
//! command's own critical section does nothing about the fundamental
//! shape of the problem: every command commit to one bounded context is
//! *fully serialized* through that one `sequence` row's lock (required for
//! `SequenceIsGaplessPerBoundedContext`/`DynamicConsistencyBoundaryHonoured`,
//! not something this pass touches), so under real concurrent load the
//! throughput ceiling is `1 / (lock hold time)`, full stop, no matter how
//! short each individual hold is.
//!
//! **The actual lever left: make one lock acquisition serve more than
//! one command.** `CommandBatcher` implements a "group commit" -
//! standard database terminology for the same pattern write-ahead-log
//! flushes and MySQL's binlog use: instead of every concurrent caller
//! independently opening its own transaction and queueing for the lock,
//! callers that arrive close together in time are coalesced into one
//! shared batch, processed by whichever one of them becomes that batch's
//! *leader* inside a single transaction (`db::submit_command_batch`,
//! each command isolated by its own `SAVEPOINT` so one command's real
//! failure never affects another's), committed once. N queued callers
//! that used to mean N separate lock acquisitions now mean one.
//!
//! **Self-tuning, no batch-window or batch-size knob to guess**: a
//! caller becomes the leader for a new batch only when it finds the
//! per-bounded-context queue empty at the moment it pushes onto it
//! (`submit`'s own `is_leader` check, guarded by the queue's own
//! `Mutex`, so exactly one caller can ever observe that transition per
//! batch). The leader does *not* immediately grab whatever's in the
//! queue - it first does the slow part (`pool.begin()` plus the actual
//! `SELECT ... FOR UPDATE` wait, which is exactly the time other
//! commands are still arriving and queueing behind it under real
//! contention) and only *then* drains the queue, taking everyone who
//! joined in the meantime. Under low load, a lone caller becomes its own
//! leader, finds nobody else queued once its lock is acquired, and pays
//! essentially the identical latency `submit_command` alone would have
//! (batch of one). Under heavy load - exactly the case the load test
//! found collapsing - the leader's own wait for the lock is naturally
//! longer (every earlier batch is still ahead of it), so more callers
//! accumulate in that same window, producing a *larger* batch precisely
//! when amortising the lock acquisition matters most. No artificial
//! sleep, no fixed batch-size cap needed for this to self-tune (though
//! `MAX_BATCH_SIZE` below still bounds the pathological case).
//!
//! **Correctness, not just throughput**: every invariant `submit_command`
//! itself guarantees still holds, unweakened - see `db::submit_command_batch`'s
//! own doc comment for exactly how (per-command `SAVEPOINT` isolation,
//! `extra_committed_events` giving same-batch commands the identical DCB
//! conflict visibility a truly separate transaction would have had via
//! the lock alone, replies only ever sent after the shared `tx.commit()`
//! actually succeeds).

use crate::db::{
    BatchedCommand, BatchedCommandResult, OwnedSnapshotContext, Pool, SnapshotContext,
    SubmitCommandOutcome,
};
use crate::encryption::EncryptionMasterKey;
use crate::error::{Error, Result};
use crate::event_cache::EventCache;
use crate::event_store::{CommandType, Event, EventBroadcaster};
use crate::plugin::{CommandDispatcher, ProjectionDispatcher};
use crate::shared::{CommandDecision, Tag};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{oneshot, Mutex, RwLock};

/// A hard ceiling on how many commands one lock acquisition processes,
/// regardless of how many piled up while the leader waited for the
/// lock - a pathological worst case (thousands of callers queued behind
/// one very slow commit) still bounds one batch's own lock hold time and
/// memory, at the cost of spilling the remainder into a follow-up batch
/// (whichever caller is first to find the queue empty next becomes that
/// one's own leader - nothing is lost or dropped, just processed one
/// batch later).
const MAX_BATCH_SIZE: usize = 256;

/// One caller's request, sitting in a per-bounded-context queue - see
/// this module's own doc comment. Every entry carries a `reply` sender,
/// leader's own included, but `run_as_leader` never sends to its own -
/// it reads its own outcome directly out of the batch results instead of
/// round-tripping through a channel to itself (see that function's own
/// comment).
struct PendingCommand {
    command: BatchedCommand,
    reply: oneshot::Sender<BatchedCommandResult>,
}

type Queue = Arc<Mutex<Vec<PendingCommand>>>;

/// See this module's own doc comment for the full design. `Clone` is
/// cheap - `Arc`-wrapped internals, the identical "hand out a cheap
/// clone, not an `Arc<Skilj>`" reasoning `EventCache`/`EventBroadcaster`
/// already use.
#[derive(Clone)]
pub struct CommandBatcher {
    queues: Arc<RwLock<HashMap<String, Queue>>>,
}

impl Default for CommandBatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl CommandBatcher {
    pub fn new() -> Self {
        Self {
            queues: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// The `Queue` for `bounded_context`, creating it (empty) on first
    /// use - a read-mostly lookup (`RwLock::read`) for every bounded
    /// context this process has already touched at least once, falling
    /// back to a brief write lock only the first time.
    async fn queue_for(&self, bounded_context: &str) -> Queue {
        if let Some(queue) = self.queues.read().await.get(bounded_context) {
            return queue.clone();
        }
        self.queues
            .write()
            .await
            .entry(bounded_context.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(Vec::new())))
            .clone()
    }

    /// The batched drop-in replacement for `db::submit_command` - same
    /// contract (identical `SubmitCommandOutcome`, identical set of
    /// error cases a single unbatched call could produce), just routed
    /// through this bounded context's own shared queue instead of always
    /// opening its own transaction. See this module's own doc comment
    /// for why joining that queue never changes what a caller observes,
    /// only how many other commands happen to share the lock acquisition
    /// that produces its result.
    #[allow(clippy::too_many_arguments)]
    pub async fn submit(
        &self,
        pool: &Pool,
        dispatcher: &dyn CommandDispatcher,
        projection_dispatcher: &dyn ProjectionDispatcher,
        broadcaster: &EventBroadcaster,
        event_cache: &EventCache,
        command_type: &CommandType,
        payload: &str,
        client_id: &str,
        correlation_id: Option<&str>,
        causation_id: Option<&str>,
        bounded_context_events: &[Event],
        consistency_tags: &[Tag],
        matching_events: &[Event],
        initial_decision: CommandDecision,
        encryption_master_key: Option<&EncryptionMasterKey>,
        now: DateTime<Utc>,
        snapshot: Option<SnapshotContext<'_>>,
        idempotency_key: Option<&str>,
    ) -> Result<SubmitCommandOutcome> {
        let bounded_context_name = command_type.bounded_context.name.clone();

        // Pre-lock warm-up, done here - fully in parallel across every
        // concurrently-submitting caller, before any of them join the
        // shared queue - rather than by whichever one becomes leader,
        // serially, for the whole batch. See
        // `db::warm_up_event_types_and_encryption_keys`'s own doc
        // comment for why this is always safe.
        let (event_types_by_name, resolved) = crate::db::warm_up_event_types_and_encryption_keys(
            pool,
            &bounded_context_name,
            command_type,
            payload,
            &initial_decision,
            encryption_master_key,
        )
        .await?;

        let command = BatchedCommand {
            command_type: command_type.clone(),
            payload: payload.to_string(),
            client_id: client_id.to_string(),
            correlation_id: correlation_id.map(str::to_string),
            causation_id: causation_id.map(str::to_string),
            bounded_context_events: bounded_context_events.to_vec(),
            consistency_tags: consistency_tags.to_vec(),
            matching_events: matching_events.to_vec(),
            initial_decision,
            now,
            snapshot: snapshot.map(|s| OwnedSnapshotContext {
                state_json: s.state_json.to_string(),
                as_of_sequence: s.as_of_sequence,
            }),
            idempotency_key: idempotency_key.map(str::to_string),
            event_types_by_name,
            resolved,
        };

        let queue = self.queue_for(&bounded_context_name).await;
        let (reply_tx, reply_rx) = oneshot::channel();
        let is_leader = {
            let mut queue = queue.lock().await;
            queue.push(PendingCommand {
                command,
                reply: reply_tx,
            });
            // Exactly one pusher can ever observe the queue going from
            // empty to non-empty - the push and this check happen under
            // the same lock acquisition, so leadership for a given batch
            // is assigned unambiguously, exactly once.
            queue.len() == 1
        };

        if !is_leader {
            return reply_rx.await.unwrap_or_else(|_| {
                Err(Error::BatchFailed(
                    "command batch leader task ended without producing a result".to_string(),
                ))
            });
        }

        self.run_as_leader(
            pool,
            dispatcher,
            projection_dispatcher,
            broadcaster,
            event_cache,
            encryption_master_key,
            &bounded_context_name,
            &queue,
        )
        .await
    }

    /// The batched drop-in replacement for `db::decide_and_submit_command` -
    /// same optimistic-resolve-then-submit shape
    /// (`db::resolve_command_submission` computes the identical optimistic
    /// half either way), just handing its result to this batcher's own
    /// [`submit`](Self::submit) instead of `db::submit_command` directly.
    /// The one real entry point REST/GraphQL command submission and
    /// parked-delivery redrive are meant to call - see `db::decide_and_submit_command`'s
    /// own doc comment for why the lower-volume, periodic internal
    /// callers (the cross-context router's tick, `fire_due_deadlines`)
    /// deliberately still call that one directly instead.
    #[allow(clippy::too_many_arguments)]
    pub async fn decide_and_submit(
        &self,
        pool: &Pool,
        dispatcher: &dyn CommandDispatcher,
        projection_dispatcher: &dyn ProjectionDispatcher,
        snapshot_dispatcher: &dyn crate::plugin::SnapshotDispatcher,
        broadcaster: &EventBroadcaster,
        event_cache: &EventCache,
        command_type: &CommandType,
        payload: &str,
        client_id: &str,
        correlation_id: Option<&str>,
        causation_id: Option<&str>,
        encryption_master_key: Option<&EncryptionMasterKey>,
        now: DateTime<Utc>,
        idempotency_key: Option<&str>,
    ) -> Result<SubmitCommandOutcome> {
        let resolved = crate::db::resolve_command_submission(
            pool,
            dispatcher,
            snapshot_dispatcher,
            event_cache,
            command_type,
            payload,
        )
        .await?;

        self.submit(
            pool,
            dispatcher,
            projection_dispatcher,
            broadcaster,
            event_cache,
            command_type,
            payload,
            client_id,
            correlation_id,
            causation_id,
            &resolved.bounded_context_events,
            &resolved.consistency_tags,
            &resolved.matching_events,
            resolved.decision,
            encryption_master_key,
            now,
            resolved
                .snapshot_context
                .as_ref()
                .map(|ctx| SnapshotContext {
                    state_json: &ctx.state_json,
                    as_of_sequence: ctx.as_of_sequence,
                }),
            idempotency_key,
        )
        .await
    }

    /// Only ever called by whichever `submit` caller found itself the
    /// leader above. Everything between here and the `tx.begin()`
    /// `db::submit_command_batch` does internally - the pool checkout,
    /// the `SELECT ... FOR UPDATE` wait - is exactly the window other
    /// callers keep queueing behind; only once that's done do we drain
    /// the queue (taking everyone who joined, capped at
    /// `MAX_BATCH_SIZE`), so the batch this call ends up processing can
    /// be considerably larger than one.
    #[allow(clippy::too_many_arguments)]
    async fn run_as_leader(
        &self,
        pool: &Pool,
        dispatcher: &dyn CommandDispatcher,
        projection_dispatcher: &dyn ProjectionDispatcher,
        broadcaster: &EventBroadcaster,
        event_cache: &EventCache,
        encryption_master_key: Option<&EncryptionMasterKey>,
        bounded_context_name: &str,
        queue: &Queue,
    ) -> Result<SubmitCommandOutcome> {
        // `drain_up_to` never returns empty for the leader's own call -
        // it just pushed itself onto this exact queue above. Split into
        // parallel vecs up front: `db::submit_command_batch` wants owned
        // `BatchedCommand`s to process, and the `reply` senders are
        // needed again afterward, in the same order, to distribute
        // results - `unzip` keeps that pairing without the awkwardness
        // of trying to partially move out of `PendingCommand` twice.
        let pending = self.drain_up_to(queue, MAX_BATCH_SIZE).await;
        let (batch, mut replies): (
            Vec<BatchedCommand>,
            Vec<oneshot::Sender<BatchedCommandResult>>,
        ) = pending.into_iter().map(|p| (p.command, p.reply)).unzip();

        let results = crate::db::submit_command_batch(
            pool,
            dispatcher,
            projection_dispatcher,
            encryption_master_key,
            bounded_context_name,
            batch,
        )
        .await;

        let results = match results {
            Ok(results) => results,
            Err(e) => {
                // The shared transaction itself failed (lock acquisition
                // or the final commit) - nothing in this batch is
                // trustworthy, so every command, leader's own included,
                // gets an equivalent error. `Error` isn't `Clone`
                // (`sqlx::Error` inside it isn't), so every follower gets
                // `BatchFailed` carrying the same rendered message rather
                // than the original typed error - only the leader's own
                // return value below keeps that original.
                let message = e.to_string();
                for reply in replies.drain(1..) {
                    let _ = reply.send(Err(Error::BatchFailed(message.clone())));
                }
                return Err(e);
            }
        };

        // Every accepted command's own events, across the whole batch,
        // broadcast together once the shared commit has actually
        // succeeded - the identical post-commit choke point
        // `db::broadcast_appended_events` already is for the standalone
        // path, just run once per batch member here instead of once per
        // call.
        for outcome in results.iter().flatten() {
            crate::db::broadcast_appended_events(pool, broadcaster, event_cache, outcome).await;
        }

        // The leader is always `batch[0]`/`replies[0]`/`results[0]` - it
        // was alone in the queue when it pushed itself (that's what made
        // it the leader), and every follower only ever joins *after*
        // that push.
        let mut results = results.into_iter();
        let own_result = results
            .next()
            .expect("run_as_leader's own batch always has at least its own leader in it");
        for (reply, result) in replies.into_iter().skip(1).zip(results) {
            let _ = reply.send(result);
        }
        own_result
    }

    /// Takes everything currently queued (up to `max`), leaving the
    /// queue empty for whoever pushes next to become a fresh leader for
    /// a new batch. A push that arrives while this drain holds the
    /// queue's lock simply waits for it, then (finding the queue empty
    /// again) becomes that new batch's own leader - no request is ever
    /// silently skipped or merged into the wrong batch.
    async fn drain_up_to(&self, queue: &Queue, max: usize) -> Vec<PendingCommand> {
        let mut queue = queue.lock().await;
        if queue.len() <= max {
            std::mem::take(&mut *queue)
        } else {
            queue.drain(..max).collect()
        }
    }
}
