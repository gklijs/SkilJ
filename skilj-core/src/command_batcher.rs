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
//! the configured max batch size below still bounds the pathological case).
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
use std::sync::Mutex;
use tokio::sync::{oneshot, RwLock, Semaphore};
use tracing::Instrument;

/// The default for [`CommandBatcher::with_max_batch_size`]. A hard ceiling on how many commands one lock acquisition processes,
/// regardless of how many piled up while the leader waited for the
/// lock - a pathological worst case (thousands of callers queued behind
/// one very slow commit) still bounds one batch's own lock hold time and
/// memory, at the cost of spilling the remainder into a follow-up batch
/// (the same leader keeps draining and processing follow-up batches until
/// the queue is empty - see `run_as_leader` - so nothing is lost or
/// dropped, just processed one batch later).
pub const DEFAULT_MAX_BATCH_SIZE: usize = 256;

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

/// The default for [`CommandBatcher::with_idle_in_transaction_timeout`] -
/// see that method's own doc comment. 30 seconds is generous for
/// everything a batch leader's own transaction legitimately does (one row
/// lock, a handful of metadata reads, up to the configured max batch size commands'
/// worth of decider/projection work, all of it CPU-bound or against the
/// same already-warm database), so tripping it is always a real stall,
/// never an ordinary slow batch.
const DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// See this module's own doc comment for the full design. `Clone` is
/// cheap - `Arc`-wrapped internals, the identical "hand out a cheap
/// clone, not an `Arc<Skilj>`" reasoning `EventCache`/`EventBroadcaster`
/// already use.
#[derive(Clone)]
pub struct CommandBatcher {
    queues: Arc<RwLock<HashMap<String, Queue>>>,
    idle_in_transaction_timeout: std::time::Duration,
    /// Caps how many batch leaders (each pinning one pool connection for
    /// its whole batch while still needing *more* pool connections for
    /// its decide/persist reads) can run at once across every bounded
    /// context, sized lazily from the pool's own `max_connections` - see
    /// `leader_permits`.
    leader_permits: Arc<std::sync::OnceLock<Arc<Semaphore>>>,
    max_batch_size: usize,
    max_concurrent_leaders: Option<usize>,
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
            idle_in_transaction_timeout: DEFAULT_IDLE_IN_TRANSACTION_TIMEOUT,
            leader_permits: Arc::new(std::sync::OnceLock::new()),
            max_batch_size: DEFAULT_MAX_BATCH_SIZE,
            max_concurrent_leaders: None,
        }
    }

    /// The most commands one lock acquisition processes (default
    /// [`DEFAULT_MAX_BATCH_SIZE`]). A larger value amortizes the lock and
    /// commit over more commands; a smaller one bounds how long one batch
    /// holds the bounded-context lock and how much work a single failed
    /// batch takes down with it. Values below 1 are treated as 1.
    pub fn with_max_batch_size(mut self, max_batch_size: usize) -> Self {
        self.max_batch_size = max_batch_size.max(1);
        self
    }

    /// The most batch leaders that may run at once across all bounded
    /// contexts (each pins one pool connection for its whole batch).
    /// Defaults to half the pool's `max_connections` (minimum 1) - see
    /// `leader_permits`. Values below 1 are treated as 1. Must be set
    /// before the first command is submitted.
    pub fn with_max_concurrent_leaders(mut self, max_concurrent_leaders: usize) -> Self {
        self.max_concurrent_leaders = Some(max_concurrent_leaders.max(1));
        self
    }

    /// At most half the pool (minimum one) may be pinned by batch leaders
    /// at once, so every other caller's warm-up/optimistic resolve can
    /// still get a connection while leaders hold theirs. A leader itself
    /// needs no second connection under its lock - everything it reads
    /// or provisions there runs on its own transaction
    /// (docs/architecture.md §117).
    fn leader_permits(&self, pool: &Pool) -> Arc<Semaphore> {
        self.leader_permits
            .get_or_init(|| {
                let permits = self
                    .max_concurrent_leaders
                    .unwrap_or((pool.options().get_max_connections() as usize / 2).max(1));
                Arc::new(Semaphore::new(permits))
            })
            .clone()
    }

    /// Codeberg issue #36's own recommendation #3: a defense-in-depth
    /// backstop against a batch leader's transaction stalling - for any
    /// reason, root cause fixed or not - while holding the bounded-context
    /// lock. Issued as `SET LOCAL idle_in_transaction_session_timeout` on
    /// the leader's own transaction (see `db::begin_command_batch_leader_tx`'s
    /// own doc comment), so Postgres itself kills a genuinely stuck leader
    /// and releases the lock instead of every writer to that bounded
    /// context queueing forever behind a wedge nothing else ever clears.
    pub fn with_idle_in_transaction_timeout(mut self, timeout: std::time::Duration) -> Self {
        self.idle_in_transaction_timeout = timeout;
        self
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
        dispatcher: &Arc<dyn CommandDispatcher>,
        projection_dispatcher: &Arc<dyn ProjectionDispatcher>,
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
            let mut queue = lock_queue(&queue);
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
                Err(Error::batch_failed_msg(
                    "command batch leader task ended without producing a result",
                ))
            });
        }

        // The batch runs on its own task, not on this caller's future: a
        // leader whose client disconnects (axum drops the handler future)
        // would otherwise drop the shared transaction mid-batch, failing
        // every batch-mate's command - or, after the commit, leaving them
        // told `BatchFailed` for a command that did commit, and its events
        // unbroadcast (docs/architecture.md §114). Detached, the batch
        // finishes and answers everyone; the leader's own command
        // commits even if nobody is left to read its result, as any
        // command already on its way into a transaction can.
        let leader = self.clone();
        let pool = pool.clone();
        let dispatcher = dispatcher.clone();
        let projection_dispatcher = projection_dispatcher.clone();
        let broadcaster = broadcaster.clone();
        let event_cache = event_cache.clone();
        let encryption_master_key = encryption_master_key.cloned();
        let batch = tokio::spawn(
            async move {
                leader
                    .run_as_leader(
                        &pool,
                        dispatcher.as_ref(),
                        projection_dispatcher.as_ref(),
                        &broadcaster,
                        &event_cache,
                        encryption_master_key.as_ref(),
                        &bounded_context_name,
                        &queue,
                    )
                    .await
            }
            .instrument(tracing::Span::current()),
        );
        batch.await.unwrap_or_else(|_| {
            // Only a panic gets here (the task is never aborted);
            // `LeaderGuard` has already answered whoever was still queued.
            Err(Error::batch_failed_msg(
                "command batch leader task ended without producing a result",
            ))
        })
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
        dispatcher: &Arc<dyn CommandDispatcher>,
        projection_dispatcher: &Arc<dyn ProjectionDispatcher>,
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
            dispatcher.as_ref(),
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
    /// leader above. Everything between here and the lock actually being
    /// granted (`db::begin_command_batch_leader_tx`'s own `pool.begin()`
    /// plus the real `SELECT ... FOR UPDATE` wait) is exactly the window
    /// other callers keep queueing behind; only once that's done do we
    /// drain the queue (taking everyone who joined, capped at
    /// the configured max batch size), so the batch this call ends up processing can
    /// be considerably larger than one.
    ///
    /// Codeberg issue #36: this ordering - lock first, drain second - used
    /// to be reversed, which quietly defeated the self-tuning design this
    /// module's own doc comment describes. Draining is unconditional once
    /// the lock step finishes, success or failure alike - a follower that
    /// queued during the wait must never be left behind in a queue nobody
    /// drains again, since leadership for a *new* batch is only ever
    /// assigned to whoever finds the queue empty (see `submit`'s own
    /// `is_leader` check), and a queue nobody drains never goes empty.
    ///
    /// Two further guarantees keep that invariant true:
    /// - **Over-cap remainder**: if more than the configured max batch size commands
    ///   are queued, this leader keeps going - lock, drain, process -
    ///   until a drain empties the queue, and only then returns its own
    ///   (already-known) result. No one else can become leader while the
    ///   queue is non-empty, so it must be this one.
    /// - **Cancellation**: `submit` runs this on its own task, so a
    ///   leader's caller being dropped (client disconnect, timeout) no
    ///   longer stops it (docs/architecture.md §114). A panic still can:
    ///   a [`LeaderGuard`] stays armed until a drain empties the queue,
    ///   and if this future ends first it fails everything still queued
    ///   (a retryable `BatchFailed`) and empties the queue, so the next
    ///   caller becomes a fresh leader instead of every later caller
    ///   hanging forever.
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
        let mut guard = LeaderGuard {
            queue: queue.clone(),
            armed: true,
        };
        let permits = self.leader_permits(pool);
        let mut own_result: Option<Result<SubmitCommandOutcome>> = None;

        loop {
            // Held for the whole lock-wait-plus-batch, released at the
            // end of this iteration - see `leader_permits`.
            let _permit = permits
                .acquire()
                .await
                .expect("leader semaphore is never closed");

            // Timed separately from `commit_command_batch`'s own
            // per-phase log - this is the "waiting for the lock" half.
            let lock_wait_started = std::time::Instant::now();
            let leader_tx = crate::db::begin_command_batch_leader_tx(
                pool,
                bounded_context_name,
                Some(self.idle_in_transaction_timeout),
            )
            .await;
            tracing::debug!(
                bounded_context = %bounded_context_name,
                lock_wait_us = lock_wait_started.elapsed().as_micros(),
                "command batch leader lock wait"
            );

            // Never empty on the first pass (the leader's own entry is
            // in there); on later passes only entered when the previous
            // drain reported a remainder, which nobody else can take.
            let (pending, more_remaining) = drain_up_to(queue, self.max_batch_size);
            if !more_remaining {
                // Atomic with the drain above (no await between them):
                // the queue is empty, so leadership is released.
                guard.armed = false;
            }
            let first_pass = own_result.is_none();
            let (batch, mut replies): (
                Vec<BatchedCommand>,
                Vec<oneshot::Sender<BatchedCommandResult>>,
            ) = pending.into_iter().map(|p| (p.command, p.reply)).unzip();
            // The leader is always `batch[0]` of the *first* batch only.
            let skip = usize::from(first_pass);

            let outcome = match leader_tx {
                Err(e) => Err(e),
                Ok(leader_tx) => {
                    crate::db::commit_command_batch(
                        leader_tx,
                        dispatcher,
                        projection_dispatcher,
                        encryption_master_key,
                        batch,
                    )
                    .await
                }
            };

            match outcome {
                Err(e) => {
                    // Lock acquisition or the shared transaction's final
                    // commit failed - nothing in this batch is
                    // trustworthy, so every command gets an equivalent
                    // error. `Error` isn't `Clone`, so followers get a
                    // `BatchFailed` carrying the original's code and
                    // message; only the leader's own return value keeps
                    // the original typed error.
                    for reply in replies.drain(skip..) {
                        let _ = reply.send(Err(Error::batch_failed(&e)));
                    }
                    if first_pass {
                        own_result = Some(Err(e));
                    }
                }
                Ok(results) => {
                    // Every accepted command's own events, across the
                    // whole batch, broadcast together once the shared
                    // commit has actually succeeded.
                    for outcome in results.iter().flatten() {
                        crate::db::broadcast_appended_events(
                            pool,
                            broadcaster,
                            event_cache,
                            outcome,
                        )
                        .await;
                    }
                    let mut results = results.into_iter();
                    if first_pass {
                        own_result = Some(results.next().expect(
                            "run_as_leader's own batch always has at least its own leader in it",
                        ));
                    }
                    for (reply, result) in replies.into_iter().skip(skip).zip(results) {
                        let _ = reply.send(result);
                    }
                }
            }

            if !more_remaining {
                break;
            }
        }

        own_result.expect("the first pass always records the leader's own result")
    }
}

fn lock_queue(queue: &Queue) -> std::sync::MutexGuard<'_, Vec<PendingCommand>> {
    // Never held across an await and never panics while held, but a
    // poisoned lock must not itself wedge every later caller.
    queue.lock().unwrap_or_else(|e| e.into_inner())
}

/// Takes everything currently queued (up to `max`). The second element
/// is whether entries remain afterwards - if so the queue is *not* empty,
/// so no new leader can arise and the current one must keep draining. When
/// it is `false` the queue is empty and a push that arrives next becomes
/// a fresh leader for a new batch.
fn drain_up_to(queue: &Queue, max: usize) -> (Vec<PendingCommand>, bool) {
    let mut queue = lock_queue(queue);
    if queue.len() <= max {
        (std::mem::take(&mut *queue), false)
    } else {
        (queue.drain(..max).collect(), true)
    }
}

/// Armed for as long as the current leader is the one responsible for
/// eventually emptying the queue. If the leader's future ends while
/// armed (a panic - it runs on its own task, so a caller's disconnect
/// no longer cancels it), everything still queued is failed and the
/// queue emptied so leadership can be re-elected by the next push.
struct LeaderGuard {
    queue: Queue,
    armed: bool,
}

impl Drop for LeaderGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let stranded = std::mem::take(&mut *lock_queue(&self.queue));
        for pending in stranded {
            let _ = pending.reply.send(Err(Error::batch_failed_msg(
                "the command batch leader stopped before processing this command; retry",
            )));
        }
    }
}
