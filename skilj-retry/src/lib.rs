//! A small, dependency-free backoff/attempt-cap policy - see
//! docs/architecture.md's parked-deliveries section. Pure data and pure
//! functions only, deliberately: no `tokio::time::sleep`, no clock read,
//! no I/O. Every caller (`skilj-core`'s `CrossContextRoute` catch-up
//! loop, and each of `skilj-kafka`/`skilj-amqp`/`skilj-nats`) already has
//! its own notion of "now" and its own way of sleeping/rescheduling, so
//! this crate only ever answers two questions a caller asks it: "how
//! long should I wait before the next attempt" and "have I tried enough
//! (or for long enough) that I should give up".

use std::time::Duration;

/// The error codes (GraphQL `extensions.code`, the REST body's `code`)
/// that mean "this skilj instance can't do it, another can": it doesn't
/// declare the command type's `decide()`, or a sync projection the
/// resulting event feeds - ordinary mid rolling deploy. A caller retrying
/// on one of these waits [`ANOTHER_INSTANCE_RETRY_DELAY`] instead of
/// spending an attempt of its [`RetryPolicy`], and never parks for it
/// (docs/architecture.md §161).
pub const ANOTHER_INSTANCE_CODES: [&str; 2] =
    ["no_decider_registered", "sync_projection_not_declared"];

/// Whether `code` is one of [`ANOTHER_INSTANCE_CODES`].
pub fn another_instance_can_do_it(code: &str) -> bool {
    ANOTHER_INSTANCE_CODES.contains(&code)
}

/// How long work refused with one of [`ANOTHER_INSTANCE_CODES`] waits
/// before it is tried again.
pub const ANOTHER_INSTANCE_RETRY_DELAY: Duration = Duration::from_secs(10);

/// Geometric backoff with an optional attempt cap and/or elapsed-time
/// cap. Either cap alone is enough to exhaust the policy - see
/// `is_exhausted`'s own doc comment. Leaving both `None`
/// (`RetryPolicy::unbounded`) means "retry forever, just with growing
/// backoff" - the shape the outbound bridge direction wants (a
/// connectivity failure should self-heal once the target comes back, not
/// give up), versus `RetryPolicy::bounded`'s "give up and park it" shape
/// `CrossContextRoute` and each bridge's inbound path want instead.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    pub initial_backoff: Duration,
    pub multiplier: f64,
    pub max_backoff: Duration,
    pub max_attempts: Option<u32>,
    pub max_elapsed: Option<Duration>,
}

impl RetryPolicy {
    /// Retries forever, backoff still growing (capped at `max_backoff`)
    /// rather than hammering a down target at a fixed interval. See this
    /// struct's own doc comment for why this is the outbound bridge
    /// direction's own default shape.
    pub const fn unbounded(
        initial_backoff: Duration,
        multiplier: f64,
        max_backoff: Duration,
    ) -> Self {
        Self {
            initial_backoff,
            multiplier,
            max_backoff,
            max_attempts: None,
            max_elapsed: None,
        }
    }

    /// Gives up once `max_attempts` is reached - the shape a caller that
    /// parks a delivery on exhaustion wants.
    pub const fn bounded(
        initial_backoff: Duration,
        multiplier: f64,
        max_backoff: Duration,
        max_attempts: u32,
    ) -> Self {
        Self {
            initial_backoff,
            multiplier,
            max_backoff,
            max_attempts: Some(max_attempts),
            max_elapsed: None,
        }
    }

    /// Adds (or replaces) an elapsed-time cap on top of whichever
    /// constructor built `self` - e.g. `RetryPolicy::unbounded(...)
    /// .with_max_elapsed(...)` for "keep retrying, but give up on this
    /// one item after an hour regardless of attempt count".
    pub const fn with_max_elapsed(mut self, max_elapsed: Duration) -> Self {
        self.max_elapsed = Some(max_elapsed);
        self
    }

    /// `attempt` is 1-based - the attempt number that just failed. Grows
    /// geometrically from `initial_backoff` by `multiplier` each further
    /// attempt, capped at `max_backoff` so it never grows unbounded.
    ///
    /// Always within `[0, max_backoff]` and never panics, whatever the
    /// (public) fields hold - it runs inside long-lived background tasks,
    /// where a panic stops the task. A multiplier that isn't a finite,
    /// non-negative number is treated as `1.0` (constant backoff) rather
    /// than producing a negative or NaN delay, and `max_backoff` is
    /// returned as itself once reached - never round-tripped through
    /// `f64`, which for `Duration::MAX` (an "uncapped" policy) doesn't fit
    /// back into a `Duration`.
    pub fn next_backoff(&self, attempt: u32) -> Duration {
        let multiplier = if self.multiplier.is_finite() && self.multiplier >= 0.0 {
            self.multiplier
        } else {
            1.0
        };
        let exponent = i32::try_from(attempt.saturating_sub(1)).unwrap_or(i32::MAX);
        let scaled = self.initial_backoff.as_secs_f64() * multiplier.powi(exponent);
        if scaled.is_nan() || scaled >= self.max_backoff.as_secs_f64() {
            return self.max_backoff;
        }
        Duration::try_from_secs_f64(scaled).map_or(self.max_backoff, |d| d.min(self.max_backoff))
    }

    /// When the next attempt is due: `now` plus [`next_backoff`], for a
    /// caller that stores a due time rather than sleeping. Saturates at
    /// the latest representable instant instead of either failing the
    /// conversion to `chrono` - which callers used to answer with a zero
    /// delay, retrying immediately and forever - or overflowing the
    /// addition, which panics in chrono.
    ///
    /// [`next_backoff`]: RetryPolicy::next_backoff
    pub fn next_attempt_at(
        &self,
        now: chrono::DateTime<chrono::Utc>,
        attempt: u32,
    ) -> chrono::DateTime<chrono::Utc> {
        let backoff = chrono::TimeDelta::from_std(self.next_backoff(attempt))
            .unwrap_or(chrono::TimeDelta::MAX);
        now.checked_add_signed(backoff)
            .unwrap_or(chrono::DateTime::<chrono::Utc>::MAX_UTC)
    }

    /// `attempt` is 1-based (the attempt number that just failed),
    /// `elapsed` is the time since the very first failure. A policy with
    /// both caps `None` (`RetryPolicy::unbounded` with no
    /// `with_max_elapsed`) is never exhausted - the caller retries
    /// forever.
    pub fn is_exhausted(&self, attempt: u32, elapsed: Duration) -> bool {
        self.max_attempts.is_some_and(|max| attempt >= max)
            || self.max_elapsed.is_some_and(|max| elapsed >= max)
    }
}

impl Default for RetryPolicy {
    /// 1s initial backoff, x2 each attempt, capped at 5 minutes, giving
    /// up after 5 attempts. `CrossContextRoute`'s own catch-up loop and
    /// each bridge's inbound path use this unless a caller overrides it;
    /// the outbound bridge direction uses `RetryPolicy::unbounded`
    /// instead - see this struct's own doc comment.
    fn default() -> Self {
        Self::bounded(Duration::from_secs(1), 2.0, Duration::from_secs(300), 5)
    }
}

/// What to do after one failed attempt at a message - see
/// [`MessageRetry::on_failure`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryDecision<T> {
    /// Give up on it and park it: `attempt` attempts, the first failing at
    /// `first_failed_at`.
    Park { attempt: u32, first_failed_at: T },
    /// Try again after this long.
    Wait(Duration),
}

/// One message's retry state in a bridge's inbound loop: how many attempts
/// have failed, and when the first did. Pure like the rest of this crate -
/// the caller supplies the time, in whatever type it keeps time in (`T`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MessageRetry<T> {
    attempt: u32,
    first_failed_at: Option<T>,
}

impl<T> Default for MessageRetry<T> {
    fn default() -> Self {
        Self {
            attempt: 0,
            first_failed_at: None,
        }
    }
}

impl<T: Copy> MessageRetry<T> {
    /// Attempts failed so far, not counting refusals another instance can
    /// take (see `on_failure`).
    pub fn attempt(&self) -> u32 {
        self.attempt
    }

    /// Records a failed attempt made at `now` and decides what follows.
    /// `elapsed_since(t)` is how long ago `t` was. `another_instance` - the
    /// refusal was one of [`ANOTHER_INSTANCE_CODES`] - waits
    /// [`ANOTHER_INSTANCE_RETRY_DELAY`], spending no attempt and never
    /// parking (docs/architecture.md §161). `permanent` - no retry can
    /// change the outcome, such as a payload that isn't JSON - parks at
    /// once. Otherwise it parks once `policy` is exhausted and waits its
    /// backoff until then.
    pub fn on_failure(
        &mut self,
        policy: &RetryPolicy,
        now: T,
        elapsed_since: impl FnOnce(T) -> Duration,
        another_instance: bool,
        permanent: bool,
    ) -> RetryDecision<T> {
        if another_instance {
            return RetryDecision::Wait(ANOTHER_INSTANCE_RETRY_DELAY);
        }
        self.attempt += 1;
        let first_failed_at = *self.first_failed_at.get_or_insert(now);
        if permanent || policy.is_exhausted(self.attempt, elapsed_since(first_failed_at)) {
            RetryDecision::Park {
                attempt: self.attempt,
                first_failed_at,
            }
        } else {
            RetryDecision::Wait(policy.next_backoff(self.attempt))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_retry_parks_once_the_policy_is_exhausted() {
        let policy = RetryPolicy::bounded(Duration::from_secs(1), 2.0, Duration::from_secs(60), 3);
        let mut retry = MessageRetry::<u64>::default();
        let elapsed = |_| Duration::ZERO;
        assert_eq!(
            retry.on_failure(&policy, 10, elapsed, false, false),
            RetryDecision::Wait(policy.next_backoff(1))
        );
        assert_eq!(
            retry.on_failure(&policy, 11, elapsed, false, false),
            RetryDecision::Wait(policy.next_backoff(2))
        );
        assert_eq!(
            retry.on_failure(&policy, 12, elapsed, false, false),
            RetryDecision::Park {
                attempt: 3,
                first_failed_at: 10
            }
        );
    }

    #[test]
    fn message_retry_never_spends_an_attempt_on_another_instance_refusals() {
        let policy = RetryPolicy::bounded(Duration::from_secs(1), 2.0, Duration::from_secs(60), 1);
        let mut retry = MessageRetry::<u64>::default();
        for now in 0..10 {
            assert_eq!(
                retry.on_failure(&policy, now, |_| Duration::from_secs(3600), true, false),
                RetryDecision::Wait(ANOTHER_INSTANCE_RETRY_DELAY)
            );
        }
        assert_eq!(retry.attempt(), 0);
        // The first ordinary failure is attempt 1, first failing now.
        assert_eq!(
            retry.on_failure(&policy, 42, |_| Duration::ZERO, false, false),
            RetryDecision::Park {
                attempt: 1,
                first_failed_at: 42
            }
        );
    }

    #[test]
    fn message_retry_parks_a_permanent_failure_at_once() {
        let policy = RetryPolicy::unbounded(Duration::from_secs(1), 2.0, Duration::from_secs(60));
        let mut retry = MessageRetry::<u64>::default();
        assert_eq!(
            retry.on_failure(&policy, 7, |_| Duration::ZERO, false, true),
            RetryDecision::Park {
                attempt: 1,
                first_failed_at: 7
            }
        );
    }

    #[test]
    fn another_instance_codes() {
        assert!(another_instance_can_do_it("no_decider_registered"));
        assert!(another_instance_can_do_it("sync_projection_not_declared"));
        assert!(!another_instance_can_do_it("payload_does_not_match_schema"));
    }

    #[test]
    fn backoff_grows_geometrically_then_caps() {
        let policy = RetryPolicy::bounded(Duration::from_secs(1), 2.0, Duration::from_secs(10), 10);
        assert_eq!(policy.next_backoff(1), Duration::from_secs(1));
        assert_eq!(policy.next_backoff(2), Duration::from_secs(2));
        assert_eq!(policy.next_backoff(3), Duration::from_secs(4));
        assert_eq!(policy.next_backoff(4), Duration::from_secs(8));
        // 16s would be next, but max_backoff caps it at 10s.
        assert_eq!(policy.next_backoff(5), Duration::from_secs(10));
        assert_eq!(policy.next_backoff(20), Duration::from_secs(10));
    }

    #[test]
    fn bounded_policy_exhausts_by_attempt_count() {
        let policy = RetryPolicy::bounded(Duration::from_secs(1), 2.0, Duration::from_secs(300), 3);
        assert!(!policy.is_exhausted(1, Duration::ZERO));
        assert!(!policy.is_exhausted(2, Duration::ZERO));
        assert!(policy.is_exhausted(3, Duration::ZERO));
        assert!(policy.is_exhausted(4, Duration::ZERO));
    }

    #[test]
    fn unbounded_policy_never_exhausts_by_attempt_count() {
        let policy = RetryPolicy::unbounded(Duration::from_secs(1), 2.0, Duration::from_secs(300));
        assert!(!policy.is_exhausted(1_000_000, Duration::ZERO));
    }

    #[test]
    fn max_elapsed_exhausts_independently_of_attempt_count() {
        let policy = RetryPolicy::unbounded(Duration::from_secs(1), 2.0, Duration::from_secs(300))
            .with_max_elapsed(Duration::from_secs(3600));
        assert!(!policy.is_exhausted(1, Duration::from_secs(3599)));
        assert!(policy.is_exhausted(1, Duration::from_secs(3600)));
        // A single attempt can still exhaust purely on elapsed time -
        // the two caps are independent, either alone is enough.
        assert!(policy.is_exhausted(1, Duration::from_secs(7200)));
    }

    #[test]
    fn either_cap_alone_is_enough() {
        let policy = RetryPolicy::bounded(Duration::from_secs(1), 2.0, Duration::from_secs(300), 5)
            .with_max_elapsed(Duration::from_secs(60));
        // Exhausted by elapsed time well before the attempt cap.
        assert!(policy.is_exhausted(1, Duration::from_secs(61)));
        // Exhausted by attempt count well before the elapsed cap.
        assert!(policy.is_exhausted(5, Duration::ZERO));
    }
}
