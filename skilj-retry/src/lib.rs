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
    pub fn next_backoff(&self, attempt: u32) -> Duration {
        let exponent = attempt.saturating_sub(1);
        let scaled = self.initial_backoff.as_secs_f64() * self.multiplier.powi(exponent as i32);
        Duration::from_secs_f64(scaled.min(self.max_backoff.as_secs_f64()))
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

#[cfg(test)]
mod tests {
    use super::*;

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
