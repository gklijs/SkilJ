//! `next_backoff` feeds `Duration::from_secs_f64`-style conversions and
//! runs inside long-lived background tasks (cross-context routes, every
//! bridge's retry loop), where a panic stops the task. Any policy a
//! caller can build - the fields are public - must produce a backoff in
//! `[0, max_backoff]`, never panic.

use skilj_retry::RetryPolicy;
use std::time::Duration;

fn policy(multiplier: f64, max_backoff: Duration) -> RetryPolicy {
    RetryPolicy::unbounded(Duration::from_millis(100), multiplier, max_backoff)
}

fn assert_sane(policy: RetryPolicy) {
    for attempt in [0, 1, 2, 3, 10, 64, 1_000, 100_000, u32::MAX] {
        let backoff = policy.next_backoff(attempt);
        assert!(
            backoff <= policy.max_backoff,
            "attempt {attempt}: {backoff:?} > {:?}",
            policy.max_backoff
        );
    }
}

#[test]
fn an_uncapped_max_backoff_never_panics() {
    assert_sane(policy(2.0, Duration::MAX));
}

#[test]
fn a_negative_multiplier_never_panics() {
    assert_sane(policy(-2.0, Duration::from_secs(60)));
}

#[test]
fn a_non_finite_multiplier_never_panics() {
    assert_sane(policy(f64::NAN, Duration::from_secs(60)));
    assert_sane(policy(f64::INFINITY, Duration::from_secs(60)));
}

#[test]
fn ordinary_policies_still_grow_geometrically_up_to_the_cap() {
    let p = policy(2.0, Duration::from_secs(1));
    assert_eq!(p.next_backoff(1), Duration::from_millis(100));
    assert_eq!(p.next_backoff(2), Duration::from_millis(200));
    assert_eq!(p.next_backoff(4), Duration::from_millis(800));
    assert_eq!(p.next_backoff(5), Duration::from_secs(1));
    assert_eq!(p.next_backoff(50), Duration::from_secs(1));
}

/// Callers that store a due time used to write `now +
/// chrono::Duration::from_std(backoff).unwrap_or(zero)`: a backoff too
/// large for chrono became *zero* (an immediate, endless retry), and one
/// that fit could still overflow the addition, which panics in chrono.
#[test]
fn next_attempt_at_saturates_instead_of_retrying_immediately_or_panicking() {
    let now = chrono::Utc::now();
    let uncapped = policy(2.0, Duration::MAX);
    for attempt in [1, 64, 1_000, u32::MAX] {
        let due = uncapped.next_attempt_at(now, attempt);
        assert!(due >= now, "attempt {attempt}: due {due} is before now");
    }
    assert_eq!(
        uncapped.next_attempt_at(now, u32::MAX),
        chrono::DateTime::<chrono::Utc>::MAX_UTC,
        "a far-off backoff is 'as late as possible', never 'right away'"
    );
    // Near the end of representable time, still no overflow panic.
    let late = chrono::DateTime::<chrono::Utc>::MAX_UTC - chrono::TimeDelta::seconds(1);
    assert_eq!(
        policy(2.0, Duration::from_secs(60)).next_attempt_at(late, 10),
        chrono::DateTime::<chrono::Utc>::MAX_UTC
    );
}

#[test]
fn next_attempt_at_is_now_plus_the_backoff_normally() {
    let now = chrono::Utc::now();
    let p = policy(2.0, Duration::from_secs(1));
    assert_eq!(
        p.next_attempt_at(now, 2),
        now + chrono::TimeDelta::milliseconds(200)
    );
}
