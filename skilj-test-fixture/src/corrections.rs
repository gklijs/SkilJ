//! Race scenarios for a unique value that can be corrected and retracted
//! (docs/architecture.md §235, issue #133).
//!
//! Unlike the rest of this crate these are about the store as well as your
//! `decide()`: a race is only a race against something that commits. Implement
//! [`UniqueValueCommands`] by submitting your own commands to a real deployment
//! on a real Postgres, then call [`correction_racing_a_claim`],
//! [`corrections_racing_each_other`] and [`retraction_then_registration`] from
//! your tests, or [`all_races`] for the three. Each panics on a violation, like
//! the rest of this crate's assertions.
//!
//! ```rust,ignore
//! struct Customers { /* your Skilj, or a client of it */ }
//!
//! impl UniqueValueCommands for Customers {
//!     async fn claim(&self, identity: &str, value: &str) -> Result<(), String> { /* ... */ }
//!     async fn correct(&self, identity: &str, value: &str) -> Result<(), String> { /* ... */ }
//!     async fn retract_claim(&self, identity: &str) -> Result<(), String> { /* ... */ }
//! }
//!
//! skilj_test_fixture::corrections::all_races(&Customers { /* ... */ }).await;
//! ```

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};

/// How often [`correction_racing_a_claim`] repeats its race: which of the two
/// commits first is up to timing, and both orders have to hold.
const ROUNDS: usize = 8;

/// The commands of a bounded context with a unique value. A value is shaped like
/// an email address, so an ordinary normalizer leaves it unique. Each answers
/// `Ok` when the command was accepted, and `Err` with the rejection's kind when
/// it wasn't.
pub trait UniqueValueCommands {
    /// `identity` claims `value`, which it may do while nobody holds it.
    fn claim(&self, identity: &str, value: &str) -> impl Future<Output = Result<(), String>>;

    /// `identity` replaces the value it holds with `value`, as one append: a
    /// retraction and the new claim (`skilj::retraction::corrected`).
    fn correct(&self, identity: &str, value: &str) -> impl Future<Output = Result<(), String>>;

    /// The claim `identity` holds is retracted, with nothing replacing it.
    fn retract_claim(&self, identity: &str) -> impl Future<Output = Result<(), String>>;
}

/// Unique across this process and across runs against one database.
fn unique(prefix: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let run = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!("{prefix}{run}x{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

fn unique_value(prefix: &str) -> String {
    format!("{}@example.com", unique(prefix))
}

fn expect_accepted(what: &str, outcome: Result<(), String>) {
    if let Err(kind) = outcome {
        panic!("{what} should have been accepted, was rejected with `{kind}`");
    }
}

/// A correction racing a new claim of its target value: they conflict on the
/// value, so exactly one of them gets it.
pub async fn correction_racing_a_claim(commands: &impl UniqueValueCommands) {
    for _ in 0..ROUNDS {
        let corrector = unique("corrector");
        let claimant = unique("claimant");
        let target = unique_value("target");
        expect_accepted(
            "the first claim",
            commands.claim(&corrector, &unique_value("start")).await,
        );
        let (corrected, claimed) = tokio::join!(
            commands.correct(&corrector, &target),
            commands.claim(&claimant, &target),
        );
        assert!(
            corrected.is_ok() != claimed.is_ok(),
            "exactly one may get {target}: the correction answered {corrected:?}, the claim \
             {claimed:?}"
        );
    }
}

/// Two corrections of one identity: they conflict on the identity, so whichever
/// goes second acts on what the first left. The original value is free
/// afterwards, and of the two new ones, the identity holds only one.
pub async fn corrections_racing_each_other(commands: &impl UniqueValueCommands) {
    let identity = unique("corrected");
    let start = unique_value("start");
    let first = unique_value("first");
    let second = unique_value("second");
    expect_accepted("the first claim", commands.claim(&identity, &start).await);
    let (first_outcome, second_outcome) = tokio::join!(
        commands.correct(&identity, &first),
        commands.correct(&identity, &second),
    );
    assert!(
        first_outcome.is_ok() || second_outcome.is_ok(),
        "at least one correction has to go through: {first_outcome:?}, {second_outcome:?}"
    );
    expect_accepted(
        "claiming the corrected-away value",
        commands.claim(&unique("other"), &start).await,
    );
    let first_free = commands.claim(&unique("other"), &first).await.is_ok();
    let second_free = commands.claim(&unique("other"), &second).await.is_ok();
    assert!(
        first_free != second_free,
        "one of the two values is held and the other free: {first} free is {first_free}, \
         {second} free is {second_free}"
    );
}

/// A retraction followed by a new claim: the value is free for anyone, and the
/// retracted claim doesn't count against its new holder.
pub async fn retraction_then_registration(commands: &impl UniqueValueCommands) {
    let original = unique("original");
    let value = unique_value("retracted");
    expect_accepted("the first claim", commands.claim(&original, &value).await);
    expect_accepted("the retraction", commands.retract_claim(&original).await);
    expect_accepted(
        "claiming the retracted value",
        commands.claim(&unique("next"), &value).await,
    );
    assert!(
        commands.claim(&unique("late"), &value).await.is_err(),
        "{value} has a new holder, so another claim has to be rejected"
    );
}

/// The three scenarios, one after the other.
pub async fn all_races(commands: &impl UniqueValueCommands) {
    correction_racing_a_claim(commands).await;
    corrections_racing_each_other(commands).await;
    retraction_then_registration(commands).await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// A correct model: value to holder, one identity holding one value.
    #[derive(Default)]
    struct Model {
        holders: Mutex<HashMap<String, String>>,
        /// A retraction that forgets to free the value, what the scenarios
        /// exist to catch.
        retraction_keeps_the_value: bool,
        /// A correction that doesn't release the old value.
        correction_keeps_the_old_value: bool,
    }

    impl UniqueValueCommands for Model {
        async fn claim(&self, identity: &str, value: &str) -> Result<(), String> {
            let mut holders = self.holders.lock().unwrap();
            if holders.contains_key(value) {
                return Err("taken".to_string());
            }
            holders.insert(value.to_string(), identity.to_string());
            Ok(())
        }

        async fn correct(&self, identity: &str, value: &str) -> Result<(), String> {
            let mut holders = self.holders.lock().unwrap();
            if holders.contains_key(value) {
                return Err("taken".to_string());
            }
            if !self.correction_keeps_the_old_value {
                holders.retain(|_, holder| holder != identity);
            }
            holders.insert(value.to_string(), identity.to_string());
            Ok(())
        }

        async fn retract_claim(&self, identity: &str) -> Result<(), String> {
            if !self.retraction_keeps_the_value {
                self.holders
                    .lock()
                    .unwrap()
                    .retain(|_, holder| holder != identity);
            }
            Ok(())
        }
    }

    fn run(future: impl Future<Output = ()>) {
        tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap()
            .block_on(future);
    }

    #[test]
    fn a_correct_model_passes() {
        run(all_races(&Model::default()));
    }

    #[test]
    #[should_panic(expected = "another claim has to be rejected")]
    fn a_value_nobody_ever_holds_fails() {
        struct AlwaysFree(Model);
        impl UniqueValueCommands for AlwaysFree {
            async fn claim(&self, identity: &str, value: &str) -> Result<(), String> {
                self.0.holders.lock().unwrap().remove(value);
                self.0.claim(identity, value).await
            }
            async fn correct(&self, identity: &str, value: &str) -> Result<(), String> {
                self.0.correct(identity, value).await
            }
            async fn retract_claim(&self, identity: &str) -> Result<(), String> {
                self.0.retract_claim(identity).await
            }
        }
        run(retraction_then_registration(&AlwaysFree(Model::default())));
    }

    #[test]
    #[should_panic(expected = "claiming the retracted value should have been accepted")]
    fn a_retraction_keeping_the_value_fails() {
        run(retraction_then_registration(&Model {
            retraction_keeps_the_value: true,
            ..Model::default()
        }));
    }

    #[test]
    #[should_panic(expected = "claiming the corrected-away value should have been accepted")]
    fn a_correction_keeping_the_old_value_fails() {
        run(corrections_racing_each_other(&Model {
            correction_keeps_the_old_value: true,
            ..Model::default()
        }));
    }
}
