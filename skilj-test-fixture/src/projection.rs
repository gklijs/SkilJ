//! `GivenEvents<T: Projection>` - the projection half of this crate's
//! given/when/then harness. See the crate root doc comment for scope.

use skilj_core::plugin::Projection;

use crate::pretty;

/// Folds `T::project()` over a given sequence of events, starting from
/// `T::State::default()` - the same starting point `db::catch_up_bounded_context`/
/// a `sync` projection's own transactional fold both use for an instance
/// that hasn't been touched yet. Always folds under the single default
/// key (`""`, `Projection::keys`'s own default) - a projection that
/// overrides `keys()` to fan one event across several instances isn't
/// covered by this crate yet (out of scope for this pass, same register
/// as `decide()`'s consistency-tag derivation - see the crate root doc
/// comment).
pub struct GivenEvents<T: Projection> {
    events: Vec<T::Event>,
}

impl<T: Projection> GivenEvents<T> {
    pub fn new() -> Self {
        Self { events: Vec::new() }
    }

    pub fn event(mut self, event: T::Event) -> Self {
        self.events.push(event);
        self
    }

    pub fn events(mut self, events: impl IntoIterator<Item = T::Event>) -> Self {
        self.events.extend(events);
        self
    }

    /// Folds every given event into `T::State::default()`, in order, via
    /// `T::project()` directly - no database, no background consumer.
    fn folded(&self) -> T::State {
        let mut state = T::State::default();
        for event in &self.events {
            T::project(&mut state, event, "");
        }
        state
    }

    /// Folds the given events and asserts the resulting state equals
    /// `expected`, compared via `serde_json::Value` (so no `PartialEq`/
    /// `Debug` bound is required on `T::State` beyond what `Projection`
    /// already demands) - a mismatch panics with both sides
    /// pretty-printed.
    pub fn then_state(&self, expected: T::State) {
        let actual = self.folded();
        let actual_value = serde_json::to_value(&actual).expect("Projection::State serializes");
        let expected_value = serde_json::to_value(&expected).expect("Projection::State serializes");
        if actual_value != expected_value {
            panic!(
                "projected state didn't match:\n  expected: {}\n  actual:   {}",
                pretty(&expected_value),
                pretty(&actual_value),
            );
        }
    }

    /// Escape hatch: run an arbitrary assertion against the folded state,
    /// for anything `then_state` doesn't cover.
    pub fn then(&self, f: impl FnOnce(&T::State)) {
        f(&self.folded());
    }
}

impl<T: Projection> Default for GivenEvents<T> {
    fn default() -> Self {
        Self::new()
    }
}
