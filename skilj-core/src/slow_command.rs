//! Warnings for a command that is slow to read or to decide
//! (docs/architecture.md §203). [`SlowCommandWarnings`] holds the two
//! thresholds and when each command type last warned; the command batcher
//! owns one and hands it to the batch it commits, so a decide under the
//! lock is checked too.

use crate::event_store::CommandType;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// How long a command's read before the lock - the snapshot rows and the
/// history its `decide()` needs - may take before a warning. In the
/// throughput bench, reads of an empty history, of a snapshot plus its
/// tail and of nothing stayed under 185 ms even at 80 concurrent callers,
/// while reading one account's whole history took up to 515 ms there;
/// uncontended, every read took under 25 ms.
pub const DEFAULT_SLOW_READ_WARNING: Duration = Duration::from_millis(250);

/// How long one `decide()` may take before a warning, before the lock or
/// under it. See docs/architecture.md §203 for the bench figures.
pub const DEFAULT_SLOW_DECIDE_WARNING: Duration = Duration::from_millis(10);

/// At most one warning of a kind per command type this often; the next
/// one says how many more there were in between.
const WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// Which threshold a warning is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum Kind {
    Read,
    Decide,
}

/// When a command type last warned of a kind, and how many slow ones it
/// has had since without warning.
#[derive(Debug, Clone, Copy)]
struct Warned {
    at: Instant,
    since: u64,
}

type Key = (String, String, Kind);

/// The thresholds, and the once-a-minute bookkeeping per command type.
/// `Clone` shares the bookkeeping.
#[derive(Clone)]
pub struct SlowCommandWarnings {
    read: Option<Duration>,
    decide: Option<Duration>,
    warned: Arc<Mutex<HashMap<Key, Warned>>>,
}

impl Default for SlowCommandWarnings {
    fn default() -> Self {
        Self {
            read: Some(DEFAULT_SLOW_READ_WARNING),
            decide: Some(DEFAULT_SLOW_DECIDE_WARNING),
            warned: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl SlowCommandWarnings {
    /// `None` never warns about reads.
    pub fn with_read(mut self, threshold: Option<Duration>) -> Self {
        self.read = threshold;
        self
    }

    /// `None` never warns about decides.
    pub fn with_decide(mut self, threshold: Option<Duration>) -> Self {
        self.decide = threshold;
        self
    }

    /// The optimistic half of a command: its read before the lock, then
    /// its first `decide()`.
    pub fn note_resolved(
        &self,
        command_type: &CommandType,
        resolved: &crate::db::ResolvedCommandSubmission,
    ) {
        if let Some(threshold) = self.read.filter(|t| resolved.read_time > *t) {
            if let Some(more) = self.due(command_type, Kind::Read) {
                tracing::warn!(
                    bounded_context = %command_type.bounded_context.name,
                    command_type = %command_type.name,
                    read_ms = resolved.read_time.as_millis() as u64,
                    events_read = resolved.bounded_context_events.len(),
                    threshold_ms = threshold.as_millis() as u64,
                    more_since_last_warning = more,
                    "a command's read before the lock was slow (docs/architecture.md §203). \
                     With many events read, narrow what decide() reads: a consistency_query(), \
                     a snapshot item, or QueryItemMapping::nothing(). With few, the time went to \
                     waiting for a pool connection or for Postgres. \
                     SkiljBuilder::slow_command_read_warning sets the threshold"
                );
            }
        }
        self.note_decide(
            command_type,
            resolved.decide_time,
            resolved.matching_events.len(),
            false,
        );
    }

    /// One `decide()`: the optimistic one, or one again under the lock
    /// after a conflict (`under_lock`), which every other writer to the
    /// bounded context waits for.
    pub fn note_decide(
        &self,
        command_type: &CommandType,
        took: Duration,
        events: usize,
        under_lock: bool,
    ) {
        let Some(threshold) = self.decide.filter(|t| took > *t) else {
            return;
        };
        let Some(more) = self.due(command_type, Kind::Decide) else {
            return;
        };
        tracing::warn!(
            bounded_context = %command_type.bounded_context.name,
            command_type = %command_type.name,
            decide_ms = took.as_millis() as u64,
            events_decided_on = events,
            under_lock,
            threshold_ms = threshold.as_millis() as u64,
            more_since_last_warning = more,
            "a command's decide() was slow (docs/architecture.md §203). decide() runs again \
             under the bounded context's lock after a conflict, holding up every other \
             command there. With many events decided on, narrow them: a consistency_query(), \
             a snapshot item, or QueryItemMapping::nothing(). With few, look at the work \
             decide() itself does. SkiljBuilder::slow_command_decide_warning sets the threshold"
        );
    }

    fn due(&self, command_type: &CommandType, kind: Kind) -> Option<u64> {
        due(
            &mut self.warned.lock().unwrap_or_else(|e| e.into_inner()),
            (
                command_type.bounded_context.name.clone(),
                command_type.name.clone(),
                kind,
            ),
            Instant::now(),
        )
    }
}

/// Records one slow read or decide for `key` at `now`, and says whether to
/// warn: `Some(n)` with the `n` slow ones since the last warning, or
/// `None` within [`WARNING_INTERVAL`] of it.
fn due(warned: &mut HashMap<Key, Warned>, key: Key, now: Instant) -> Option<u64> {
    match warned.get_mut(&key) {
        Some(last) if now.duration_since(last.at) < WARNING_INTERVAL => {
            last.since += 1;
            None
        }
        Some(last) => {
            let more = last.since;
            *last = Warned { at: now, since: 0 };
            Some(more)
        }
        None => {
            warned.insert(key, Warned { at: now, since: 0 });
            Some(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{due, Kind, WARNING_INTERVAL};
    use std::collections::HashMap;
    use std::time::{Duration, Instant};

    #[test]
    fn a_slow_command_warns_once_per_kind_and_interval_and_counts_the_rest() {
        let mut warned = HashMap::new();
        let key = |command: &str, kind| ("bank".to_string(), command.to_string(), kind);
        let start = Instant::now();
        let at = |secs| start + Duration::from_secs(secs);
        assert_eq!(
            due(&mut warned, key("Withdraw", Kind::Read), start),
            Some(0)
        );
        assert_eq!(due(&mut warned, key("Withdraw", Kind::Read), at(1)), None);
        assert_eq!(due(&mut warned, key("Withdraw", Kind::Read), at(2)), None);
        // Another command type, or the other kind, has its own minute.
        assert_eq!(due(&mut warned, key("Deposit", Kind::Read), at(3)), Some(0));
        assert_eq!(
            due(&mut warned, key("Withdraw", Kind::Decide), at(3)),
            Some(0)
        );
        let later = start + WARNING_INTERVAL;
        assert_eq!(
            due(&mut warned, key("Withdraw", Kind::Read), later),
            Some(2)
        );
        assert_eq!(
            due(
                &mut warned,
                key("Withdraw", Kind::Read),
                later + Duration::from_secs(1)
            ),
            None
        );
    }
}
