//! How large a stored payload may be (issue #132, docs/architecture.md
//! §238). Event and command payloads are stored inline, and nothing used
//! to stop one from carrying a document: a large one is read back by every
//! consistency query and replay that matches it, held whole in the event
//! cache, which counts events rather than bytes, and published as one
//! message by the bridges, where Kafka's default limit is 1 MB.
//!
//! [`PayloadLimits`] bounds what is stored. The builder sets the default
//! (`SkiljBuilder::payload_limits`, handed to the write paths through
//! [`crate::plugin::ProjectionDispatcher::payload_limits`]), and a
//! superadmin can override it per bounded context
//! ([`crate::db::set_payload_limits`]). The override lives on the bounded
//! context's own `sequence` row: every write already locks that row before
//! it allocates a sequence number, so the lock query reads the override
//! too - no round trip of its own, and never a stale copy.
//!
//! The limit applies to what is stored - a sensitive field's ciphertext,
//! not its plaintext - and to each payload on its own: a command's, each
//! of its events', and an external event's `origin_source_content`. Every
//! write path checks it: command submission, single and batched, the
//! events its `decide()` returns, direct and external event creation,
//! single and batched, and scheduled events. What skilj records about its
//! own state (`SkiljRecorded`) is not checked; it is small by construction.
//!
//! A payload over the limit is refused with
//! [`crate::event_store::Error::PayloadTooLarge`], an ordinary per-item
//! outcome: one command or external event in a batch is refused alone. A
//! payload over [`PayloadLimits::warn_payload_bytes`] is stored and
//! logged, at most once a minute per type, like a slow command
//! (docs/architecture.md §203).

use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

/// The default [`PayloadLimits::max_payload_bytes`]: 256 KiB. Well under
/// the GraphQL request limit (2 MiB) and Kafka's default message size
/// (1 MB), and far above what an event that records a fact needs.
pub const DEFAULT_MAX_PAYLOAD_BYTES: usize = 256 * 1024;

/// The default [`PayloadLimits::warn_payload_bytes`]: 64 KiB.
pub const DEFAULT_WARN_PAYLOAD_BYTES: usize = 64 * 1024;

/// At most one warning per type and kind of payload this often; the next
/// one says how many more there were in between.
const WARNING_INTERVAL: Duration = Duration::from_secs(60);

/// How large a stored payload may be, and from what size one is logged.
/// See this module's doc comment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct PayloadLimits {
    /// A payload larger than this, in bytes, is refused.
    pub max_payload_bytes: usize,
    /// A payload larger than this, in bytes, is stored and logged. `None`
    /// never logs.
    pub warn_payload_bytes: Option<usize>,
}

impl Default for PayloadLimits {
    fn default() -> Self {
        Self {
            max_payload_bytes: DEFAULT_MAX_PAYLOAD_BYTES,
            warn_payload_bytes: Some(DEFAULT_WARN_PAYLOAD_BYTES),
        }
    }
}

impl PayloadLimits {
    /// Payloads up to `max_payload_bytes`, warning above the default
    /// [`DEFAULT_WARN_PAYLOAD_BYTES`] - or never, when that is not below
    /// `max_payload_bytes`.
    pub fn new(max_payload_bytes: usize) -> Self {
        Self {
            max_payload_bytes,
            warn_payload_bytes: Some(DEFAULT_WARN_PAYLOAD_BYTES)
                .filter(|warn| *warn < max_payload_bytes),
        }
    }

    /// The same limit, warning above `warn_payload_bytes` instead; `None`
    /// never warns.
    pub fn with_warning(mut self, warn_payload_bytes: Option<usize>) -> Self {
        self.warn_payload_bytes = warn_payload_bytes;
        self
    }

    /// These limits with a bounded context's override applied, as stored:
    /// a column that is null keeps the value it overrides. A stored value
    /// below zero can't be written ([`crate::db::set_payload_limits`]
    /// refuses it) and counts as zero.
    pub fn overridden(
        self,
        max_payload_bytes: Option<i64>,
        warn_payload_bytes: Option<i64>,
    ) -> Self {
        let bytes = |value: i64| usize::try_from(value).unwrap_or(0);
        Self {
            max_payload_bytes: max_payload_bytes.map_or(self.max_payload_bytes, bytes),
            warn_payload_bytes: match warn_payload_bytes {
                Some(value) => Some(bytes(value)),
                None => self.warn_payload_bytes,
            },
        }
    }

    /// Refuses `payload` if it is over the limit, and logs it if it is over
    /// the warning threshold. `kind` and `type_name` say which payload it
    /// is, for the error and the log line.
    pub fn check(
        &self,
        bounded_context: &str,
        kind: PayloadKind,
        type_name: &str,
        payload: &str,
    ) -> crate::error::Result<()> {
        let size = payload.len();
        if size > self.max_payload_bytes {
            return Err(crate::event_store::Error::PayloadTooLarge {
                kind,
                size,
                limit: self.max_payload_bytes,
            }
            .into());
        }
        if let Some(threshold) = self.warn_payload_bytes.filter(|t| size > *t) {
            if let Some(more) = due(bounded_context, kind, type_name) {
                tracing::warn!(
                    bounded_context = %bounded_context,
                    payload = kind.as_str(),
                    type_name = %type_name,
                    size_bytes = size,
                    warn_payload_bytes = threshold,
                    max_payload_bytes = self.max_payload_bytes,
                    more_since_last_warning = more,
                    "a large payload was stored (docs/architecture.md §238). Every consistency \
                     query and replay that matches it reads it whole; a document belongs in a \
                     blob, with a fact about it in the event"
                );
            }
        }
        Ok(())
    }

    /// [`Self::check`] for a command and every event it produced, in that
    /// order: the first payload over the limit refuses the command.
    pub fn check_command(
        &self,
        command: &crate::event_store::Command,
        events: &[crate::event_store::Event],
    ) -> crate::error::Result<()> {
        self.check(
            &command.bounded_context.name,
            PayloadKind::Command,
            &command.command_type.name,
            &command.payload,
        )?;
        events.iter().try_for_each(|event| self.check_event(event))
    }

    /// [`Self::check`] for an event's payload and, for an external event,
    /// the source message it keeps.
    pub fn check_event(&self, event: &crate::event_store::Event) -> crate::error::Result<()> {
        self.check(
            &event.bounded_context.name,
            PayloadKind::Event,
            &event.event_type.name,
            &event.payload,
        )?;
        if let crate::event_store::EventOrigin::ExternalTriggered { source_content, .. } =
            &event.origin
        {
            self.check(
                &event.bounded_context.name,
                PayloadKind::SourceContent,
                &event.event_type.name,
                source_content,
            )?;
        }
        Ok(())
    }
}

/// Which payload [`PayloadLimits::check`] measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum PayloadKind {
    /// A command's payload.
    Command,
    /// An event's payload.
    Event,
    /// The source message an external event keeps (`origin_source_content`).
    SourceContent,
}

impl PayloadKind {
    /// The kind as the error message and the log name it.
    pub fn as_str(self) -> &'static str {
        match self {
            PayloadKind::Command => "command payload",
            PayloadKind::Event => "event payload",
            PayloadKind::SourceContent => "external event source content",
        }
    }
}

impl std::fmt::Display for PayloadKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// When a type last warned about a kind of payload, and how many large
/// ones it has had since without warning.
struct Warned {
    at: Instant,
    since: u64,
}

type Key = (String, PayloadKind, String);

fn warned() -> &'static Mutex<HashMap<Key, Warned>> {
    static WARNED: OnceLock<Mutex<HashMap<Key, Warned>>> = OnceLock::new();
    WARNED.get_or_init(Default::default)
}

/// `Some(how many large payloads went unlogged since the last warning)`
/// when this one should be logged.
fn due(bounded_context: &str, kind: PayloadKind, type_name: &str) -> Option<u64> {
    let mut warned = warned().lock().unwrap_or_else(|e| e.into_inner());
    let key = (bounded_context.to_string(), kind, type_name.to_string());
    let now = Instant::now();
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
    use super::*;

    #[test]
    fn new_drops_a_default_warning_that_is_not_below_the_limit() {
        assert_eq!(
            PayloadLimits::new(1024 * 1024).warn_payload_bytes,
            Some(DEFAULT_WARN_PAYLOAD_BYTES)
        );
        assert_eq!(
            PayloadLimits::new(DEFAULT_WARN_PAYLOAD_BYTES).warn_payload_bytes,
            None
        );
        assert_eq!(PayloadLimits::new(10).warn_payload_bytes, None);
    }

    #[test]
    fn an_override_replaces_only_what_it_sets() {
        let base = PayloadLimits::new(1000).with_warning(Some(500));
        assert_eq!(base.overridden(None, None), base);
        assert_eq!(base.overridden(Some(2000), None).max_payload_bytes, 2000);
        assert_eq!(
            base.overridden(Some(2000), None).warn_payload_bytes,
            Some(500)
        );
        assert_eq!(base.overridden(None, Some(10)).warn_payload_bytes, Some(10));
        assert_eq!(base.overridden(Some(-1), None).max_payload_bytes, 0);
    }

    #[test]
    fn check_refuses_only_above_the_limit() {
        let limits = PayloadLimits::new(4).with_warning(None);
        assert!(limits.check("bc", PayloadKind::Event, "t", "1234").is_ok());
        let err = limits
            .check("bc", PayloadKind::Event, "t", "12345")
            .unwrap_err();
        assert!(matches!(
            err,
            crate::error::Error::EventStore(crate::event_store::Error::PayloadTooLarge {
                kind: PayloadKind::Event,
                size: 5,
                limit: 4,
            })
        ));
    }

    #[test]
    fn warnings_are_throttled_per_type() {
        let first = due("bc-throttle", PayloadKind::Command, "a");
        let second = due("bc-throttle", PayloadKind::Command, "a");
        let other = due("bc-throttle", PayloadKind::Command, "b");
        assert_eq!(first, Some(0));
        assert_eq!(second, None);
        assert_eq!(other, Some(0));
    }
}
