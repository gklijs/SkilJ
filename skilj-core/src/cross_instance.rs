//! Cross-instance push completeness (Codeberg issue #2; `@guarantee
//! DeliverySpansInstances`/`RegistrationReachesEveryInstance` in
//! specs/skilj.allium) - see docs/architecture.md's own write-up of this
//! pass for the full design and its narrower spec `Excludes` entry for
//! what stays deliberately out of scope.
//!
//! Three Postgres `NOTIFY` channels: this crate's own `db` module sends
//! on them (`db::notify_event_appended`/`db::notify_registration_changed`/
//! `db::notify_revocation`, called at the same choke points
//! `EventBroadcaster::publish`/`RevocationBroadcaster::publish`/every
//! type-registration write already go through), and this module's
//! [`Listener`] receives on them. Every instance both sends and
//! receives, symmetrically - coordinated purely through the shared
//! Postgres database `PgListener` already connects to, never through
//! instance discovery, a leader, or any other shared infrastructure.
//! `skilj` (the facade crate) is what actually drives a [`Listener`] in
//! a background task and dispatches each [`Message`] into the right
//! destination (`EventBroadcaster`/`RevocationBroadcaster`/a GraphQL
//! schema rebuild) - this module only owns the `LISTEN` mechanics and
//! the channel-name/payload contract, staying inside "skilj-core is the
//! only crate that owns the database driver" (docs/architecture.md
//! §3.1); it has no reason to know `skilj-graphql`'s `SchemaRegistry`
//! exists.
//!
//! **Delivery is at-most-once, deliberately** - a dropped connection
//! reconnects on its own (`sqlx::postgres::PgListener::recv`'s own
//! documented behavior: it re-establishes a connection from the same
//! pool and automatically re-`LISTEN`s on every channel this `Listener`
//! ever subscribed to), but any notification sent while disconnected is
//! gone for good. Every message here is a liveliness signal only, never
//! a correctness-bearing write - Postgres remains the durable source of
//! truth throughout, so a missed message just means the same self-heal
//! path a same-process lagged receiver already takes (a fresh read),
//! never data loss for events or registrations. Revocation is the one
//! asymmetric case - see `@guarantee DeliverySpansInstances`'s own
//! second paragraph in the spec: a missed revocation reach doesn't leak
//! events (`DeliverToSubscriptions` re-checks `access_mapping.status`
//! live on every delivery regardless), but it does leave a revoked
//! subscription open and silent for longer than the happy path.

use crate::access_control::RevokedMapping;
use crate::db::Pool;

/// `db::notify_event_appended`'s channel.
pub const EVENTS_CHANNEL: &str = "skilj_events";
/// `db::notify_revocation`'s channel.
pub const REVOCATIONS_CHANNEL: &str = "skilj_revocations";
/// `db::notify_registration_changed`'s channel.
pub const REGISTRATION_CHANGED_CHANNEL: &str = "skilj_registration_changed";

/// One decoded cross-instance notification - see this module's own doc
/// comment for the three channels these come from. Deliberately not
/// `Event`/`RevokedMapping` themselves reused verbatim as the payload
/// shape: `EventAppended` is a pointer (see its own doc comment for
/// why), and `RegistrationChanged` carries nothing at all.
///
/// `EventAppended` and `Revoked` both carry `origin_instance_id` - the
/// sending instance's own `EventBroadcaster::instance_id`/
/// `RevocationBroadcaster::instance_id` at the moment it published
/// locally and NOTIFYed, alongside each other, from the same choke
/// point. This module stays a decode-only layer (see its own doc
/// comment - it "has no reason to know skilj-graphql's SchemaRegistry
/// exists"), so it makes no decision based on this field itself; it's
/// the caller's own local broadcasters this needs comparing against, and
/// `skilj::SkiljBuilder::build`'s own dispatch loop is where that
/// comparison happens - see its own doc comment for why a match there
/// means "skip, this is my own write echoing back" rather than
/// "republish." `RegistrationChanged` carries no such field: unlike the
/// other two channels, nothing publishes a schema rebuild directly on
/// the write path - this NOTIFY is the *only* way any instance, including
/// the one that made the change, ever rebuilds its schema, so a
/// same-instance echo of it must always be acted on, never skipped.
#[derive(Debug, Clone)]
pub enum Message {
    /// `skilj_events` - a pointer, not the full event (Postgres's own
    /// 8000-byte `NOTIFY` payload cap, and every subscriber already
    /// re-fetches/renders the real event via the existing DB-backed path
    /// anyway). The receiving instance fetches the real event with this
    /// (via the plain, uncached `db::get_event_by_sequence` -
    /// `skilj::SkiljBuilder::build`'s own dispatch loop explains why not
    /// the cached variant) and republishes it into its own local
    /// `EventBroadcaster` - unless `origin_instance_id` names this exact
    /// instance, see this enum's own doc comment.
    EventAppended {
        bounded_context: String,
        sequence: i64,
        origin_instance_id: String,
    },
    /// `skilj_revocations` - small enough to carry in full.
    Revoked {
        revoked: RevokedMapping,
        origin_instance_id: String,
    },
    /// `skilj_registration_changed` - a bare signal, no payload: which
    /// exact type or bounded context changed doesn't matter, every
    /// listener reacts identically (rebuild the whole schema).
    RegistrationChanged,
}

#[derive(serde::Deserialize)]
struct EventAppendedPayload {
    bounded_context: String,
    sequence: i64,
    /// `#[serde(default)]` so an older instance's payload (predating
    /// this field) still parses instead of being dropped as malformed -
    /// this module's own established "a gap self-heals, isn't fatal"
    /// principle. Defaulting to `""` (never a real
    /// `EventBroadcaster::instance_id`, always a fresh random id) means
    /// such a message is simply never mistaken for a self-echo, which
    /// only briefly reintroduces the pre-fix double-delivery this field
    /// exists to prevent, self-healing away as soon as every instance in
    /// a rolling deploy is running code that sends it.
    #[serde(default)]
    origin_instance_id: String,
}

#[derive(serde::Deserialize)]
struct RevokedPayload {
    role_id: String,
    bounded_context: String,
    /// See `EventAppendedPayload::origin_instance_id`'s own doc comment.
    #[serde(default)]
    origin_instance_id: String,
}

/// Thin wrapper around `sqlx::postgres::PgListener`, subscribed to all
/// three channels from [`connect`](Listener::connect) onward - callers
/// only ever see already-decoded [`Message`]s, never raw channel names
/// or JSON payloads.
pub struct Listener(sqlx::postgres::PgListener);

impl Listener {
    /// Opens one dedicated listening connection (from `pool`) and
    /// subscribes to all three channels. One `Listener` per `Skilj`
    /// instance is the intended shape - see `skilj::SkiljBuilder::build`'s
    /// own call site.
    pub async fn connect(pool: &Pool) -> crate::error::Result<Self> {
        let mut listener = sqlx::postgres::PgListener::connect_with(pool).await?;
        listener
            .listen_all([
                EVENTS_CHANNEL,
                REVOCATIONS_CHANNEL,
                REGISTRATION_CHANGED_CHANNEL,
            ])
            .await?;
        Ok(Self(listener))
    }

    /// Blocks until the next cross-instance message. `PgListener::recv`
    /// already auto-reconnects and re-subscribes to every channel on a
    /// dropped connection (its own doc comment) - callers get that for
    /// free just by looping on this. A notification this process can't
    /// parse (an unknown channel, or a payload from some future/older
    /// version's shape) is logged and skipped, never a panic and never
    /// something that stops the loop over one bad message - the same
    /// "a gap self-heals, isn't fatal" principle every channel here
    /// already follows.
    pub async fn recv(&mut self) -> crate::error::Result<Message> {
        loop {
            let notification = self.0.recv().await?;
            match notification.channel() {
                EVENTS_CHANNEL => {
                    match serde_json::from_str::<EventAppendedPayload>(notification.payload()) {
                        Ok(p) => {
                            return Ok(Message::EventAppended {
                                bounded_context: p.bounded_context,
                                sequence: p.sequence,
                                origin_instance_id: p.origin_instance_id,
                            })
                        }
                        Err(err) => tracing::warn!(
                            error = %err,
                            "malformed skilj_events NOTIFY payload, skipping"
                        ),
                    }
                }
                REVOCATIONS_CHANNEL => {
                    match serde_json::from_str::<RevokedPayload>(notification.payload()) {
                        Ok(p) => {
                            return Ok(Message::Revoked {
                                revoked: RevokedMapping {
                                    role_id: p.role_id,
                                    bounded_context: p.bounded_context,
                                },
                                origin_instance_id: p.origin_instance_id,
                            })
                        }
                        Err(err) => tracing::warn!(
                            error = %err,
                            "malformed skilj_revocations NOTIFY payload, skipping"
                        ),
                    }
                }
                REGISTRATION_CHANGED_CHANNEL => return Ok(Message::RegistrationChanged),
                other => {
                    tracing::warn!(channel = other, "unexpected NOTIFY channel, skipping")
                }
            }
        }
    }
}
