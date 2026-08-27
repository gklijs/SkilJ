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
#[derive(Debug, Clone)]
pub enum Message {
    /// `skilj_events` - a pointer, not the full event (Postgres's own
    /// 8000-byte `NOTIFY` payload cap, and every subscriber already
    /// re-fetches/renders the real event via the existing DB-backed path
    /// anyway). The receiving instance fetches the real event with this
    /// (via the plain, uncached `db::get_event_by_sequence` -
    /// `skilj::SkiljBuilder::build`'s own dispatch loop explains why not
    /// the cached variant) and republishes it into its own local
    /// `EventBroadcaster`.
    EventAppended {
        bounded_context: String,
        sequence: i64,
    },
    /// `skilj_revocations` - small enough to carry in full.
    Revoked(RevokedMapping),
    /// `skilj_registration_changed` - a bare signal, no payload: which
    /// exact type or bounded context changed doesn't matter, every
    /// listener reacts identically (rebuild the whole schema).
    RegistrationChanged,
}

#[derive(serde::Deserialize)]
struct EventAppendedPayload {
    bounded_context: String,
    sequence: i64,
}

#[derive(serde::Deserialize)]
struct RevokedPayload {
    role_id: String,
    bounded_context: String,
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
                            return Ok(Message::Revoked(RevokedMapping {
                                role_id: p.role_id,
                                bounded_context: p.bounded_context,
                            }))
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
