//! Capability-based routes, not type-or-context-in-path - the presented
//! `AccessToken` alone determines what's being read or written. See
//! docs/architecture.md §7.2 for the full route table:
//!
//! - `POST /v1/events/external`    - ExternalEventIngestion
//! - `POST /v1/events/direct`      - DirectEventCreation
//! - `GET  /v1/events`             - EventFetch::FetchEvents (client-tracked)
//! - `GET  /v1/events/consume`     - EventFetch::ConsumeEvents (server-tracked)
//! - `POST /v1/events/consume/ack` - EventFetch::AcknowledgeEvents (manual_ack only)
//! - `POST /v1/commands/trigger`   - CommandTrigger
//!
//! Presenting the wrong token variant at a route is a 403, not a 404 -
//! the route exists, the credential just doesn't authorize that action.

// TODO: one handler per route above, each extracting and validating the
// bearer token (see auth.rs) before delegating to the matching
// skilj-core rule. Request/response bodies per §7.3; error mapping per
// §7.5 (CommandTrigger's rejection is 200, not an HTTP error - §5.4/§7.3).
