//! `skilj-inspector` - a read-only Ratatui console connecting directly to
//! Postgres, for browsing a skilj deployment's registered types,
//! projections and events when `skilj-graphql` itself isn't running (the
//! exact moment an operator most wants to look). See docs/architecture.md
//! §14 for the full design and Codeberg issue #6's own "5b".
//!
//! **Read-only by construction, not just by convention**: every function
//! in [`data`] calls only `skilj_core::db` read functions - never
//! `db::migrate` (this crate's own binary never provisions or alters
//! schema; a real `skilj` server owns that) and never a write path. This
//! is worth stating explicitly here since nothing in the type system
//! enforces it - `skilj_core::db` exposes both.
//!
//! **Sensitive fields are never decrypted, ever** (confirmed with the
//! project owner before building this - see the module's own design
//! note in docs/architecture.md §14): this crate never accepts an
//! `EncryptionMasterKey` and has no decryption code path at all. A
//! sensitive field's ciphertext is already what
//! `skilj_core::encryption::encrypt_leaf` substitutes directly into the
//! stored payload at write time, so every event/command row `data`
//! fetches already carries ciphertext for those leaves - rendering it
//! as-is needs no redaction logic to get wrong. Anyone needing plaintext
//! goes through GraphQL, where the real entitlement check
//! (`can_read_sensitive`/subject-match) lives and stays the only path to
//! it.

pub mod app;
pub mod cli;
pub mod data;
pub mod ui;
