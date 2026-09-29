//! Caller-supplied idempotency keys are bounded (docs/architecture.md
//! §134): one is stored with every accepted command that carries it, and
//! with a parked delivery, and only the request size bounded it before.

use skilj_core::event_store::{self, MAX_IDEMPOTENCY_KEY_CHARS};

#[test]
fn an_idempotency_key_over_the_bound_is_refused() {
    assert!(event_store::reject_reserved_idempotency_key(None).is_ok());
    let at_bound = "k".repeat(MAX_IDEMPOTENCY_KEY_CHARS);
    assert!(event_store::reject_reserved_idempotency_key(Some(&at_bound)).is_ok());
    let over = "k".repeat(MAX_IDEMPOTENCY_KEY_CHARS + 1);
    let err = event_store::reject_reserved_idempotency_key(Some(&over)).unwrap_err();
    assert!(err.to_string().contains("at most 255 characters"), "{err}");
    // Characters, not bytes: a multi-byte key at the bound is fine.
    let wide = "é".repeat(MAX_IDEMPOTENCY_KEY_CHARS);
    assert!(event_store::reject_reserved_idempotency_key(Some(&wide)).is_ok());
    // The reserved prefixes are still refused, as before.
    let reserved = format!("{}x", event_store::RESERVED_IDEMPOTENCY_KEY_PREFIX);
    assert!(event_store::reject_reserved_idempotency_key(Some(&reserved)).is_err());
}
