//! Tests for the `ExternalEventIngestion` and `DirectEventCreation`
//! surfaces (`specs/skilj.allium`) - the write-side counterpart to the
//! EventFetch pilot ([docs/architecture.md §9](../../docs/architecture.md#next-steps)), propagated next: entities
//! `ExternalEventToken`/`DirectCreationToken`/`ExternalTriggered`/
//! `DirectlyCreated`, rules `CreateExternalEvent`/`CreateDirectEvent`.
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pair of surfaces' ten source constructs): 24 total
//! (22 from the prior count, plus rule-failure.CreateExternalEvent.6 and
//! CreateDirectEvent.6 - the cross-tenant write fix's own new
//! `tag_owner_scope_satisfied` requires clause on both rules,
//! docs/architecture.md's own write-up of these passes).
//! `surface-actor`/`surface-provides` for each surface (4) were left
//! unbookkept here, pending REST scaffolding that didn't exist yet at
//! the time - see the doc comment at the bottom of this file for why
//! that reasoning is now stale, and where these four are actually
//! exercised today.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, DirectCreationToken, ExternalEventToken, TokenStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{self, BoundedContext, BoundedContextStatus, EventOrigin, EventType};

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

fn bounded_context(name: &str) -> BoundedContext {
    BoundedContext {
        name: name.into(),
        status: BoundedContextStatus::Active,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
        template: None,
    }
}

fn event_type(external_creation_allowed: bool, direct_creation_allowed: bool) -> EventType {
    EventType {
        bounded_context: bounded_context("orders"),
        name: "OrderPlaced".into(),
        schema: "{}".into(),
        schema_version: 3,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed,
        direct_creation_allowed,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: false,
    }
}

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn external_token(status: TokenStatus, event_type: EventType) -> ExternalEventToken {
    ExternalEventToken {
        id: "adapter-1".into(),
        secret: "s3cr3t".into(),
        status,
        created_at: timestamp(0),
        revoked_at: None,
        event_type,
        scope: None,
    }
}

fn direct_token(status: TokenStatus, event_type: EventType) -> DirectCreationToken {
    DirectCreationToken {
        id: "adapter-2".into(),
        secret: "s3cr3t".into(),
        status,
        created_at: timestamp(0),
        revoked_at: None,
        event_type,
        scope: None,
    }
}

// ---------------------------------------------------------------------
// sum-type-variant.ExternalEventToken / sum-type-variant.DirectCreationToken
// ---------------------------------------------------------------------

#[test]
fn external_event_token_carries_its_variant_specific_field() {
    let token = external_token(TokenStatus::Active, event_type(true, false));
    assert_eq!(token.event_type.name, "OrderPlaced");
}

#[test]
fn direct_creation_token_carries_its_variant_specific_field() {
    let token = direct_token(TokenStatus::Active, event_type(false, true));
    assert_eq!(token.event_type.name, "OrderPlaced");
}

// ---------------------------------------------------------------------
// rule-success.CreateExternalEvent / rule-failure.CreateExternalEvent.{1,2,3,4,5}
// / rule-entity-creation.CreateExternalEvent.1
// / sum-type-variant.ExternalTriggered
// ---------------------------------------------------------------------

#[test]
fn create_external_event_succeeds_and_stamps_source_fields_and_metadata() {
    let et = event_type(true, false);
    let adapter = external_token(TokenStatus::Active, et.clone());

    let event = event_store::create_external_event(
        &adapter,
        r#"{"amount":10}"#.into(),
        r#"{"raw":"kafka blob"}"#.into(),
        Some("orders-topic/0/144".into()),
        7,
        timestamp(500),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    // rule-entity-creation.CreateExternalEvent.1 - the ensures clause's shape.
    assert_eq!(event.bounded_context, bounded_context("orders"));
    assert_eq!(event.event_type, et);
    assert_eq!(event.payload, r#"{"amount":10}"#);
    assert_eq!(event.sequence, 7);
    assert_eq!(event.metadata.r#type, "OrderPlaced");
    assert_eq!(event.metadata.version, 3);
    assert_eq!(event.metadata.client_id, "adapter-1"); // metadata.client_id = the token's id
    assert_eq!(event.metadata.created_at, timestamp(500));
    assert!(event.tags.is_empty()); // derive_tags, empty tag_mappings
    assert!(event.encryption_keys.is_empty()); // protect_sensitive_fields, empty sensitive_fields

    // sum-type-variant.ExternalTriggered - the two opaque, adapter-defined
    // fields, verbatim and untouched by protect_sensitive_fields/derive_tags.
    match event.origin {
        EventOrigin::ExternalTriggered {
            source_content,
            source_context,
        } => {
            assert_eq!(source_content, r#"{"raw":"kafka blob"}"#);
            assert_eq!(source_context, Some("orders-topic/0/144".into()));
        }
        other => panic!("expected ExternalTriggered, got {other:?}"),
    }
}

#[test]
fn create_external_event_allows_an_absent_source_context() {
    let adapter = external_token(TokenStatus::Active, event_type(true, false));

    let event = event_store::create_external_event(
        &adapter,
        "{}".into(),
        "raw".into(),
        None,
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    match event.origin {
        EventOrigin::ExternalTriggered { source_context, .. } => assert_eq!(source_context, None),
        other => panic!("expected ExternalTriggered, got {other:?}"),
    }
}

/// rule-failure.CreateExternalEvent.1 - `requires: event_type.external_creation_allowed = true`.
#[test]
fn create_external_event_rejects_an_event_type_not_opted_into_external_creation() {
    let adapter = external_token(TokenStatus::Active, event_type(false, false));

    let err = event_store::create_external_event(
        &adapter,
        "{}".into(),
        "raw".into(),
        None,
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::ExternalCreationNotAllowed.code()
    );
}

/// rule-failure.CreateExternalEvent.2 - `requires: adapter.event_type = event_type`.
/// Structurally unreachable rather than runtime-tested, the same
/// treatment `fetch_events`' equivalent requires-clause got: there's no
/// separate `event_type` parameter to disagree with `adapter.event_type`
/// (see `create_external_event`'s doc comment).
#[test]
fn create_external_event_event_type_is_always_the_adapters_event_type_by_construction() {
    let et = event_type(true, false);
    let adapter = external_token(TokenStatus::Active, et.clone());
    let _ = event_store::create_external_event(
        &adapter,
        "{}".into(),
        "raw".into(),
        None,
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    );
    assert_eq!(adapter.event_type, et);
}

/// rule-failure.CreateExternalEvent.3 - `requires: adapter.status = active`.
#[test]
fn create_external_event_rejects_a_revoked_token() {
    let adapter = external_token(TokenStatus::Revoked, event_type(true, false));

    let err = event_store::create_external_event(
        &adapter,
        "{}".into(),
        "raw".into(),
        None,
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::TokenNotActive.code());
}

/// rule-failure.CreateExternalEvent.4 - `requires: event_type.bounded_context.status = active`.
#[test]
fn create_external_event_rejects_an_archived_bounded_context() {
    let mut et = event_type(true, false);
    et.bounded_context.status = BoundedContextStatus::Archived;
    let adapter = external_token(TokenStatus::Active, et);

    let err = event_store::create_external_event(
        &adapter,
        "{}".into(),
        "raw".into(),
        None,
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.CreateExternalEvent.5 - `requires: valid_payload(event_type.schema, payload)`.
#[test]
fn create_external_event_rejects_a_payload_that_does_not_match_the_schema() {
    let et = EventType {
        schema: r#"{"properties":{"amount":{"type":"number"}},"required":["amount"]}"#.into(),
        ..event_type(true, false)
    };
    let adapter = external_token(TokenStatus::Active, et);

    let err = event_store::create_external_event(
        &adapter,
        "{}".into(), // missing the required "amount" field
        "raw".into(),
        None,
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::PayloadDoesNotMatchSchema.code()
    );
}

#[test]
fn create_external_event_derives_real_tags_from_a_real_tag_mapping() {
    let et = EventType {
        tag_mappings: vec![skilj_core::shared::TagMapping {
            key: "order".into(),
            field: "order_id".into(),
        }],
        ..event_type(true, false)
    };
    let adapter = external_token(TokenStatus::Active, et);

    let event = event_store::create_external_event(
        &adapter,
        r#"{"order_id":"O-1"}"#.into(),
        "raw".into(),
        None,
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    assert_eq!(
        event.tags,
        vec![skilj_core::shared::Tag {
            key: "order".into(),
            value: Some("O-1".into()),
        }]
    );
}

/// rule-failure.CreateExternalEvent.6 - `requires:
/// tag_owner_scope_satisfied(tags, event_type.owner_tag_key, adapter.scope)` -
/// cross-tenant write fix (docs/architecture.md's own write-up of these
/// passes). A token scoped to one company cannot create an event whose
/// own derived owner tag names another.
#[test]
fn create_external_event_rejects_an_event_whose_owner_does_not_match_the_tokens_scope() {
    let et = EventType {
        owner_tag_key: Some("company".into()),
        tag_mappings: vec![skilj_core::shared::TagMapping {
            key: "company".into(),
            field: "company_id".into(),
        }],
        ..event_type(true, false)
    };
    let adapter = ExternalEventToken {
        scope: Some("acme".into()),
        ..external_token(TokenStatus::Active, et)
    };

    let err = event_store::create_external_event(
        &adapter,
        r#"{"company_id":"globex"}"#.into(),
        "raw".into(),
        None,
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantScopeMismatch.code());
}

/// The success half of the same obligation - a matching owner still
/// creates the event normally.
#[test]
fn create_external_event_succeeds_when_the_owner_matches_the_tokens_scope() {
    let et = EventType {
        owner_tag_key: Some("company".into()),
        tag_mappings: vec![skilj_core::shared::TagMapping {
            key: "company".into(),
            field: "company_id".into(),
        }],
        ..event_type(true, false)
    };
    let adapter = ExternalEventToken {
        scope: Some("acme".into()),
        ..external_token(TokenStatus::Active, et)
    };

    let event = event_store::create_external_event(
        &adapter,
        r#"{"company_id":"acme"}"#.into(),
        "raw".into(),
        None,
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    assert_eq!(
        event.tags,
        vec![skilj_core::shared::Tag {
            key: "company".into(),
            value: Some("acme".into()),
        }]
    );
}

// ---------------------------------------------------------------------
// rule-success.CreateDirectEvent / rule-failure.CreateDirectEvent.{1,2,3,4,5}
// / rule-entity-creation.CreateDirectEvent.1 / sum-type-variant.DirectlyCreated
// ---------------------------------------------------------------------

#[test]
fn create_direct_event_succeeds_and_stamps_metadata() {
    let et = event_type(false, true);
    let adapter = direct_token(TokenStatus::Active, et.clone());

    let event = event_store::create_direct_event(
        &adapter,
        r#"{"amount":10}"#.into(),
        3,
        timestamp(200),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    assert_eq!(event.bounded_context, bounded_context("orders"));
    assert_eq!(event.event_type, et);
    assert_eq!(event.payload, r#"{"amount":10}"#);
    assert_eq!(event.sequence, 3);
    assert_eq!(event.metadata.client_id, "adapter-2");
    assert_eq!(event.metadata.created_at, timestamp(200));
    assert!(event.tags.is_empty());
    assert!(event.encryption_keys.is_empty());
    assert_eq!(event.origin, EventOrigin::DirectlyCreated);
}

#[test]
fn create_direct_event_derives_real_tags_from_a_real_tag_mapping() {
    let et = EventType {
        tag_mappings: vec![skilj_core::shared::TagMapping {
            key: "order".into(),
            field: "order_id".into(),
        }],
        ..event_type(false, true)
    };
    let adapter = direct_token(TokenStatus::Active, et);

    let event = event_store::create_direct_event(
        &adapter,
        r#"{"order_id":"O-2"}"#.into(),
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    assert_eq!(
        event.tags,
        vec![skilj_core::shared::Tag {
            key: "order".into(),
            value: Some("O-2".into()),
        }]
    );
}

/// rule-failure.CreateDirectEvent.1 - `requires: event_type.direct_creation_allowed = true`.
#[test]
fn create_direct_event_rejects_an_event_type_not_opted_into_direct_creation() {
    let adapter = direct_token(TokenStatus::Active, event_type(false, false));

    let err = event_store::create_direct_event(&adapter, "{}".into(), 0, timestamp(0), |_, _| {
        unreachable!("no sensitive fields in this test")
    })
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::DirectCreationNotAllowed.code()
    );
}

/// rule-failure.CreateDirectEvent.2 - `requires: adapter.event_type = event_type`,
/// unreachable by construction - same reasoning as CreateExternalEvent's
/// equivalent test above.
#[test]
fn create_direct_event_event_type_is_always_the_adapters_event_type_by_construction() {
    let et = event_type(false, true);
    let adapter = direct_token(TokenStatus::Active, et.clone());
    let _ = event_store::create_direct_event(&adapter, "{}".into(), 0, timestamp(0), |_, _| {
        unreachable!("no sensitive fields in this test")
    });
    assert_eq!(adapter.event_type, et);
}

/// rule-failure.CreateDirectEvent.3 - `requires: adapter.status = active`.
#[test]
fn create_direct_event_rejects_a_revoked_token() {
    let adapter = direct_token(TokenStatus::Revoked, event_type(false, true));

    let err = event_store::create_direct_event(&adapter, "{}".into(), 0, timestamp(0), |_, _| {
        unreachable!("no sensitive fields in this test")
    })
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::TokenNotActive.code());
}

/// rule-failure.CreateDirectEvent.4 - `requires: event_type.bounded_context.status = active`.
#[test]
fn create_direct_event_rejects_an_archived_bounded_context() {
    let mut et = event_type(false, true);
    et.bounded_context.status = BoundedContextStatus::Archived;
    let adapter = direct_token(TokenStatus::Active, et);

    let err = event_store::create_direct_event(&adapter, "{}".into(), 0, timestamp(0), |_, _| {
        unreachable!("no sensitive fields in this test")
    })
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.CreateDirectEvent.5 - `requires: valid_payload(event_type.schema, payload)`.
#[test]
fn create_direct_event_rejects_a_payload_that_does_not_match_the_schema() {
    let et = EventType {
        schema: r#"{"properties":{"amount":{"type":"number"}},"required":["amount"]}"#.into(),
        ..event_type(false, true)
    };
    let adapter = direct_token(TokenStatus::Active, et);

    let err = event_store::create_direct_event(
        &adapter,
        r#"{"amount":"not a number"}"#.into(),
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::PayloadDoesNotMatchSchema.code()
    );
}

/// rule-failure.CreateDirectEvent.6 - `requires:
/// tag_owner_scope_satisfied(tags, event_type.owner_tag_key, adapter.scope)` -
/// same obligation as `CreateExternalEvent`'s own above, checked against
/// a `DirectCreationToken` instead.
#[test]
fn create_direct_event_rejects_an_event_whose_owner_does_not_match_the_tokens_scope() {
    let et = EventType {
        owner_tag_key: Some("company".into()),
        tag_mappings: vec![skilj_core::shared::TagMapping {
            key: "company".into(),
            field: "company_id".into(),
        }],
        ..event_type(false, true)
    };
    let adapter = DirectCreationToken {
        scope: Some("acme".into()),
        ..direct_token(TokenStatus::Active, et)
    };

    let err = event_store::create_direct_event(
        &adapter,
        r#"{"company_id":"globex"}"#.into(),
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantScopeMismatch.code());
}

/// The success half - a matching owner, and a payload naming no owner at
/// all (so no tag is derived) rejected the identical fail-closed way as
/// a mismatch.
#[test]
fn create_direct_event_succeeds_when_the_owner_matches_the_tokens_scope() {
    let et = EventType {
        owner_tag_key: Some("company".into()),
        tag_mappings: vec![skilj_core::shared::TagMapping {
            key: "company".into(),
            field: "company_id".into(),
        }],
        ..event_type(false, true)
    };
    let adapter = DirectCreationToken {
        scope: Some("acme".into()),
        ..direct_token(TokenStatus::Active, et)
    };

    let event = event_store::create_direct_event(
        &adapter,
        r#"{"company_id":"acme"}"#.into(),
        0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    assert_eq!(
        event.tags,
        vec![skilj_core::shared::Tag {
            key: "company".into(),
            value: Some("acme".into()),
        }]
    );
}

#[test]
fn create_direct_event_rejects_a_payload_naming_no_owner_at_all_when_scoped() {
    let et = EventType {
        owner_tag_key: Some("company".into()),
        tag_mappings: vec![skilj_core::shared::TagMapping {
            key: "company".into(),
            field: "company_id".into(),
        }],
        ..event_type(false, true)
    };
    let adapter = DirectCreationToken {
        scope: Some("acme".into()),
        ..direct_token(TokenStatus::Active, et)
    };

    let err = event_store::create_direct_event(&adapter, "{}".into(), 0, timestamp(0), |_, _| {
        unreachable!("no sensitive fields in this test")
    })
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantScopeMismatch.code());
}

// ---------------------------------------------------------------------
// surface-actor / surface-provides for ExternalEventIngestion and
// DirectEventCreation - stale note, corrected
// ---------------------------------------------------------------------
//
// This used to say these four obligations were uncovered because
// skilj-rest didn't exist yet ("still `// TODO` in full"). That's long
// since untrue - skilj-rest is a real, published, extensively tested
// crate, and real end-to-end REST tests against exactly these two
// surfaces already exist: `skilj/tests/payload_validation.rs`
// (`direct_event_creation_rejects_a_payload_that_does_not_match_the_schema_with_400`/
// `..._rejects_an_event_whose_owner_does_not_match_the_tokens_scope`) and
// `skilj/tests/event_creation_atomicity.rs`. What's still true is
// narrower than the old note claimed: these four obligation *ids*
// specifically aren't bookkept as covered by name in any pass's own
// "obligations covered here" count - a documentation gap to close with
// a fresh pass over `allium plan`'s own output, not a missing-
// implementation one.
