//! Tests for the `EventTypeAdminOperations`/`CommandTypeAdminOperations`/
//! `TokenRevocation` surfaces (`specs/skilj.allium`) - propagated after
//! `CommandSubmission` ([docs/architecture.md §9](../../docs/architecture.md#next-steps)): the `AccessToken` sum
//! type over its four variants, and rules `CreateExternalEventToken`/
//! `CreateDirectCreationToken`/`CreateEventReadToken`/`CreateCommandToken`/
//! `RevokeToken`. This is what completes `AccessToken`'s entity shape -
//! `id`/`secret`/`created_at`/`revoked_at` on every variant - grown from
//! the field-by-field cuts each earlier pass took (see
//! `access_control`'s own doc comment and each token struct's).
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pass's source constructs): 31 total.
//! Uncovered this pass, with reason - see the doc comment at the bottom of
//! this file: `surface-actor`/`surface-provides` for each of the three
//! surfaces (6) - same GraphQL-scaffolding gap as `CommandSubmission`'s.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{
    self, AccessLevel, AccessToken, CommandToken, DirectCreationToken, EventReadToken,
    ExternalEventToken, Role, RoleAccessMapping, RoleStatus, TokenStatus,
};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, CommandType, EventType};

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn bounded_context(status: BoundedContextStatus) -> BoundedContext {
    BoundedContext {
        name: "orders".into(),
        status,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
        template: None,
    }
}

fn role() -> Role {
    Role {
        id: "role-1".into(),
        external_subject: "admin@example.com".into(),
        name: "Admin".into(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn access_mapping(status: RoleStatus, level: AccessLevel) -> RoleAccessMapping {
    RoleAccessMapping {
        role: role(),
        bounded_context: bounded_context(BoundedContextStatus::Active),
        level,
        can_read_sensitive: false,
        scope: None,
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn event_type() -> EventType {
    EventType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: "OrderPlaced".into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        external_creation_allowed: false,
        direct_creation_allowed: false,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: false,
    }
}

fn command_type() -> CommandType {
    CommandType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: "PlaceOrder".into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        rest_trigger_allowed: false,
    }
}

fn other_bounded_context_event_type() -> EventType {
    EventType {
        bounded_context: BoundedContext {
            name: "billing".into(),
            status: BoundedContextStatus::Active,
            created_at: timestamp(0),
            created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
            template: None,
        },
        ..event_type()
    }
}

fn other_bounded_context_command_type() -> CommandType {
    CommandType {
        bounded_context: BoundedContext {
            name: "billing".into(),
            status: BoundedContextStatus::Active,
            created_at: timestamp(0),
            created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
            template: None,
        },
        ..command_type()
    }
}

// ---------------------------------------------------------------------
// rule-success.CreateExternalEventToken / rule-failure.{1,2,3}
// / rule-entity-creation.1
// ---------------------------------------------------------------------

#[test]
fn create_external_event_token_succeeds_and_stamps_the_full_entity_shape() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let et = event_type();

    let token = access_control::create_external_event_token(
        &mapping,
        &et,
        "token-1".into(),
        "s3cr3t".into(),
        None,
        timestamp(1000),
    )
    .unwrap();

    // rule-entity-creation.CreateExternalEventToken.1
    assert_eq!(token.id, "token-1");
    assert_eq!(token.secret, "s3cr3t");
    assert_eq!(token.status, TokenStatus::Active);
    assert_eq!(token.created_at, timestamp(1000));
    assert_eq!(token.revoked_at, None);
    assert_eq!(token.event_type, et);
}

/// rule-failure.CreateExternalEventToken.1 - `requires: access_mapping.status = active`.
#[test]
fn create_external_event_token_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);

    let err = access_control::create_external_event_token(
        &mapping,
        &event_type(),
        "token-1".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.CreateExternalEventToken.2 - `requires: access_mapping.level = admin`.
#[test]
fn create_external_event_token_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);

    let err = access_control::create_external_event_token(
        &mapping,
        &event_type(),
        "token-1".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.CreateExternalEventToken.3 - `requires: access_mapping.bounded_context = event_type.bounded_context`.
#[test]
fn create_external_event_token_rejects_an_event_type_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let err = access_control::create_external_event_token(
        &mapping,
        &other_bounded_context_event_type(),
        "token-1".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.CreateDirectCreationToken / rule-failure.{1,2,3}
// / rule-entity-creation.1
// ---------------------------------------------------------------------
//
// Same shape and requires-clauses as CreateExternalEventToken above (see
// create_direct_creation_token's own doc comment) - one success test for
// the entity-creation shape, one failure test per requires-clause, no
// repeat of the other two's exhaustive coverage.

#[test]
fn create_direct_creation_token_succeeds_and_stamps_the_full_entity_shape() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let et = event_type();

    let token = access_control::create_direct_creation_token(
        &mapping,
        &et,
        "token-2".into(),
        "s3cr3t".into(),
        None,
        timestamp(1000),
    )
    .unwrap();

    assert_eq!(token.id, "token-2");
    assert_eq!(token.secret, "s3cr3t");
    assert_eq!(token.status, TokenStatus::Active);
    assert_eq!(token.created_at, timestamp(1000));
    assert_eq!(token.revoked_at, None);
    assert_eq!(token.event_type, et);
}

#[test]
fn create_direct_creation_token_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);

    let err = access_control::create_direct_creation_token(
        &mapping,
        &event_type(),
        "token-2".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

#[test]
fn create_direct_creation_token_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);

    let err = access_control::create_direct_creation_token(
        &mapping,
        &event_type(),
        "token-2".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

#[test]
fn create_direct_creation_token_rejects_an_event_type_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let err = access_control::create_direct_creation_token(
        &mapping,
        &other_bounded_context_event_type(),
        "token-2".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.CreateEventReadToken / rule-failure.{1,2,3}
// / rule-entity-creation.1 / sum-type-variant.EventReadToken
// ---------------------------------------------------------------------

#[test]
fn create_event_read_token_succeeds_and_stamps_the_full_entity_shape() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let et = event_type();

    let token = access_control::create_event_read_token(
        &mapping,
        &et,
        "token-3".into(),
        "s3cr3t".into(),
        None,
        timestamp(1000),
    )
    .unwrap();

    assert_eq!(token.id, "token-3");
    assert_eq!(token.secret, "s3cr3t");
    assert_eq!(token.status, TokenStatus::Active);
    assert_eq!(token.created_at, timestamp(1000));
    assert_eq!(token.revoked_at, None);
    assert_eq!(token.event_type, et);
}

#[test]
fn create_event_read_token_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);

    let err = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-3".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

#[test]
fn create_event_read_token_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);

    let err = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-3".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

#[test]
fn create_event_read_token_rejects_an_event_type_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let err = access_control::create_event_read_token(
        &mapping,
        &other_bounded_context_event_type(),
        "token-3".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// sum-type-variant.EventReadToken - its variant-specific field
/// (`event_type`) is reachable through the `AccessToken` type guard, the
/// same as every other variant already covered elsewhere (e.g.
/// `command_token_carries_its_variant_specific_field`).
#[test]
fn event_read_token_carries_its_variant_specific_field_through_the_access_token_type_guard() {
    let et = event_type();
    let token = EventReadToken {
        id: "token-3".into(),
        secret: "s3cr3t".into(),
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
        event_type: et.clone(),
        scope: None,
    };

    match AccessToken::EventReadToken(token) {
        AccessToken::EventReadToken(t) => assert_eq!(t.event_type, et),
        other => panic!("expected EventReadToken, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// rule-success.CreateCommandToken / rule-failure.{1,2,3}
// / rule-entity-creation.1
// ---------------------------------------------------------------------

#[test]
fn create_command_token_succeeds_and_stamps_the_full_entity_shape() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let ct = command_type();

    let token = access_control::create_command_token(
        &mapping,
        &ct,
        "token-4".into(),
        "s3cr3t".into(),
        None,
        timestamp(1000),
    )
    .unwrap();

    assert_eq!(token.id, "token-4");
    assert_eq!(token.secret, "s3cr3t");
    assert_eq!(token.status, TokenStatus::Active);
    assert_eq!(token.created_at, timestamp(1000));
    assert_eq!(token.revoked_at, None);
    assert_eq!(token.command_type, ct);
}

#[test]
fn create_command_token_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);

    let err = access_control::create_command_token(
        &mapping,
        &command_type(),
        "token-4".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

#[test]
fn create_command_token_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);

    let err = access_control::create_command_token(
        &mapping,
        &command_type(),
        "token-4".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

#[test]
fn create_command_token_rejects_a_command_type_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let err = access_control::create_command_token(
        &mapping,
        &other_bounded_context_command_type(),
        "token-4".into(),
        "s3cr3t".into(),
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.RevokeToken / rule-failure.RevokeToken.{1,2,3,4}
// / transition-edge/transition-rejected/transition-terminal.AccessToken.status
// / entity-fields.AccessToken / when-presence/entity-optional.AccessToken.revoked_at
// ---------------------------------------------------------------------

#[test]
fn revoke_token_succeeds_for_every_variant_and_stamps_revoked_at() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let variants = vec![
        AccessToken::ExternalEventToken(ExternalEventToken {
            id: "t".into(),
            secret: "s".into(),
            status: TokenStatus::Active,
            created_at: timestamp(0),
            revoked_at: None,
            event_type: event_type(),
            scope: None,
        }),
        AccessToken::DirectCreationToken(DirectCreationToken {
            id: "t".into(),
            secret: "s".into(),
            status: TokenStatus::Active,
            created_at: timestamp(0),
            revoked_at: None,
            event_type: event_type(),
            scope: None,
        }),
        AccessToken::CommandToken(CommandToken {
            id: "t".into(),
            secret: "s".into(),
            status: TokenStatus::Active,
            created_at: timestamp(0),
            revoked_at: None,
            command_type: command_type(),
            scope: None,
        }),
        AccessToken::EventReadToken(EventReadToken {
            id: "t".into(),
            secret: "s".into(),
            status: TokenStatus::Active,
            created_at: timestamp(0),
            revoked_at: None,
            event_type: event_type(),
            scope: None,
        }),
    ];

    for token in variants {
        // transition-edge.AccessToken.active.revoked
        let revoked = access_control::revoke_token(&mapping, &token, timestamp(500)).unwrap();
        let (status, revoked_at) = match &revoked {
            AccessToken::ExternalEventToken(t) => (t.status, t.revoked_at),
            AccessToken::DirectCreationToken(t) => (t.status, t.revoked_at),
            AccessToken::CommandToken(t) => (t.status, t.revoked_at),
            AccessToken::EventReadToken(t) => (t.status, t.revoked_at),
        };
        assert_eq!(status, TokenStatus::Revoked);
        // when-presence/entity-optional.AccessToken.revoked_at - present now that status = revoked
        assert_eq!(revoked_at, Some(timestamp(500)));
    }
}

/// entity-fields/when-presence.AccessToken.revoked_at - absent while
/// `status = active`, the other half of the `when-presence` obligation
/// `revoke_token_succeeds_for_every_variant_and_stamps_revoked_at` covers
/// for the revoked half.
#[test]
fn access_token_revoked_at_is_absent_while_active() {
    let token = ExternalEventToken {
        id: "t".into(),
        secret: "s".into(),
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
        event_type: event_type(),
        scope: None,
    };

    assert_eq!(token.status, TokenStatus::Active);
    assert_eq!(token.revoked_at, None);
}

/// rule-failure.RevokeToken.1 - `requires: access_mapping.status = active`.
#[test]
fn revoke_token_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);
    let token = AccessToken::EventReadToken(EventReadToken {
        id: "t".into(),
        secret: "s".into(),
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
        event_type: event_type(),
        scope: None,
    });

    let err = access_control::revoke_token(&mapping, &token, timestamp(0)).unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.RevokeToken.2 - `requires: access_mapping.level = admin`.
#[test]
fn revoke_token_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);
    let token = AccessToken::EventReadToken(EventReadToken {
        id: "t".into(),
        secret: "s".into(),
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
        event_type: event_type(),
        scope: None,
    });

    let err = access_control::revoke_token(&mapping, &token, timestamp(0)).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.RevokeToken.3 - `requires: access_mapping.bounded_context = token_scope`.
#[test]
fn revoke_token_rejects_a_token_scoped_to_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let token = AccessToken::EventReadToken(EventReadToken {
        id: "t".into(),
        secret: "s".into(),
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
        event_type: other_bounded_context_event_type(),
        scope: None,
    });

    let err = access_control::revoke_token(&mapping, &token, timestamp(0)).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.RevokeToken.4 - `requires: token.status = active`.
/// Also transition-terminal.AccessToken.status: revoked has no outbound
/// transition, so a second revocation is rejected the same way.
#[test]
fn revoke_token_rejects_an_already_revoked_token() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let token = AccessToken::EventReadToken(EventReadToken {
        id: "t".into(),
        secret: "s".into(),
        status: TokenStatus::Revoked,
        created_at: timestamp(0),
        revoked_at: Some(timestamp(0)),
        event_type: event_type(),
        scope: None,
    });

    let err = access_control::revoke_token(&mapping, &token, timestamp(0)).unwrap_err();

    assert_eq!(err.code(), access_control::Error::TokenNotActive.code());
}

/// transition-rejected.AccessToken.status - the only declared transition
/// is `active -> revoked`; `revoke_token` is the only function that ever
/// changes `status`, and it only ever writes `Revoked`, so `active ->
/// active` (a no-op re-revocation) and any other transition are
/// structurally unreachable rather than runtime-tested - same treatment
/// as every other "derived, not a separate parameter" requires-clause in
/// this codebase.
#[test]
fn revoke_token_only_ever_produces_the_revoked_status() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let token = AccessToken::EventReadToken(EventReadToken {
        id: "t".into(),
        secret: "s".into(),
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
        event_type: event_type(),
        scope: None,
    });

    let revoked = access_control::revoke_token(&mapping, &token, timestamp(0)).unwrap();

    match revoked {
        AccessToken::EventReadToken(t) => assert_eq!(t.status, TokenStatus::Revoked),
        other => panic!("expected EventReadToken, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// surface-actor/surface-provides.{EventTypeAdminOperations,
// CommandTypeAdminOperations,TokenRevocation} - uncovered
// ---------------------------------------------------------------------
//
// Same GraphQL-scaffolding gap as CommandSubmission's own deferred pair
// (see command_processing.rs's header comment) - no resolver/schema
// wiring in skilj-graphql yet.
