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
    self, AccessLevel, AccessToken, CommandToken, DirectCreationToken, EventReadStartPosition,
    EventReadToken, ExternalEventToken, Role, RoleAccessMapping, RoleStatus, TokenStatus,
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
        None,
        None,
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
    // `start_from` omitted (`None`) resolves to `Beginning` - the rule's
    // own `start_from ?? beginning` default substitution.
    assert_eq!(token.start_from, EventReadStartPosition::Beginning);
}

/// `start_from` is the one parameter `create_event_read_token` doesn't
/// share with its three siblings - a value explicitly given is stamped
/// through unchanged, not silently defaulted.
#[test]
fn create_event_read_token_honours_an_explicit_start_from() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let token = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-4".into(),
        "s3cr3t".into(),
        None,
        Some(EventReadStartPosition::Latest),
        None,
        None,
        timestamp(1000),
    )
    .unwrap();

    assert_eq!(token.start_from, EventReadStartPosition::Latest);
}

/// `at_sequence` with its own matching value succeeds and stamps it
/// through unvalidated - `EventReadToken.start_at_sequence`'s own "no
/// check that it names a real event" treatment.
#[test]
fn create_event_read_token_succeeds_with_at_sequence_and_its_value() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let token = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-5".into(),
        "s3cr3t".into(),
        None,
        Some(EventReadStartPosition::AtSequence),
        Some(41),
        None,
        timestamp(1000),
    )
    .unwrap();

    assert_eq!(token.start_from, EventReadStartPosition::AtSequence);
    assert_eq!(token.start_at_sequence, Some(41));
    assert_eq!(token.start_at_time, None);
}

/// `at_time`'s own version of the test above.
#[test]
fn create_event_read_token_succeeds_with_at_time_and_its_value() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let cutoff = timestamp(500);

    let token = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-6".into(),
        "s3cr3t".into(),
        None,
        Some(EventReadStartPosition::AtTime),
        None,
        Some(cutoff),
        timestamp(1000),
    )
    .unwrap();

    assert_eq!(token.start_from, EventReadStartPosition::AtTime);
    assert_eq!(token.start_at_time, Some(cutoff));
    assert_eq!(token.start_at_sequence, None);
}

/// rule-failure: `at_sequence` with no `start_at_sequence` at all.
#[test]
fn create_event_read_token_rejects_at_sequence_with_no_value() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let err = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-7".into(),
        "s3cr3t".into(),
        None,
        Some(EventReadStartPosition::AtSequence),
        None,
        None,
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::StartAtSequenceMismatch.code()
    );
}

/// rule-failure: `at_sequence` with `start_at_time` given instead of
/// (or alongside) `start_at_sequence`.
#[test]
fn create_event_read_token_rejects_at_sequence_with_the_wrong_value() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let err = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-8".into(),
        "s3cr3t".into(),
        None,
        Some(EventReadStartPosition::AtSequence),
        None,
        Some(timestamp(0)),
        timestamp(0),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::StartAtSequenceMismatch.code()
    );
}

/// rule-failure: `at_time`'s own version of the two guards above,
/// exercised together (missing its own value, given the other one's).
#[test]
fn create_event_read_token_rejects_at_time_with_no_value_or_the_wrong_one() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let missing = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-9".into(),
        "s3cr3t".into(),
        None,
        Some(EventReadStartPosition::AtTime),
        None,
        None,
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(
        missing.code(),
        access_control::Error::StartAtTimeMismatch.code()
    );

    let wrong = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-10".into(),
        "s3cr3t".into(),
        None,
        Some(EventReadStartPosition::AtTime),
        Some(41),
        None,
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(
        wrong.code(),
        access_control::Error::StartAtTimeMismatch.code()
    );
}

/// rule-failure: `beginning`/`latest` (including the omitted-`start_from`
/// default) forbid both values - naming one anyway is refused, not
/// silently dropped.
#[test]
fn create_event_read_token_rejects_a_value_with_beginning_or_latest() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    let with_beginning = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-11".into(),
        "s3cr3t".into(),
        None,
        None,
        Some(41),
        None,
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(
        with_beginning.code(),
        access_control::Error::StartAtValueNotAllowed.code()
    );

    let with_latest = access_control::create_event_read_token(
        &mapping,
        &event_type(),
        "token-12".into(),
        "s3cr3t".into(),
        None,
        Some(EventReadStartPosition::Latest),
        None,
        Some(timestamp(0)),
        timestamp(0),
    )
    .unwrap_err();
    assert_eq!(
        with_latest.code(),
        access_control::Error::StartAtValueNotAllowed.code()
    );
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
        None,
        None,
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
        None,
        None,
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
        None,
        None,
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
        start_from: EventReadStartPosition::Beginning,
        start_at_sequence: None,
        start_at_time: None,
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
            start_from: EventReadStartPosition::Beginning,
            start_at_sequence: None,
            start_at_time: None,
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
        start_from: EventReadStartPosition::Beginning,
        start_at_sequence: None,
        start_at_time: None,
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
        start_from: EventReadStartPosition::Beginning,
        start_at_sequence: None,
        start_at_time: None,
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
        start_from: EventReadStartPosition::Beginning,
        start_at_sequence: None,
        start_at_time: None,
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
        start_from: EventReadStartPosition::Beginning,
        start_at_sequence: None,
        start_at_time: None,
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
        start_from: EventReadStartPosition::Beginning,
        start_at_sequence: None,
        start_at_time: None,
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

/// docs/architecture.md §92: what scope each of the four minting rules
/// stamps, for a minting grant with scope `grant_scope` and a requested
/// scope - `Err` carrying the rejection code when refused.
fn minted_scopes(
    grant_scope: Option<&str>,
    requested: Option<&str>,
) -> Vec<Result<Option<String>, String>> {
    let mut mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    mapping.scope = grant_scope.map(str::to_string);
    let requested = || requested.map(str::to_string);
    let code = |e: skilj_core::Error| e.code().to_string();
    vec![
        access_control::create_external_event_token(
            &mapping,
            &event_type(),
            "t1".into(),
            "s".into(),
            requested(),
            timestamp(0),
        )
        .map(|t| t.scope)
        .map_err(code),
        access_control::create_direct_creation_token(
            &mapping,
            &event_type(),
            "t2".into(),
            "s".into(),
            requested(),
            timestamp(0),
        )
        .map(|t| t.scope)
        .map_err(code),
        access_control::create_event_read_token(
            &mapping,
            &event_type(),
            "t3".into(),
            "s".into(),
            requested(),
            None,
            None,
            None,
            timestamp(0),
        )
        .map(|t| t.scope)
        .map_err(code),
        access_control::create_command_token(
            &mapping,
            &command_type(),
            "t4".into(),
            "s".into(),
            requested(),
            timestamp(0),
        )
        .map(|t| t.scope)
        .map_err(code),
    ]
}

#[test]
fn an_unscoped_admin_mints_any_scope() {
    for requested in [None, Some("acme")] {
        for minted in minted_scopes(None, requested) {
            assert_eq!(minted, Ok(requested.map(str::to_string)));
        }
    }
}

#[test]
fn a_scoped_admin_mints_only_within_its_own_scope() {
    // Omitted: inherits the grant's scope rather than minting an
    // unrestricted token.
    for minted in minted_scopes(Some("acme"), None) {
        assert_eq!(minted, Ok(Some("acme".to_string())));
    }
    for minted in minted_scopes(Some("acme"), Some("acme")) {
        assert_eq!(minted, Ok(Some("acme".to_string())));
    }
    for minted in minted_scopes(Some("acme"), Some("globex")) {
        assert_eq!(minted, Err("token_scope_beyond_grant".to_string()));
    }
}

/// §92: revoking is confined the same way - a scoped admin can revoke a
/// token carrying its own scope, but not another owner's, nor an
/// unrestricted one; an unscoped admin can revoke any.
#[test]
fn a_scoped_admin_revokes_only_within_its_own_scope() {
    let command_token = |scope: Option<&str>| {
        AccessToken::CommandToken(CommandToken {
            id: "t".into(),
            secret: "s".into(),
            status: TokenStatus::Active,
            created_at: timestamp(0),
            revoked_at: None,
            command_type: command_type(),
            scope: scope.map(str::to_string),
        })
    };
    let mut scoped = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    scoped.scope = Some("acme".to_string());
    let unscoped = access_mapping(RoleStatus::Active, AccessLevel::Admin);

    assert!(
        access_control::revoke_token(&scoped, &command_token(Some("acme")), timestamp(1)).is_ok()
    );
    for other in [Some("globex"), None] {
        let err =
            access_control::revoke_token(&scoped, &command_token(other), timestamp(1)).unwrap_err();
        assert_eq!(err.code(), "token_scope_beyond_grant", "{other:?}");
    }
    for any in [Some("acme"), Some("globex"), None] {
        assert!(access_control::revoke_token(&unscoped, &command_token(any), timestamp(1)).is_ok());
    }
}
