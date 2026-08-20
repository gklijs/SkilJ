//! Tests for the `CommandTrigger`/`CommandSubmission` surfaces and
//! `ProcessCommand` (`specs/skilj.allium`) - propagated after the two
//! event-creation surfaces (docs/architecture.md §9): entity
//! `CommandToken`, rules `AuthoriseCommandTrigger`/`ProcessCommand`.
//! `AuthoriseCommandSubmission` (the GraphQL/`RoleAccessMapping` path) was
//! added in a later pass, once `access_management.rs` supplied
//! `access_control::Role`/`RoleAccessMapping`/`AccessLevel` to authorise
//! it against - see `authorise_command_submission`'s own doc comment.
//! `process_command` itself is shared by both authorisation paths and is
//! fully covered here via the `CommandToken` one.
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pass's source constructs): 27 total (25 from the
//! original pass, plus rule-failure.AuthoriseCommandTrigger.5/
//! AuthoriseCommandSubmission.5 - `valid_payload`'s own new requires
//! clause on both rules).
//! Uncovered/deferred, with reason - see the doc comment at the bottom of
//! this file:
//!   - `surface-actor`/`surface-provides.CommandTrigger` (2) - REST-
//!     scaffolding gap, same as every prior surface's.
//!   - `surface-actor`/`surface-provides.CommandSubmission` (2) - the
//!     GraphQL counterpart, same as `access_management.rs`'s own
//!     `AccessManagement` gap - no resolver/schema wiring in
//!     `skilj-graphql` yet.
//!   - `SequenceIsGaplessPerBoundedContext`(1) - a property of the real,
//!     Postgres-lock-backed `next_sequence`, not this pure engine.
//!   - `ConsistencyTagKeysAreDeclared`(1) - per the invariant's own text,
//!     "held within one reconciliation pass... not by any check in
//!     ProcessCommand itself" - it's `RegisterEventType`/
//!     `RegisterCommandType`'s registration-ordering obligation, not
//!     ProcessCommand's.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{
    self, AccessLevel, CommandToken, Role, RoleAccessMapping, RoleStatus, TokenStatus,
};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, CommandType, Event, EventOrigin, EventType,
};
use skilj_core::shared::{CommandDecision, EventSpec, Tag};

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

fn bounded_context(status: BoundedContextStatus) -> BoundedContext {
    BoundedContext {
        name: "accounts".into(),
        status,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
    }
}

fn command_type(
    rest_trigger_allowed: bool,
    tag_mappings: Vec<skilj_core::shared::TagMapping>,
) -> CommandType {
    CommandType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: "WithdrawFunds".into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings,
        sensitive_fields: Vec::new(),
        rest_trigger_allowed,
    }
}

fn command_token(status: TokenStatus, command_type: CommandType) -> CommandToken {
    CommandToken {
        id: "trigger-adapter".into(),
        secret: "s3cr3t".into(),
        status,
        created_at: timestamp(0),
        revoked_at: None,
        command_type,
    }
}

fn event_type() -> EventType {
    EventType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: "FundsWithdrawn".into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        sensitive_fields: Vec::new(),
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

fn tag(key: &str, value: &str) -> Tag {
    Tag {
        key: key.into(),
        value: Some(value.into()),
    }
}

fn event_with_tags(sequence: i64, tags: Vec<Tag>) -> Event {
    Event {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        event_type: event_type(),
        payload: "{}".into(),
        metadata: skilj_core::shared::Metadata {
            r#type: "FundsWithdrawn".into(),
            version: 1,
            client_id: "someone".into(),
            created_at: timestamp(0),
        },
        sequence,
        tags,
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn role(status: RoleStatus) -> Role {
    Role {
        id: "role-1".into(),
        external_subject: "user@example.com".into(),
        name: "User".into(),
        superadmin: false,
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn access_mapping(
    status: RoleStatus,
    level: AccessLevel,
    bounded_context: BoundedContext,
) -> RoleAccessMapping {
    RoleAccessMapping {
        role: role(RoleStatus::Active),
        bounded_context,
        level,
        can_read_sensitive: false,
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn accepted(events: Vec<EventSpec>) -> CommandDecision {
    CommandDecision::Accepted { events }
}

fn rejected(reason: &str, kind: &str) -> CommandDecision {
    CommandDecision::Rejected {
        reason: reason.into(),
        kind: kind.into(),
    }
}

// ---------------------------------------------------------------------
// sum-type-variant.CommandToken (entity-fields obligation for the variant)
// ---------------------------------------------------------------------

#[test]
fn command_token_carries_its_variant_specific_field() {
    let token = command_token(TokenStatus::Active, command_type(true, Vec::new()));
    assert_eq!(token.command_type.name, "WithdrawFunds");
}

// ---------------------------------------------------------------------
// rule-success.AuthoriseCommandTrigger / rule-failure.AuthoriseCommandTrigger.{1,2,3,4,5}
// ---------------------------------------------------------------------

#[test]
fn authorise_command_trigger_succeeds_and_stamps_client_id_from_the_token() {
    let ct = command_type(true, Vec::new());
    let token = command_token(TokenStatus::Active, ct.clone());

    let authorised =
        event_store::authorise_command_trigger(&token, r#"{"amount":10}"#.into()).unwrap();

    assert_eq!(authorised.command_type, ct);
    assert_eq!(authorised.payload, r#"{"amount":10}"#);
    assert_eq!(authorised.client_id, "trigger-adapter");
}

/// rule-failure.AuthoriseCommandTrigger.1 - `requires: token.status = active`.
#[test]
fn authorise_command_trigger_rejects_a_revoked_token() {
    let token = command_token(TokenStatus::Revoked, command_type(true, Vec::new()));

    let err = event_store::authorise_command_trigger(&token, "{}".into()).unwrap_err();

    assert_eq!(err.code(), access_control::Error::TokenNotActive.code());
}

/// rule-failure.AuthoriseCommandTrigger.2 - `requires: token.command_type = command_type`.
/// Structurally unreachable rather than runtime-tested - same treatment
/// as every other rule's "derived, not a separate parameter" requires
/// clause in this codebase (see `authorise_command_trigger`'s doc comment).
#[test]
fn authorise_command_trigger_command_type_is_always_the_tokens_command_type_by_construction() {
    let ct = command_type(true, Vec::new());
    let token = command_token(TokenStatus::Active, ct.clone());
    let _ = event_store::authorise_command_trigger(&token, "{}".into());
    assert_eq!(token.command_type, ct);
}

/// rule-failure.AuthoriseCommandTrigger.3 - `requires: command_type.rest_trigger_allowed = true`.
#[test]
fn authorise_command_trigger_rejects_a_command_type_not_opted_into_rest_triggering() {
    let token = command_token(TokenStatus::Active, command_type(false, Vec::new()));

    let err = event_store::authorise_command_trigger(&token, "{}".into()).unwrap_err();

    assert_eq!(err.code(), event_store::Error::RestTriggerNotAllowed.code());
}

/// rule-failure.AuthoriseCommandTrigger.4 - `requires: command_type.bounded_context.status = active`.
#[test]
fn authorise_command_trigger_rejects_an_archived_bounded_context() {
    let mut ct = command_type(true, Vec::new());
    ct.bounded_context = bounded_context(BoundedContextStatus::Archived);
    let token = command_token(TokenStatus::Active, ct);

    let err = event_store::authorise_command_trigger(&token, "{}".into()).unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.AuthoriseCommandTrigger.5 - `requires: valid_payload(command_type.schema, payload)`.
#[test]
fn authorise_command_trigger_rejects_a_payload_that_does_not_match_the_schema() {
    let ct = CommandType {
        schema: r#"{"properties":{"amount":{"type":"number"}},"required":["amount"]}"#.into(),
        ..command_type(true, Vec::new())
    };
    let token = command_token(TokenStatus::Active, ct);

    let err = event_store::authorise_command_trigger(&token, r#"{"amount":"not a number"}"#.into())
        .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::PayloadDoesNotMatchSchema.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.AuthoriseCommandSubmission / rule-failure.AuthoriseCommandSubmission.{1,2,3,4,5}
// ---------------------------------------------------------------------

#[test]
fn authorise_command_submission_succeeds_and_stamps_client_id_from_the_roles_id() {
    let ct = command_type(true, Vec::new());
    let mapping = access_mapping(
        RoleStatus::Active,
        AccessLevel::Write,
        ct.bounded_context.clone(),
    );

    let authorised =
        event_store::authorise_command_submission(&mapping, &ct, r#"{"amount":10}"#.into())
            .unwrap();

    assert_eq!(authorised.command_type, ct);
    assert_eq!(authorised.payload, r#"{"amount":10}"#);
    assert_eq!(authorised.client_id, "role-1"); // mapping.role.id, not mapping.role.external_subject
}

/// An `admin`-level grant authorises submission too - `requires:
/// access_mapping.level in {write, admin}`, not `= write`.
#[test]
fn authorise_command_submission_succeeds_for_an_admin_level_grant() {
    let ct = command_type(true, Vec::new());
    let mapping = access_mapping(
        RoleStatus::Active,
        AccessLevel::Admin,
        ct.bounded_context.clone(),
    );

    let authorised = event_store::authorise_command_submission(&mapping, &ct, "{}".into()).unwrap();

    assert_eq!(authorised.command_type, ct);
}

/// rule-failure.AuthoriseCommandSubmission.1 - `requires: access_mapping.status = active`.
#[test]
fn authorise_command_submission_rejects_a_revoked_mapping() {
    let ct = command_type(true, Vec::new());
    let mapping = access_mapping(
        RoleStatus::Revoked,
        AccessLevel::Write,
        ct.bounded_context.clone(),
    );

    let err = event_store::authorise_command_submission(&mapping, &ct, "{}".into()).unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.AuthoriseCommandSubmission.2 - `requires: access_mapping.level in {write, admin}`.
#[test]
fn authorise_command_submission_rejects_a_read_level_mapping() {
    let ct = command_type(true, Vec::new());
    let mapping = access_mapping(
        RoleStatus::Active,
        AccessLevel::Read,
        ct.bounded_context.clone(),
    );

    let err = event_store::authorise_command_submission(&mapping, &ct, "{}".into()).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.AuthoriseCommandSubmission.3 - `requires: access_mapping.bounded_context = command_type.bounded_context`.
#[test]
fn authorise_command_submission_rejects_a_mapping_scoped_to_a_different_bounded_context() {
    let ct = command_type(true, Vec::new());
    let other_context = BoundedContext {
        name: "billing".into(),
        status: BoundedContextStatus::Active,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
    };
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write, other_context);

    let err = event_store::authorise_command_submission(&mapping, &ct, "{}".into()).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

/// rule-failure.AuthoriseCommandSubmission.4 - `requires: command_type.bounded_context.status = active`.
#[test]
fn authorise_command_submission_rejects_an_archived_bounded_context() {
    let mut ct = command_type(true, Vec::new());
    ct.bounded_context = bounded_context(BoundedContextStatus::Archived);
    let mapping = access_mapping(
        RoleStatus::Active,
        AccessLevel::Write,
        ct.bounded_context.clone(),
    );

    let err = event_store::authorise_command_submission(&mapping, &ct, "{}".into()).unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::BoundedContextArchived.code()
    );
}

/// rule-failure.AuthoriseCommandSubmission.5 - `requires: valid_payload(command_type.schema, payload)`.
#[test]
fn authorise_command_submission_rejects_a_payload_that_does_not_match_the_schema() {
    let ct = CommandType {
        schema: r#"{"properties":{"amount":{"type":"number"}},"required":["amount"]}"#.into(),
        ..command_type(true, Vec::new())
    };
    let mapping = access_mapping(
        RoleStatus::Active,
        AccessLevel::Write,
        ct.bounded_context.clone(),
    );

    let err = event_store::authorise_command_submission(&mapping, &ct, "{}".into()).unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::PayloadDoesNotMatchSchema.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.ProcessCommand / rule-entity-creation.ProcessCommand.1
// / CommandDecision / EventSpec / Command / CommandTriggered
// ---------------------------------------------------------------------

#[test]
fn process_command_accepts_and_creates_the_command_and_its_triggered_events() {
    let ct = command_type(true, Vec::new());
    let et = event_type();
    let mut next_seq = 10i64;

    let result = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        r#"{"amount":10}"#,
        "trigger-adapter",
        &[], // no prior events in this bounded context
        accepted(vec![EventSpec {
            event_type: "FundsWithdrawn".into(),
            payload: serde_json::json!({"amount": 10}),
        }]),
        |name| (name == "FundsWithdrawn").then(|| et.clone()),
        || {
            let seq = next_seq;
            next_seq += 1;
            seq
        },
        timestamp(1000),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    // rule-entity-creation.ProcessCommand.1 - Command.created's shape.
    assert_eq!(result.command.bounded_context, ct.bounded_context);
    assert_eq!(result.command.command_type, ct);
    assert_eq!(result.command.payload, r#"{"amount":10}"#);
    assert_eq!(result.command.metadata.client_id, "trigger-adapter");
    assert_eq!(result.command.metadata.created_at, timestamp(1000));
    assert!(result.command.encryption_keys.is_empty());
    assert!(result.command.consistency_tags.is_empty()); // empty tag_mappings
    assert_eq!(result.command.consistency_boundary, None); // no consistency_tags -> no boundary

    // CommandTriggered.created's shape - one event, matching the single EventSpec.
    assert_eq!(result.events.len(), 1);
    let event = &result.events[0];
    assert_eq!(event.event_type, et);
    assert_eq!(event.payload, r#"{"amount":10}"#);
    assert_eq!(event.sequence, 10); // next_sequence, called once
    assert_eq!(event.metadata.client_id, "trigger-adapter"); // same client_id as the command
    match &event.origin {
        EventOrigin::CommandTriggered { command } => assert_eq!(**command, result.command),
        other => panic!("expected CommandTriggered, got {other:?}"),
    }
}

#[test]
fn process_command_calls_next_sequence_once_per_triggered_event_in_order() {
    let ct = command_type(true, Vec::new());
    let et = event_type();
    let mut calls = Vec::new();
    let mut next_seq = 100i64;

    let result = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        "{}",
        "trigger-adapter",
        &[],
        accepted(vec![
            EventSpec {
                event_type: "FundsWithdrawn".into(),
                payload: serde_json::json!({}),
            },
            EventSpec {
                event_type: "FundsWithdrawn".into(),
                payload: serde_json::json!({}),
            },
        ]),
        |_| Some(et.clone()),
        || {
            calls.push(next_seq);
            let seq = next_seq;
            next_seq += 1;
            seq
        },
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    assert_eq!(calls, vec![100, 101]);
    assert_eq!(
        result.events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        vec![100, 101]
    );
}

/// `decision.accepted = false` - `process_command` surfaces this as
/// `crate::error::Error::CommandRejected`, business data rather than a
/// library-level rejection (see docs/architecture.md §4.1/§5.4/§7.3).
#[test]
fn process_command_surfaces_a_rejected_decision_verbatim() {
    let ct = command_type(true, Vec::new());

    let err = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        "{}",
        "trigger-adapter",
        &[],
        rejected("insufficient balance", "insufficient_balance"),
        |_| None,
        || 0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap_err();

    match err {
        skilj_core::Error::CommandRejected { reason, kind } => {
            assert_eq!(reason, "insufficient balance");
            assert_eq!(kind, "insufficient_balance");
        }
        other => panic!("expected CommandRejected, got {other:?}"),
    }
}

/// Not spec-modeled (see `Error::UnregisteredEventType`'s own doc
/// comment) - a defensive check, not a `ProcessCommand` requires-clause.
#[test]
fn process_command_rejects_an_event_spec_naming_an_unregistered_event_type() {
    let ct = command_type(true, Vec::new());

    let err = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        "{}",
        "trigger-adapter",
        &[],
        accepted(vec![EventSpec {
            event_type: "NoSuchType".into(),
            payload: serde_json::json!({}),
        }]),
        |_| None,
        || 0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::UnregisteredEventType(String::new()).code()
    );
}

// ---------------------------------------------------------------------
// DynamicConsistencyBoundaryHonoured / consistency_boundary_and_matching_events
// ---------------------------------------------------------------------

#[test]
fn consistency_boundary_is_the_highest_sequence_among_tag_matching_events() {
    let events = vec![
        event_with_tags(0, vec![tag("account", "A")]),
        event_with_tags(1, vec![tag("account", "B")]), // different tag value - excluded
        event_with_tags(2, vec![tag("account", "A")]),
        event_with_tags(3, vec![tag("account", "B")]),
    ];

    let (boundary, matching) =
        event_store::consistency_boundary_and_matching_events(&events, &[tag("account", "A")]);

    assert_eq!(boundary, Some(2));
    assert_eq!(
        matching.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        vec![0, 2]
    );
}

/// No `consistency_tags` at all - DCB isn't in play for this command.
#[test]
fn consistency_boundary_is_none_when_no_consistency_tags_are_given() {
    let events = vec![event_with_tags(0, vec![tag("account", "A")])];

    let (boundary, matching) = event_store::consistency_boundary_and_matching_events(&events, &[]);

    assert_eq!(boundary, None);
    assert!(matching.is_empty());
}

/// `consistency_tags` given, but nothing has matched them yet - also
/// `None`, and behaviourally indistinguishable from the no-tags case
/// above (see `consistency_boundary_and_matching_events`'s doc comment
/// for why that's fine).
#[test]
fn consistency_boundary_is_none_when_nothing_has_matched_the_given_tags_yet() {
    let events = vec![event_with_tags(0, vec![tag("account", "B")])];

    let (boundary, matching) =
        event_store::consistency_boundary_and_matching_events(&events, &[tag("account", "A")]);

    assert_eq!(boundary, None);
    assert!(matching.is_empty());
}

/// DynamicConsistencyBoundaryHonoured, restated for a single-snapshot
/// pure function: every event this boundary computation reports as
/// "matching" has `sequence <= boundary` by construction (`boundary` is
/// exactly that set's own maximum) - so nothing past the boundary can
/// ever appear in `matching_events`, the property the invariant exists to
/// guarantee. See the doc comment on `consistency_boundary_and_matching_events`
/// for what this does and doesn't cover (the concurrent-append case is a
/// caller/locking concern, not tested here).
#[test]
fn matching_events_never_exceed_their_own_boundary() {
    let events = vec![
        event_with_tags(0, vec![tag("account", "A")]),
        event_with_tags(5, vec![tag("account", "A")]),
        event_with_tags(9, vec![tag("account", "A")]),
    ];

    let (boundary, matching) =
        event_store::consistency_boundary_and_matching_events(&events, &[tag("account", "A")]);

    let boundary = boundary.expect("non-empty matching set has a boundary");
    assert!(matching.iter().all(|e| e.sequence <= boundary));
}

// ---------------------------------------------------------------------
// CommandConsistencyTagsMatchDeclaredMappings
// ---------------------------------------------------------------------
//
// "every tag in c.consistency_tags matches one of c.command_type.tag_mappings"
// holds by construction of derive_tags, not a separate check - same
// "true by construction" treatment as the by-construction tests above.
// Covered for both the empty-tag_mappings case (vacuously, per
// derive_tags' own doc comment) and a real, non-empty tag_mappings case
// below, now that derive_tags is real.

#[test]
fn command_consistency_tags_are_empty_when_the_command_type_declares_no_tag_mappings() {
    let ct = command_type(true, Vec::new());

    let result = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        "{}",
        "trigger-adapter",
        &[],
        accepted(Vec::new()),
        |_| None,
        || 0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    assert!(result.command.consistency_tags.is_empty());
}

#[test]
fn command_consistency_tags_are_derived_for_real_from_a_real_tag_mapping() {
    let ct = command_type(
        true,
        vec![skilj_core::shared::TagMapping {
            key: "account".into(),
            field: "account_id".into(),
        }],
    );

    let result = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        r#"{"account_id":"A"}"#,
        "trigger-adapter",
        &[],
        accepted(Vec::new()),
        |_| None,
        || 0,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    assert_eq!(result.command.consistency_tags, vec![tag("account", "A")]);
}

#[test]
fn a_full_dcb_scenario_uses_real_derive_tags_output_not_hand_built_fixtures() {
    // Two commands against the same bounded context, sharing a tag key
    // ("account") derived from real payloads via real `derive_tags` -
    // unlike every other DCB test in this file, which hand-builds its
    // own `Tag`/`Event` fixtures directly.
    let ct = command_type(
        true,
        vec![skilj_core::shared::TagMapping {
            key: "account".into(),
            field: "account_id".into(),
        }],
    );

    let first = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        r#"{"account_id":"A"}"#,
        "trigger-adapter",
        &[],
        accepted(Vec::new()),
        |_| None,
        || 1,
        timestamp(0),
        |_, _| unreachable!("no sensitive fields in this test"),
    )
    .unwrap();

    assert_eq!(first.command.consistency_tags, vec![tag("account", "A")]);

    // A prior event carrying the same real-derived tag is now in the
    // bounded context's history; a second command against the same
    // account must see it as its own consistency boundary/matching set.
    let prior_event = Event {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        event_type: event_type(),
        payload: r#"{"account_id":"A"}"#.into(),
        metadata: skilj_core::shared::Metadata {
            r#type: "FundsWithdrawn".into(),
            version: 1,
            client_id: "trigger-adapter".into(),
            created_at: timestamp(1),
        },
        sequence: 1,
        tags: first.command.consistency_tags.clone(),
        encryption_keys: Vec::new(),
        origin: EventOrigin::CommandTriggered {
            command: Box::new(first.command.clone()),
        },
    };

    let (boundary, matching) = event_store::consistency_boundary_and_matching_events(
        std::slice::from_ref(&prior_event),
        &event_store::derive_tags(&ct.tag_mappings, r#"{"account_id":"A"}"#),
    );

    assert_eq!(boundary, Some(1));
    assert_eq!(matching.len(), 1);
    assert_eq!(matching[0].payload, r#"{"account_id":"A"}"#);

    // A different account's tag doesn't match at all.
    let (boundary_other, matching_other) = event_store::consistency_boundary_and_matching_events(
        std::slice::from_ref(&prior_event),
        &event_store::derive_tags(&ct.tag_mappings, r#"{"account_id":"B"}"#),
    );
    assert_eq!(boundary_other, None);
    assert!(matching_other.is_empty());
}

// ---------------------------------------------------------------------
// surface-actor/surface-provides.{CommandTrigger,CommandSubmission} -
// uncovered
// ---------------------------------------------------------------------
//
// CommandTrigger (2): same REST-scaffolding gap as every prior surface's
// deferred obligations (no bearer extractor, no route table in skilj-rest
// yet).
//
// CommandSubmission (2): the GraphQL counterpart - no resolver/schema
// wiring in skilj-graphql yet, same gap access_management.rs's own
// AccessManagement surface has.
//
// SequenceIsGaplessPerBoundedContext (1) - a property of the real,
// Postgres-lock-backed next_sequence (see the note above the rules in
// the spec: "enforced at the Postgres level... a single row per bounded
// context is locked with SELECT ... FOR UPDATE"), not testable against
// this pure engine, which takes next_sequence's output as a given.
//
// ConsistencyTagKeysAreDeclared (1) - per the invariant's own text, this
// is "held within one reconciliation pass by registering EventTypes
// before CommandTypes... not by any check in ProcessCommand itself" -
// RegisterEventType/RegisterCommandType's obligation, not this one's.
