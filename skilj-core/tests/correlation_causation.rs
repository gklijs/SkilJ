//! Tests for Codeberg issue #18 (correlation/causation ids on commands
//! and events) - `process_command`'s own generate-if-absent/inherit
//! behaviour, `create_external_event`/`create_direct_event`'s identical
//! generate-if-absent behaviour, and `valid_correlation_id`'s length
//! guard. Fixtures mirror `command_processing.rs`/`event_creation_surfaces.rs`
//! rather than sharing them - each test file in this suite builds its
//! own minimal fixtures, the established pattern here.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{DirectCreationToken, ExternalEventToken, TokenStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, CommandType, EventOrigin, EventType,
};
use skilj_core::shared::{CommandDecision, EventSpec};

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn bounded_context() -> BoundedContext {
    BoundedContext {
        name: "accounts".into(),
        status: BoundedContextStatus::Active,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
        template: None,
    }
}

fn command_type() -> CommandType {
    CommandType {
        bounded_context: bounded_context(),
        name: "WithdrawFunds".into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        rest_trigger_allowed: true,
    }
}

fn event_type(external_creation_allowed: bool, direct_creation_allowed: bool) -> EventType {
    EventType {
        bounded_context: bounded_context(),
        name: "FundsWithdrawn".into(),
        schema: "{}".into(),
        schema_version: 1,
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

fn no_sensitive_fields(
    _: &str,
    _: &str,
) -> (event_store::EncryptionKey, skilj_core::encryption::DataKey) {
    unreachable!("no sensitive fields in any fixture this file uses")
}

// ---------------------------------------------------------------------
// rule ProcessCommand - correlation_id generation/inheritance,
// causation_id passthrough/stamping
// ---------------------------------------------------------------------

#[test]
fn process_command_generates_a_correlation_id_when_the_caller_supplies_none() {
    let ct = command_type();
    let result = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        "{}",
        "trigger-adapter",
        None,
        None,
        &[],
        CommandDecision::Accepted { events: Vec::new() },
        |_| None,
        || 0,
        timestamp(0),
        no_sensitive_fields,
    )
    .unwrap();

    assert!(result.command.metadata.correlation_id.is_some());
    assert!(!result.command.metadata.correlation_id.unwrap().is_empty());
    assert_eq!(result.command.metadata.causation_id, None);
}

#[test]
fn process_command_preserves_a_caller_supplied_correlation_id_verbatim() {
    let ct = command_type();
    let result = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        "{}",
        "trigger-adapter",
        Some("caller-chosen-corr-1"),
        Some("upstream-cause-1"),
        &[],
        CommandDecision::Accepted { events: Vec::new() },
        |_| None,
        || 0,
        timestamp(0),
        no_sensitive_fields,
    )
    .unwrap();

    assert_eq!(
        result.command.metadata.correlation_id,
        Some("caller-chosen-corr-1".to_string())
    );
    assert_eq!(
        result.command.metadata.causation_id,
        Some("upstream-cause-1".to_string())
    );
}

#[test]
fn every_command_triggered_event_inherits_the_commands_correlation_id_and_is_caused_by_it() {
    let ct = command_type();
    let et = event_type(false, false);
    let mut next_seq = 10i64;

    let result = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        "{}",
        "trigger-adapter",
        Some("corr-shared-across-the-chain"),
        None,
        &[],
        CommandDecision::Accepted {
            events: vec![
                EventSpec {
                    event_type: "FundsWithdrawn".into(),
                    payload: serde_json::json!({}),
                },
                EventSpec {
                    event_type: "FundsWithdrawn".into(),
                    payload: serde_json::json!({}),
                },
            ],
        },
        |name| (name == "FundsWithdrawn").then(|| et.clone()),
        || {
            let seq = next_seq;
            next_seq += 1;
            seq
        },
        timestamp(0),
        no_sensitive_fields,
    )
    .unwrap();

    assert_eq!(result.events.len(), 2);
    for event in &result.events {
        // Every triggered event inherits the command's own
        // correlation_id verbatim - the whole chain shares one value.
        assert_eq!(
            event.metadata.correlation_id,
            Some("corr-shared-across-the-chain".to_string())
        );
        // ... and is directly caused by that command, regardless of
        // what (if anything) caused the command itself.
        assert_eq!(event.metadata.causation_id, Some(result.command.id.clone()));
        match &event.origin {
            EventOrigin::CommandTriggered { command } => {
                assert_eq!(command.id, result.command.id);
            }
            other => panic!("expected CommandTriggered, got {other:?}"),
        }
    }
}

#[test]
fn process_command_rejects_an_over_length_correlation_id() {
    let ct = command_type();
    let too_long: String = "x".repeat(201);
    let err = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        "{}",
        "trigger-adapter",
        Some(&too_long),
        None,
        &[],
        CommandDecision::Accepted { events: Vec::new() },
        |_| None,
        || 0,
        timestamp(0),
        no_sensitive_fields,
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::CorrelationIdTooLong.code());
}

#[test]
fn process_command_rejects_an_over_length_causation_id() {
    let ct = command_type();
    let too_long: String = "x".repeat(201);
    let err = event_store::process_command(
        skilj_core::shared::generate_token_id(),
        &ct,
        "{}",
        "trigger-adapter",
        None,
        Some(&too_long),
        &[],
        CommandDecision::Accepted { events: Vec::new() },
        |_| None,
        || 0,
        timestamp(0),
        no_sensitive_fields,
    )
    .unwrap_err();

    assert_eq!(err.code(), event_store::Error::CorrelationIdTooLong.code());
}

// ---------------------------------------------------------------------
// valid_correlation_id - the shared black box both fields pass through
// ---------------------------------------------------------------------

#[test]
fn valid_correlation_id_accepts_absent_and_empty() {
    assert!(event_store::valid_correlation_id(None));
    assert!(event_store::valid_correlation_id(Some("")));
}

#[test]
fn valid_correlation_id_accepts_exactly_the_length_cap() {
    let exactly_200 = "x".repeat(200);
    assert!(event_store::valid_correlation_id(Some(&exactly_200)));
}

#[test]
fn valid_correlation_id_rejects_one_character_over_the_cap() {
    let over_by_one = "x".repeat(201);
    assert!(!event_store::valid_correlation_id(Some(&over_by_one)));
}

// ---------------------------------------------------------------------
// rule CreateExternalEvent / rule CreateDirectEvent - root-of-chain
// generation, causation_id passthrough
// ---------------------------------------------------------------------

fn external_token(event_type: EventType) -> ExternalEventToken {
    ExternalEventToken {
        id: "adapter-1".into(),
        secret: "s3cr3t".into(),
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
        event_type,
        scope: None,
    }
}

fn direct_token(event_type: EventType) -> DirectCreationToken {
    DirectCreationToken {
        id: "adapter-2".into(),
        secret: "s3cr3t".into(),
        status: TokenStatus::Active,
        created_at: timestamp(0),
        revoked_at: None,
        event_type,
        scope: None,
    }
}

#[test]
fn create_external_event_generates_a_correlation_id_when_the_caller_supplies_none() {
    let adapter = external_token(event_type(true, false));
    let event = event_store::create_external_event(
        &adapter,
        "{}".into(),
        "raw".into(),
        None,
        None,
        None,
        0,
        timestamp(0),
        no_sensitive_fields,
    )
    .unwrap();

    assert!(event.metadata.correlation_id.is_some());
    assert_eq!(event.metadata.causation_id, None);
}

#[test]
fn create_external_event_preserves_a_caller_supplied_pair_verbatim() {
    let adapter = external_token(event_type(true, false));
    let event = event_store::create_external_event(
        &adapter,
        "{}".into(),
        "raw".into(),
        None,
        Some("bridge-forwarded-corr".into()),
        Some("upstream-broker-msg-1".into()),
        0,
        timestamp(0),
        no_sensitive_fields,
    )
    .unwrap();

    assert_eq!(
        event.metadata.correlation_id,
        Some("bridge-forwarded-corr".to_string())
    );
    assert_eq!(
        event.metadata.causation_id,
        Some("upstream-broker-msg-1".to_string())
    );
}

#[test]
fn create_direct_event_generates_a_correlation_id_when_the_caller_supplies_none() {
    let adapter = direct_token(event_type(false, true));
    let event = event_store::create_direct_event(
        &adapter,
        "{}".into(),
        None,
        None,
        0,
        timestamp(0),
        no_sensitive_fields,
    )
    .unwrap();

    assert!(event.metadata.correlation_id.is_some());
    assert_eq!(event.metadata.causation_id, None);
}

// ---------------------------------------------------------------------
// event_causation_id - the composed string a causing Event is named by
// ---------------------------------------------------------------------

#[test]
fn event_causation_id_composes_bounded_context_and_sequence() {
    let adapter = direct_token(event_type(false, true));
    let event = event_store::create_direct_event(
        &adapter,
        "{}".into(),
        None,
        None,
        42,
        timestamp(0),
        no_sensitive_fields,
    )
    .unwrap();

    assert_eq!(event_store::event_causation_id(&event), "accounts:42");
}
