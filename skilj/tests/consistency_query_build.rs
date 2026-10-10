//! What `build()` refuses in a command type's `consistency_query()`
//! (docs/architecture.md §198). Checked before `build()` connects, so no
//! database is needed: the URL below is never dialled.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{CommandType, EventType, Skilj, Snapshot};
use skilj_core::event_store::Event;
use skilj_core::plugin::BoundedContextEvent;
use skilj_core::shared::{CommandDecision, QueryItemMapping, TagMapping};

#[derive(Debug, Default, Serialize, Deserialize, JsonSchema)]
struct Payload {
    company_id: String,
    ticket_id: String,
}

fn company() -> TagMapping {
    TagMapping {
        key: "company".into(),
        field: "company_id".into(),
    }
}

fn ticket() -> TagMapping {
    TagMapping {
        key: "ticket".into(),
        field: "ticket_id".into(),
    }
}

struct TicketCreated;

impl EventType for TicketCreated {
    type Payload = Payload;
    const NAME: &'static str = "TicketCreated";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![company(), ticket()]
    }
}

struct HelpdeskEvent;

impl BoundedContextEvent for HelpdeskEvent {
    fn try_from_event(_event: &Event) -> Option<Result<Self, serde_json::Error>> {
        None
    }
}

/// `CreateTicket` with the query `QUERY` selects.
struct CreateTicket<const QUERY: u8>;

impl<const QUERY: u8> CommandType for CreateTicket<QUERY> {
    type Payload = Payload;
    type Event = HelpdeskEvent;
    const NAME: &'static str = "CreateTicket";
    fn tag_mappings() -> Vec<TagMapping> {
        vec![company()]
    }
    fn consistency_query() -> Vec<QueryItemMapping> {
        match QUERY {
            0 => vec![QueryItemMapping::types(&["CompanySignedUp"])
                .tagged(vec![company()])
                .latest()],
            1 => vec![QueryItemMapping::types(&["TicketCreated"]).tagged(vec![ticket()])],
            2 => vec![QueryItemMapping::tags(Vec::new())],
            4 => vec![QueryItemMapping::types(&["TicketCreated"]).last(0)],
            _ => vec![QueryItemMapping::types(&["TicketCreated"]).tagged(vec![company()])],
        }
    }
    fn snapshot() -> Option<&'static str> {
        (QUERY == 3).then_some("CompanySnapshot")
    }
    fn decide(_payload: &Self::Payload, _matching_events: &[Self::Event]) -> CommandDecision {
        CommandDecision::Accepted { events: vec![] }
    }
}

struct CompanySnapshot;

impl Snapshot for CompanySnapshot {
    type State = Vec<String>;
    type Event = HelpdeskEvent;
    const NAME: &'static str = "CompanySnapshot";
    const TAG_KEY: &'static str = "company";
    const VERSION: u64 = 1;
    fn fold(_state: &mut Self::State, _event: &Self::Event) {}
}

fn refusal<const QUERY: u8>() -> String {
    tokio::runtime::Runtime::new()
        .unwrap()
        .block_on(
            Skilj::builder("postgres://never:dialled@127.0.0.1:1/none")
                .bounded_context("helpdesk")
                .event_type::<TicketCreated>()
                .command_type::<CreateTicket<QUERY>>()
                .snapshot::<CompanySnapshot>()
                .build(),
        )
        .err()
        .expect("the query is refused")
        .to_string()
}

#[test]
fn an_unregistered_event_type_is_refused() {
    let err = refusal::<0>();
    assert!(err.contains("names event type CompanySignedUp"), "{err}");
}

#[test]
fn a_tag_the_command_type_does_not_map_is_refused() {
    let err = refusal::<1>();
    assert!(
        err.contains("tag ticket (field ticket_id) isn't one of its tag_mappings()"),
        "{err}"
    );
}

#[test]
fn an_item_matching_everything_is_refused() {
    let err = refusal::<2>();
    assert!(err.contains("names neither event types nor tags"), "{err}");
}

#[test]
fn a_query_with_a_snapshot_is_refused() {
    let err = refusal::<3>();
    assert!(
        err.contains("both consistency_query() and snapshot()"),
        "{err}"
    );
}

#[test]
fn keeping_the_last_zero_matches_is_refused() {
    let err = refusal::<4>();
    assert!(err.contains("keeps the last 0 matching events"), "{err}");
}
