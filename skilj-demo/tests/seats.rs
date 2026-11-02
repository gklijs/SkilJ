//! A bounded context generated from `tests/seats.skilj.toml` (`build.rs`) with
//! retractions and doubts (docs/architecture.md §235, §236): what's
//! hand-written is the `decide_*` functions, which get `Retractable` events
//! from the generated `retractable` commands. The rest is proof that the
//! generated built-ins and queries compile and work against a real Postgres.

use skilj::retraction::{
    corrected, doubt, open_doubts, standing, Retractable, RetractionGround, ValueClaims,
    ValueEffect,
};
use skilj::{KeyService, Skilj};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool, SubmitCommandOutcome};
use skilj_core::event_cache::EventCache;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, EventBroadcaster};
use skilj_core::shared::{generate_token_id, CommandDecision, EventSpec};

include!(concat!(env!("OUT_DIR"), "/seats_generated.rs"));

fn claims(events: &[Retractable<SeatsEvent>]) -> ValueClaims<'_, SeatsEvent> {
    ValueClaims::fold(events, "seat", |event| match event {
        SeatsEvent::SeatClaimed(_) => ValueEffect::Claims,
    })
}

fn rejected(kind: &str) -> CommandDecision {
    CommandDecision::Rejected {
        reason: kind.replace('_', " "),
        kind: kind.into(),
    }
}

fn claim(seat: &str, holder: &str) -> EventSpec {
    EventSpec {
        event_type: "SeatClaimed".into(),
        payload: serde_json::json!({ "seat": seat, "holder": holder }),
    }
}

fn decide_claim_seat(
    payload: &ClaimSeatPayload,
    events: &[Retractable<SeatsEvent>],
) -> CommandDecision {
    if !claims(events).held().is_empty() {
        return rejected("seat_taken");
    }
    CommandDecision::Accepted {
        events: vec![claim(&payload.seat, &payload.holder)],
    }
}

fn decide_correct_seat(
    payload: &CorrectSeatPayload,
    events: &[Retractable<SeatsEvent>],
) -> CommandDecision {
    let claims = claims(events);
    if claims.held_by_another("holder", &payload.holder).is_some() {
        return rejected("seat_taken");
    }
    let Some(current) = claims.held_by("holder", &payload.holder) else {
        return rejected("nothing_to_correct");
    };
    corrected(
        current.position,
        RetractionGround::WrongDataSubmitted,
        "wrong seat",
        [claim(&payload.seat, &payload.holder)],
    )
}

fn decide_question_seat(
    _payload: &QuestionSeatPayload,
    events: &[Retractable<SeatsEvent>],
) -> CommandDecision {
    let open = open_doubts(events);
    let unquestioned = standing(events)
        .into_iter()
        .find(|claim| !open.contains_key(&claim.position));
    match unquestioned {
        Some(claim) => CommandDecision::Accepted {
            events: vec![doubt(claim.position, "seen in another seat")],
        },
        None => rejected("nothing_to_question"),
    }
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

fn now() -> chrono::DateTime<chrono::Utc> {
    use chrono::SubsecRound;
    chrono::Utc::now().trunc_subsecs(6)
}

fn master_key() -> KeyService {
    KeyService::new(skilj_core::encryption::InMemoryKeyProvider::from_bytes(
        [8; 32],
    ))
}

struct Seats {
    skilj: Skilj,
    pool: Pool,
    cache: EventCache,
}

impl Seats {
    async fn submit(&self, name: &str, payload: serde_json::Value) -> String {
        let command_type = db::get_command_type(&self.pool, BOUNDED_CONTEXT, name)
            .await
            .unwrap()
            .unwrap();
        let outcome = db::decide_and_submit_command(
            &self.pool,
            &*self.skilj.command_dispatcher(),
            &*self.skilj.projection_dispatcher(),
            &*self.skilj.snapshot_dispatcher(),
            &EventBroadcaster::new(16),
            &self.cache,
            &command_type,
            &payload.to_string(),
            "client",
            None,
            None,
            Some(&master_key()),
            now(),
            None,
            None,
        )
        .await
        .unwrap();
        match outcome {
            SubmitCommandOutcome::Accepted { .. } => "accepted".into(),
            SubmitCommandOutcome::Rejected { kind, .. } => kind,
            other => panic!("unexpected {other:?}"),
        }
    }
}

async fn seats() -> Option<Seats> {
    let database_url = skilj_test_support::database_url("skilj_demo_seats_test").await?;
    let pool = db::connect(&database_url).await.unwrap();
    db::migrate(&pool).await.unwrap();
    let bounded_context = BoundedContext {
        name: BOUNDED_CONTEXT.to_string(),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(&pool, &bounded_context)
        .await
        .unwrap();
    let role = Role {
        id: generate_token_id(),
        external_subject: format!("subject_{}", generate_token_id()),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    };
    db::insert_role(&pool, &role).await.unwrap();
    db::insert_role_access_mapping(
        &pool,
        &RoleAccessMapping {
            role: role.clone(),
            bounded_context,
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            capabilities: Default::default(),
            delegable: Default::default(),
            status: RoleStatus::Active,
            created_at: now(),
            revoked_at: None,
        },
    )
    .await
    .unwrap();
    // `auto_register` picks up the generated types (and the library's
    // banking/courses ones, whose bounded contexts this role can't reach,
    // so reconciliation skips them).
    let (skilj, _) = Skilj::builder(database_url)
        .auto_register()
        .key_service(master_key())
        .reconciliation_role(role.external_subject)
        .build()
        .await
        .unwrap();
    Some(Seats {
        skilj,
        pool,
        cache: EventCache::new(1000),
    })
}

#[test]
fn a_generated_context_corrects_and_doubts() {
    runtime().block_on(async {
        let Some(seats) = seats().await else {
            return;
        };
        let holding =
            |seat: &str, holder: &str| serde_json::json!({ "seat": seat, "holder": holder });
        assert_eq!(
            seats.submit("ClaimSeat", holding("1a", "ann")).await,
            "accepted"
        );
        assert_eq!(
            seats.submit("ClaimSeat", holding("1a", "bob")).await,
            "seat_taken"
        );
        assert_eq!(
            seats.submit("ClaimSeat", holding("2b", "bob")).await,
            "accepted"
        );

        // Ann moves, in one append, so the old seat is free.
        assert_eq!(
            seats.submit("CorrectSeat", holding("2b", "ann")).await,
            "seat_taken"
        );
        assert_eq!(
            seats.submit("CorrectSeat", holding("3c", "ann")).await,
            "accepted"
        );
        assert_eq!(
            seats.submit("ClaimSeat", holding("1a", "cas")).await,
            "accepted"
        );
        assert_eq!(
            seats.submit("ClaimSeat", holding("3c", "dan")).await,
            "seat_taken"
        );

        // A doubt is read back by the same command, through the generated query.
        let question = serde_json::json!({ "holder": "bob" });
        assert_eq!(
            seats.submit("QuestionSeat", question.clone()).await,
            "accepted"
        );
        assert_eq!(
            seats.submit("QuestionSeat", question).await,
            "nothing_to_question"
        );
        assert_eq!(
            seats.submit("ClaimSeat", holding("2b", "eve")).await,
            "seat_taken"
        );
        seats
            .skilj
            .shutdown(std::time::Duration::from_secs(10))
            .await;
    });
}
