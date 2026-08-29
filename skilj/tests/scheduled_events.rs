//! End-to-end tests for the background scheduler `SkiljBuilder::build()`
//! spawns backing `rule CreateSystemEvent`/`rule SkipMissedOccurrences` -
//! the drift audit's #7 finding (see project memory
//! `skilj-drift-audit-2026-08-18`). Real HTTP is beside the point here
//! (nothing triggers a scheduled event from the outside - that's the
//! whole reason `SystemTriggered` needed a spec rule of its own in the
//! first place): these tests just build a real `Skilj`, let its spawned
//! task run against a real Postgres database on its own clock, and
//! observe the result through `db::` queries. Proof the task is real and
//! running, and that `EventType::scheduled_payload()`/`db::
//! fire_system_event`/`db::skip_missed_occurrences_for_event_type` are
//! correctly wired end to end - `skilj-core/tests/scheduled_events.rs`
//! already covers the pure `create_system_event`/`skip_missed_occurrences`
//! logic itself in isolation, not repeated here. Same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `skilj/tests/async_projections.rs` - see its own doc comment for the
//! details, not repeated a third time here.

use chrono::{SubsecRound, Utc};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{EventType, Skilj};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{
    BoundedContext, BoundedContextStatus, EventOrigin, MissedOccurrencePolicy,
};
use skilj_core::shared::generate_token_id;

// --- fixtures ---

/// Fires every second on the clock - see `skilj-core/tests/scheduled_events.rs`'s
/// own `every_second()` for why this, not a step range, is this crate's
/// own dialect's way of saying it.
const EVERY_SECOND: &str = "* * * * * * *";

#[derive(Debug, Serialize, Deserialize, JsonSchema, PartialEq)]
struct HeartBeatPayload {
    beat: String,
}

struct HeartBeat;

impl EventType for HeartBeat {
    type Payload = HeartBeatPayload;
    const NAME: &'static str = "HeartBeat";
    fn system_triggered_allowed() -> bool {
        true
    }
    fn system_triggered_schedule() -> Option<String> {
        Some(EVERY_SECOND.to_string())
    }
    fn missed_occurrence_policy() -> Option<MissedOccurrencePolicy> {
        Some(MissedOccurrencePolicy::ReplayBacklog)
    }
    fn scheduled_payload() -> Self::Payload {
        HeartBeatPayload {
            beat: "ping".to_string(),
        }
    }
}

struct OnceBeat;

impl EventType for OnceBeat {
    type Payload = HeartBeatPayload;
    const NAME: &'static str = "OnceBeat";
    fn system_triggered_allowed() -> bool {
        true
    }
    fn system_triggered_schedule() -> Option<String> {
        Some(EVERY_SECOND.to_string())
    }
    fn missed_occurrence_policy() -> Option<MissedOccurrencePolicy> {
        Some(MissedOccurrencePolicy::FireOnce)
    }
    fn scheduled_payload() -> Self::Payload {
        HeartBeatPayload {
            beat: "ping".to_string(),
        }
    }
}

struct SkippyBeat;

impl EventType for SkippyBeat {
    type Payload = HeartBeatPayload;
    const NAME: &'static str = "SkippyBeat";
    fn system_triggered_allowed() -> bool {
        true
    }
    fn system_triggered_schedule() -> Option<String> {
        Some(EVERY_SECOND.to_string())
    }
    fn missed_occurrence_policy() -> Option<MissedOccurrencePolicy> {
        Some(MissedOccurrencePolicy::Skip)
    }
    fn scheduled_payload() -> Self::Payload {
        HeartBeatPayload {
            beat: "ping".to_string(),
        }
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    database_url: String,
    pool: Pool,
    _embedded: Option<postgresql_embedded::PostgreSQL>,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for scheduled_events tests")
    })
}

async fn test_db() -> Option<(String, Pool)> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| (db.database_url.clone(), db.pool.clone()))
}

async fn connect_and_migrate(database_url: &str, label: &str) -> Option<Pool> {
    let pool = match db::connect(database_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to {label} failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating {label} failed: {e}");
        return None;
    }
    Some(pool)
}

async fn provision() -> Option<TestDb> {
    let database_url = if let Ok(database_url) = std::env::var("DATABASE_URL") {
        database_url
    } else {
        let mut server = postgresql_embedded::PostgreSQL::default();
        if let Err(e) = server.setup().await {
            eprintln!(
                "skipping: DATABASE_URL not set and embedded PostgreSQL setup failed \
                 (no network egress to fetch the binary, or a missing system library \
                 like libxml2 it links against): {e}"
            );
            return None;
        }
        if let Err(e) = server.start().await {
            eprintln!("skipping: embedded PostgreSQL failed to start: {e}");
            return None;
        }
        let database_name = "skilj_scheduled_events_e2e_test";
        if let Err(e) = server.create_database(database_name).await {
            eprintln!("skipping: embedded PostgreSQL create_database failed: {e}");
            return None;
        }
        let url = server.settings().url(database_name);
        let pool = connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb {
            database_url: url,
            pool,
            _embedded: Some(server),
        });
    };

    let pool = connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb {
        database_url,
        pool,
        _embedded: None,
    })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

/// Sets up a fresh `Role`/`BoundedContext`/admin `RoleAccessMapping`,
/// returning everything a caller needs to build a `Skilj` reconciling
/// against it - the shared prefix `async_projections.rs`'s own single
/// test inlines, factored out here since both tests below need it.
async fn setup(pool: &Pool) -> (String, String) {
    let external_subject = unique_name("subject");
    let role = Role {
        id: generate_token_id(),
        external_subject: external_subject.clone(),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role(pool, &role).await.unwrap();

    let bc_name = unique_name("scheduling");
    let bc = BoundedContext {
        name: bc_name.clone(),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();

    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc.clone(),
        level: AccessLevel::Admin,
        can_read_sensitive: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role_access_mapping(pool, &mapping)
        .await
        .unwrap();

    (bc_name, external_subject)
}

#[test]
fn a_scheduled_event_type_fires_repeatedly_through_the_real_background_scheduler() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc_name, external_subject) = setup(&pool).await;

        let (_skilj, report) = Skilj::builder(database_url)
            .bounded_context(bc_name.clone())
            .event_type::<HeartBeat>()
            .reconciliation_role(external_subject)
            .scheduler_poll_interval(std::time::Duration::from_millis(150))
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());
        assert_eq!(report.registered, vec![format!("{bc_name}/HeartBeat")]);

        // The `* * * * * * *` schedule fires once a second; give the
        // spawned task several ticks' worth of margin to have caught at
        // least two occurrences, rather than relying on exactly one.
        let mut events = Vec::new();
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            events = db::list_events_for_bounded_context(&pool, &bc_name)
                .await
                .unwrap();
            if events.len() >= 2 {
                break;
            }
        }
        assert!(
            events.len() >= 2,
            "expected at least 2 HeartBeat occurrences to have fired, got {}",
            events.len()
        );
        for event in &events {
            assert_eq!(event.event_type.name, "HeartBeat");
            assert!(matches!(event.origin, EventOrigin::SystemTriggered));
            let payload: HeartBeatPayload = serde_json::from_str(&event.payload).unwrap();
            assert_eq!(
                payload,
                HeartBeatPayload {
                    beat: "ping".to_string()
                }
            );
        }
        // Sequence numbers are gapless and strictly increasing - every
        // firing genuinely went through `next_sequence`, not a
        // duplicate-published-but-not-persisted event.
        let mut sequences: Vec<_> = events.iter().map(|e| e.sequence).collect();
        sequences.sort_unstable();
        for pair in sequences.windows(2) {
            assert_eq!(pair[1], pair[0] + 1);
        }

        let event_type = db::get_event_type(&pool, &bc_name, "HeartBeat")
            .await
            .unwrap()
            .unwrap();
        let last_fired_at = event_type.last_fired_at.expect("must have fired by now");
        let schedule_position = event_type
            .schedule_position
            .expect("must have advanced by now");
        assert_eq!(last_fired_at, schedule_position);
    });
}

#[test]
fn skip_policy_resolves_a_real_backlog_without_replaying_it() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc_name, external_subject) = setup(&pool).await;

        let (_skilj, report) = Skilj::builder(database_url)
            .bounded_context(bc_name.clone())
            .event_type::<SkippyBeat>()
            .reconciliation_role(external_subject)
            .scheduler_poll_interval(std::time::Duration::from_millis(150))
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        // Simulate a real outage: force `schedule_position` back to 10s
        // ago, well past several of this schedule's own occurrences - a
        // genuine backlog no in-process action ever produced, the same
        // "the whole cluster was down" scenario `rule
        // SkipMissedOccurrences` exists for.
        let mut event_type = db::get_event_type(&pool, &bc_name, "SkippyBeat")
            .await
            .unwrap()
            .unwrap();
        let backlog_start = Utc::now() - chrono::Duration::seconds(10);
        event_type.schedule_position = Some(backlog_start);
        db::upsert_event_type(&pool, &event_type).await.unwrap();

        // Give the scheduler several ticks to notice the backlog and
        // resolve it via SkipMissedOccurrences, then a little more to
        // resume ordinary firing afterward.
        let mut caught_up = None;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let current = db::get_event_type(&pool, &bc_name, "SkippyBeat")
                .await
                .unwrap()
                .unwrap();
            if let Some(position) = current.schedule_position {
                if position > backlog_start + chrono::Duration::seconds(5) {
                    caught_up = Some(position);
                    break;
                }
            }
        }
        assert!(
            caught_up.is_some(),
            "schedule_position never advanced past the simulated backlog"
        );

        // The whole point of skip: the 10-occurrence-wide backlog was
        // discarded, not replayed - only however many *legitimately*
        // came due afterward (a handful at most, given the poll interval
        // and margin above) ever produced an event.
        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert!(
            events.len() <= 5,
            "skip policy must not replay a backlog: got {} events for a ~10s gap",
            events.len()
        );
    });
}

/// Real end-to-end proof of the 2026-08-20 drift audit's #1 fix (`fire_once`
/// permanently stalling after any backlog - see project memory
/// `skilj-drift-audit-2026-08-20`): before the fix, `scheduler_tick` only
/// ever raised the single *earliest* unaccounted occurrence per tick, so
/// once a `fire_once` type fell more than one occurrence behind,
/// `create_system_event`'s own `nothing_later_is_due` guard correctly
/// rejected that same earliest occurrence forever - `schedule_position`
/// never advanced, and the type never fired again for the rest of the
/// process's lifetime. This simulates exactly that: a real, multi-second
/// backlog, then proves the scheduler both fires (once) and, just as
/// importantly, keeps advancing afterward rather than getting stuck.
#[test]
fn fire_once_resolves_a_real_backlog_into_its_own_last_occurrence_and_does_not_stall() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_db().await else {
            return;
        };
        let (bc_name, external_subject) = setup(&pool).await;

        let (_skilj, report) = Skilj::builder(database_url)
            .bounded_context(bc_name.clone())
            .event_type::<OnceBeat>()
            .reconciliation_role(external_subject)
            .scheduler_poll_interval(std::time::Duration::from_millis(150))
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());

        // Simulate a real outage: force `schedule_position` back to 10s
        // ago, well past several of this schedule's own occurrences - the
        // same backlog-simulation `skip_policy_resolves_a_real_backlog_without_replaying_it`
        // above uses, but under `fire_once` this time.
        let mut event_type = db::get_event_type(&pool, &bc_name, "OnceBeat")
            .await
            .unwrap()
            .unwrap();
        let backlog_start = Utc::now() - chrono::Duration::seconds(10);
        event_type.schedule_position = Some(backlog_start);
        db::upsert_event_type(&pool, &event_type).await.unwrap();

        // Give the scheduler several ticks to resolve the backlog - if
        // the bug this test guards against ever regresses, this loop
        // simply times out with `schedule_position` still stuck at
        // `backlog_start`, never advancing at all.
        let mut caught_up = None;
        for _ in 0..40 {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let current = db::get_event_type(&pool, &bc_name, "OnceBeat")
                .await
                .unwrap()
                .unwrap();
            if let Some(position) = current.schedule_position {
                if position > backlog_start + chrono::Duration::seconds(5) {
                    caught_up = Some(position);
                    break;
                }
            }
        }
        assert!(
            caught_up.is_some(),
            "schedule_position never advanced past the simulated backlog - fire_once stalled"
        );

        // The whole point of fire_once: exactly one event for the entire
        // ~10-occurrence-wide backlog (its own last occurrence), not zero
        // (the stall bug) and not the whole backlog (that's replay_backlog's
        // own job) - a handful more may have legitimately come due since,
        // given the poll interval and margin above, but the backlog itself
        // collapses to one.
        let events = db::list_events_for_bounded_context(&pool, &bc_name)
            .await
            .unwrap();
        assert!(
            !events.is_empty(),
            "fire_once must produce at least the backlog's own collapsed event - got none"
        );
        assert!(
            events.len() <= 5,
            "fire_once must not replay a backlog: got {} events for a ~10s gap",
            events.len()
        );

        // And, critically, it must not be stuck: waiting for one more
        // legitimate occurrence past the point already reached proves the
        // scheduler is still making real, ongoing progress, not just that
        // the one backlog jump above happened to succeed once.
        let after_catch_up = caught_up.unwrap();
        let mut advanced_again = false;
        for _ in 0..20 {
            tokio::time::sleep(std::time::Duration::from_millis(150)).await;
            let current = db::get_event_type(&pool, &bc_name, "OnceBeat")
                .await
                .unwrap()
                .unwrap();
            if current.schedule_position.unwrap() > after_catch_up {
                advanced_again = true;
                break;
            }
        }
        assert!(
            advanced_again,
            "fire_once stalled again after resolving the initial backlog"
        );
    });
}
