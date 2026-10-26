//! The access trail (docs/architecture.md §229): every change to who holds
//! what is also an event in the admin bounded context, written in the same
//! transaction, and `fold_access_events` answers what a grant held at a
//! past instant from those events. The fold is pure; the writers and the
//! baseline need Postgres (see `persistence.rs`'s module comment for how
//! the database is found).

use chrono::{Duration, SubsecRound, TimeZone, Utc};
use skilj_core::access_control::{
    AccessLevel, PrivateFieldGrant, Role, RoleAccessMapping, RoleStatus, TokenStatus,
};
use skilj_core::access_trail::{fold_access_events, AccessEventKind, AccessEventRecord};
use skilj_core::bootstrap::{ContextCreator, ADMIN_BOUNDED_CONTEXT_NAME};
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus, EventOrigin};
use skilj_core::shared::generate_token_id;
use std::collections::BTreeSet;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().expect("a tokio runtime"))
}

static TEST_DB: tokio::sync::OnceCell<Option<Pool>> = tokio::sync::OnceCell::const_new();

/// `baseline_access_events` covers every raw grant in the database, so a
/// test that makes one and then looks at its trail cannot share the
/// database with another test's baseline. Those tests take this first.
static BASELINES: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn test_pool() -> Option<Pool> {
    TEST_DB.get_or_init(provision).await.clone()
}

async fn provision() -> Option<Pool> {
    let url = skilj_test_support::database_url("skilj_access_trail").await?;
    let pool = match db::connect(&url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to the test database failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating the test database failed: {e}");
        return None;
    }
    // From here a failure is the code under test failing, not a missing
    // service, so it fails the run instead of skipping it.
    let existing = db::get_bounded_context(&pool, ADMIN_BOUNDED_CONTEXT_NAME)
        .await
        .expect("reading the admin bounded context");
    if let Some(admin) =
        skilj_core::bootstrap::stamp_admin_bounded_context(existing.as_ref(), Utc::now())
    {
        db::insert_bounded_context(&pool, &admin)
            .await
            .expect("creating the admin bounded context");
    }
    db::ensure_access_trail(&pool)
        .await
        .expect("ensuring the access trail");
    Some(pool)
}

fn now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

async fn seed_role(pool: &Pool) -> Role {
    let role = Role {
        id: generate_token_id(),
        external_subject: format!("subject_{}", generate_token_id()),
        name: "Trail Role".into(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    };
    db::insert_role(pool, &role).await.unwrap();
    role
}

async fn seed_bounded_context(pool: &Pool) -> BoundedContext {
    let bc = BoundedContext {
        name: format!("bc_{}", generate_token_id()),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(pool, &bc).await.unwrap();
    bc
}

fn mapping(role: &Role, bc: &BoundedContext, level: AccessLevel) -> RoleAccessMapping {
    RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc.clone(),
        level,
        can_read_sensitive: false,
        scope: Some("tenant-a".into()),
        capabilities: Default::default(),
        delegable: Default::default(),
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    }
}

async fn trail(pool: &Pool, role: &Role, bc: &BoundedContext) -> Vec<AccessEventRecord> {
    db::list_access_event_records(pool, &role.id, &bc.name)
        .await
        .unwrap()
}

fn kinds(records: &[AccessEventRecord]) -> Vec<AccessEventKind> {
    records.iter().map(|record| record.kind).collect()
}

// --- the fold (pure) ---

fn record(kind: AccessEventKind, at: i64, payload: serde_json::Value) -> AccessEventRecord {
    AccessEventRecord {
        kind,
        payload,
        created_at: Utc.timestamp_opt(at, 0).unwrap(),
    }
}

fn at(seconds: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(seconds, 0).unwrap()
}

fn history() -> Vec<AccessEventRecord> {
    use serde_json::json;
    vec![
        record(
            AccessEventKind::AccessGranted,
            10,
            json!({"level": "write", "can_read_sensitive": false, "scope": "t"}),
        ),
        record(
            AccessEventKind::CapabilityGranted,
            20,
            json!({"capability": "staff"}),
        ),
        record(
            AccessEventKind::DelegableCapabilitiesSet,
            25,
            json!({"delegable": ["staff", "audit"]}),
        ),
        record(
            AccessEventKind::CapabilityRevoked,
            30,
            json!({"capability": "staff"}),
        ),
        record(AccessEventKind::AccessRevoked, 40, json!({})),
        record(
            AccessEventKind::AccessGranted,
            50,
            json!({"level": "read", "can_read_sensitive": true, "scope": null}),
        ),
    ]
}

#[test]
fn before_the_trail_began_the_answer_is_unknown_not_empty() {
    let answer = fold_access_events(&history(), at(5));
    assert!(!answer.known);
    assert!(!answer.held);
}

#[test]
fn a_capability_is_held_between_its_grant_and_its_revoke() {
    let held = |seconds| {
        fold_access_events(&history(), at(seconds))
            .capabilities
            .contains("staff")
    };
    assert!(!held(15));
    assert!(held(20), "an event at as_of counts");
    assert!(held(29));
    assert!(!held(30));
}

#[test]
fn the_fold_reports_level_scope_and_delegable_as_of_the_instant() {
    let early = fold_access_events(&history(), at(26));
    assert!(early.known && early.held);
    assert_eq!(early.level.as_deref(), Some("write"));
    assert_eq!(early.scope.as_deref(), Some("t"));
    assert_eq!(
        early.delegable,
        BTreeSet::from(["staff".to_string(), "audit".to_string()])
    );
}

#[test]
fn a_revoked_grant_holds_nothing_and_a_new_grant_starts_clean() {
    let revoked = fold_access_events(&history(), at(45));
    assert!(revoked.known);
    assert!(!revoked.held);
    assert!(revoked.capabilities.is_empty() && revoked.delegable.is_empty());
    assert_eq!(revoked.level, None);

    let regranted = fold_access_events(&history(), at(60));
    assert!(regranted.held);
    assert_eq!(regranted.level.as_deref(), Some("read"));
    assert!(regranted.can_read_sensitive);
    assert!(
        regranted.capabilities.is_empty() && regranted.delegable.is_empty(),
        "nothing carries over from the earlier grant"
    );
}

#[test]
fn a_baseline_starts_from_what_the_grant_held() {
    let records = vec![
        record(
            AccessEventKind::AccessBaselined,
            10,
            serde_json::json!({
                "level": "admin", "can_read_sensitive": true, "scope": null,
                "capabilities": ["staff"], "delegable": ["staff"]
            }),
        ),
        record(
            AccessEventKind::CapabilityGranted,
            20,
            serde_json::json!({"capability": "audit"}),
        ),
    ];
    let answer = fold_access_events(&records, at(30));
    assert!(answer.held);
    assert_eq!(
        answer.capabilities,
        BTreeSet::from(["staff".to_string(), "audit".to_string()])
    );
    assert_eq!(answer.delegable, BTreeSet::from(["staff".to_string()]));
}

#[test]
fn every_kind_has_a_schema_naming_the_two_tagged_fields() {
    for kind in AccessEventKind::ALL {
        assert_eq!(AccessEventKind::from_name(kind.name()), Some(kind));
        let schema: serde_json::Value =
            serde_json::from_str(&skilj_core::access_trail::access_event_schema(kind)).unwrap();
        let required = schema["required"].as_array().unwrap();
        assert!(required.contains(&"role_id".into()));
        assert!(required.contains(&"bounded_context".into()));
    }
    assert_eq!(AccessEventKind::from_name("MoneyDeposited"), None);
}

// --- the writers ---

#[test]
fn a_grant_and_its_revoke_each_leave_an_event_with_the_system_origin() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool).await;
        let actor = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        let granted = mapping(&role, &bc, AccessLevel::Write);

        db::grant_role_access_mapping_with_trail(&pool, &granted, Some(&actor.id))
            .await
            .unwrap();
        db::revoke_role_access_mapping_with_trail(&pool, &role.id, &bc.name, now(), &actor.id)
            .await
            .unwrap();

        let records = trail(&pool, &role, &bc).await;
        assert_eq!(
            kinds(&records),
            vec![
                AccessEventKind::AccessGranted,
                AccessEventKind::AccessRevoked
            ]
        );
        assert_eq!(records[0].payload["actor_role_id"], actor.id.as_str());
        assert_eq!(records[0].payload["level"], "write");
        assert_eq!(records[0].payload["scope"], "tenant-a");
        assert_eq!(records[1].payload["reason"], "grant_revoked");

        // The stored event itself: SkiljRecorded, client_id "system",
        // tagged by role and bounded context.
        let stored = db::list_events_for_bounded_context(&pool, ADMIN_BOUNDED_CONTEXT_NAME)
            .await
            .unwrap()
            .into_iter()
            .filter(|event| event.payload.contains(&role.id))
            .collect::<Vec<_>>();
        assert_eq!(stored.len(), 2);
        for event in &stored {
            assert_eq!(event.origin, EventOrigin::SkiljRecorded);
            assert_eq!(event.metadata.client_id, "system");
            let tag = |key: &str| {
                event
                    .tags
                    .iter()
                    .find(|t| t.key == key)
                    .and_then(|t| t.value.clone())
            };
            assert_eq!(tag("role").as_deref(), Some(role.id.as_str()));
            assert_eq!(tag("bounded_context").as_deref(), Some(bc.name.as_str()));
        }
    });
}

#[test]
fn capability_changes_are_recorded_only_when_they_apply() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool).await;
        let actor = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        db::grant_role_access_mapping_with_trail(
            &pool,
            &mapping(&role, &bc, AccessLevel::Admin),
            Some(&actor.id),
        )
        .await
        .unwrap();

        let change = |grant, via_admin| {
            let pool = pool.clone();
            let (role_id, bc_name, actor_id) = (role.id.clone(), bc.name.clone(), actor.id.clone());
            async move {
                db::change_mapping_capability_with_trail(
                    &pool, &role_id, &bc_name, "staff", grant, &actor_id, via_admin,
                )
                .await
                .unwrap()
            }
        };
        assert!(change(true, false).await);
        assert!(!change(true, false).await, "already held: nothing applied");
        assert!(change(false, true).await, "the revoke applies");
        assert!(
            !change(false, true).await,
            "no longer held: nothing applied"
        );

        assert!(db::set_mapping_delegable_with_trail(
            &pool,
            &role.id,
            &bc.name,
            &BTreeSet::from(["staff".to_string()]),
            &actor.id,
        )
        .await
        .unwrap());

        let records = trail(&pool, &role, &bc).await;
        assert_eq!(
            kinds(&records),
            vec![
                AccessEventKind::AccessGranted,
                AccessEventKind::CapabilityGranted,
                AccessEventKind::CapabilityRevoked,
                AccessEventKind::DelegableCapabilitiesSet,
            ],
            "the two no-ops left nothing behind"
        );
        assert_eq!(records[1].payload["via"], "superadmin");
        assert_eq!(records[2].payload["via"], "admin");
        assert_eq!(records[3].payload["delegable"][0], "staff");
    });
}

#[test]
fn a_capability_change_on_a_revoked_grant_applies_nothing_and_records_nothing() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool).await;
        let actor = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        db::grant_role_access_mapping_with_trail(
            &pool,
            &mapping(&role, &bc, AccessLevel::Read),
            Some(&actor.id),
        )
        .await
        .unwrap();
        db::revoke_role_access_mapping_with_trail(&pool, &role.id, &bc.name, now(), &actor.id)
            .await
            .unwrap();

        assert!(!db::change_mapping_capability_with_trail(
            &pool, &role.id, &bc.name, "staff", true, &actor.id, false,
        )
        .await
        .unwrap());
        assert_eq!(trail(&pool, &role, &bc).await.len(), 2);
    });
}

#[test]
fn revoking_a_role_records_one_event_per_ended_grant() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool).await;
        let actor = seed_role(&pool).await;
        let (bc_a, bc_b) = (
            seed_bounded_context(&pool).await,
            seed_bounded_context(&pool).await,
        );
        let mut revoked_mappings = Vec::new();
        let revoked_at = now();
        for bc in [&bc_a, &bc_b] {
            let granted = mapping(&role, bc, AccessLevel::Read);
            db::grant_role_access_mapping_with_trail(&pool, &granted, Some(&actor.id))
                .await
                .unwrap();
            revoked_mappings.push(RoleAccessMapping {
                status: RoleStatus::Revoked,
                revoked_at: Some(revoked_at),
                ..granted
            });
        }
        let revoked_role = Role {
            status: RoleStatus::Revoked,
            revoked_at: Some(revoked_at),
            ..role.clone()
        };
        db::revoke_role_and_mappings(&pool, &revoked_role, &revoked_mappings, &actor.id)
            .await
            .unwrap();

        for bc in [&bc_a, &bc_b] {
            let records = trail(&pool, &role, bc).await;
            assert_eq!(
                kinds(&records),
                vec![
                    AccessEventKind::AccessGranted,
                    AccessEventKind::AccessRevoked
                ]
            );
            assert_eq!(records[1].payload["reason"], "role_revoked");
            assert_eq!(records[1].payload["actor_role_id"], actor.id.as_str());
        }
    });
}

#[test]
fn the_admin_log_stays_gapless_across_many_writers() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let actor = seed_role(&pool).await;
        let mut tasks = Vec::new();
        for _ in 0..8 {
            let pool = pool.clone();
            let actor_id = actor.id.clone();
            tasks.push(tokio::spawn(async move {
                let role = seed_role(&pool).await;
                let bc = seed_bounded_context(&pool).await;
                db::grant_role_access_mapping_with_trail(
                    &pool,
                    &mapping(&role, &bc, AccessLevel::Read),
                    Some(&actor_id),
                )
                .await
                .unwrap();
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        let events = db::list_events_for_bounded_context(&pool, ADMIN_BOUNDED_CONTEXT_NAME)
            .await
            .unwrap();
        for (index, event) in events.iter().enumerate() {
            assert_eq!(event.sequence, index as i64, "no gap, no duplicate");
        }
    });
}

// --- private-field grants ---

fn blanket_grant(grantor: &Role, grantee: &Role, bc: &BoundedContext) -> PrivateFieldGrant {
    PrivateFieldGrant {
        id: generate_token_id(),
        bounded_context: bc.clone(),
        grantor: grantor.clone(),
        grantee: grantee.clone(),
        event_sequence: None,
        command_id: None,
        status: TokenStatus::Active,
        created_at: now(),
        revoked_at: None,
    }
}

/// The admin context's events that mention `grant_id`, oldest first, as
/// `(event type name, payload)`.
async fn private_field_events(pool: &Pool, grant_id: &str) -> Vec<(String, serde_json::Value)> {
    db::list_events_for_bounded_context(pool, ADMIN_BOUNDED_CONTEXT_NAME)
        .await
        .unwrap()
        .into_iter()
        .filter(|event| event.payload.contains(grant_id))
        .map(|event| {
            (
                event.event_type.name.clone(),
                serde_json::from_str(&event.payload).unwrap(),
            )
        })
        .collect()
}

#[test]
fn a_private_field_grant_and_its_revoke_are_recorded_by_the_grantee() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let grantor = seed_role(&pool).await;
        let grantee = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        let grant = blanket_grant(&grantor, &grantee, &bc);

        db::grant_private_field_access_with_trail(&pool, &grant)
            .await
            .unwrap();
        let revoked = PrivateFieldGrant {
            status: TokenStatus::Revoked,
            revoked_at: Some(now()),
            ..grant.clone()
        };
        let first = db::revoke_private_field_access_with_trail(&pool, &revoked)
            .await
            .unwrap();
        let second = db::revoke_private_field_access_with_trail(&pool, &revoked)
            .await
            .unwrap();

        assert!(first);
        assert!(!second, "an already revoked grant records nothing");
        let events = private_field_events(&pool, &grant.id).await;
        assert_eq!(
            events
                .iter()
                .map(|(name, _)| name.as_str())
                .collect::<Vec<_>>(),
            vec!["PrivateFieldAccessGranted", "PrivateFieldAccessRevoked"]
        );
        for (_, payload) in &events {
            assert_eq!(payload["role_id"], grantee.id.as_str());
            assert_eq!(payload["actor_role_id"], grantor.id.as_str());
            assert_eq!(payload["bounded_context"], bc.name.as_str());
            assert_eq!(payload["grant_id"], grant.id.as_str());
        }
        assert!(events[0].1["event_sequence"].is_null() && events[0].1["command_id"].is_null());

        // The grantee's mapping history is not touched by it.
        assert!(trail(&pool, &grantee, &bc).await.is_empty());
        assert!(!fold_access_events(&[], now()).known);
    });
}

#[test]
fn a_private_field_grant_that_cannot_be_recorded_is_not_made() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let grantor = seed_role(&pool).await;
        let grantee = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        let grant = blanket_grant(&grantor, &grantee, &bc);
        // The grant row and its event commit together: a second insert of
        // the same id fails on the primary key and leaves one event.
        db::grant_private_field_access_with_trail(&pool, &grant)
            .await
            .unwrap();
        assert!(db::grant_private_field_access_with_trail(&pool, &grant)
            .await
            .is_err());
        assert_eq!(private_field_events(&pool, &grant.id).await.len(), 1);
    });
}

#[test]
fn private_field_events_carry_the_record_they_name_and_nothing_personal() {
    let grantor = Role {
        id: "grantor".into(),
        external_subject: "someone@example.com".into(),
        name: "Some Person".into(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: now(),
        revoked_at: None,
    };
    let grantee = Role {
        id: "grantee".into(),
        ..grantor.clone()
    };
    let bc = BoundedContext {
        name: "helpdesk".into(),
        status: BoundedContextStatus::Active,
        created_at: now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    let grant = PrivateFieldGrant {
        event_sequence: Some(42),
        ..blanket_grant(&grantor, &grantee, &bc)
    };
    let payload = skilj_core::access_trail::AccessChange::private_field_granted(&grant).payload();
    assert!(!payload.contains("someone@example.com") && !payload.contains("Some Person"));
    let payload: serde_json::Value = serde_json::from_str(&payload).unwrap();
    assert_eq!(payload["event_sequence"], 42);
    assert_eq!(payload["role_id"], "grantee");
    assert_eq!(payload["actor_role_id"], "grantor");
}

#[test]
fn deleting_a_bounded_context_ends_its_active_grants_in_the_trail() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let deleter = seed_role(&pool).await;
        let held = seed_role(&pool).await;
        let already_revoked = seed_role(&pool).await;
        let sharer = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        let mut with_capability = mapping(&held, &bc, AccessLevel::Write);
        with_capability.capabilities = ["support".to_string()].into();
        db::grant_role_access_mapping_with_trail(&pool, &with_capability, None)
            .await
            .unwrap();
        db::grant_role_access_mapping_with_trail(
            &pool,
            &mapping(&already_revoked, &bc, AccessLevel::Read),
            None,
        )
        .await
        .unwrap();
        db::revoke_role_access_mapping_with_trail(
            &pool,
            &already_revoked.id,
            &bc.name,
            now(),
            &deleter.id,
        )
        .await
        .unwrap();
        let open_share = blanket_grant(&sharer, &held, &bc);
        db::grant_private_field_access_with_trail(&pool, &open_share)
            .await
            .unwrap();
        let closed_share = blanket_grant(&sharer, &already_revoked, &bc);
        db::grant_private_field_access_with_trail(&pool, &closed_share)
            .await
            .unwrap();
        db::revoke_private_field_access_with_trail(
            &pool,
            &PrivateFieldGrant {
                status: TokenStatus::Revoked,
                revoked_at: Some(now()),
                ..closed_share.clone()
            },
        )
        .await
        .unwrap();

        db::hard_delete_bounded_context_with_trail(&pool, &bc.name, &deleter.id)
            .await
            .unwrap();

        let held_trail = trail(&pool, &held, &bc).await;
        assert_eq!(
            kinds(&held_trail),
            vec![
                AccessEventKind::AccessGranted,
                AccessEventKind::AccessRevoked
            ]
        );
        let ended = &held_trail[1].payload;
        assert_eq!(ended["reason"], "bounded_context_deleted");
        assert_eq!(ended["actor_role_id"], deleter.id.as_str());
        assert!(
            !fold_access_events(&held_trail, now()).held,
            "a deleted context's grants are no longer held"
        );
        assert_eq!(
            kinds(&trail(&pool, &already_revoked, &bc).await),
            vec![
                AccessEventKind::AccessGranted,
                AccessEventKind::AccessRevoked
            ],
            "a grant that was already revoked is not revoked twice"
        );

        let open = private_field_events(&pool, &open_share.id).await;
        assert_eq!(
            open.iter().map(|(n, _)| n.as_str()).collect::<Vec<_>>(),
            vec!["PrivateFieldAccessGranted", "PrivateFieldAccessRevoked"]
        );
        assert_eq!(open[1].1["reason"], "bounded_context_deleted");
        assert_eq!(open[1].1["actor_role_id"], deleter.id.as_str());
        assert_eq!(private_field_events(&pool, &closed_share.id).await.len(), 2);
    });
}

#[test]
fn deleting_a_bounded_context_nobody_was_granted_in_records_nothing() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let deleter = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        db::hard_delete_bounded_context_with_trail(&pool, &bc.name, &deleter.id)
            .await
            .unwrap();
        assert!(db::get_bounded_context(&pool, &bc.name)
            .await
            .unwrap()
            .is_none());
    });
}

#[test]
fn the_fold_ignores_private_field_events() {
    use serde_json::json;
    let events = vec![record(
        AccessEventKind::PrivateFieldAccessGranted,
        10,
        json!({"grant_id": "g"}),
    )];
    assert_eq!(fold_access_events(&events, at(100)), Default::default());
}

// --- the baseline ---

#[test]
fn a_grant_with_no_history_is_baselined_once() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let _serial = BASELINES.lock().await;
        let role = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        let mut raw = mapping(&role, &bc, AccessLevel::Admin);
        raw.capabilities = BTreeSet::from(["staff".to_string()]);
        raw.delegable = BTreeSet::from(["staff".to_string()]);
        // A writer that records nothing: a grant from before the trail.
        db::insert_role_access_mapping(&pool, &raw).await.unwrap();
        assert!(trail(&pool, &role, &bc).await.is_empty());

        db::baseline_access_events(&pool).await.unwrap();
        // Not asserted on the returned count: the tests of this binary share
        // one database, and another test's raw grant may be baselined by
        // either call. This grant's own trail is what must not grow.
        db::baseline_access_events(&pool).await.unwrap();

        let records = trail(&pool, &role, &bc).await;
        assert_eq!(
            kinds(&records),
            vec![AccessEventKind::AccessBaselined],
            "a covered grant is not baselined again"
        );
        let answer = fold_access_events(&records, now() + Duration::seconds(1));
        assert!(answer.held);
        assert_eq!(answer.level.as_deref(), Some("admin"));
        assert_eq!(answer.capabilities, raw.capabilities);
        assert_eq!(answer.delegable, raw.delegable);
    });
}

#[test]
fn instances_baselining_together_write_one_event_per_grant() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let _serial = BASELINES.lock().await;
        let role = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        db::insert_role_access_mapping(&pool, &mapping(&role, &bc, AccessLevel::Read))
            .await
            .unwrap();

        let tasks: Vec<_> = (0..4)
            .map(|_| {
                let pool = pool.clone();
                tokio::spawn(async move { db::baseline_access_events(&pool).await.unwrap() })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(trail(&pool, &role, &bc).await.len(), 1);
    });
}

#[test]
fn a_raw_grant_with_a_capability_event_is_still_baselined() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let _serial = BASELINES.lock().await;
        let role = seed_role(&pool).await;
        let actor = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        db::insert_role_access_mapping(&pool, &mapping(&role, &bc, AccessLevel::Write))
            .await
            .unwrap();
        // A change after a raw write records `CapabilityGranted` and no start.
        assert!(db::change_mapping_capability_with_trail(
            &pool, &role.id, &bc.name, "staff", true, &actor.id, false,
        )
        .await
        .unwrap());
        assert_eq!(kinds(&trail(&pool, &role, &bc).await).len(), 1);

        db::baseline_access_events(&pool).await.unwrap();

        let records = trail(&pool, &role, &bc).await;
        assert_eq!(
            kinds(&records),
            vec![
                AccessEventKind::CapabilityGranted,
                AccessEventKind::AccessBaselined
            ]
        );
        let answer = fold_access_events(&records, now() + Duration::seconds(1));
        assert!(answer.held, "an active grant is held");
        assert_eq!(answer.capabilities, BTreeSet::from(["staff".to_string()]));
    });
}

#[test]
fn a_raw_regrant_after_a_revoke_is_baselined() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let _serial = BASELINES.lock().await;
        let role = seed_role(&pool).await;
        let actor = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        db::grant_role_access_mapping_with_trail(
            &pool,
            &mapping(&role, &bc, AccessLevel::Read),
            Some(&actor.id),
        )
        .await
        .unwrap();
        db::revoke_role_access_mapping_with_trail(&pool, &role.id, &bc.name, now(), &actor.id)
            .await
            .unwrap();
        db::insert_role_access_mapping(&pool, &mapping(&role, &bc, AccessLevel::Admin))
            .await
            .unwrap();

        db::baseline_access_events(&pool).await.unwrap();

        let records = trail(&pool, &role, &bc).await;
        assert_eq!(
            records.last().unwrap().kind,
            AccessEventKind::AccessBaselined
        );
        let answer = fold_access_events(&records, now() + Duration::seconds(1));
        assert!(answer.held);
        assert_eq!(answer.level.as_deref(), Some("admin"));
    });
}

#[test]
fn a_revoke_of_a_grant_that_is_already_revoked_records_nothing() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool).await;
        let actor = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        db::grant_role_access_mapping_with_trail(
            &pool,
            &mapping(&role, &bc, AccessLevel::Read),
            Some(&actor.id),
        )
        .await
        .unwrap();

        let first =
            db::revoke_role_access_mapping_with_trail(&pool, &role.id, &bc.name, now(), &actor.id)
                .await
                .unwrap();
        let second =
            db::revoke_role_access_mapping_with_trail(&pool, &role.id, &bc.name, now(), &actor.id)
                .await
                .unwrap();

        assert!(first);
        assert!(!second, "nothing was left to revoke");
        assert_eq!(
            kinds(&trail(&pool, &role, &bc).await),
            vec![
                AccessEventKind::AccessGranted,
                AccessEventKind::AccessRevoked
            ]
        );
    });
}

#[test]
fn a_grant_made_with_capabilities_records_them() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool).await;
        let actor = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        let mut granted = mapping(&role, &bc, AccessLevel::Admin);
        granted.capabilities = BTreeSet::from(["staff".to_string()]);
        granted.delegable = BTreeSet::from(["audit".to_string()]);
        db::grant_role_access_mapping_with_trail(&pool, &granted, Some(&actor.id))
            .await
            .unwrap();

        let answer = fold_access_events(
            &trail(&pool, &role, &bc).await,
            now() + Duration::seconds(1),
        );
        assert_eq!(answer.capabilities, granted.capabilities);
        assert_eq!(answer.delegable, granted.delegable);
    });
}

#[test]
fn a_stored_event_that_does_not_parse_is_an_error_not_a_gap() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool).await;
        let actor = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        db::grant_role_access_mapping_with_trail(
            &pool,
            &mapping(&role, &bc, AccessLevel::Read),
            Some(&actor.id),
        )
        .await
        .unwrap();
        // Revoked, so no other test's baseline reads the row broken below.
        db::revoke_role_access_mapping_with_trail(&pool, &role.id, &bc.name, now(), &actor.id)
            .await
            .unwrap();
        let schema = format!("\"bc_{ADMIN_BOUNDED_CONTEXT_NAME}\"");
        let like = format!("%{}%", role.id);
        let originals: Vec<(i64, String)> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT sequence, payload FROM {schema}.events WHERE payload LIKE $1"
        )))
        .bind(&like)
        .fetch_all(&pool)
        .await
        .unwrap();
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "UPDATE {schema}.events SET payload = 'not json' WHERE payload LIKE $1"
        )))
        .bind(&like)
        .execute(&pool)
        .await
        .unwrap();

        let read = db::list_access_event_records(&pool, &role.id, &bc.name).await;

        for (sequence, payload) in originals {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "UPDATE {schema}.events SET payload = $1 WHERE sequence = $2"
            )))
            .bind(payload)
            .bind(sequence)
            .execute(&pool)
            .await
            .unwrap();
        }
        assert!(
            read.is_err(),
            "a trail that cannot be read in full is not answered from part of it"
        );
    });
}

#[test]
fn a_revoked_grant_with_no_history_is_not_invented() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let _serial = BASELINES.lock().await;
        let role = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        let revoked = RoleAccessMapping {
            status: RoleStatus::Revoked,
            revoked_at: Some(now()),
            ..mapping(&role, &bc, AccessLevel::Read)
        };
        db::insert_role_access_mapping(&pool, &revoked)
            .await
            .unwrap();
        db::baseline_access_events(&pool).await.unwrap();
        assert!(trail(&pool, &role, &bc).await.is_empty());
    });
}

#[test]
fn the_registered_types_are_the_eight_and_registering_again_changes_nothing() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        db::register_access_event_types(&pool).await.unwrap();
        for kind in AccessEventKind::ALL {
            let registered = db::get_event_type(&pool, ADMIN_BOUNDED_CONTEXT_NAME, kind.name())
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{} is registered", kind.name()));
            assert!(!registered.external_creation_allowed);
            assert!(!registered.direct_creation_allowed);
            assert!(!registered.system_triggered_allowed);
            assert_eq!(registered.schema_version, 1);
        }
        // The origin check is widened once and stays so.
        db::ensure_admin_events_origin_kind(&pool).await.unwrap();
    });
}
