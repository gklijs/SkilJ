//! The access trail (docs/architecture.md §229): every change to who holds
//! what is also an event in the admin bounded context, written in the same
//! transaction, and `fold_access_events` answers what a grant held at a
//! past instant from those events. The fold is pure; the writers and the
//! baseline need Postgres (see `persistence.rs`'s module comment for how
//! the database is found).

use chrono::{Duration, SubsecRound, TimeZone, Utc};
use skilj_core::access_control::{AccessLevel, Role, RoleAccessMapping, RoleStatus};
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

async fn test_pool() -> Option<Pool> {
    TEST_DB.get_or_init(provision).await.clone()
}

async fn provision() -> Option<Pool> {
    let url = skilj_test_support::database_url("skilj_access_trail").await?;
    let pool = db::connect(&url).await.ok()?;
    db::migrate(&pool).await.ok()?;
    let existing = db::get_bounded_context(&pool, ADMIN_BOUNDED_CONTEXT_NAME)
        .await
        .ok()?;
    if let Some(admin) =
        skilj_core::bootstrap::stamp_admin_bounded_context(existing.as_ref(), Utc::now())
    {
        db::insert_bounded_context(&pool, &admin).await.ok()?;
    }
    db::ensure_access_trail(&pool).await.ok()?;
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

// --- the baseline ---

#[test]
fn a_grant_with_no_history_is_baselined_once() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
        let role = seed_role(&pool).await;
        let bc = seed_bounded_context(&pool).await;
        let mut raw = mapping(&role, &bc, AccessLevel::Admin);
        raw.capabilities = BTreeSet::from(["staff".to_string()]);
        raw.delegable = BTreeSet::from(["staff".to_string()]);
        // A writer that records nothing: a grant from before the trail.
        db::insert_role_access_mapping(&pool, &raw).await.unwrap();
        assert!(trail(&pool, &role, &bc).await.is_empty());

        db::baseline_access_events(&pool).await.unwrap();
        let second = db::baseline_access_events(&pool).await.unwrap();

        let records = trail(&pool, &role, &bc).await;
        assert_eq!(kinds(&records), vec![AccessEventKind::AccessBaselined]);
        assert_eq!(second, 0, "a covered grant is not baselined again");
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
fn a_revoked_grant_with_no_history_is_not_invented() {
    runtime().block_on(async {
        let Some(pool) = test_pool().await else {
            return;
        };
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
fn the_registered_types_are_the_six_and_registering_again_changes_nothing() {
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
