mod common;

use std::sync::{Arc, Mutex};

use common::*;
use serde_json::json;
use skilj_core::crypto::LocalMasterKey;
use skilj_core::decl::*;
use skilj_core::engine::CommandRequest;
use skilj_core::model::*;
use skilj_core::Skilj;

#[tokio::test]
async fn bootstrap_closes_and_context_rules() {
    let env = start("lc_boot").await;
    let s = &env.skilj;
    assert!(s.create_superadmin("whatever", "x", "x@idp").await.is_err());
    // A context created grants nobody anything.
    s.add_bounded_context(&env.superadmin, "empty").await.unwrap();
    assert!(s.resolve_grant("root@idp", "empty").await.is_err());
    assert!(s.add_bounded_context(&env.superadmin, "empty").await.is_err());
    assert!(s.add_bounded_context(&env.superadmin, "Bad-Name").await.is_err());
    // admin can't be archived or deleted
    s.grant_access(&env.superadmin, &env.admin_role.id, "admin", AccessLevel::Admin, false, None)
        .await
        .unwrap();
    let on_admin = s.resolve_grant("ops@idp", "admin").await.unwrap();
    assert!(s.archive_bounded_context(&on_admin).await.is_err());
    assert!(s.delete_bounded_context(&env.superadmin, "admin").await.is_err());
    // Delete needs archive first.
    assert!(s.delete_bounded_context(&env.superadmin, "bank").await.is_err());
    s.submit_command(&env.admin, CommandRequest::new("OpenAccount", open("a1", "acme"))).await.unwrap();
    s.archive_bounded_context(&env.admin).await.unwrap();
    assert!(matches!(
        s.submit_command(&env.admin, CommandRequest::new("OpenAccount", open("a2", "acme"))).await,
        Err(skilj_core::Error::Archived(_))
    ));
    // Reads continue on an archived context.
    assert_eq!(s.count_events(&env.admin, &[], None).await.unwrap(), 1);
    s.delete_bounded_context(&env.superadmin, "bank").await.unwrap();
    assert!(s.resolve_grant("ops@idp", "bank").await.is_err());
    // The name comes free again.
    s.add_bounded_context(&env.superadmin, "bank").await.unwrap();
    // The trail recorded the revocation by deletion.
    let h = s
        .capability_history(&env.superadmin, &env.admin_role.id, "bank", chrono::Utc::now())
        .await
        .unwrap();
    assert!(h.known && !h.held);
}

#[tokio::test]
async fn capabilities_and_delegation() {
    let env = start("lc_caps").await;
    let s = &env.skilj;
    let member = env.role("member@idp", AccessLevel::Read, false, None).await;
    // Admin can't delegate before a superadmin allows it.
    assert!(s.grant_capability_as_admin(&env.admin, &member.mapping.id, "staff").await.is_err());
    s.set_delegable(&env.superadmin, &env.admin.mapping.id, ["staff".to_string()].into_iter().collect())
        .await
        .unwrap();
    let admin = s.resolve_grant("ops@idp", "bank").await.unwrap();
    s.grant_capability_as_admin(&admin, &member.mapping.id, "staff").await.unwrap();
    assert!(s.grant_capability_as_admin(&admin, &admin.mapping.id, "staff").await.is_err(), "self grant");
    let overview = s.capability_overview(&env.superadmin).await.unwrap();
    let bank = overview.iter().find(|r| r.bounded_context == "bank").unwrap();
    assert!(bank.held_not_required.contains("staff"));
    let h = s
        .capability_history(&env.superadmin, &member.role.id, "bank", chrono::Utc::now())
        .await
        .unwrap();
    assert!(h.held && h.capabilities.contains("staff"));
}

fn rebuilt_decl(schema_extra: bool, sync: bool) -> BoundedContextDecl {
    let mut d = bank_decl();
    let p = d.projections.iter_mut().find(|p| p.name == "AccountCount").unwrap();
    p.sync = sync;
    if schema_extra {
        p.schema = json!({"type":"object","properties":{"count":{"type":"integer"},"note":{"type":"string"}}});
    }
    d
}

#[tokio::test]
async fn projection_changes_are_staged_then_promoted() {
    let env = start("lc_rebuild").await;
    let s = &env.skilj;
    s.submit_command(&env.admin, CommandRequest::new("OpenAccount", open("a1", "acme"))).await.unwrap();
    // A new binary declaring a changed schema: staged, live untouched.
    let p = rebuilt_decl(true, true).projections.into_iter().find(|p| p.name == "AccountCount").unwrap();
    assert_eq!(s.register_projection(&env.admin, "bank", &p).await.unwrap(), "staged");
    let reg = s.registry().await;
    assert_eq!(reg.projection("bank", "AccountCount").unwrap().schema_version, 1);
    assert_eq!(reg.rebuilds.len(), 1);
    // Same again: still one pending row.
    assert_eq!(s.register_projection(&env.admin, "bank", &p).await.unwrap(), "unchanged");
    // Discard, re-stage, rebuild, promote.
    s.discard_projection_rebuild(&env.admin, "AccountCount").await.unwrap();
    assert!(s.discard_projection_rebuild(&env.admin, "AccountCount").await.is_err());
    s.register_projection(&env.admin, "bank", &p).await.unwrap();
    s.rebuild_projection(&env.admin, "AccountCount").await.unwrap();
    s.tick().await.unwrap();
    let reg = s.registry().await;
    assert_eq!(reg.projection("bank", "AccountCount").unwrap().schema_version, 2);
    assert!(reg.rebuilds.is_empty());
    let c = s.query_projection(&env.admin, "AccountCount", None, None).await.unwrap().unwrap();
    assert_eq!(c["count"], 1);
}

#[tokio::test]
async fn schema_evolution_rules() {
    let env = start("lc_schema").await;
    let s = &env.skilj;
    let mut d = bank_decl();
    let et = d.event_types.iter_mut().find(|e| e.name == "MoneyDeposited").unwrap();
    // Adding an optional field: fine, version 2.
    et.schema["properties"]["memo"] = json!({"type": "string"});
    assert!(s.register_event_type(&env.admin, "bank", et).await.unwrap());
    assert_eq!(s.registry().await.event_type("bank", "MoneyDeposited").unwrap().0.schema_version, 2);
    // Retyping: refused.
    et.schema["properties"]["amount"] = json!({"type": "number"});
    assert!(s.register_event_type(&env.admin, "bank", et).await.is_err());
    et.schema["properties"]["amount"] = json!({"type": "integer"});
    // Dropping a tag key: refused.
    let mut et2 = et.clone();
    et2.tag_mappings.retain(|m| m.key != "account");
    assert!(s.register_event_type(&env.admin, "bank", &et2).await.is_err());
}

#[tokio::test]
async fn templates_stamp_tenants() {
    let mut tenant_decl = bank_decl();
    tenant_decl.name = "tmpl".into();
    let env = start_with("lc_tmpl", vec![tenant_decl], &["tmpl"]).await;
    let s = &env.skilj;
    let owner = s.create_role(&env.superadmin, "t1-owner", false, "t1@idp").await.unwrap();
    let errors = s
        .create_from_template(&env.superadmin, "tmpl", "tenant1", &owner.id, AccessLevel::Write, false, None)
        .await
        .unwrap();
    assert!(errors.is_empty(), "{errors:?}");
    let g = s.resolve_grant("t1@idp", "tenant1").await.unwrap();
    // Deciders come from the template's declaration.
    let o = s.submit_command(&g, CommandRequest::new("OpenAccount", open("a1", "acme"))).await.unwrap();
    assert!(o.is_accepted());
    // A tenant can't be a template.
    assert!(s
        .create_from_template(&env.superadmin, "tenant1", "tenant2", &owner.id, AccessLevel::Write, false, None)
        .await
        .is_err());
    // Resync is a no-op when in step.
    assert!(s.resync_from_template(&env.superadmin, "tenant1").await.unwrap().is_empty());
}

#[tokio::test]
async fn scheduled_events_fire_once_per_occurrence() {
    let ticks = BoundedContextDecl::new("clock").event(
        EventTypeDecl::new("Tick", json!({"type":"object","properties":{"at":{"type":"string"}}}))
            .scheduled("* * * * *", MissedOccurrencePolicy::ReplayBacklog, |at| json!({"at": at.to_rfc3339()})),
    );
    let pool = fresh_db("lc_sched").await;
    let secret = Arc::new(Mutex::new(String::new()));
    let s2 = secret.clone();
    let s = Skilj::builder()
        .pool(pool.clone())
        .master_key(Arc::new(LocalMasterKey::new("m1", [1u8; 32])))
        .context(ticks)
        .without_background_tasks()
        .on_bootstrap_secret(move |x| *s2.lock().unwrap() = x.into())
        .start()
        .await
        .unwrap();
    let root = s.create_superadmin(&secret.lock().unwrap(), "root", "root").await.unwrap();
    s.add_bounded_context(&root, "clock").await.unwrap();
    s.grant_access(&root, &root.id, "clock", AccessLevel::Admin, false, None).await.unwrap();
    s.reconcile("root").await.unwrap();
    // Pretend the schedule was enabled 3 minutes ago.
    sqlx::query(
        "update event_types set def = jsonb_set(def, '{schedule_position}', to_jsonb((now() - interval '3 minutes 30 seconds')::timestamptz))
         where bounded_context = 'clock'",
    )
    .execute(&pool)
    .await
    .unwrap();
    s.reload_registry_for_tests().await;
    let fired = s.step_schedules().await.unwrap();
    assert!((3..=4).contains(&fired), "replay_backlog pays off every missed occurrence ({fired})");
    assert_eq!(s.step_schedules().await.unwrap(), 0, "never twice");
    let t = s.registry().await.event_type("clock", "Tick").unwrap();
    assert!(t.0.last_fired_at.unwrap() <= t.0.schedule_position.unwrap());
}
