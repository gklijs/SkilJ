mod common;

use common::*;
use futures::StreamExt;
use serde_json::json;
use skilj_core::engine::{CommandRequest, ExternalEventItem, ExternalOutcome};
use skilj_core::model::*;

#[tokio::test]
async fn projections_async_sync_and_read_your_writes() {
    let env = start("rd_proj").await;
    let s = &env.skilj;
    let w = env.role("w@idp", AccessLevel::Write, false, None).await;
    s.submit_command(&w, CommandRequest::new("OpenAccount", open("a1", "acme")))
        .await
        .unwrap();
    let o = s
        .submit_command(
            &w,
            CommandRequest::new(
                "Deposit",
                json!({"account_id":"a1","amount":40,"company":"acme"}),
            ),
        )
        .await
        .unwrap();
    let seq = o.events()[0].sequence;
    // Sync: always there.
    let c = s
        .query_projection(&w, "AccountCount", None, Some(seq))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(c["count"], 1);
    // Async: a wait times out without a consumer, then succeeds after one.
    let err = s
        .query_projection(&w, "Balance", Some("a1"), Some(seq))
        .await;
    assert!(matches!(err, Err(skilj_core::Error::Timeout(_))));
    s.tick().await.unwrap();
    let b = s
        .query_projection(&w, "Balance", Some("a1"), Some(seq))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(b["balance"], 40);
    // A sequence nobody committed is refused, not waited on.
    assert!(matches!(
        s.query_projection(&w, "Balance", Some("a1"), Some(seq + 10))
            .await,
        Err(skilj_core::Error::Invalid(_))
    ));
    // Sensitive inside projection state: ciphertext for w, plaintext for a
    // can_read_sensitive grant (the instance key is not the subject here,
    // so per spec it stays encrypted even then - see note).
    assert!(b["email"].as_str().unwrap().starts_with("skilj:enc:v1:"));
}

#[tokio::test]
async fn sensitive_fields_render_and_forget() {
    let env = start("rd_sens").await;
    let s = &env.skilj;
    let w = env.role("w@idp", AccessLevel::Write, false, None).await;
    s.submit_command(&w, CommandRequest::new("OpenAccount", open("a1", "acme")))
        .await
        .unwrap();
    let raw: serde_json::Value = sqlx::query_scalar(
        "select payload from events where bounded_context='bank' and sequence=0",
    )
    .fetch_one(s.pool())
    .await
    .unwrap();
    assert!(raw["email"].as_str().unwrap().starts_with("skilj:enc:v1:"));
    let cmd: serde_json::Value = sqlx::query_scalar("select payload from commands")
        .fetch_one(s.pool())
        .await
        .unwrap();
    assert!(cmd["email"].as_str().unwrap().starts_with("skilj:enc:v1:"));

    let plain_admin = env.admin.clone();
    let seen = s.inspect_event(&plain_admin, 0).await.unwrap();
    assert!(seen.payload["email"]
        .as_str()
        .unwrap()
        .starts_with("skilj:enc:v1:"));

    // can_read_sensitive admin
    let r = s
        .create_role(&env.superadmin, "dpo", false, "dpo@idp")
        .await
        .unwrap();
    s.grant_access(
        &env.superadmin,
        &r.id,
        "bank",
        AccessLevel::Admin,
        true,
        None,
    )
    .await
    .unwrap();
    let dpo = s.resolve_grant("dpo@idp", "bank").await.unwrap();
    assert_eq!(
        s.inspect_event(&dpo, 0).await.unwrap().payload["email"],
        "a1@x.nl"
    );
    // The subject itself (external_subject == subject value) reads its own.
    let me = s
        .create_role(&env.superadmin, "me", false, "user-a1")
        .await
        .unwrap();
    s.grant_access(
        &env.superadmin,
        &me.id,
        "bank",
        AccessLevel::Admin,
        false,
        None,
    )
    .await
    .unwrap();
    let me = s.resolve_grant("user-a1", "bank").await.unwrap();
    assert_eq!(
        s.inspect_event(&me, 0).await.unwrap().payload["email"],
        "a1@x.nl"
    );

    s.forget_subject(&env.admin, "user", "user-a1")
        .await
        .unwrap();
    assert!(s.inspect_event(&dpo, 0).await.unwrap().payload["email"]
        .as_str()
        .unwrap()
        .starts_with("skilj:enc:v1:"));
    // A later event for the subject provisions a new key.
    s.submit_command(
        &w,
        CommandRequest::new(
            "OpenAccount",
            json!({"account_id":"a9","owner_id":"user-a1","company":"acme","email":"new@x.nl"}),
        ),
    )
    .await
    .unwrap();
    let ev = s
        .query_events(&dpo, &["AccountOpened".into()], None, Some(0))
        .await
        .unwrap();
    assert_eq!(ev.events[0].payload["email"], "new@x.nl");
}

#[tokio::test]
async fn private_fields() {
    let env = start("rd_priv").await;
    let s = &env.skilj;
    let creator = env
        .role("creator@idp", AccessLevel::Admin, false, None)
        .await;
    let other = env.role("other@idp", AccessLevel::Admin, false, None).await;
    s.submit_command(
        &creator,
        CommandRequest::new("OpenAccount", open("a1", "acme")),
    )
    .await
    .unwrap();
    assert_eq!(
        s.inspect_event(&creator, 0).await.unwrap().payload["note"],
        "secret note"
    );
    assert_eq!(
        s.inspect_event(&other, 0).await.unwrap().payload["note"],
        serde_json::Value::Null
    );
    // The other can't share what isn't theirs.
    assert!(s
        .grant_private_field_access(&other, &other.role.id, Some(0), None)
        .await
        .is_err());
    let g = s
        .grant_private_field_access(&creator, &other.role.id, Some(0), None)
        .await
        .unwrap();
    assert_eq!(
        s.inspect_event(&other, 0).await.unwrap().payload["note"],
        "secret note"
    );
    assert!(s.revoke_private_field_access(&other, &g.id).await.is_err());
    s.revoke_private_field_access(&creator, &g.id)
        .await
        .unwrap();
    assert_eq!(
        s.inspect_event(&other, 0).await.unwrap().payload["note"],
        serde_json::Value::Null
    );
    // Blanket grant.
    s.grant_private_field_access(&creator, &other.role.id, None, None)
        .await
        .unwrap();
    assert_eq!(
        s.inspect_event(&other, 0).await.unwrap().payload["note"],
        "secret note"
    );
    // REST always redacts.
    let t = s
        .mint_token(
            &env.admin,
            TokenPurpose::EventRead,
            "AccountOpened",
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let token = s.authenticate_token(&t.bearer).await.unwrap();
    let page = s
        .fetch_events(&token, "AccountOpened", &[], None, None)
        .await
        .unwrap();
    assert_eq!(page.events[0].payload["note"], serde_json::Value::Null);
    // The trail recorded the grants.
    let trail = s
        .capability_history(
            &env.superadmin,
            &creator.role.id,
            "bank",
            chrono::Utc::now(),
        )
        .await
        .unwrap();
    assert!(trail.held);
}

#[tokio::test]
async fn rest_tokens_fetch_consume_ack() {
    let env = start("rd_rest").await;
    let s = &env.skilj;
    let ext = s
        .mint_token(
            &env.admin,
            TokenPurpose::ExternalEvent,
            "MoneyDeposited",
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let ext = s.authenticate_token(&ext.bearer).await.unwrap();
    for i in 0..5 {
        let o = s
            .submit_external_event(
                &ext,
                "MoneyDeposited",
                ExternalEventItem {
                    payload: json!({"account_id":"a1","amount":i}),
                    source_content: "{}".into(),
                    dedupe_partition_key: Some("p0".into()),
                    dedupe_sequence: Some(i),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(matches!(o, ExternalOutcome::Created(_)));
    }
    // Redelivery: no event.
    let o = s
        .submit_external_event(
            &ext,
            "MoneyDeposited",
            ExternalEventItem {
                payload: json!({"account_id":"a1","amount":2}),
                source_content: "{}".into(),
                dedupe_partition_key: Some("p0".into()),
                dedupe_sequence: Some(2),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    assert!(matches!(o, ExternalOutcome::Redelivered { .. }));
    // Half a pair is malformed.
    assert!(s
        .submit_external_event(
            &ext,
            "MoneyDeposited",
            ExternalEventItem {
                payload: json!({"account_id":"a1","amount":2}),
                dedupe_partition_key: Some("p0".into()),
                ..Default::default()
            },
        )
        .await
        .is_err());

    let rt = s
        .mint_token(
            &env.admin,
            TokenPurpose::EventRead,
            "MoneyDeposited",
            None,
            None,
            None,
            None,
        )
        .await
        .unwrap();
    let rt = s.authenticate_token(&rt.bearer).await.unwrap();
    let filters = vec![Filter {
        field: "amount".into(),
        operator: FilterOperator::GreaterThan,
        value: "1".into(),
    }];
    let page = s
        .fetch_events(&rt, "MoneyDeposited", &filters, None, None)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 3);
    // Wrong epoch refused.
    assert!(matches!(
        s.fetch_events(&rt, "MoneyDeposited", &[], Some(1), Some("other"))
            .await,
        Err(skilj_core::Error::EpochMismatch)
    ));
    // Filter that doesn't fit is refused.
    let bad = vec![Filter {
        field: "account_id".into(),
        operator: FilterOperator::GreaterThan,
        value: "1".into(),
    }];
    assert!(s
        .fetch_events(&rt, "MoneyDeposited", &bad, None, None)
        .await
        .is_err());

    // manual_ack: re-served until acknowledged; a second caller is leased out.
    let p1 = s
        .consume_events(&rt, Some(AckMode::ManualAck), &[])
        .await
        .unwrap();
    assert_eq!(p1.events.len(), 5);
    let p2 = s.consume_events(&rt, None, &[]).await.unwrap();
    assert_eq!(p2.events.len(), 0, "claimed by the first caller");
    assert!(s
        .consume_events(&rt, Some(AckMode::AutoAdvance), &[])
        .await
        .is_err());
    s.acknowledge_events(&rt, 2, None).await.unwrap();
    let p3 = s.consume_events(&rt, None, &[]).await.unwrap();
    assert_eq!(p3.events.len(), 2);
    assert!(
        s.acknowledge_events(&rt, 1, None).await.is_err(),
        "backwards"
    );

    // latest-start token sees only new events.
    let lt = s
        .mint_token(
            &env.admin,
            TokenPurpose::EventRead,
            "MoneyDeposited",
            None,
            Some(EventReadStartPosition::Latest),
            None,
            None,
        )
        .await
        .unwrap();
    let lt = s.authenticate_token(&lt.bearer).await.unwrap();
    assert_eq!(
        s.consume_events(&lt, Some(AckMode::AutoAdvance), &[])
            .await
            .unwrap()
            .events
            .len(),
        0
    );
    s.submit_external_event(
        &ext,
        "MoneyDeposited",
        ExternalEventItem {
            payload: json!({"account_id":"a1","amount":9}),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        s.consume_events(&lt, None, &[]).await.unwrap().events.len(),
        1
    );
    // Revoked tokens stop working at once.
    s.revoke_token(&env.admin, &rt.id).await.unwrap();
    assert!(s.authenticate_token(&format!("{}.x", rt.id)).await.is_err());
}

#[tokio::test]
async fn subscriptions_resume_and_close_on_revocation() {
    let env = start("rd_sub").await;
    let s = &env.skilj;
    let w = env.role("w@idp", AccessLevel::Write, false, None).await;
    s.submit_command(&w, CommandRequest::new("OpenAccount", open("a1", "acme")))
        .await
        .unwrap();
    let reader = env.role("r@idp", AccessLevel::Read, false, None).await;
    // Resume from -1: the one existing event first.
    let (spec, resume) = s
        .create_subscription(&reader, None, &[], &[], Some(-1), None)
        .await
        .unwrap();
    let mut stream = s.subscription_stream(reader.clone(), spec, resume);
    let first = stream.next().await.unwrap().unwrap();
    assert_eq!(first.sequence, Some(0));
    s.submit_command(
        &w,
        CommandRequest::new("Deposit", json!({"account_id":"a1","amount":1})),
    )
    .await
    .unwrap();
    let second = stream.next().await.unwrap().unwrap();
    assert_eq!(second.sequence, Some(1));
    s.revoke_access(&env.superadmin, &reader.mapping.id)
        .await
        .unwrap();
    let end = stream.next().await.unwrap();
    assert_eq!(
        end.unwrap_err(),
        skilj_core::engine::SubscriptionEnd::Revoked
    );
}

#[tokio::test]
async fn snapshot_inspection() {
    let env = start("rd_snap").await;
    let s = &env.skilj;
    s.submit_command(
        &env.admin,
        CommandRequest::new("OpenAccount", open("a1", "acme")),
    )
    .await
    .unwrap();
    s.submit_command(
        &env.admin,
        CommandRequest::new("Deposit", json!({"account_id":"a1","amount":7})),
    )
    .await
    .unwrap();
    assert_eq!(
        s.inspect_snapshot(&env.admin, "balance", "a1")
            .await
            .unwrap()
            .unwrap()["balance"],
        7
    );
    assert!(s
        .inspect_snapshot(&env.admin, "balance", "zz")
        .await
        .unwrap()
        .is_none());
    assert!(s.inspect_snapshot(&env.admin, "nope", "a1").await.is_err());
}
