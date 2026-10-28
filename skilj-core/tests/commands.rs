mod common;

use common::*;
use serde_json::json;
use skilj_core::engine::{CommandOutcome, CommandRequest};
use skilj_core::model::*;

#[tokio::test]
async fn deposit_withdraw_and_rejections() {
    let env = start("cmd_basic").await;
    let s = &env.skilj;
    let writer = env
        .role("writer@idp", AccessLevel::Write, false, None)
        .await;

    let o = s
        .submit_command(
            &writer,
            CommandRequest::new("OpenAccount", open("a1", "acme")),
        )
        .await
        .unwrap();
    assert!(o.is_accepted());
    // Second open is rejected by decide().
    match s
        .submit_command(
            &writer,
            CommandRequest::new("OpenAccount", open("a1", "acme")),
        )
        .await
        .unwrap()
    {
        CommandOutcome::Rejected {
            kind,
            matching_events,
            ..
        } => {
            assert_eq!(kind, "account_exists");
            assert!(
                matching_events.is_none(),
                "write-level caller sees no matching events"
            );
        }
        other => panic!("{other:?}"),
    }
    s.submit_command(
        &writer,
        CommandRequest::new(
            "Deposit",
            json!({"account_id":"a1","amount":100,"company":"acme"}),
        ),
    )
    .await
    .unwrap();
    match s
        .submit_command(
            &writer,
            CommandRequest::new(
                "Withdraw",
                json!({"account_id":"a1","amount":150,"company":"acme"}),
            ),
        )
        .await
        .unwrap()
    {
        CommandOutcome::Rejected { kind, .. } => assert_eq!(kind, "insufficient_balance"),
        other => panic!("{other:?}"),
    }
    // Admin rejection carries matching events.
    match s
        .submit_command(
            &env.admin,
            CommandRequest::new(
                "Withdraw",
                json!({"account_id":"a1","amount":150,"company":"acme"}),
            ),
        )
        .await
        .unwrap()
    {
        CommandOutcome::Rejected {
            matching_events: Some(m),
            ..
        } => assert_eq!(m.len(), 2),
        other => panic!("{other:?}"),
    }
    let o = s
        .submit_command(
            &writer,
            CommandRequest::new(
                "Withdraw",
                json!({"account_id":"a1","amount":60,"company":"acme"}),
            ),
        )
        .await
        .unwrap();
    assert!(o.is_accepted());
    let ev = &o.events()[0];
    assert_eq!(ev.metadata.client_id, writer.role.id);
    match &ev.origin {
        Origin::CommandTriggered { command_id } => assert_eq!(
            ev.metadata.causation_id.as_deref(),
            Some(command_id.as_str())
        ),
        o => panic!("{o:?}"),
    }
    // Read-level cannot submit; payload is validated.
    let reader = env.role("reader@idp", AccessLevel::Read, false, None).await;
    assert!(s
        .submit_command(
            &reader,
            CommandRequest::new("Deposit", json!({"account_id":"a1","amount":1}))
        )
        .await
        .is_err());
    assert!(s
        .submit_command(
            &writer,
            CommandRequest::new("Deposit", json!({"account_id":"a1"}))
        )
        .await
        .is_err());
}

#[tokio::test]
async fn idempotency_is_per_caller() {
    let env = start("cmd_idem").await;
    let s = &env.skilj;
    let w1 = env.role("w1@idp", AccessLevel::Write, false, None).await;
    let w2 = env.role("w2@idp", AccessLevel::Write, false, None).await;
    s.submit_command(&w1, CommandRequest::new("OpenAccount", open("a1", "acme")))
        .await
        .unwrap();
    let dep =
        || CommandRequest::new("Deposit", json!({"account_id":"a1","amount":5})).key("order-1");
    let first = s.submit_command(&w1, dep()).await.unwrap();
    let again = s.submit_command(&w1, dep()).await.unwrap();
    match (&first, &again) {
        (
            CommandOutcome::Accepted { events: a, .. },
            CommandOutcome::Duplicate { events: b, .. },
        ) => {
            assert_eq!(a[0].sequence, b[0].sequence)
        }
        other => panic!("{other:?}"),
    }
    // The same key from another caller is another command.
    let other = s.submit_command(&w2, dep()).await.unwrap();
    assert!(matches!(other, CommandOutcome::Accepted { .. }));
}

#[tokio::test]
async fn concurrent_withdrawals_respect_the_boundary() {
    let env = start("cmd_dcb").await;
    let s = env.skilj.clone();
    let w = env.role("w@idp", AccessLevel::Write, false, None).await;
    s.submit_command(&w, CommandRequest::new("OpenAccount", open("a1", "acme")))
        .await
        .unwrap();
    s.submit_command(
        &w,
        CommandRequest::new("Deposit", json!({"account_id":"a1","amount":100})),
    )
    .await
    .unwrap();
    let mut handles = vec![];
    for _ in 0..10 {
        let s = s.clone();
        let w = w.clone();
        handles.push(tokio::spawn(async move {
            s.submit_command(
                &w,
                CommandRequest::new("Withdraw", json!({"account_id":"a1","amount":30})),
            )
            .await
            .unwrap()
            .is_accepted()
        }));
    }
    let mut accepted = 0;
    for h in handles {
        if h.await.unwrap() {
            accepted += 1;
        }
    }
    assert_eq!(accepted, 3);
    // Unrelated accounts never conflict.
    s.submit_command(&w, CommandRequest::new("OpenAccount", open("a2", "acme")))
        .await
        .unwrap();
}

#[tokio::test]
async fn batches_are_ordered_and_not_transactions() {
    let env = start("cmd_batch").await;
    let s = &env.skilj;
    let w = env.role("w@idp", AccessLevel::Write, false, None).await;
    let out = s
        .submit_commands(
            &w,
            vec![
                CommandRequest::new("OpenAccount", open("a1", "acme")),
                CommandRequest::new("OpenAccount", open("a1", "acme")),
                CommandRequest::new("Nope", json!({})),
                CommandRequest::new("Deposit", json!({"account_id":"a1","amount":3})),
            ],
        )
        .await
        .unwrap();
    assert!(out[0].is_accepted());
    assert!(matches!(out[1], CommandOutcome::Rejected { .. }));
    assert!(matches!(out[2], CommandOutcome::Refused { .. }));
    assert!(out[3].is_accepted());
    let too_many = (0..101)
        .map(|_| CommandRequest::new("Deposit", json!({"account_id":"a1","amount":1})))
        .collect();
    assert!(s.submit_commands(&w, too_many).await.is_err());
}

#[tokio::test]
async fn owner_scope_on_writes_and_reads() {
    let env = start("cmd_scope").await;
    let s = &env.skilj;
    let acme = env
        .role("acme@idp", AccessLevel::Admin, false, Some("acme"))
        .await;
    s.submit_command(
        &acme,
        CommandRequest::new("OpenAccount", open("a1", "acme")),
    )
    .await
    .unwrap();
    let err = s
        .submit_command(
            &acme,
            CommandRequest::new("OpenAccount", open("b1", "beta")),
        )
        .await;
    assert!(matches!(err, Err(skilj_core::Error::Forbidden(_))));
    // An event with no owner established (company omitted) is not writable
    // by a scoped caller either.
    let err = s
        .submit_command(
            &acme,
            CommandRequest::new("Deposit", json!({"account_id":"a1","amount":1})),
        )
        .await;
    assert!(err.is_err());
    s.submit_command(
        &env.admin,
        CommandRequest::new("OpenAccount", open("b1", "beta")),
    )
    .await
    .unwrap();
    let page = s.query_events(&acme, &[], None, None).await.unwrap();
    assert_eq!(page.events.len(), 1);
    assert_eq!(s.count_events(&env.admin, &[], None).await.unwrap(), 2);
    assert_eq!(s.count_events(&acme, &[], None).await.unwrap(), 1);
    let cmds = s
        .fetch_commands(&acme, &[], None, None, None, None)
        .await
        .unwrap();
    assert_eq!(cmds.len(), 1);
    assert!(s.inspect_event(&acme, 1).await.is_err());
}

#[tokio::test]
async fn dry_run_persists_nothing() {
    let env = start("cmd_dry").await;
    let s = &env.skilj;
    let d = s
        .dry_run_command(&env.admin, "OpenAccount", open("a1", "acme"))
        .await
        .unwrap();
    assert!(d.accepted);
    assert_eq!(d.would_be_events.len(), 1);
    // email is sensitive and the admin isn't granted: shown as null.
    assert_eq!(
        d.would_be_events[0].payload["email"],
        serde_json::Value::Null
    );
    assert_eq!(s.count_events(&env.admin, &[], None).await.unwrap(), 0);
}
