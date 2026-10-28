mod common;

use std::sync::Arc;
use std::time::Duration;

use common::*;
use futures::StreamExt;
use serde_json::json;
use skilj_core::crypto::LocalMasterKey;
use skilj_core::engine::CommandRequest;
use skilj_core::model::*;
use skilj_core::Skilj;

/// Two instances on one database: events committed on one reach live
/// subscriptions on the other; revocations close them there too.
#[tokio::test]
async fn delivery_and_revocation_span_instances() {
    let env = start("inst_two").await;
    let a = &env.skilj;
    // Instance B with background tasks (listener) on the same database.
    let b = Skilj::builder()
        .pool(a.pool().clone())
        .master_key(Arc::new(LocalMasterKey::new("m1", [9u8; 32])))
        .blind_index_root_key(b"blind-root".to_vec())
        .context(bank_decl())
        .start()
        .await
        .unwrap();
    let reader = env.role("r@idp", AccessLevel::Read, false, None).await;
    let reader_b = b.resolve_grant("r@idp", "bank").await.unwrap();
    let (spec, resume) = b
        .create_subscription(&reader_b, None, &[], &[], None, None)
        .await
        .unwrap();
    let mut stream = b.subscription_stream(reader_b, spec, resume);
    tokio::time::sleep(Duration::from_millis(300)).await;
    a.submit_command(
        &env.admin,
        CommandRequest::new("OpenAccount", open("a1", "acme")),
    )
    .await
    .unwrap();
    let got = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(got.sequence, Some(0));
    // A registration made on A is known to B.
    let mut d = bank_decl();
    let et = d
        .event_types
        .iter_mut()
        .find(|e| e.name == "MoneyDeposited")
        .unwrap();
    et.schema["properties"]["memo"] = json!({"type":"string"});
    a.register_event_type(&env.admin, "bank", et).await.unwrap();
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        b.registry()
            .await
            .event_type("bank", "MoneyDeposited")
            .unwrap()
            .0
            .schema_version,
        2
    );
    // Revocation on A closes the subscription on B.
    a.revoke_access(&env.superadmin, &reader.mapping.id)
        .await
        .unwrap();
    let end = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        end.unwrap_err(),
        skilj_core::engine::SubscriptionEnd::Revoked
    );
}
