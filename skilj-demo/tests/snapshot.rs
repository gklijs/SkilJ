//! End-to-end proof of docs/architecture.md §19's `Snapshot` mechanism,
//! against the real adopter in `skilj_demo::banking`
//! (`AccountBalanceSnapshot`/`WithdrawMoneyFast` - see that module's own
//! doc comment for why they're hand-written rather than codegen'd).
//! `skilj-core/tests/snapshot_context.rs` already covers the DB/dispatcher
//! plumbing in isolation; this file's whole point is the real thing,
//! end to end, through real HTTP - and specifically, proving
//! `decide_from_snapshot` is genuinely *read from* the stored snapshot
//! row, not silently bypassed in favour of a correct-either-way full
//! replay. A plain "does the right thing" test can't tell those two
//! apart, since both would reach the identical correct answer against
//! real, untampered data - so the decisive test here deliberately
//! corrupts the stored row's own balance (leaving `snapshot_version`
//! untouched, so it's still trusted) and shows `WithdrawMoneyFast`'s own
//! decision changes to match the corrupted number.

mod support;

use skilj_demo::banking::{AccountBalanceSnapshotState, BOUNDED_CONTEXT};
use support::{
    accepted, mapping_for, mint_command_token, rejection_kind, runtime, setup, test_db, trigger,
    unique_name,
};

#[test]
fn withdraw_money_fast_is_correct_both_cold_and_after_a_real_catch_up_tick() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let account = unique_name("account");

        let deposit = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "DepositMoney").await;
        let withdraw_fast =
            mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "WithdrawMoneyFast").await;

        let response = trigger(
            &router,
            &deposit,
            serde_json::json!({ "account_id": account, "amount": 100 }),
        )
        .await;
        assert!(
            accepted(&response),
            "deposit should be accepted: {response:?}"
        );

        // No snapshot row exists yet for this account - decide_from_snapshot's
        // own cold-start path (Snapshot::State::default() plus every real
        // event as events_since_snapshot) must still reach the correct
        // answer, the same as an ordinary full replay would.
        let response = trigger(
            &router,
            &withdraw_fast,
            serde_json::json!({ "account_id": account, "amount": 40 }),
        )
        .await;
        assert!(
            accepted(&response),
            "cold-start withdrawal within balance should be accepted: {response:?}"
        );
        // True balance is now 60.

        let response = trigger(
            &router,
            &withdraw_fast,
            serde_json::json!({ "account_id": account, "amount": 1000 }),
        )
        .await;
        assert!(!accepted(&response));
        assert_eq!(rejection_kind(&response), "insufficient_funds");

        // Force the real background catch-up tick that would normally
        // run on its own poll interval - deterministic, not a sleep.
        skilj_core::db::catch_up_snapshots(
            &pool,
            BOUNDED_CONTEXT,
            skilj.snapshot_dispatcher().as_ref(),
        )
        .await
        .unwrap();

        let (version, as_of_sequence, state_json, _updated_at) =
            skilj_core::db::get_snapshot_state(
                &pool,
                BOUNDED_CONTEXT,
                "AccountBalanceSnapshot",
                "account",
                &account,
            )
            .await
            .unwrap()
            .expect("a real row now exists after a real catch-up tick");
        assert_eq!(version, 1);
        assert!(as_of_sequence >= 0);
        let state: AccountBalanceSnapshotState = serde_json::from_str(&state_json).unwrap();
        assert_eq!(
            state.balance, 60,
            "the real, folded balance after deposit(100) - withdraw(40)"
        );

        // Same withdrawal as before, now served from the real stored
        // snapshot instead of a cold-start replay - still correct.
        let response = trigger(
            &router,
            &withdraw_fast,
            serde_json::json!({ "account_id": account, "amount": 1000 }),
        )
        .await;
        assert!(!accepted(&response));
        assert_eq!(rejection_kind(&response), "insufficient_funds");
    });
}

/// The decisive proof: a tampered stored snapshot changes
/// `WithdrawMoneyFast`'s own decision - possible only if
/// `decide_from_snapshot` genuinely reads the stored row, not a
/// coincidence a full-replay fallback could also produce.
#[test]
fn a_tampered_stored_snapshot_changes_the_decision_proving_it_is_actually_read() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let account = unique_name("account");

        let deposit = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "DepositMoney").await;
        let withdraw_fast =
            mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "WithdrawMoneyFast").await;

        let response = trigger(
            &router,
            &deposit,
            serde_json::json!({ "account_id": account, "amount": 100 }),
        )
        .await;
        assert!(accepted(&response));

        skilj_core::db::catch_up_snapshots(
            &pool,
            BOUNDED_CONTEXT,
            skilj.snapshot_dispatcher().as_ref(),
        )
        .await
        .unwrap();

        // True balance is 100 - a withdrawal of 500 must be rejected
        // against real, untampered data.
        let response = trigger(
            &router,
            &withdraw_fast,
            serde_json::json!({ "account_id": account, "amount": 500 }),
        )
        .await;
        assert!(
            !accepted(&response),
            "500 must be rejected against the real balance of 100"
        );

        // Tamper the stored row directly - leaving snapshot_version
        // untouched, so it's still trusted, not treated as a "model
        // changed" cold start. No committed event actually changed the
        // real balance; only the row's own claimed state did.
        let schema = format!("\"bc_{BOUNDED_CONTEXT}\"");
        sqlx::query(&format!(
            "UPDATE {schema}.snapshots SET state = $1::jsonb \
             WHERE snapshot_name = 'AccountBalanceSnapshot' AND tag_key = 'account' \
             AND tag_value = $2"
        ))
        .bind(serde_json::json!({ "balance": 1_000_000 }).to_string())
        .bind(&account)
        .execute(&pool)
        .await
        .unwrap();

        // The same 500 withdrawal must now succeed - only explicable if
        // decide_from_snapshot read the tampered row's own balance
        // (1,000,000), not the real history (still genuinely 100).
        let response = trigger(
            &router,
            &withdraw_fast,
            serde_json::json!({ "account_id": account, "amount": 500 }),
        )
        .await;
        assert!(
            accepted(&response),
            "a withdrawal that only clears the tampered balance must be accepted here - if this \
             fails, decide_from_snapshot is not actually reading the stored snapshot row: \
             {response:?}"
        );
    });
}
