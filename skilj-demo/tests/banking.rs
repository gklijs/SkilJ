//! Integration tests for `skilj_demo::banking` - real HTTP requests
//! through `Skilj::rest_router()`, proving both the balance logic and
//! (the actual point of this bounded context, per its own module doc
//! comment) that two different accounts never coordinate with each
//! other: everything below hammers two accounts side by side and never
//! needs to reason about ordering between them.

mod support;

use skilj_demo::banking::{AccountBalanceState, BOUNDED_CONTEXT};
use support::{
    accepted, mapping_for, mint_command_token, projection_state, rejection_kind, runtime, setup,
    test_db, trigger, unique_name,
};

#[test]
fn deposit_then_withdraw_within_balance_succeeds() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let account = unique_name("account");

        let deposit = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "DepositMoney").await;
        let withdraw = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "WithdrawMoney").await;

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

        let response = trigger(
            &router,
            &withdraw,
            serde_json::json!({ "account_id": account, "amount": 40 }),
        )
        .await;
        assert!(
            accepted(&response),
            "withdrawal within balance should be accepted: {response:?}"
        );

        let state: AccountBalanceState =
            projection_state(&pool, BOUNDED_CONTEXT, "AccountBalance", &account).await;
        assert_eq!(state.balance, 60);
    });
}

#[test]
fn withdrawing_more_than_the_balance_is_rejected_and_persists_nothing() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let account = unique_name("account");

        let deposit = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "DepositMoney").await;
        let withdraw = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "WithdrawMoney").await;

        trigger(
            &router,
            &deposit,
            serde_json::json!({ "account_id": account, "amount": 50 }),
        )
        .await;

        let response = trigger(
            &router,
            &withdraw,
            serde_json::json!({ "account_id": account, "amount": 51 }),
        )
        .await;
        assert!(
            !accepted(&response),
            "overdrawing should be rejected: {response:?}"
        );
        assert_eq!(rejection_kind(&response), "insufficient_funds");

        let state: AccountBalanceState =
            projection_state(&pool, BOUNDED_CONTEXT, "AccountBalance", &account).await;
        assert_eq!(
            state.balance, 50,
            "the rejected withdrawal must not have moved the balance"
        );
    });
}

/// The point of this bounded context (see its own module doc comment):
/// account `a`'s own commands never see account `b`'s events, and
/// account `b`'s never-funded balance is rejected exactly as if `a`
/// didn't exist. No coordination between the two accounts is attempted
/// because `WithdrawMoney`/`DepositMoney` only ever tag on `account_id`.
#[test]
fn two_accounts_never_affect_each_other() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, pool, mappings) = setup().await;
        let mapping = mapping_for(&mappings, BOUNDED_CONTEXT);
        let router = skilj.rest_router();
        let account_a = unique_name("account");
        let account_b = unique_name("account");

        let deposit = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "DepositMoney").await;
        let withdraw = mint_command_token(&pool, mapping, BOUNDED_CONTEXT, "WithdrawMoney").await;

        // Fund A generously; B is never funded at all.
        let response = trigger(
            &router,
            &deposit,
            serde_json::json!({ "account_id": account_a, "amount": 1000 }),
        )
        .await;
        assert!(accepted(&response));

        // B withdrawing anything is rejected - A's balance is irrelevant
        // to it, and matching_events for B is empty.
        let response = trigger(
            &router,
            &withdraw,
            serde_json::json!({ "account_id": account_b, "amount": 1 }),
        )
        .await;
        assert!(!accepted(&response));
        assert_eq!(rejection_kind(&response), "insufficient_funds");

        // A is completely unaffected by B's rejected attempt.
        let state: AccountBalanceState =
            projection_state(&pool, BOUNDED_CONTEXT, "AccountBalance", &account_a).await;
        assert_eq!(state.balance, 1000);
        let state_b: AccountBalanceState =
            projection_state(&pool, BOUNDED_CONTEXT, "AccountBalance", &account_b).await;
        assert_eq!(state_b.balance, 0);
    });
}
