//! Acceptance tests for tasks/second-context.md, copied into the
//! project's `tests/` by check.sh.

use skilj::{CommandType, EventType};
use skilj_core::shared::EventSpec;
use skilj_test_fixture::command::GivenEvents;
use template_check::rewards::{
    EarnPoints, EarnPointsPayload, PointsEarned, PointsEarnedPayload, PointsRedeemed,
    PointsRedeemedPayload, RedeemPoints, RedeemPointsPayload, RewardsEvent,
};

fn earned(points: i64) -> RewardsEvent {
    RewardsEvent::PointsEarned(PointsEarnedPayload {
        member_id: "m1".into(),
        points,
    })
}

fn redeemed(points: i64) -> RewardsEvent {
    RewardsEvent::PointsRedeemed(PointsRedeemedPayload {
        member_id: "m1".into(),
        points,
    })
}

fn redeem(points: i64) -> RedeemPointsPayload {
    RedeemPointsPayload {
        member_id: "m1".into(),
        points,
    }
}

#[test]
fn rewards_is_a_bounded_context_of_its_own() {
    assert_eq!(template_check::rewards::BOUNDED_CONTEXT, "rewards");
    assert_eq!(template_check::wallet::BOUNDED_CONTEXT, "wallet");
    assert_eq!(<EarnPoints as CommandType>::NAME, "EarnPoints");
    assert_eq!(<RedeemPoints as CommandType>::NAME, "RedeemPoints");
    assert_eq!(<PointsEarned as EventType>::NAME, "PointsEarned");
    assert_eq!(<PointsRedeemed as EventType>::NAME, "PointsRedeemed");
    assert!(EarnPoints::rest_trigger_allowed());
    assert!(RedeemPoints::rest_trigger_allowed());
}

#[test]
fn points_are_earned() {
    GivenEvents::<EarnPoints>::new()
        .when(EarnPointsPayload {
            member_id: "m1".into(),
            points: 10,
        })
        .then_accepted(vec![EventSpec {
            event_type: "PointsEarned".into(),
            payload: serde_json::json!({ "member_id": "m1", "points": 10 }),
        }]);
}

#[test]
fn points_up_to_the_balance_are_redeemed() {
    GivenEvents::<RedeemPoints>::new()
        .events([earned(10), redeemed(3), earned(5)])
        .when(redeem(12))
        .then_accepted(vec![EventSpec {
            event_type: "PointsRedeemed".into(),
            payload: serde_json::json!({ "member_id": "m1", "points": 12 }),
        }]);
}

#[test]
fn more_than_the_balance_is_rejected() {
    GivenEvents::<RedeemPoints>::new()
        .events([earned(10), redeemed(3)])
        .when(redeem(8))
        .then_rejected("insufficient_points");
}
