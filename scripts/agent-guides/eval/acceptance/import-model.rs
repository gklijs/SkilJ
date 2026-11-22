//! Acceptance tests for tasks/import-model.md, copied into the project's
//! `tests/` by check.sh. They hold whether the Rust is generated from
//! src/savings.skilj.toml or written by hand: they use the names the
//! import gives. check.sh compares the .skilj.toml itself with the import.

use skilj::{CommandType, EventType};
use skilj_core::shared::{EventSpec, TagMapping};
use skilj_test_fixture::command::GivenEvents;
use template_check::savings::{
    CloseGoal, CloseGoalPayload, GoalClosed, GoalClosedPayload, GoalOpened, GoalOpenedPayload,
    OpenGoal, OpenGoalPayload, SaveTowardsGoal, SaveTowardsGoalPayload, SavedTowardsGoal,
    SavedTowardsGoalPayload, SavingsEvent,
};

fn opened(goal_id: &str) -> SavingsEvent {
    SavingsEvent::GoalOpened(GoalOpenedPayload {
        goal_id: goal_id.into(),
        name: "Holiday".into(),
        target_amount: 500,
    })
}

fn saved(goal_id: &str, amount: i64) -> SavingsEvent {
    SavingsEvent::SavedTowardsGoal(SavedTowardsGoalPayload {
        goal_id: goal_id.into(),
        amount,
    })
}

fn closed(goal_id: &str) -> SavingsEvent {
    SavingsEvent::GoalClosed(GoalClosedPayload {
        goal_id: goal_id.into(),
    })
}

fn save(goal_id: &str, amount: i64) -> SaveTowardsGoalPayload {
    SaveTowardsGoalPayload {
        goal_id: goal_id.into(),
        amount,
    }
}

#[test]
fn a_goal_is_opened() {
    GivenEvents::<OpenGoal>::new()
        .when(OpenGoalPayload {
            goal_id: "g1".into(),
            name: "Holiday".into(),
            target_amount: 500,
        })
        .then_accepted(vec![EventSpec {
            event_type: "GoalOpened".into(),
            payload: serde_json::json!({ "goal_id": "g1", "name": "Holiday", "target_amount": 500 }),
        }]);
}

#[test]
fn saving_towards_an_open_goal_is_accepted() {
    GivenEvents::<SaveTowardsGoal>::new()
        .events([opened("g1"), saved("g1", 20)])
        .when(save("g1", 50))
        .then_accepted(vec![EventSpec {
            event_type: "SavedTowardsGoal".into(),
            payload: serde_json::json!({ "goal_id": "g1", "amount": 50 }),
        }]);
}

#[test]
fn saving_towards_no_goal_is_goal_not_found() {
    GivenEvents::<SaveTowardsGoal>::new()
        .when(save("g1", 50))
        .then_rejected("goal_not_found");
}

#[test]
fn saving_towards_a_closed_goal_is_goal_closed() {
    GivenEvents::<SaveTowardsGoal>::new()
        .events([opened("g1"), saved("g1", 20), closed("g1")])
        .when(save("g1", 50))
        .then_rejected("goal_closed");
}

#[test]
fn an_open_goal_is_closed() {
    GivenEvents::<CloseGoal>::new()
        .event(opened("g1"))
        .when(CloseGoalPayload {
            goal_id: "g1".into(),
        })
        .then_accepted(vec![EventSpec {
            event_type: "GoalClosed".into(),
            payload: serde_json::json!({ "goal_id": "g1" }),
        }]);
}

#[test]
fn closing_no_goal_or_a_closed_one_is_refused() {
    GivenEvents::<CloseGoal>::new()
        .when(CloseGoalPayload {
            goal_id: "g1".into(),
        })
        .then_rejected("goal_not_found");
    GivenEvents::<CloseGoal>::new()
        .events([opened("g1"), closed("g1")])
        .when(CloseGoalPayload {
            goal_id: "g1".into(),
        })
        .then_rejected("goal_closed");
}

/// The model's idAttribute: every type is tagged with the goal.
#[test]
fn everything_is_tagged_with_the_goal() {
    let goal = TagMapping::new("goal_id", "goal_id");
    for (name, tags) in [
        ("GoalOpened", GoalOpened::tag_mappings()),
        ("SavedTowardsGoal", SavedTowardsGoal::tag_mappings()),
        ("GoalClosed", GoalClosed::tag_mappings()),
        ("OpenGoal", OpenGoal::tag_mappings()),
        ("SaveTowardsGoal", SaveTowardsGoal::tag_mappings()),
        ("CloseGoal", CloseGoal::tag_mappings()),
    ] {
        assert!(tags.contains(&goal), "{name}: {tags:?}");
    }
    assert!(OpenGoal::rest_trigger_allowed());
    assert!(SaveTowardsGoal::rest_trigger_allowed());
    assert!(CloseGoal::rest_trigger_allowed());
    assert_eq!(template_check::savings::BOUNDED_CONTEXT, "savings");
    assert_eq!(template_check::wallet::BOUNDED_CONTEXT, "wallet");
}
