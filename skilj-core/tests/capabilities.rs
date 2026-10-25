//! `RoleAccessMapping.capabilities`/`delegable` and the rules that change
//! them (`GrantCapabilityAsSuperadmin`, `RevokeCapabilityAsSuperadmin`,
//! `GrantCapabilityAsAdmin`, `RevokeCapabilityAsAdmin`,
//! `SetDelegableCapabilities`), and `invariant CapabilitiesAreWellFormed`.
//! The persistence of a change is `persistence.rs`'s business; the gates
//! the capabilities open are `projection_owner_scoping.rs`'s and
//! `private_field_visibility.rs`'s.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{
    self, AccessLevel, CapabilityActor, Role, RoleAccessMapping, RoleStatus,
};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};

fn timestamp() -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(0, 0).unwrap()
}

fn role(id: &str, superadmin: bool) -> Role {
    Role {
        id: id.into(),
        external_subject: format!("{id}@x"),
        name: id.into(),
        superadmin,
        status: RoleStatus::Active,
        created_at: timestamp(),
        revoked_at: None,
    }
}

fn bc(name: &str) -> BoundedContext {
    BoundedContext {
        name: name.into(),
        status: BoundedContextStatus::Active,
        created_at: timestamp(),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
        template: None,
    }
}

fn mapping(role: Role, bc: BoundedContext, level: AccessLevel) -> RoleAccessMapping {
    RoleAccessMapping {
        role,
        bounded_context: bc,
        level,
        can_read_sensitive: false,
        scope: None,
        capabilities: Default::default(),
        delegable: Default::default(),
        status: RoleStatus::Active,
        created_at: timestamp(),
        revoked_at: None,
    }
}

fn admin_delegating(names: &[&str]) -> RoleAccessMapping {
    let mut m = mapping(role("admin", false), bc("helpdesk"), AccessLevel::Admin);
    m.delegable = names.iter().map(|n| n.to_string()).collect();
    m
}

fn agent() -> RoleAccessMapping {
    mapping(role("agent", false), bc("helpdesk"), AccessLevel::Write)
}

fn code<T>(r: skilj_core::error::Result<T>) -> String {
    r.err().expect("expected a rejection").code().to_string()
}

#[test]
fn capability_names_are_lowercase_ascii_digits_underscore_hyphen_up_to_64() {
    for ok in ["staff", "a", "staff_notes", "tier-2", "x9", &"a".repeat(64)] {
        assert!(access_control::valid_capability_name(ok), "{ok}");
    }
    for bad in [
        "",
        "Staff",
        "staff notes",
        "staff.notes",
        "é",
        &"a".repeat(65),
    ] {
        assert!(!access_control::valid_capability_name(bad), "{bad}");
    }
}

#[test]
fn a_superadmin_grants_and_revokes_any_capability_on_any_active_grant() {
    let sa = role("root", true);
    let granted =
        access_control::grant_capability(CapabilityActor::Superadmin(&sa), &agent(), "staff")
            .unwrap();
    assert!(granted.capabilities.contains("staff"));

    let revoked =
        access_control::revoke_capability(CapabilityActor::Superadmin(&sa), &granted, "staff")
            .unwrap();
    assert!(revoked.capabilities.is_empty());
}

#[test]
fn a_superadmin_may_change_its_own_grant() {
    let sa = role("root", true);
    let own = mapping(sa.clone(), bc("helpdesk"), AccessLevel::Admin);
    let granted =
        access_control::grant_capability(CapabilityActor::Superadmin(&sa), &own, "staff").unwrap();
    assert!(granted.capabilities.contains("staff"));
}

#[test]
fn only_an_active_superadmin_acts_as_one() {
    let not_super = role("nobody", false);
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Superadmin(&not_super),
            &agent(),
            "staff"
        )),
        "not_superadmin"
    );
    let mut revoked = role("root", true);
    revoked.status = RoleStatus::Revoked;
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Superadmin(&revoked),
            &agent(),
            "staff"
        )),
        "role_not_active"
    );
}

#[test]
fn a_malformed_name_is_refused_but_an_undeclared_one_is_not() {
    let sa = role("root", true);
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Superadmin(&sa),
            &agent(),
            "Staff!"
        )),
        "invalid_capability_name"
    );
    // Nothing requires "never-declared" and that is fine: types may
    // register after grants are made.
    access_control::grant_capability(CapabilityActor::Superadmin(&sa), &agent(), "never-declared")
        .unwrap();
}

#[test]
fn granting_twice_or_beyond_sixteen_is_refused() {
    let sa = role("root", true);
    let once =
        access_control::grant_capability(CapabilityActor::Superadmin(&sa), &agent(), "staff")
            .unwrap();
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Superadmin(&sa),
            &once,
            "staff"
        )),
        "capability_not_added"
    );
    let mut full = agent();
    for i in 0..access_control::MAX_CAPABILITIES {
        full.capabilities.insert(format!("c{i}"));
    }
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Superadmin(&sa),
            &full,
            "one-more"
        )),
        "capability_not_added"
    );
}

#[test]
fn revoking_a_capability_not_held_is_refused() {
    let sa = role("root", true);
    assert_eq!(
        code(access_control::revoke_capability(
            CapabilityActor::Superadmin(&sa),
            &agent(),
            "staff"
        )),
        "capability_not_revoked"
    );
}

#[test]
fn a_revoked_grant_cannot_be_changed() {
    let sa = role("root", true);
    let mut gone = agent();
    gone.status = RoleStatus::Revoked;
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Superadmin(&sa),
            &gone,
            "staff"
        )),
        "grant_not_active"
    );
}

#[test]
fn an_admin_hands_out_and_takes_back_only_what_it_may_delegate() {
    let admin = admin_delegating(&["staff"]);
    let granted =
        access_control::grant_capability(CapabilityActor::Admin(&admin), &agent(), "staff")
            .unwrap();
    assert!(granted.capabilities.contains("staff"));
    let revoked =
        access_control::revoke_capability(CapabilityActor::Admin(&admin), &granted, "staff")
            .unwrap();
    assert!(revoked.capabilities.is_empty());

    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Admin(&admin),
            &agent(),
            "finance"
        )),
        "capability_not_delegable"
    );
}

#[test]
fn an_admin_cannot_change_its_own_grant_even_for_a_delegable_capability() {
    let admin = admin_delegating(&["staff"]);
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Admin(&admin),
            &admin,
            "staff"
        )),
        "self_grant_refused"
    );
    let mut holding = admin.clone();
    holding.capabilities.insert("staff".into());
    assert_eq!(
        code(access_control::revoke_capability(
            CapabilityActor::Admin(&admin),
            &holding,
            "staff"
        )),
        "self_grant_refused"
    );
}

#[test]
fn an_admin_cannot_reach_another_bounded_context() {
    let admin = admin_delegating(&["staff"]);
    let elsewhere = mapping(role("agent", false), bc("billing"), AccessLevel::Write);
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Admin(&admin),
            &elsewhere,
            "staff"
        )),
        "grant_bounded_context_mismatch"
    );
}

#[test]
fn only_an_admin_level_grant_acts_as_an_admin() {
    let mut writer = admin_delegating(&["staff"]);
    writer.level = AccessLevel::Write;
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Admin(&writer),
            &agent(),
            "staff"
        )),
        "insufficient_access_level"
    );
    let mut revoked = admin_delegating(&["staff"]);
    revoked.status = RoleStatus::Revoked;
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Admin(&revoked),
            &agent(),
            "staff"
        )),
        "grant_not_active"
    );
}

#[test]
fn holding_a_capability_is_not_delegating_it_and_the_reverse() {
    // Delegating without holding: the admin manages the team without
    // being able to read what the team reads.
    let admin = admin_delegating(&["staff"]);
    assert!(!access_control::mapping_holds_capability(
        &admin,
        Some("staff")
    ));
    // Holding without delegating: no power to hand it out.
    let mut holder = mapping(role("holder", false), bc("helpdesk"), AccessLevel::Admin);
    holder.capabilities.insert("staff".into());
    assert_eq!(
        code(access_control::grant_capability(
            CapabilityActor::Admin(&holder),
            &agent(),
            "staff"
        )),
        "capability_not_delegable"
    );
}

#[test]
fn nothing_but_the_capability_opens_a_gate() {
    let sa = role("root", true);
    let mut m = mapping(sa, bc("helpdesk"), AccessLevel::Admin);
    m.delegable.insert("staff".into());
    assert!(!access_control::mapping_holds_capability(&m, Some("staff")));
    assert!(access_control::mapping_holds_capability(&m, None));
    m.capabilities.insert("staff".into());
    assert!(access_control::mapping_holds_capability(&m, Some("staff")));
    assert!(!access_control::mapping_holds_capability(&m, Some("staf")));
}

#[test]
fn only_a_superadmin_sets_delegable_and_only_on_an_admin_grant() {
    let sa = role("root", true);
    let names: std::collections::BTreeSet<String> = ["staff".to_string()].into();

    let set = access_control::set_delegable_capabilities(
        &sa,
        &mapping(role("admin", false), bc("helpdesk"), AccessLevel::Admin),
        names.clone(),
    )
    .unwrap();
    assert_eq!(set.delegable, names);

    assert_eq!(
        code(access_control::set_delegable_capabilities(
            &role("admin", false),
            &admin_delegating(&[]),
            names.clone()
        )),
        "not_superadmin"
    );
    assert_eq!(
        code(access_control::set_delegable_capabilities(
            &sa,
            &agent(),
            names.clone()
        )),
        "insufficient_access_level"
    );
    assert_eq!(
        code(access_control::set_delegable_capabilities(
            &sa,
            &admin_delegating(&[]),
            ["Bad Name".to_string()].into()
        )),
        "invalid_capability_name"
    );
}

#[test]
fn lowering_delegable_leaves_what_was_already_granted() {
    let sa = role("root", true);
    let admin = admin_delegating(&["staff"]);
    let granted =
        access_control::grant_capability(CapabilityActor::Admin(&admin), &agent(), "staff")
            .unwrap();
    let lowered =
        access_control::set_delegable_capabilities(&sa, &admin, Default::default()).unwrap();
    assert!(lowered.delegable.is_empty());
    // The pure functions never reach back into other grants: the agent
    // keeps what it was given.
    assert!(granted.capabilities.contains("staff"));
}
