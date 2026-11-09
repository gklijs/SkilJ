//! Tests for the `EventQuery` surface (`specs/skilj.allium`) - propagated
//! after `projection_registration.rs` ([docs/architecture.md §9](../../docs/architecture.md#next-steps)): rules
//! `QueryEvents`/`CountEvents`/`InspectEvent`, and a real (empty-
//! `sensitive_fields`-case) `render_event`.
//!
//! Obligations covered here (from `allium plan specs/skilj.allium`,
//! filtered to this pass's source constructs): 12 of 15 total.
//! Uncovered, with reason - see the doc comment at the bottom of this
//! file: `surface-actor`/`surface-exposure`/`surface-provides.EventQuery`
//! (3) - the usual GraphQL-scaffolding gap.

use chrono::{TimeZone, Utc};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::error::SkiljRejection;
use skilj_core::event_store::{
    self, BoundedContext, BoundedContextStatus, Event, EventOrigin, EventType,
};
use skilj_core::shared::{Metadata, Tag};

// ---------------------------------------------------------------------
// Fixtures
// ---------------------------------------------------------------------

fn timestamp(secs: i64) -> chrono::DateTime<Utc> {
    Utc.timestamp_opt(secs, 0).unwrap()
}

fn bounded_context(status: BoundedContextStatus) -> BoundedContext {
    BoundedContext {
        name: "orders".into(),
        status,
        created_at: timestamp(0),
        created_by: skilj_core::bootstrap::ContextCreator::SystemCreator,
        template: None,
    }
}

fn access_mapping(status: RoleStatus, level: AccessLevel) -> RoleAccessMapping {
    RoleAccessMapping {
        role: Role {
            id: "role-1".into(),
            external_subject: "admin@example.com".into(),
            name: "Admin".into(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: timestamp(0),
            revoked_at: None,
        },
        bounded_context: bounded_context(BoundedContextStatus::Active),
        level,
        can_read_sensitive: false,
        scope: None,
        capabilities: Default::default(),
        delegable: Default::default(),
        status,
        created_at: timestamp(0),
        revoked_at: None,
    }
}

fn event_type(name: &str) -> EventType {
    EventType {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        name: name.into(),
        schema: "{}".into(),
        schema_version: 1,
        tag_mappings: Vec::new(),
        owner_tag_key: None,
        sensitive_fields: Vec::new(),
        private_fields: Vec::new(),
        blob_fields: Vec::new(),
        external_creation_allowed: false,
        direct_creation_allowed: false,
        system_triggered_allowed: false,
        system_triggered_schedule: None,
        missed_occurrence_policy: None,
        schedule_position: None,
        last_fired_at: None,
        event_read_allowed: false,
    }
}

fn tag(key: &str, value: &str) -> Tag {
    Tag {
        key: key.into(),
        value: Some(value.into()),
    }
}

fn event(event_type: EventType, sequence: i64, payload: &str, tags: Vec<Tag>) -> Event {
    Event {
        bounded_context: bounded_context(BoundedContextStatus::Active),
        event_type,
        payload: payload.into(),
        metadata: Metadata {
            r#type: "OrderPlaced".into(),
            version: 1,
            client_id: "someone".into(),
            created_at: timestamp(0),
            correlation_id: None,
            causation_id: None,
        },
        sequence,
        tags,
        encryption_keys: Vec::new(),
        origin: EventOrigin::DirectlyCreated,
    }
}

// ---------------------------------------------------------------------
// render_event - direct coverage of the new black-box logic itself
// ---------------------------------------------------------------------

#[test]
fn render_event_passes_the_payload_through_unchanged_when_no_fields_are_sensitive() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let e = event(event_type("OrderPlaced"), 0, r#"{"amount":10}"#, Vec::new());

    assert_eq!(
        event_store::render_event(&e, &mapping, &|_, _| unreachable!(), &[]),
        r#"{"amount":10}"#
    );
}

// ---------------------------------------------------------------------
// rule-success.QueryEvents / rule-failure.QueryEvents.{1,2,3}
// ---------------------------------------------------------------------

#[test]
fn query_events_returns_only_matching_later_events_rendered() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let et = event_type("OrderPlaced");
    let events = vec![
        event(et.clone(), 0, "{}", Vec::new()),
        event(et.clone(), 1, r#"{"amount":10}"#, Vec::new()),
        event(et.clone(), 2, r#"{"amount":20}"#, Vec::new()),
    ];

    let rendered = event_store::query_events(
        &mapping,
        &[],
        None,
        Some(0),
        None,
        &events,
        |_, _| unreachable!(),
        &[],
    )
    .unwrap();

    assert_eq!(
        rendered,
        vec![
            (1, r#"{"amount":10}"#.to_string()),
            (2, r#"{"amount":20}"#.to_string())
        ]
    );
}

#[test]
fn query_events_with_no_after_sequence_starts_from_the_beginning() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let et = event_type("OrderPlaced");
    let events = vec![event(et, 0, "first", Vec::new())];

    let rendered = event_store::query_events(
        &mapping,
        &[],
        None,
        None,
        None,
        &events,
        |_, _| unreachable!(),
        &[],
    )
    .unwrap();

    assert_eq!(rendered, vec![(0, "first".to_string())]);
}

/// An empty `event_types` means "no restriction", not "no results" - the
/// unrestricted state itself, per the spec's own convention.
#[test]
fn query_events_with_empty_event_types_matches_every_type() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let events = vec![
        event(event_type("OrderPlaced"), 0, "a", Vec::new()),
        event(event_type("OrderCancelled"), 1, "b", Vec::new()),
    ];

    let rendered = event_store::query_events(
        &mapping,
        &[],
        None,
        None,
        None,
        &events,
        |_, _| unreachable!(),
        &[],
    )
    .unwrap();

    assert_eq!(rendered.len(), 2);
}

#[test]
fn query_events_filters_by_named_event_type() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let placed = event_type("OrderPlaced");
    let cancelled = event_type("OrderCancelled");
    let events = vec![
        event(placed.clone(), 0, "a", Vec::new()),
        event(cancelled, 1, "b", Vec::new()),
    ];

    let rendered = event_store::query_events(
        &mapping,
        &[placed],
        None,
        None,
        None,
        &events,
        |_, _| unreachable!(),
        &[],
    )
    .unwrap();

    assert_eq!(rendered, vec![(0, "a".to_string())]);
}

/// `tags` uses ANY-match semantics against an event's own derived tags -
/// the same convention `ProcessCommand`'s consistency boundary uses.
#[test]
fn query_events_filters_by_any_matching_tag() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let et = event_type("OrderPlaced");
    let events = vec![
        event(et.clone(), 0, "a", vec![tag("account", "A")]),
        event(et, 1, "b", vec![tag("account", "B")]),
    ];

    let rendered = event_store::query_events(
        &mapping,
        &[],
        Some(&[tag("account", "A")]),
        None,
        None,
        &events,
        |_, _| unreachable!(),
        &[],
    )
    .unwrap();

    assert_eq!(rendered, vec![(0, "a".to_string())]);
}

/// Events outside the grant's own bounded context never appear, even
/// though `bounded_context_events` here carries one.
#[test]
fn query_events_never_returns_events_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let foreign_context = BoundedContext {
        name: "billing".into(),
        ..bounded_context(BoundedContextStatus::Active)
    };
    let foreign_event = Event {
        bounded_context: foreign_context,
        ..event(event_type("InvoiceIssued"), 0, "a", Vec::new())
    };

    let rendered = event_store::query_events(
        &mapping,
        &[],
        None,
        None,
        None,
        &[foreign_event],
        |_, _| unreachable!(),
        &[],
    )
    .unwrap();

    assert!(rendered.is_empty());
}

/// rule-failure.QueryEvents.1 - `requires: access_mapping.status = active`.
#[test]
fn query_events_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);

    let err = event_store::query_events(
        &mapping,
        &[],
        None,
        None,
        None,
        &[],
        |_, _| unreachable!(),
        &[],
    )
    .unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.QueryEvents.2 - `requires: access_mapping.level = admin`.
#[test]
fn query_events_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);

    let err = event_store::query_events(
        &mapping,
        &[],
        None,
        None,
        None,
        &[],
        |_, _| unreachable!(),
        &[],
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.QueryEvents.3 - `requires: event_types.all(et =>
/// et.bounded_context = access_mapping.bounded_context)`.
#[test]
fn query_events_rejects_a_named_event_type_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let foreign_type = EventType {
        bounded_context: BoundedContext {
            name: "billing".into(),
            ..bounded_context(BoundedContextStatus::Active)
        },
        ..event_type("InvoiceIssued")
    };

    let err = event_store::query_events(
        &mapping,
        &[foreign_type],
        None,
        None,
        None,
        &[],
        |_, _| unreachable!(),
        &[],
    )
    .unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::EventTypeNotInBoundedContext.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.CountEvents / rule-failure.CountEvents.{1,2,3}
// ---------------------------------------------------------------------

#[test]
fn count_events_counts_every_matching_event_regardless_of_sequence() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let et = event_type("OrderPlaced");
    let events = vec![
        event(et.clone(), 0, "a", Vec::new()),
        event(et, 1, "b", Vec::new()),
    ];

    let count = event_store::count_events(&mapping, &[], None, None, &events).unwrap();

    assert_eq!(count, 2);
}

#[test]
fn count_events_with_empty_event_types_matches_every_type() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let events = vec![
        event(event_type("OrderPlaced"), 0, "a", Vec::new()),
        event(event_type("OrderCancelled"), 1, "b", Vec::new()),
    ];

    let count = event_store::count_events(&mapping, &[], None, None, &events).unwrap();

    assert_eq!(count, 2);
}

#[test]
fn count_events_filters_by_named_event_type_and_tag() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let et = event_type("OrderPlaced");
    let events = vec![
        event(et.clone(), 0, "a", vec![tag("account", "A")]),
        event(et.clone(), 1, "b", vec![tag("account", "B")]),
        event(
            event_type("OrderCancelled"),
            2,
            "c",
            vec![tag("account", "A")],
        ),
    ];

    let count =
        event_store::count_events(&mapping, &[et], Some(&[tag("account", "A")]), None, &events)
            .unwrap();

    assert_eq!(count, 1);
}

/// rule-failure.CountEvents.1
#[test]
fn count_events_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);

    let err = event_store::count_events(&mapping, &[], None, None, &[]).unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.CountEvents.2
#[test]
fn count_events_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);

    let err = event_store::count_events(&mapping, &[], None, None, &[]).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.CountEvents.3
#[test]
fn count_events_rejects_a_named_event_type_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let foreign_type = EventType {
        bounded_context: BoundedContext {
            name: "billing".into(),
            ..bounded_context(BoundedContextStatus::Active)
        },
        ..event_type("InvoiceIssued")
    };

    let err = event_store::count_events(&mapping, &[foreign_type], None, None, &[]).unwrap_err();

    assert_eq!(
        err.code(),
        event_store::Error::EventTypeNotInBoundedContext.code()
    );
}

// ---------------------------------------------------------------------
// rule-success.InspectEvent / rule-failure.InspectEvent.{1,2,3}
// ---------------------------------------------------------------------

#[test]
fn inspect_event_succeeds_and_delivers_the_event_alongside_its_rendered_payload() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let e = event(event_type("OrderPlaced"), 5, r#"{"amount":10}"#, Vec::new());

    let inspected = event_store::inspect_event(&mapping, &e, |_, _| unreachable!(), &[]).unwrap();

    assert_eq!(inspected.event, e);
    assert_eq!(inspected.rendered_payload, r#"{"amount":10}"#);
}

/// rule-failure.InspectEvent.1 - `requires: access_mapping.status = active`.
#[test]
fn inspect_event_rejects_a_revoked_mapping() {
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);
    let e = event(event_type("OrderPlaced"), 0, "{}", Vec::new());

    let err = event_store::inspect_event(&mapping, &e, |_, _| unreachable!(), &[]).unwrap_err();

    assert_eq!(err.code(), access_control::Error::GrantNotActive.code());
}

/// rule-failure.InspectEvent.2 - `requires: access_mapping.level = admin`.
#[test]
fn inspect_event_rejects_a_write_level_mapping() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);
    let e = event(event_type("OrderPlaced"), 0, "{}", Vec::new());

    let err = event_store::inspect_event(&mapping, &e, |_, _| unreachable!(), &[]).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::InsufficientAccessLevel.code()
    );
}

/// rule-failure.InspectEvent.3 - `requires: access_mapping.bounded_context = event.bounded_context`.
#[test]
fn inspect_event_rejects_an_event_from_another_bounded_context() {
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let foreign_context = BoundedContext {
        name: "billing".into(),
        ..bounded_context(BoundedContextStatus::Active)
    };
    let e = Event {
        bounded_context: foreign_context,
        ..event(event_type("InvoiceIssued"), 0, "{}", Vec::new())
    };

    let err = event_store::inspect_event(&mapping, &e, |_, _| unreachable!(), &[]).unwrap_err();

    assert_eq!(
        err.code(),
        access_control::Error::GrantBoundedContextMismatch.code()
    );
}

// ---------------------------------------------------------------------
// surface-actor/surface-exposure/surface-provides.EventQuery - uncovered
// ---------------------------------------------------------------------
//
// GraphQL-scaffolding gap, same as every prior admin surface's deferred
// pair/triple (see command_processing.rs's header comment) - no
// resolver/schema wiring in skilj-graphql yet.

/// SQL is runtime checked: compare selection/counts against the independent
/// pure rules, including current policy changes after a plan was prepared.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authorized_sql_matches_pure_rules_and_cursor_progress() {
    use skilj_core::db::{self, AuthorizedEventQuery};
    let Some(url) = skilj_test_support::database_url("skilj_authorized_events_test").await else {
        eprintln!("SKIPPED: Postgres unavailable");
        return;
    };
    let pool = db::connect(&url).await.unwrap();
    db::migrate(&pool).await.unwrap();
    let bc = bounded_context(BoundedContextStatus::Active);
    db::insert_bounded_context(&pool, &bc).await.unwrap();
    let mut owned = event_type("Owned");
    owned.owner_tag_key = Some("owner".into());
    let mut other = event_type("Other");
    other.owner_tag_key = Some("tenant".into());
    let public = event_type("Public");
    for ty in [&owned, &other, &public] {
        db::upsert_event_type(&pool, ty).await.unwrap();
    }
    for sequence in 0..48 {
        let ty = [&owned, &other, &public][sequence as usize % 3].clone();
        let mut tags = vec![tag("group", if sequence % 2 == 0 { "a" } else { "b" })];
        match sequence % 4 {
            0 => tags.extend([tag("owner", "alice"), tag("tenant", "bob")]),
            1 => tags.extend([tag("owner", "bob"), tag("tenant", "alice")]),
            2 => tags.push(Tag {
                key: "owner".into(),
                value: None,
            }),
            _ => {}
        }
        if sequence % 7 == 0 {
            tags.push(tags[0].clone());
        }
        let mut e = event(ty, sequence, "{\"amount\":10}", tags);
        e.metadata.correlation_id = (sequence % 5 != 0).then(|| format!("corr-{}", sequence % 2));
        db::insert_event(&pool, &e, None).await.unwrap();
    }
    let events = db::list_events_for_bounded_context(&pool, &bc.name)
        .await
        .unwrap();
    let mut mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let tag_cases = [
        None,
        Some(vec![]),
        Some(vec![tag("group", "a")]),
        Some(vec![
            tag("group", "a"),
            tag("owner", "alice"),
            tag("group", "a"),
        ]),
        Some(vec![Tag {
            key: "owner".into(),
            value: None,
        }]),
        Some(vec![tag("missing", "x")]),
    ];
    for scope in [None, Some("alice"), Some("bob"), Some("nobody")] {
        mapping.scope = scope.map(str::to_owned);
        for types in [
            vec![],
            vec![owned.clone()],
            vec![public.clone(), other.clone()],
        ] {
            for tags in &tag_cases {
                for correlation in [None, Some("corr-0"), Some("absent")] {
                    let query = AuthorizedEventQuery::for_admin(
                        &mapping,
                        &types,
                        tags.as_deref(),
                        Some(5),
                        Some(39),
                        correlation,
                    )
                    .unwrap();
                    let bounded: Vec<_> = events
                        .iter()
                        .filter(|e| e.sequence > 5 && e.sequence <= 39)
                        .cloned()
                        .collect();
                    let expected = event_store::query_events_select(
                        &mapping,
                        &types,
                        tags.as_deref(),
                        Some(5),
                        correlation,
                        &bounded,
                        4,
                    )
                    .unwrap();
                    let page =
                        db::collect_authorized_event_page(&pool, &query, 4, |rows, remaining| {
                            event_store::query_events_select(
                                &mapping,
                                &types,
                                tags.as_deref(),
                                Some(5),
                                correlation,
                                rows,
                                remaining,
                            )
                        })
                        .await
                        .unwrap();
                    assert_eq!(page.events, expected, "{scope:?} {tags:?} {correlation:?}");
                    let expected_count = event_store::count_events(
                        &mapping,
                        &types,
                        tags.as_deref(),
                        correlation,
                        &bounded,
                    )
                    .unwrap();
                    assert_eq!(
                        db::count_authorized_events(&pool, &query).await.unwrap(),
                        expected_count
                    );
                }
            }
        }
    }
    mapping.scope = Some("nobody".into());
    let query = AuthorizedEventQuery::for_admin(&mapping, &[owned.clone()], None, None, None, None)
        .unwrap();
    let empty = db::collect_authorized_event_page(&pool, &query, 4, |rows, _| Ok(rows.to_vec()))
        .await
        .unwrap();
    assert!(empty.events.is_empty());
    assert_eq!(empty.scanned_through, Some(45));

    // The plan was prepared while Owned required an owner. The SQL statement
    // must observe its new ownerless registration, just as fresh hydration does.
    owned.owner_tag_key = None;
    db::upsert_event_type(&pool, &owned).await.unwrap();
    assert_eq!(
        db::count_authorized_events(&pool, &query).await.unwrap(),
        16
    );
    let page = db::collect_authorized_event_page(&pool, &query, 4, |rows, _| Ok(rows.to_vec()))
        .await
        .unwrap();
    assert_eq!(page.events.len(), 4);
    assert_eq!(page.scanned_through, Some(9));
    owned.owner_tag_key = Some("tenant".into());
    db::upsert_event_type(&pool, &owned).await.unwrap();
    assert_eq!(db::count_authorized_events(&pool, &query).await.unwrap(), 0);

    // Residual payload filtering can span multiple SQL batches, without losing
    // position in the filtered tail. Zero capacity serves and advances nothing.
    mapping.scope = None;
    let query = AuthorizedEventQuery::for_admin(&mapping, &[], None, None, None, None).unwrap();
    let page = db::collect_authorized_event_page(&pool, &query, 3, |rows, _| {
        Ok(rows
            .iter()
            .filter(|e| e.sequence % 10 == 0)
            .cloned()
            .collect())
    })
    .await
    .unwrap();
    assert_eq!(
        page.events.iter().map(|e| e.sequence).collect::<Vec<_>>(),
        [0, 10, 20]
    );
    let empty = db::collect_authorized_event_page(&pool, &query, 0, |_, _| unreachable!())
        .await
        .unwrap();
    assert!(empty.events.is_empty());
    assert_eq!(empty.scanned_through, None);
    let page = db::collect_authorized_event_page(&pool, &query, 3, |_, _| Ok(vec![]))
        .await
        .unwrap();
    assert_eq!(page.scanned_through, Some(47));

    // An uncommitted append is excluded, and appears on the next count after
    // commit; no plaintext hydration or cache is involved in either count.
    let mut tx = pool.begin().await.unwrap();
    db::insert_event(&mut *tx, &event(public.clone(), 48, "{}", vec![]), None)
        .await
        .unwrap();
    assert_eq!(
        db::count_authorized_events(&pool, &query).await.unwrap(),
        48
    );
    tx.commit().await.unwrap();
    assert_eq!(
        db::count_authorized_events(&pool, &query).await.unwrap(),
        49
    );
    // An append between candidate batches cannot extend this request's bound.
    // A later request still sees it. Force the interleaving inside the first
    // residual predicate rather than depending on scheduling or sleeps.
    let mut appended = false;
    let page = db::collect_authorized_event_page(&pool, &query, 7, |_, _| {
        if !appended {
            appended = true;
            tokio::task::block_in_place(|| {
                tokio::runtime::Handle::current().block_on(db::insert_event(
                    &pool,
                    &event(public.clone(), 49, "{}", vec![]),
                    None,
                ))
            })
            .unwrap();
        }
        Ok(vec![])
    })
    .await
    .unwrap();
    assert_eq!(page.scanned_through, Some(48));
    assert_eq!(
        db::count_authorized_events(&pool, &query).await.unwrap(),
        50
    );
    pool.close().await;
}

#[test]
fn authorized_query_validates_before_database_access() {
    use skilj_core::db::AuthorizedEventQuery;
    let mapping = access_mapping(RoleStatus::Revoked, AccessLevel::Admin);
    assert!(AuthorizedEventQuery::for_admin(&mapping, &[], None, None, None, None).is_err());
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Write);
    assert!(AuthorizedEventQuery::for_admin(&mapping, &[], None, None, None, None).is_err());
    let mapping = access_mapping(RoleStatus::Active, AccessLevel::Admin);
    let mut foreign = event_type("Foreign");
    foreign.bounded_context.name = "foreign".into();
    assert!(AuthorizedEventQuery::for_admin(&mapping, &[foreign], None, None, None, None).is_err());
    let excessive = vec![tag("key", "value"); event_store::MAX_QUERY_TAGS + 1];
    assert!(
        AuthorizedEventQuery::for_admin(&mapping, &[], Some(&excessive), None, None, None).is_err()
    );
}
