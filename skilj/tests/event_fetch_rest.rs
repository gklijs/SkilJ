//! End-to-end test for `GET /v1/events`'s real `filter=field:op:value`
//! wire shape (docs/architecture.md §7.3) - a real HTTP request, through
//! `Skilj::rest_router()`, exercising `valid_filters`/`matches_filters`
//! for real over the actual REST surface, not just the pure-function
//! layer (`skilj-core/tests/event_filtering.rs` covers that). Same
//! `DATABASE_URL`-then-embedded-Postgres-then-skip harness as
//! `skilj/tests/command_trigger.rs` - see its own doc comment for the
//! details, not repeated a third time here.

use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::{SubsecRound, Utc};
use http_body_util::BodyExt;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use skilj::{EventType, Skilj};
use skilj_core::access_control::{
    self, AccessLevel, EventReadStartPosition, Role, RoleAccessMapping, RoleStatus,
};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};
use tower::ServiceExt;

// --- fixtures ---

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
struct MoneyDepositedPayload {
    amount: i64,
}

struct MoneyDeposited;

impl EventType for MoneyDeposited {
    type Payload = MoneyDepositedPayload;
    const NAME: &'static str = "MoneyDeposited";
    fn direct_creation_allowed() -> bool {
        true
    }
    fn event_read_allowed() -> bool {
        true
    }
}

// --- provisioning: DATABASE_URL, else embedded Postgres, else skip ---

struct TestDb {
    database_url: String,
}

static TEST_DB: tokio::sync::OnceCell<Option<TestDb>> = tokio::sync::OnceCell::const_new();

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Runtime::new()
            .expect("failed to build a tokio runtime for event_fetch_rest tests")
    })
}

async fn test_db() -> Option<String> {
    TEST_DB
        .get_or_init(provision)
        .await
        .as_ref()
        .map(|db| db.database_url.clone())
}

async fn connect_and_migrate(database_url: &str, label: &str) -> Option<()> {
    let pool = match db::connect(database_url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to {label} failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating {label} failed: {e}");
        return None;
    }
    Some(())
}

async fn provision() -> Option<TestDb> {
    let database_url = if let Ok(database_url) = std::env::var("DATABASE_URL") {
        database_url
    } else {
        let url = skilj_test_support::database_url("skilj_event_fetch_rest_test").await?;
        connect_and_migrate(&url, "embedded PostgreSQL").await?;
        return Some(TestDb { database_url: url });
    };

    connect_and_migrate(&database_url, "DATABASE_URL").await?;
    Some(TestDb { database_url })
}

fn unique_name(prefix: &str) -> String {
    format!("{prefix}_{}", generate_token_id())
}

fn test_now() -> chrono::DateTime<Utc> {
    Utc::now().trunc_subsecs(6)
}

/// Builds a fully reconciled `Skilj` plus real, minted
/// `DirectCreationToken`/`EventReadToken` credentials for `MoneyDeposited`.
async fn setup() -> (Skilj, String, String) {
    let database_url = test_db()
        .await
        .expect("test_db() must be Some - caller already checked");
    let pool = db::connect(&database_url).await.unwrap();

    let external_subject = unique_name("subject");
    let role = Role {
        id: generate_token_id(),
        external_subject: external_subject.clone(),
        name: "Reconciliation Role".to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role(&pool, &role).await.unwrap();

    let bc_name = unique_name("banking");
    let bc = BoundedContext {
        name: bc_name.clone(),
        status: BoundedContextStatus::Active,
        created_at: test_now(),
        created_by: ContextCreator::SystemCreator,
        template: None,
    };
    db::insert_bounded_context(&pool, &bc).await.unwrap();

    let mapping = RoleAccessMapping {
        role: role.clone(),
        bounded_context: bc.clone(),
        level: AccessLevel::Admin,
        can_read_sensitive: false,
        scope: None,
        status: RoleStatus::Active,
        created_at: test_now(),
        revoked_at: None,
    };
    db::insert_role_access_mapping(&pool, &mapping)
        .await
        .unwrap();

    // Small per-instance pool: every test leaks a live `Skilj`, so the default
    // 10-connection pools of ~10 tests exhaust the shared embedded Postgres's
    // 100 connections and the last `build()` fails with `PoolTimedOut`.

    let (skilj, report) = Skilj::builder(database_url)
        .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
        .bounded_context(bc_name.clone())
        .event_type::<MoneyDeposited>()
        .reconciliation_role(external_subject)
        .build()
        .await
        .unwrap();
    assert_eq!(report.skipped_no_access, Vec::<String>::new());

    let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
        .await
        .unwrap()
        .unwrap();

    let direct_token = access_control::create_direct_creation_token(
        &mapping,
        &event_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        test_now(),
    )
    .unwrap();
    db::insert_direct_creation_token(&pool, &direct_token)
        .await
        .unwrap();
    let direct_credential = format!("{}.{}", direct_token.id, direct_token.secret);

    let read_token = access_control::create_event_read_token(
        &mapping,
        &event_type,
        generate_token_id(),
        generate_token_secret(),
        None,
        None,
        None,
        None,
        test_now(),
    )
    .unwrap();
    db::insert_event_read_token(&pool, &read_token)
        .await
        .unwrap();
    let read_credential = format!("{}.{}", read_token.id, read_token.secret);

    (skilj, direct_credential, read_credential)
}

async fn deposit(router: &axum::Router, credential: &str, amount: i64) {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/events/direct")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(format!(
            r#"{{"payload":{{"amount":{amount}}}}}"#
        )))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    assert_eq!(response.status(), StatusCode::CREATED);
}

#[test]
fn get_events_filter_param_narrows_results_for_real_over_rest() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        deposit(&router, &direct_credential, 5).await;
        deposit(&router, &direct_credential, 20).await;

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events?filter=amount:greater_than:10")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let events = json["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["payload"]["amount"], 20);
    });
}

/// Codeberg issue #18 - `GET /v1/events?correlationId=...` narrows to
/// exactly the one transaction named, over a real REST round trip: a
/// direct event posted with an explicit `correlationId` body field comes
/// back with that same id on `metadata.correlationId`, and is the only
/// one `?correlationId=` finds among several unrelated events.
#[test]
fn get_events_correlation_id_param_narrows_results_for_real_over_rest() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        deposit(&router, &direct_credential, 5).await;

        let request = Request::builder()
            .method("POST")
            .uri("/v1/events/direct")
            .header("authorization", format!("Bearer {direct_credential}"))
            .header("content-type", "application/json")
            .body(Body::from(
                r#"{"payload":{"amount":99},"correlationId":"rest-corr-1"}"#,
            ))
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::CREATED);

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events?correlationId=rest-corr-1")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let events = json["events"].as_array().unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["payload"]["amount"], 99);
        assert_eq!(events[0]["metadata"]["correlationId"], "rest-corr-1");
    });
}

/// Drift audit finding #8 (2026-08-20, see project memory
/// `skilj-drift-audit-2026-08-20`): `surface EventFetch`'s own `exposes:
/// event_type.name/schema/schema_version` had no REST route returning
/// `schema`/`schema_version` at all - only the bare name, per delivered
/// event. Checks both `FetchEvents` and `ConsumeEvents`, since the fix
/// touches both responses independently.
#[test]
fn get_events_and_consume_expose_the_real_event_type_schema() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        deposit(&router, &direct_credential, 5).await;

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["eventTypeName"], "MoneyDeposited");
        assert_eq!(json["eventTypeSchemaVersion"], 1);
        let schema = json["eventTypeSchema"].as_str().unwrap();
        assert!(
            schema.contains("amount"),
            "the real MoneyDepositedPayload schema must be returned, not a placeholder: {schema}"
        );

        let request = Request::builder()
            .method("GET")
            // mode=auto - a brand new cursor must state its own mode
            // (rule-failure.ConsumeEvents.4), and this token has never
            // consumed before.
            .uri("/v1/events/consume?mode=auto")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["eventTypeName"], "MoneyDeposited");
        assert_eq!(json["eventTypeSchemaVersion"], 1);
        assert!(json["eventTypeSchema"].as_str().unwrap().contains("amount"));
    });
}

/// One of the new operators (docs/architecture.md's filterable-scalar-types
/// pass) exercised through the real REST surface, not just the
/// pure-function layer (`skilj-core/tests/event_filtering.rs` covers the
/// full matrix, including the format-gated ones) - proves `In`'s wire
/// parsing (`parse_filter_param`) actually reaches `valid_filters`/
/// `matches_filters` end to end. `Near`/`SimilarColor`/`InSubnet` share
/// the identical parsing plumbing (`parse_filter_param`'s own match), just
/// gated to a different `format` - not re-proven per-operator here.
#[test]
fn get_events_filter_param_supports_the_in_operator_for_real_over_rest() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        deposit(&router, &direct_credential, 5).await;
        deposit(&router, &direct_credential, 20).await;
        deposit(&router, &direct_credential, 99).await;

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events?filter=amount:in:5,20")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(
            status,
            StatusCode::OK,
            "body: {}",
            String::from_utf8_lossy(&body)
        );
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();

        let events = json["events"].as_array().unwrap();
        let amounts: Vec<i64> = events
            .iter()
            .map(|e| e["payload"]["amount"].as_i64().unwrap())
            .collect();
        assert_eq!(amounts.len(), 2, "amounts: {amounts:?}");
        assert!(amounts.contains(&5));
        assert!(amounts.contains(&20));
        assert!(!amounts.contains(&99));
    });
}

#[test]
fn get_events_rejects_a_malformed_filter_param_with_400() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, _direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events?filter=amount-only-no-colons")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    });
}

#[test]
fn get_events_rejects_a_filter_naming_an_undeclared_field_with_400() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, _direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events?filter=bogus_field:equals:x")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    });
}

/// Real end-to-end regression guard for the `hash_secret`-at-rest fix
/// (`AccessToken.secret` - "stored hashed and never compared in
/// plaintext"): a known token `id` with any secret other than the real
/// one is still an "unrecognised credential" 401, the same as an
/// entirely unknown `id` - proving `resolve_event_read_token`'s
/// `secret_matches(hash_secret(presented), token.secret)` compare (both
/// sides hashed now `token.secret` is the DB's stored hash) actually
/// discriminates right from wrong, not just accepting or rejecting
/// everything by accident.
/// `skilj-core/tests/persistence.rs`'s own round-trip tests are what
/// prove the *storage* half - that `secret` never lands in Postgres as
/// plaintext at all.
/// The scenario this feature exists for, over the real `GET
/// /v1/events/consume` wire path rather than the pure-function layer
/// `skilj-core/tests/event_fetch_surface.rs` already covers: a token
/// minted `startFrom: LATEST` *after* history already exists must never
/// serve that history, even on its very first call, while a genuinely
/// new occurrence afterward is served exactly as an ordinary
/// `startFrom: BEGINNING` token's would be. Doesn't reuse `setup()` -
/// this needs its own `RoleAccessMapping`/`EventType` in scope to mint a
/// second read token *after* the historical deposits have already
/// happened, which `setup()`'s own opaque `(Skilj, String, String)`
/// return doesn't expose.
#[test]
fn a_latest_token_never_serves_history_that_predates_its_own_minting() {
    runtime().block_on(async {
        let Some(database_url) = test_db().await else {
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();

        let external_subject = unique_name("subject");
        let role = Role {
            id: generate_token_id(),
            external_subject: external_subject.clone(),
            name: "Reconciliation Role".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role(&pool, &role).await.unwrap();

        let bc_name = unique_name("banking");
        let bc = BoundedContext {
            name: bc_name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &bc).await.unwrap();

        let mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping)
            .await
            .unwrap();

        // Small per-instance pool: every test leaks a live `Skilj`, so the default
        // 10-connection pools of ~10 tests exhaust the shared embedded Postgres's
        // 100 connections and the last `build()` fails with `PoolTimedOut`.

        let (skilj, report) = Skilj::builder(database_url)
            .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());
        let router = skilj.rest_router();

        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        let direct_token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct_token)
            .await
            .unwrap();
        let direct_credential = format!("{}.{}", direct_token.id, direct_token.secret);

        // Two "historical" deposits, committed before the latest token
        // is even minted.
        deposit(&router, &direct_credential, 5).await;
        deposit(&router, &direct_credential, 20).await;

        let latest_token = access_control::create_event_read_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            Some(EventReadStartPosition::Latest),
            None,
            None,
            test_now(),
        )
        .unwrap();
        db::insert_event_read_token(&pool, &latest_token)
            .await
            .unwrap();
        let latest_credential = format!("{}.{}", latest_token.id, latest_token.secret);

        // First call, `mode=auto` (a brand-new cursor must state its own
        // mode) - must serve nothing at all, even though two deposits
        // already exist.
        let request = Request::builder()
            .method("GET")
            .uri("/v1/events/consume?mode=auto")
            .header("authorization", format!("Bearer {latest_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json["events"].as_array().unwrap().len(),
            0,
            "a latest token's first call must never serve pre-existing history: {json:?}"
        );

        // A genuinely new deposit, after the latest token was minted -
        // this one must be served.
        deposit(&router, &direct_credential, 99).await;

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events/consume?mode=auto")
            .header("authorization", format!("Bearer {latest_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let events = json["events"].as_array().unwrap();
        assert_eq!(events.len(), 1, "events: {events:?}");
        assert_eq!(events[0]["payload"]["amount"], 99);
    });
}

/// `at_sequence`'s own distinguishing value over `latest`, over the real
/// wire: an admin can mint a token that replays *some* history from a
/// chosen cutoff, even history that already existed before the token
/// itself was minted - impossible with `latest` (nothing before minting
/// time) or `beginning` (everything). Deposit 0 stays unserved (at the
/// cutoff), deposit 1 - already historical relative to minting - is
/// served anyway, and a genuinely new deposit afterward is served too.
#[test]
fn an_at_sequence_token_replays_history_after_a_chosen_cutoff_but_not_before_it() {
    runtime().block_on(async {
        let Some(database_url) = test_db().await else {
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();

        let external_subject = unique_name("subject");
        let role = Role {
            id: generate_token_id(),
            external_subject: external_subject.clone(),
            name: "Reconciliation Role".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role(&pool, &role).await.unwrap();

        let bc_name = unique_name("banking");
        let bc = BoundedContext {
            name: bc_name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &bc).await.unwrap();

        let mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping)
            .await
            .unwrap();

        // Small per-instance pool: every test leaks a live `Skilj`, so the default
        // 10-connection pools of ~10 tests exhaust the shared embedded Postgres's
        // 100 connections and the last `build()` fails with `PoolTimedOut`.

        let (skilj, report) = Skilj::builder(database_url)
            .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());
        let router = skilj.rest_router();

        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        let direct_token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct_token)
            .await
            .unwrap();
        let direct_credential = format!("{}.{}", direct_token.id, direct_token.secret);

        deposit(&router, &direct_credential, 5).await; // sequence 0 - the cutoff itself
        deposit(&router, &direct_credential, 7).await; // sequence 1 - historical, but after the cutoff

        let at_sequence_token = access_control::create_event_read_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            Some(EventReadStartPosition::AtSequence),
            Some(0),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_event_read_token(&pool, &at_sequence_token)
            .await
            .unwrap();
        let at_sequence_credential =
            format!("{}.{}", at_sequence_token.id, at_sequence_token.secret);

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events/consume?mode=auto")
            .header("authorization", format!("Bearer {at_sequence_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let events = json["events"].as_array().unwrap();
        assert_eq!(events.len(), 1, "events: {events:?}");
        assert_eq!(events[0]["payload"]["amount"], 7);

        deposit(&router, &direct_credential, 99).await; // sequence 2 - genuinely new

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events/consume?mode=auto")
            .header("authorization", format!("Bearer {at_sequence_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let events = json["events"].as_array().unwrap();
        assert_eq!(events.len(), 1, "events: {events:?}");
        assert_eq!(events[0]["payload"]["amount"], 99);
    });
}

/// `at_time`'s own version of the test above - real wall-clock gaps
/// (`sleep`) around the cutoff rather than a chosen sequence number, so
/// the cutoff sits provably strictly between the two deposits'
/// `metadata.created_at` and no same-second ambiguity can make the test
/// flaky either way.
#[test]
fn an_at_time_token_replays_history_after_a_chosen_cutoff_but_not_before_it() {
    runtime().block_on(async {
        let Some(database_url) = test_db().await else {
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();

        let external_subject = unique_name("subject");
        let role = Role {
            id: generate_token_id(),
            external_subject: external_subject.clone(),
            name: "Reconciliation Role".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role(&pool, &role).await.unwrap();

        let bc_name = unique_name("banking");
        let bc = BoundedContext {
            name: bc_name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &bc).await.unwrap();

        let mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: bc.clone(),
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping)
            .await
            .unwrap();

        // Small per-instance pool: every test leaks a live `Skilj`, so the default
        // 10-connection pools of ~10 tests exhaust the shared embedded Postgres's
        // 100 connections and the last `build()` fails with `PoolTimedOut`.

        let (skilj, report) = Skilj::builder(database_url)
            .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        assert_eq!(report.skipped_no_access, Vec::<String>::new());
        let router = skilj.rest_router();

        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        let direct_token = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct_token)
            .await
            .unwrap();
        let direct_credential = format!("{}.{}", direct_token.id, direct_token.secret);

        deposit(&router, &direct_credential, 5).await; // before the cutoff - excluded
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        let cutoff = Utc::now();
        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
        deposit(&router, &direct_credential, 7).await; // after the cutoff, but before minting - still served

        let at_time_token = access_control::create_event_read_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            Some(EventReadStartPosition::AtTime),
            None,
            Some(cutoff),
            test_now(),
        )
        .unwrap();
        db::insert_event_read_token(&pool, &at_time_token)
            .await
            .unwrap();
        let at_time_credential = format!("{}.{}", at_time_token.id, at_time_token.secret);

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events/consume?mode=auto")
            .header("authorization", format!("Bearer {at_time_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.clone().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let events = json["events"].as_array().unwrap();
        assert_eq!(events.len(), 1, "events: {events:?}");
        assert_eq!(events[0]["payload"]["amount"], 7);

        deposit(&router, &direct_credential, 99).await; // genuinely new

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events/consume?mode=auto")
            .header("authorization", format!("Bearer {at_time_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        let events = json["events"].as_array().unwrap();
        assert_eq!(events.len(), 1, "events: {events:?}");
        assert_eq!(events[0]["payload"]["amount"], 99);
    });
}

#[test]
fn get_events_rejects_a_wrong_secret_for_a_known_token_id_with_401() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, _direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        let (id, real_secret) = read_credential.split_once('.').unwrap();
        let wrong_secret = generate_token_secret();
        assert_ne!(wrong_secret, real_secret);
        let wrong_credential = format!("{id}.{wrong_secret}");

        let request = Request::builder()
            .method("GET")
            .uri("/v1/events")
            .header("authorization", format!("Bearer {wrong_credential}"))
            .body(Body::empty())
            .unwrap();
        let response = router.oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    });
}

/// Codeberg issue #25's investigation (docs/architecture.md §53) - the
/// real end-to-end proof, not just the pure-function layer
/// (`skilj-core/tests/event_fetch_surface.rs` covers that): two genuinely
/// concurrent `GET /v1/events/consume?mode=manual` calls for the same
/// token, real HTTP through `Skilj::rest_router()`, `tokio::join!` the
/// same real-concurrency pattern this codebase's own catch-up races use.
/// Before this fix both would have been served the identical batch -
/// exactly the gap that let two Kafka/AMQP/NATS bridge instances of the
/// same `OutboundMapping` double-publish. Now exactly one must be.
#[test]
fn two_concurrent_manual_consumes_never_both_serve_the_same_batch() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();

        deposit(&router, &direct_credential, 5).await;

        let request_a = Request::builder()
            .method("GET")
            .uri("/v1/events/consume?mode=manual")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();
        let request_b = Request::builder()
            .method("GET")
            .uri("/v1/events/consume?mode=manual")
            .header("authorization", format!("Bearer {read_credential}"))
            .body(Body::empty())
            .unwrap();

        let (response_a, response_b) = tokio::join!(
            router.clone().oneshot(request_a),
            router.clone().oneshot(request_b),
        );
        let response_a = response_a.unwrap();
        let response_b = response_b.unwrap();
        assert_eq!(response_a.status(), StatusCode::OK);
        assert_eq!(response_b.status(), StatusCode::OK);

        let served_a = response_a.into_body().collect().await.unwrap().to_bytes();
        let served_b = response_b.into_body().collect().await.unwrap().to_bytes();
        let json_a: serde_json::Value = serde_json::from_slice(&served_a).unwrap();
        let json_b: serde_json::Value = serde_json::from_slice(&served_b).unwrap();
        let count_a = json_a["events"].as_array().unwrap().len();
        let count_b = json_b["events"].as_array().unwrap().len();

        assert_eq!(
            count_a + count_b,
            1,
            "exactly one of two concurrent manual-ack consumes must serve the one \
             deposit, never both (double-publish) and never neither (lost delivery): \
             a={count_a} b={count_b}"
        );
    });
}

async fn get_json(router: &axum::Router, credential: &str, uri: &str) -> serde_json::Value {
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .header("authorization", format!("Bearer {credential}"))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(status, StatusCode::OK, "{json}");
    json
}

fn amounts(page: &serde_json::Value) -> Vec<i64> {
    page["events"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["payload"]["amount"].as_i64().unwrap())
        .collect()
}

/// `config.max_events_per_read`: every read serves at most that many
/// events (here 3, which also makes each history chunk 3 events), in
/// sequence order, and continues exactly where it stopped - `GET
/// /v1/events` via `nextCursor`, consume via its server-side cursor. The
/// cap counts *served* events: a filtered page spans as many chunks as
/// it takes to fill. A new `Latest` consumer's seed is found by scanning
/// every chunk, not just the first.
#[test]
fn reads_serve_bounded_pages_and_continue_where_they_stopped() {
    runtime().block_on(async {
        let Some(database_url) = test_db().await else {
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();
        let external_subject = unique_name("subject");
        let role = Role {
            id: generate_token_id(),
            external_subject: external_subject.clone(),
            name: "Reconciliation Role".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role(&pool, &role).await.unwrap();
        let bc_name = unique_name("banking");
        let bc = BoundedContext {
            name: bc_name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &bc).await.unwrap();
        let mapping = RoleAccessMapping {
            role,
            bounded_context: bc,
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping)
            .await
            .unwrap();
        let (skilj, _) = Skilj::builder(database_url)
            .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .reconciliation_role(external_subject)
            .max_events_per_read(3)
            // A two-event cache window: earlier chunks come from
            // Postgres (`LIMIT`ed), the tail from the cache - both of
            // `for_each_event_chunk`'s paths.
            .event_cache_warm_up_count(2)
            .build()
            .await
            .unwrap();
        let router = skilj.rest_router();
        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        let mint_read = |start_from: Option<EventReadStartPosition>| {
            let token = access_control::create_event_read_token(
                &mapping,
                &event_type,
                generate_token_id(),
                generate_token_secret(),
                None,
                start_from,
                None,
                None,
                test_now(),
            )
            .unwrap();
            let pool = pool.clone();
            async move {
                db::insert_event_read_token(&pool, &token).await.unwrap();
                format!("{}.{}", token.id, token.secret)
            }
        };
        let direct = access_control::create_direct_creation_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            test_now(),
        )
        .unwrap();
        db::insert_direct_creation_token(&pool, &direct)
            .await
            .unwrap();
        let direct = format!("{}.{}", direct.id, direct.secret);
        for amount in 1..=7 {
            deposit(&router, &direct, amount).await;
        }

        // GET /v1/events pages through all seven via nextCursor.
        let reader = mint_read(None).await;
        let mut uri = "/v1/events".to_string();
        let mut pages = Vec::new();
        loop {
            let page = get_json(&router, &reader, &uri).await;
            let served = amounts(&page);
            if served.is_empty() {
                break;
            }
            pages.push(served);
            uri = format!("/v1/events?after={}", page["nextCursor"].as_str().unwrap());
        }
        assert_eq!(pages, vec![vec![1, 2, 3], vec![4, 5, 6], vec![7]]);

        // A filtered page fills from as many chunks as it takes.
        let page = get_json(&router, &reader, "/v1/events?filter=amount:in:1,3,5,7").await;
        assert_eq!(amounts(&page), vec![1, 3, 5]);

        // Consume pages the same way, off its own cursor.
        let consumer = mint_read(None).await;
        let mut consumed = Vec::new();
        for call in 0..4 {
            let uri = if call == 0 {
                "/v1/events/consume?mode=auto"
            } else {
                "/v1/events/consume"
            };
            consumed.push(amounts(&get_json(&router, &consumer, uri).await));
        }
        assert_eq!(
            consumed,
            vec![vec![1, 2, 3], vec![4, 5, 6], vec![7], Vec::<i64>::new()]
        );

        // A Latest consumer minted now seeds at the true latest event
        // (seventh, found in the third chunk), so it sees only what comes
        // after.
        let latest = mint_read(Some(EventReadStartPosition::Latest)).await;
        let first = get_json(&router, &latest, "/v1/events/consume?mode=auto").await;
        assert_eq!(amounts(&first), Vec::<i64>::new());
        deposit(&router, &direct, 8).await;
        let next = get_json(&router, &latest, "/v1/events/consume").await;
        assert_eq!(amounts(&next), vec![8]);
    });
}

async fn post_ack(router: &axum::Router, credential: &str, sequence: i64) -> StatusCode {
    let request = Request::builder()
        .method("POST")
        .uri("/v1/events/consume/ack")
        .header("authorization", format!("Bearer {credential}"))
        .header("content-type", "application/json")
        .body(Body::from(format!(r#"{{"sequence":{sequence}}}"#)))
        .unwrap();
    router.clone().oneshot(request).await.unwrap().status()
}

/// Two acknowledgements racing on one manual-ack cursor must never move
/// it backwards (`rule AcknowledgeEvents`' `sequence >= cursor.sequence`).
/// A trigger stalls the ack of 10 inside its own write (holding the row
/// lock); the ack of 5 meanwhile reads the still-uncommitted cursor,
/// passes its check against it, and queues on the row lock.
/// Unserialized, it then overwrites 10 with 5 - the cursor regresses and
/// events 6-10 would be redelivered. Serialized with consume's per-token
/// lock, the ack of 5 waits for the whole ack of 10 and then re-reads the
/// cursor, so it is refused as a regression and the cursor stays at 10.
#[test]
fn concurrent_acknowledgements_never_move_the_cursor_backwards() {
    runtime().block_on(async {
        let Some(database_url) = test_db().await else {
            return;
        };
        let pool = db::connect(&database_url).await.unwrap();
        let external_subject = unique_name("subject");
        let role = Role {
            id: generate_token_id(),
            external_subject: external_subject.clone(),
            name: "Reconciliation Role".to_string(),
            superadmin: false,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role(&pool, &role).await.unwrap();
        let bc_name = unique_name("banking");
        let bc = BoundedContext {
            name: bc_name.clone(),
            status: BoundedContextStatus::Active,
            created_at: test_now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        };
        db::insert_bounded_context(&pool, &bc).await.unwrap();
        let mapping = RoleAccessMapping {
            role,
            bounded_context: bc,
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: test_now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping)
            .await
            .unwrap();
        let (skilj, _) = Skilj::builder(database_url)
            .pool_options(skilj_core::db::PgPoolOptions::new().max_connections(4))
            .bounded_context(bc_name.clone())
            .event_type::<MoneyDeposited>()
            .reconciliation_role(external_subject)
            .build()
            .await
            .unwrap();
        let router = skilj.rest_router();
        let event_type = db::get_event_type(&pool, &bc_name, "MoneyDeposited")
            .await
            .unwrap()
            .unwrap();
        let token = access_control::create_event_read_token(
            &mapping,
            &event_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            None,
            None,
            None,
            test_now(),
        )
        .unwrap();
        db::insert_event_read_token(&pool, &token).await.unwrap();
        let reader = format!("{}.{}", token.id, token.secret);
        // Creates the manual-ack cursor (at -1, nothing to serve).
        get_json(&router, &reader, "/v1/events/consume?mode=manual").await;

        let schema = format!("\"bc_{bc_name}\"");
        for ddl in [
            format!(
                "CREATE FUNCTION {schema}.stall_ack_of_10() RETURNS trigger LANGUAGE plpgsql AS \
                 $$ BEGIN IF NEW.sequence = 10 THEN PERFORM pg_sleep(0.5); END IF; RETURN NEW; END $$"
            ),
            format!(
                "CREATE TRIGGER stall_ack_of_10 BEFORE UPDATE ON {schema}.read_cursors \
                 FOR EACH ROW EXECUTE FUNCTION {schema}.stall_ack_of_10()"
            ),
        ] {
            sqlx::query(sqlx::AssertSqlSafe(ddl))
                .execute(&pool)
                .await
                .unwrap();
        }

        let (ten, five) = tokio::join!(post_ack(&router, &reader, 10), async {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            post_ack(&router, &reader, 5).await
        });
        assert_eq!(ten, StatusCode::OK);
        // Serialized, the ack of 5 sees the committed 10 and is refused.
        assert_eq!(five, StatusCode::CONFLICT);
        let cursor = db::get_read_cursor(&pool, &token).await.unwrap().unwrap();
        assert_eq!(cursor.sequence, 10, "the cursor regressed");
    });
}
