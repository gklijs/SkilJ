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
    let database_url = skilj_test_support::database_url("skilj_event_fetch_rest_test").await?;

    connect_and_migrate(&database_url, "the test database").await?;
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
    setup_with_pool(skilj_core::db::PgPoolOptions::new().max_connections(4)).await
}

/// [`setup`] with the `Skilj`'s own pool options chosen by the caller.
async fn setup_with_pool(pool_options: skilj_core::db::PgPoolOptions) -> (Skilj, String, String) {
    let database_url = test_db()
        .await
        .expect("test_db() must be Some - caller already checked");
    setup_in(database_url, pool_options).await
}

/// [`setup_with_pool`] on a database of the caller's choosing.
async fn setup_in(
    database_url: String,
    pool_options: skilj_core::db::PgPoolOptions,
) -> (Skilj, String, String) {
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
        .pool_options(pool_options)
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

/// A consume holds its transaction's connection for the per-token lock.
/// Every concurrent consume of the same token waits on that lock holding a
/// connection too, so if the holder then needed a *second* pooled
/// connection to read events, a pool's worth of consumers would starve it
/// until the acquire timeout, and every one of them failed
/// (docs/architecture.md §116). Here more consumers than the pool has
/// connections all get an answer, well before the acquire timeout.
#[test]
fn more_concurrent_consumes_than_pool_connections_all_complete() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup_with_pool(
            skilj_core::db::PgPoolOptions::new()
                .max_connections(3)
                .acquire_timeout(std::time::Duration::from_secs(10)),
        )
        .await;
        let router = skilj.rest_router();
        for amount in 1..=3 {
            deposit(&router, &direct_credential, amount).await;
        }

        let consumes = (0..8).map(|_| {
            let router = router.clone();
            let credential = read_credential.clone();
            async move {
                let request = Request::builder()
                    .method("GET")
                    .uri("/v1/events/consume?mode=auto")
                    .header("authorization", format!("Bearer {credential}"))
                    .body(Body::empty())
                    .unwrap();
                let response = router.oneshot(request).await.unwrap();
                let status = response.status();
                let body = response.into_body().collect().await.unwrap().to_bytes();
                (status, String::from_utf8_lossy(&body).into_owned())
            }
        });
        let started = std::time::Instant::now();
        let results = tokio::time::timeout(
            std::time::Duration::from_secs(8),
            futures_util::future::join_all(consumes),
        )
        .await
        .expect("concurrent consumes stalled on the connection pool");
        for (status, body) in &results {
            assert_eq!(*status, StatusCode::OK, "{body}");
        }
        let served: usize = results
            .iter()
            .map(|(_, body)| {
                serde_json::from_str::<serde_json::Value>(body).unwrap()["events"]
                    .as_array()
                    .unwrap()
                    .len()
            })
            .sum();
        assert_eq!(served, 3, "auto mode serves each event exactly once");
        assert!(started.elapsed() < std::time::Duration::from_secs(8));
    });
}

/// The write side of the same starvation: every event write takes its
/// bounded context's sequence lock inside a transaction, and each
/// concurrent writer waits on that lock holding a connection. The holder
/// must finish on the connection it has (docs/architecture.md §116).
#[test]
fn more_concurrent_writes_than_pool_connections_all_complete() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup_with_pool(
            skilj_core::db::PgPoolOptions::new()
                .max_connections(3)
                .acquire_timeout(std::time::Duration::from_secs(10)),
        )
        .await;
        let router = skilj.rest_router();

        let writes = (1..=8).map(|amount| {
            let router = router.clone();
            let credential = direct_credential.clone();
            async move { deposit(&router, &credential, amount).await }
        });
        tokio::time::timeout(
            std::time::Duration::from_secs(8),
            futures_util::future::join_all(writes),
        )
        .await
        .expect("concurrent writes stalled on the connection pool");

        let page = get_json(&router, &read_credential, "/v1/events").await;
        let mut served = amounts(&page);
        served.sort();
        assert_eq!(served, (1..=8).collect::<Vec<_>>());
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

        // docs/architecture.md §112: a short page walked to the end of
        // history, so its cursor passes over the non-matching events it
        // examined instead of stopping at the last event served - a
        // narrow filter no longer re-walks them on every poll.
        let newest = get_json(&router, &reader, "/v1/events?filter=amount:in:8").await;
        let newest = newest["events"][0]["sequence"]
            .as_i64()
            .unwrap()
            .to_string();
        let page = get_json(&router, &reader, "/v1/events?filter=amount:in:1").await;
        assert_eq!(amounts(&page), vec![1]);
        assert_eq!(page["nextCursor"].as_str(), Some(newest.as_str()));
        let page = get_json(&router, &reader, "/v1/events?filter=amount:in:99").await;
        assert_eq!(amounts(&page), Vec::<i64>::new());
        assert_eq!(page["nextCursor"].as_str(), Some(newest.as_str()));
        // A full page stops at its last event: what follows wasn't examined.
        let page = get_json(&router, &reader, "/v1/events?filter=amount:in:1,2,3,4").await;
        assert_eq!(amounts(&page), vec![1, 2, 3]);
        let third = page["events"][2]["sequence"].as_i64().unwrap().to_string();
        assert_eq!(page["nextCursor"].as_str(), Some(third.as_str()));

        // An auto-advance consumer's cursor passes over them too: after a
        // poll matching nothing, an unfiltered poll has nothing left.
        let consumer = mint_read(None).await;
        let first = get_json(
            &router,
            &consumer,
            "/v1/events/consume?mode=auto&filter=amount:in:99",
        )
        .await;
        assert_eq!(amounts(&first), Vec::<i64>::new());
        let next = get_json(&router, &consumer, "/v1/events/consume").await;
        assert_eq!(amounts(&next), Vec::<i64>::new());

        // A manual-ack poll that serves nothing claims nothing: the next
        // poll gets a new event at once, not after the checkout lease
        // lapses - and it's the new event, the examined ones passed over.
        let manual = mint_read(None).await;
        let first = get_json(
            &router,
            &manual,
            "/v1/events/consume?mode=manual&filter=amount:in:99",
        )
        .await;
        assert_eq!(amounts(&first), Vec::<i64>::new());
        deposit(&router, &direct, 9).await;
        let next = get_json(&router, &manual, "/v1/events/consume").await;
        assert_eq!(amounts(&next), vec![9]);
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

/// Every outcome of resolving a bearer token, now that the right kind is
/// looked up first and the kind only on a miss (docs/architecture.md
/// §128): the right token serves, a wrong secret or an unknown id is 401,
/// and a real token of another kind is 403.
#[test]
fn token_resolution_answers_each_outcome() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();
        let status = |credential: String| {
            let router = router.clone();
            async move {
                let request = Request::builder()
                    .method("GET")
                    .uri("/v1/events")
                    .header("authorization", format!("Bearer {credential}"))
                    .body(Body::empty())
                    .unwrap();
                router.oneshot(request).await.unwrap().status()
            }
        };
        let (read_id, _) = read_credential.split_once('.').unwrap();
        assert_eq!(status(read_credential.clone()).await, StatusCode::OK);
        assert_eq!(
            status(format!("{read_id}.not-the-secret")).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(
            status(format!("{}.whatever", generate_token_id())).await,
            StatusCode::UNAUTHORIZED
        );
        // Another kind's id with the wrong secret says nothing about that
        // token (docs/architecture.md §142); its whole credential is a 403.
        let (direct_id, _) = direct_credential.split_once('.').unwrap();
        assert_eq!(
            status(format!("{direct_id}.not-the-secret")).await,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(status(direct_credential).await, StatusCode::FORBIDDEN);
    });
}

/// docs/architecture.md §140: `GET /v1/events/consume` loaded events
/// (for a new `Latest`/`AtTime` cursor, the type's whole history) before
/// its rule could refuse the request, and `GET /v1/events` a chunk, so a
/// revoked token, an invalid filter or a first call without a `mode` paid
/// for a scan every time. With the events table out of reach, each must
/// still get its own refusal rather than the scan's failure.
#[test]
fn a_refused_read_loads_no_events() {
    runtime().block_on(async {
        // A database of its own: the renamed `events` table below would
        // otherwise fail any other test's `build()` meanwhile, which warms
        // every bounded context's event cache.
        let Some(database_url) =
            skilj_test_support::database_url("skilj_event_fetch_rest_refused_read").await
        else {
            return;
        };
        if connect_and_migrate(&database_url, "the exclusive PostgreSQL database")
            .await
            .is_none()
        {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup_in(
            database_url.clone(),
            skilj_core::db::PgPoolOptions::new().max_connections(4),
        )
        .await;
        let router = skilj.rest_router();
        deposit(&router, &direct_credential, 5).await;

        let pool = db::connect(&database_url).await.unwrap();
        let token_id = read_credential.split('.').next().unwrap();
        let token = db::get_event_read_token(&pool, token_id)
            .await
            .unwrap()
            .unwrap();
        let schema = format!("\"bc_{}\"", token.event_type.bounded_context.name);
        let rename = |from: &str, to: &str| format!("ALTER TABLE {schema}.{from} RENAME TO {to}");
        sqlx::query(sqlx::AssertSqlSafe(rename("events", "events_unreachable")))
            .execute(&pool)
            .await
            .unwrap();

        let consume = |uri: &'static str| {
            let router = router.clone();
            let credential = read_credential.clone();
            async move {
                let request = Request::builder()
                    .method("GET")
                    .uri(uri)
                    .header("authorization", format!("Bearer {credential}"))
                    .body(Body::empty())
                    .unwrap();
                router.oneshot(request).await.unwrap().status()
            }
        };
        assert_eq!(consume("/v1/events/consume").await, StatusCode::BAD_REQUEST);
        assert_eq!(
            consume("/v1/events?filter=bogus_field:equals:x").await,
            StatusCode::BAD_REQUEST
        );
        assert_eq!(
            consume("/v1/events/consume?mode=auto&filter=bogus_field:equals:x").await,
            StatusCode::BAD_REQUEST
        );
        db::revoke_access_token(&pool, token_id, test_now())
            .await
            .unwrap();
        assert_eq!(
            consume("/v1/events/consume?mode=auto").await,
            StatusCode::FORBIDDEN
        );

        sqlx::query(sqlx::AssertSqlSafe(rename("events_unreachable", "events")))
            .execute(&pool)
            .await
            .unwrap();
    });
}

/// docs/architecture.md §148: an `after` past the latest committed
/// sequence isn't a cursor this route handed out - served, it would come
/// back unchanged as `nextCursor` and silently skip every event up to it.
/// Refused; the latest itself is an ordinary caught-up poll.
#[test]
fn get_events_refuses_a_cursor_past_the_latest_committed_sequence() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct_credential, read_credential) = setup().await;
        let router = skilj.rest_router();
        deposit(&router, &direct_credential, 5).await;
        let latest = get_json(&router, &read_credential, "/v1/events").await["events"][0]
            ["sequence"]
            .as_i64()
            .unwrap();

        let status = |after: i64| {
            let (router, credential) = (router.clone(), read_credential.clone());
            async move {
                let request = Request::builder()
                    .method("GET")
                    .uri(format!("/v1/events?after={after}"))
                    .header("authorization", format!("Bearer {credential}"))
                    .body(Body::empty())
                    .unwrap();
                router.oneshot(request).await.unwrap().status()
            }
        };
        assert_eq!(status(latest + 1000).await, StatusCode::BAD_REQUEST);
        assert_eq!(status(latest).await, StatusCode::OK);
    });
}

/// `GET /v1/events` with `query` (after `?`, may be empty): its status
/// and JSON body.
async fn get_events(
    router: &axum::Router,
    credential: &str,
    query: &str,
) -> (StatusCode, serde_json::Value) {
    let request = Request::builder()
        .uri(format!("/v1/events?{query}"))
        .header("authorization", format!("Bearer {credential}"))
        .body(Body::empty())
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&bytes).unwrap())
}

/// docs/architecture.md §176, against a real failover: an asynchronous
/// standby promoted in the primary's place, on the same port, without the
/// two events committed after its base backup. Their sequences are reused
/// by the next two. A cursor read before the failover is refused with
/// `epoch_changed` rather than skipping the new events, and the event
/// cache - which held the lost events, under a table OID the promotion
/// kept - serves the new history, not the old one.
#[test]
fn a_failover_refuses_old_cursors_and_the_cache_drops_the_lost_events() {
    runtime().block_on(async {
        let Some((server, database_url)) =
            skilj_test_support::FailoverServer::start("skilj_failover_test").await
        else {
            return;
        };
        db::migrate(&db::connect(&database_url).await.unwrap())
            .await
            .unwrap();
        let (skilj, direct, read) = setup_in(
            database_url.clone(),
            skilj_core::db::PgPoolOptions::new().max_connections(4),
        )
        .await;
        let router = skilj.rest_router();

        deposit(&router, &direct, 1).await;
        server.base_backup().unwrap();
        deposit(&router, &direct, 2).await;
        deposit(&router, &direct, 3).await;
        let (status, before) = get_events(&router, &read, "").await;
        assert_eq!(status, StatusCode::OK, "{before}");
        assert_eq!(amounts(&before), vec![1, 2, 3]);
        let old_epoch = before["epoch"].as_str().unwrap().to_string();
        let old_cursor = before["nextCursor"].as_str().unwrap().to_string();

        server.fail_over().unwrap();

        // The pool's connections to the old primary are dead; the first
        // requests may fail while it reconnects.
        let mut after = serde_json::Value::Null;
        for _ in 0..50 {
            let (status, body) = get_events(&router, &read, "").await;
            if status == StatusCode::OK {
                after = body;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        }
        assert_eq!(
            amounts(&after),
            vec![1],
            "only the backed-up event survives"
        );
        assert_ne!(after["epoch"].as_str().unwrap(), old_epoch);

        deposit(&router, &direct, 4).await;
        deposit(&router, &direct, 5).await;
        let (status, refused) = get_events(
            &router,
            &read,
            &format!("after={old_cursor}&epoch={old_epoch}"),
        )
        .await;
        assert_eq!(status, StatusCode::CONFLICT, "{refused}");
        assert_eq!(refused["code"], "epoch_changed");

        let (status, now) = get_events(&router, &read, "").await;
        assert_eq!(status, StatusCode::OK, "{now}");
        assert_eq!(
            amounts(&now),
            vec![1, 4, 5],
            "the cache must not keep serving the events the failover lost"
        );

        // Recorded at startup, so the change is reported once.
        let pool = db::connect(&database_url).await.unwrap();
        let current = db::current_epoch(&pool).await.unwrap();
        db::record_epoch(&pool, &current, test_now()).await.unwrap();
        assert_eq!(
            db::record_epoch(&pool, &current, test_now()).await.unwrap(),
            None,
            "an unchanged epoch is not reported"
        );
        drop(server);
    });
}

/// docs/architecture.md §176 without a failover: every read response
/// names the epoch, the same one each time, and a position sent back
/// with any other epoch is refused with `409 epoch_changed` - a cursor
/// on `GET /v1/events`, a consumed sequence on the acknowledgement. Sent
/// back with the epoch it came with, or with none, it's served as before.
#[test]
fn positions_from_another_epoch_are_refused() {
    runtime().block_on(async {
        if test_db().await.is_none() {
            return;
        }
        let (skilj, direct, read) = setup().await;
        let router = skilj.rest_router();
        deposit(&router, &direct, 1).await;

        let (status, page) = get_events(&router, &read, "").await;
        assert_eq!(status, StatusCode::OK, "{page}");
        let epoch = page["epoch"].as_str().unwrap().to_string();
        let cursor = page["nextCursor"].as_str().unwrap().to_string();
        let (status, refused) =
            get_events(&router, &read, &format!("after={cursor}&epoch=elsewhere-1")).await;
        assert_eq!(status, StatusCode::CONFLICT, "{refused}");
        assert_eq!(refused["code"], "epoch_changed");
        let (status, same) =
            get_events(&router, &read, &format!("after={cursor}&epoch={epoch}")).await;
        assert_eq!(status, StatusCode::OK, "{same}");
        assert_eq!(same["epoch"], epoch.as_str());

        let consumed = get_json(&router, &read, "/v1/events/consume?mode=manual").await;
        assert_eq!(consumed["epoch"], epoch.as_str());
        let sequence = consumed["events"][0]["sequence"].as_i64().unwrap();
        let ack = |epoch: &str| {
            let body = format!(r#"{{"sequence":{sequence},"epoch":"{epoch}"}}"#);
            let request = Request::builder()
                .method("POST")
                .uri("/v1/events/consume/ack")
                .header("authorization", format!("Bearer {read}"))
                .header("content-type", "application/json")
                .body(Body::from(body))
                .unwrap();
            router.clone().oneshot(request)
        };
        assert_eq!(
            ack("elsewhere-1").await.unwrap().status(),
            StatusCode::CONFLICT
        );
        assert_eq!(ack(&epoch).await.unwrap().status(), StatusCode::OK);
        assert_eq!(post_ack(&router, &read, sequence).await, StatusCode::OK);
    });
}
