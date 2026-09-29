//! docs/architecture.md §109: the bootstrap secret ends at the first
//! claim (`entity BootstrapSecret`), and "no active superadmin" is checked
//! atomically with the claim (`rule CreateSuperadmin`). Its own test
//! binary and database: whether an active superadmin exists is global
//! state, which any other test claiming one would disturb.

use axum::body::Body;
use axum::http::Request;
use http_body_util::BodyExt;
use serde_json::{json, Value};
use skilj::Skilj;
use skilj_core::db::{self, Pool};
use tower::ServiceExt;

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: std::sync::OnceLock<tokio::runtime::Runtime> = std::sync::OnceLock::new();
    RUNTIME.get_or_init(|| tokio::runtime::Runtime::new().unwrap())
}

async fn test_database() -> Option<(String, Pool)> {
    let url = match std::env::var("DATABASE_URL") {
        Ok(url) => url,
        Err(_) => skilj_test_support::database_url("skilj_superadmin_bootstrap_test").await?,
    };
    let pool = match db::connect(&url).await {
        Ok(pool) => pool,
        Err(e) => {
            eprintln!("skipping: connecting to PostgreSQL failed: {e}");
            return None;
        }
    };
    if let Err(e) = db::migrate(&pool).await {
        eprintln!("skipping: migrating PostgreSQL failed: {e}");
        return None;
    }
    Some((url, pool))
}

async fn start(database_url: &str) -> (Skilj, axum::Router) {
    let (skilj, _) = Skilj::builder(database_url)
        .pool_options(db::PgPoolOptions::new().max_connections(2))
        .build()
        .await
        .unwrap();
    let router = skilj.graphql_router().await.unwrap();
    (skilj, router)
}

async fn claim(router: &axum::Router, secret: &str, subject: &str) -> Value {
    let body = json!({
        "query": "mutation($secret: String!, $subject: String!) { \
            createSuperadmin(bootstrapSecret: $secret, name: \"Root\", externalSubject: $subject) { id } }",
        "variables": { "secret": secret, "subject": subject },
    });
    let request = Request::builder()
        .method("POST")
        .uri("/graphql")
        .header("content-type", "application/json")
        .body(Body::from(body.to_string()))
        .unwrap();
    let response = router.clone().oneshot(request).await.unwrap();
    serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
}

async fn active_superadmins(pool: &Pool) -> i64 {
    let (count,): (i64,) =
        sqlx::query_as("SELECT count(*) FROM roles WHERE superadmin AND status = 'active'")
            .fetch_one(pool)
            .await
            .unwrap();
    count
}

#[test]
fn the_bootstrap_secret_ends_at_the_first_claim_and_claims_cannot_race() {
    runtime().block_on(async {
        let Some((database_url, pool)) = test_database().await else {
            return;
        };
        assert_eq!(active_superadmins(&pool).await, 0);

        // Two instances start before anyone has claimed: each holds its own
        // secret. They claim at the same time; exactly one may win.
        let (a, router_a) = start(&database_url).await;
        let (b, router_b) = start(&database_url).await;
        let secret_a = a
            .bootstrap_secret()
            .expect("no superadmin yet, so A holds a secret");
        let secret_b = b
            .bootstrap_secret()
            .expect("no superadmin yet, so B holds a secret");
        let (claim_a, claim_b) = tokio::join!(
            claim(&router_a, &secret_a, "root-a"),
            claim(&router_b, &secret_b, "root-b"),
        );
        let won = [&claim_a, &claim_b]
            .iter()
            .filter(|r| r.get("errors").is_none())
            .count();
        assert_eq!(won, 1, "{claim_a} / {claim_b}");
        assert_eq!(active_superadmins(&pool).await, 1);
        // Both secrets have ended: the winner's by its claim, the loser's
        // because a superadmin now exists.
        assert_eq!(a.bootstrap_secret(), None);
        assert_eq!(b.bootstrap_secret(), None);

        // Every superadmin revoked: the old, printed secret must not work
        // again on a process that never restarted.
        sqlx::query("UPDATE roles SET status = 'revoked', revoked_at = now() WHERE superadmin")
            .execute(&pool)
            .await
            .unwrap();
        let (old_router, old_secret) = if claim_a.get("errors").is_none() {
            (&router_a, &secret_a)
        } else {
            (&router_b, &secret_b)
        };
        let reused = claim(old_router, old_secret, "root-again").await;
        assert_eq!(
            reused["errors"][0]["extensions"]["code"], "bootstrap_secret_unavailable",
            "{reused}"
        );
        assert_eq!(active_superadmins(&pool).await, 0);

        // The recovery path: a restart while no active superadmin exists
        // generates a new secret, which works.
        let (c, router_c) = start(&database_url).await;
        let fresh = c
            .bootstrap_secret()
            .expect("no active superadmin, so a new secret");
        assert_ne!(&fresh, old_secret);
        let recovered = claim(&router_c, &fresh, "root-recovered").await;
        assert!(recovered.get("errors").is_none(), "{recovered}");
        assert_eq!(active_superadmins(&pool).await, 1);

        // The race, made deterministic: a claim that read "no active
        // superadmin" before another instance's claim committed. The pure
        // rule accepts that stale snapshot; the insert must still refuse.
        let secret = skilj_core::bootstrap::BootstrapSecret {
            secret: "stale".to_string(),
        };
        let stale = skilj_core::bootstrap::create_superadmin(
            &secret,
            "stale",
            "Late".to_string(),
            "root-late".to_string(),
            &[],
            skilj_core::shared::generate_token_id(),
            chrono::Utc::now(),
        )
        .unwrap();
        assert!(!db::insert_superadmin_if_none_active(&pool, &stale)
            .await
            .unwrap());
        assert_eq!(active_superadmins(&pool).await, 1);
    });
}
