//! Configuration (environment):
//!   DATABASE_URL              postgres connection string
//!   SKILJ_MASTER_KEY          base64 32-byte local master key
//!   SKILJ_BLIND_KEY           blind index root key (optional)
//!   SKILJ_JWT_ISSUER / SKILJ_JWT_AUDIENCE
//!   SKILJ_JWT_JWKS_URL        or SKILJ_JWT_HS256_SECRET
//!   SKILJ_JWT_SUBJECT_CLAIM   default "sub"
//!   SKILJ_RECONCILE_AS        external subject of the reconciliation Role
//!   SKILJ_BIND                default 0.0.0.0:8080

use std::sync::Arc;

use jsonwebtoken::Algorithm;
use skilj_core::crypto::LocalMasterKey;
use skilj_core::Skilj;
use skilj_server::auth::{JwtConfig, KeySource, Verifier};

fn env(k: &str) -> Option<String> {
    std::env::var(k).ok().filter(|v| !v.is_empty())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();
    let master = LocalMasterKey::from_base64(
        "local-1",
        &env("SKILJ_MASTER_KEY").ok_or("SKILJ_MASTER_KEY is required")?,
    )?;
    let mut b = Skilj::builder()
        .database_url(env("DATABASE_URL").ok_or("DATABASE_URL is required")?)
        .master_key(Arc::new(master))
        .context(skilj_demo::bank_decl());
    if let Some(k) = env("SKILJ_BLIND_KEY") {
        b = b.blind_index_root_key(k.into_bytes());
    }
    if let Some(r) = env("SKILJ_RECONCILE_AS") {
        b = b.reconcile_as(r);
    }
    let skilj = b.start().await?;
    let (algorithm, keys) = match (env("SKILJ_JWT_JWKS_URL"), env("SKILJ_JWT_HS256_SECRET")) {
        (Some(url), _) => (Algorithm::RS256, KeySource::JwksUrl(url)),
        (None, Some(secret)) => (Algorithm::HS256, KeySource::Secret(secret.into_bytes())),
        _ => return Err("configure SKILJ_JWT_JWKS_URL or SKILJ_JWT_HS256_SECRET".into()),
    };
    let verifier = Verifier::new(JwtConfig {
        issuer: env("SKILJ_JWT_ISSUER").ok_or("SKILJ_JWT_ISSUER is required")?,
        audience: env("SKILJ_JWT_AUDIENCE").ok_or("SKILJ_JWT_AUDIENCE is required")?,
        algorithm,
        subject_claim: env("SKILJ_JWT_SUBJECT_CLAIM").unwrap_or_else(|| "sub".into()),
        keys,
    });
    let bind = env("SKILJ_BIND").unwrap_or_else(|| "0.0.0.0:8080".into());
    let listener = tokio::net::TcpListener::bind(&bind).await?;
    tracing::info!("listening on {bind}");
    axum::serve(listener, skilj_server::router(skilj, verifier)).await?;
    Ok(())
}
