//! A real, runnable server for the demo bounded contexts in
//! `skilj_demo::{banking, courses}` - `cargo run -p skilj-demo --bin
//! server`. Not a test: this boots an actual `axum` process serving both
//! REST and GraphQL, prints `CommandToken` credentials for every
//! `rest_trigger_allowed` command, and then serves until killed. Point
//! `curl` at the printed examples, or a GraphQL client at `/graphql`.
//!
//! Needs `DATABASE_URL` pointing at a real Postgres (`PORT` optionally
//! overrides the default `8080`). Every run is safe to repeat against the
//! same database: bounded contexts are only created if they don't exist
//! yet, and each run mints its own fresh admin `Role` and `CommandToken`s
//! rather than reusing a previous run's.
//!
//! **Also the reference example for tracing, logging, and metrics.**
//! `skilj-core`/`skilj-rest`/`skilj-graphql`/`skilj` only ever emit
//! `tracing` spans/events and record measurements through
//! `opentelemetry::global::meter(...)` - none of them install a
//! subscriber or a `MeterProvider` (see docs/architecture.md's tracing
//! section). `init_telemetry` below is where a real consuming app
//! actually does that: always a console `fmt` layer, and - only when
//! `OTEL_EXPORTER_OTLP_ENDPOINT` is set - all three OpenTelemetry signals
//! over OTLP/HTTP: trace spans via `tracing-opentelemetry`'s layer, every
//! `tracing::info!`/`warn!`/etc. event *also* exported as a correlated
//! OTel log record via `opentelemetry-appender-tracing`'s bridge, and the
//! counters/histograms already recorded throughout the codebase exported
//! as OTel metrics - no new call sites needed anywhere for any of the
//! three, since they all instrument code that already exists. `cargo
//! run` works with no collector present either way.
//!
//! **The bootstrap below is a shortcut, not the intended production
//! flow.** It seeds a `Role`/`RoleAccessMapping` directly via
//! `skilj_core::db`, the same way every end-to-end test in this
//! repository does (see e.g. `skilj/tests/command_trigger.rs`'s own
//! `setup()`) - convenient for a self-contained demo binary, but a real
//! deployment doesn't have code that writes to `roles`/
//! `role_access_mappings` directly. There, a human claims the
//! once-only bootstrap secret `Skilj::builder(...).build()` prints
//! (`ClosesPermanentlyOnFirstClaim`) to create the first superadmin, and
//! everything past that - creating bounded contexts, granting access -
//! happens over the GraphQL admin console (docs/architecture.md §6,
//! `entity AccessManagement`).
//!
//! **Also a real `identity_provider`, for the same shortcut reason.**
//! GraphQL's Role-based auth (`skilj-graphql::auth::verify_jwt_to_role`)
//! always requires a JWT verified against a configured IdP - there is no
//! bypass, by design (`entity AccessManagement`'s whole point is that
//! SkilJ never authenticates callers itself). Without one, nothing could
//! ever call GraphQL as the seeded admin `Role` above - only the REST
//! `CommandToken`s below would work. `serve_local_jwks`/`sign_jwt` spin
//! up a tiny local JWKS endpoint signing with a fixed, publicly-known
//! test RSA keypair (the same one `skilj/tests/graphql_admin_console.rs`
//! already uses) - never a real secret, since this only ever answers
//! requests from this same process's own loopback interface. A real
//! deployment points `.identity_provider(...)` at its own actual IdP
//! instead; this is `cargo run`'s own "so there's something to try
//! GraphQL with at all" shortcut, printed alongside the command tokens
//! below.

use chrono::Utc;
use jsonwebtoken::{EncodingKey, Header};
use serde_json::json;
use skilj::{IdpConfig, SigningAlgorithm, Skilj};
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db;
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};

// --- local JWKS/IdP shortcut - see this file's own doc comment above ---
//
// The exact same fixed test RSA keypair `skilj/tests/graphql_admin_console.rs`
// and `skilj-core/tests/jwt_verification.rs` already use - never a real
// secret (see the doc comment above), so reusing it here rather than
// generating a fresh one is simpler with no downside.

const TEST_PRIVATE_KEY_PEM: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQDPHVFsUHiWXSbG
/TCig1cTQHNT6FnoYoZtMEjvDiQArsOL/dFoM9pmGRM9CfEtQGNum4TsimPtgJec
awfdPnW0uJCRlIF9wGmYdh2mYNBKw8jqxwp664Gd5uqH5L6A4pN8bfGO7+2niD6p
8t0cNeyYOd0PusbAEDcpzCUZmr6KQyM5i8/wk5oO98gntp+ZpMjUZabAD6R8DyhM
IZmV645jo5NPJG7zuSz+3dmKkNY0/GXz8YwvZ2swqmmOANRZHHfN1vgP2ycK02WZ
4yihx6EiuQCDseddBw+xit9KSvSq6GwmwnV1qVpMVNlSGGOeVX7v7JQ3z/BNbQ85
5p6s/FjhAgMBAAECggEAFu8fKghLIhNUjOpSbVxv0vDrFFqBQitOyV50ZQxCzlSL
0L+dZZWAVJfoOnUUYLdli0TrVioI4K7Bmw97AnO9IvLhB03TfPJGfxxtMhQ8XFsL
r3u03GGhq7N7OusIcUslm7ys5/AHd+qtTbJX65zJAx49LVW4VmI1SYqSfSBWgway
8uGYaXyCfwuxQ+xB4fQd6llm/+9dqS+U36LVSMWgEmVjceorYFhPVLfuX4A1wHjF
mDl40AwPBqzVbOIzFDMDikk4heFi6wlt6N3LGDtyBUUuzEg5TBhyiirvNvTjW+4V
Z4MZs3tez+IqM0+F4EsgAEQUU12YQxa4lobm8/zgZQKBgQD81FMzymNR6xWhUSwY
4RtkVntfMBOMp1rVGcVyBxOLKxEXF6ctk2rV38krfUI50h/lWzrbpl+zJvEe8D1H
vZjYj28sL3wf0CSnPYUeGANTxrW1dTiz1HVzzChfbAEWj3fsVrlghNcnHBkDDhqz
L/rPEfp//fB0SyLAEAJt87cgFwKBgQDRtjtH1gIkGn5GCS3u0FAbxV+qrUlTvu4t
Di1GcEw32jootQQSMZN1PxEvLuehaBlaASEL2OZzZlQ4q60LV1Jisvd7wqv5EYnG
o+sKtrCS5iXKfkxqTmg+JS7OZazggyvgBnv4GXT0US6/G4nw7C9JaS2jyOvPGIPS
K8dsWDIxxwKBgQCgr4FBxTticPqKUECqf0cdeilm0fNazXJZRcvLMNwm8vQlrQ6/
VJXt4BDG5xEUFovXBShfOVpRTkqo0x7fXYyq9l49wuAsh+kDsYHNIo3azMvny9yB
zmHnerWeD9KROBWLy4J96W+kl6L94hTuFWxd9psyhX4xKx+m2YXxw5d7eQKBgFB2
I86PHOkvRQ2oDfiX8nSFSQxaSk0Yb5fX3aUuBwBS+YeO1E4KuXH9zaEV1QeHwlpX
Ho/GG71hIKVRsSYtzc1Sr0PL0GHSydLuJ4tHxv3F0fAcf0M2bCaT656DQk4t5dKh
ikUJt2baEx59+XH3nLkE4t75gwhFdqZX5775I+EXAoGAfnpHlLZdGW48rl9Cl887
hRDjXDm/gP/ljCrvxxiWselEgaLj2o4NiT28QAfq7KgtOIpAeLAGzIBP6vkE7KFp
nAF+t4gRpooXXSI5oXCBcGI9a26q68UV3iDEmQGiP8kVHOsdzcOKY0qk1ulNAIV4
fU919gnTKorSq3FdV6zGZ8s=
-----END PRIVATE KEY-----
";
const TEST_MODULUS_N: &str = "zx1RbFB4ll0mxv0wooNXE0BzU-hZ6GKGbTBI7w4kAK7Di_3RaDPaZhkTPQnxLUBjbpuE7Ipj7YCXnGsH3T51tLiQkZSBfcBpmHYdpmDQSsPI6scKeuuBnebqh-S-gOKTfG3xju_tp4g-qfLdHDXsmDndD7rGwBA3KcwlGZq-ikMjOYvP8JOaDvfIJ7afmaTI1GWmwA-kfA8oTCGZleuOY6OTTyRu87ks_t3ZipDWNPxl8_GML2drMKppjgDUWRx3zdb4D9snCtNlmeMoocehIrkAg7HnXQcPsYrfSkr0quhsJsJ1dalaTFTZUhhjnlV-7-yUN8_wTW0POeaerPxY4Q";
const TEST_EXPONENT_E: &str = "AQAB";
const TEST_KID: &str = "test-key-1";
const TEST_ISSUER: &str = "https://idp.example.test/";

/// Spins up a tiny local JWKS-serving `axum` server on an ephemeral
/// loopback port, detached for the process's lifetime (same "runs
/// forever, no shutdown API this pass" treatment `skilj`'s own
/// background tasks get) - returns its `/jwks.json` URL, ready to hand
/// to `IdpConfig::new`.
async fn serve_local_jwks() -> String {
    let jwks = json!({
        "keys": [{
            "kty": "RSA",
            "use": "sig",
            "alg": "RS256",
            "kid": TEST_KID,
            "n": TEST_MODULUS_N,
            "e": TEST_EXPONENT_E,
        }]
    });
    let app = axum::Router::new().route(
        "/jwks.json",
        axum::routing::get(move || {
            let jwks = jwks.clone();
            async move { axum::Json(jwks) }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("failed to bind an ephemeral port for the local JWKS server");
    let addr = listener
        .local_addr()
        .expect("a bound listener always has a local address");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("the local JWKS server stopped unexpectedly");
    });
    format!("http://{addr}/jwks.json")
}

/// Signs a JWT for `subject`, with the matching private half of
/// `serve_local_jwks`'s own keypair - stands in for whatever a real
/// external IdP's own login flow would hand back.
fn sign_jwt(subject: &str) -> String {
    let mut header = Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some(TEST_KID.to_string());
    let claims = json!({
        "sub": subject,
        "iss": TEST_ISSUER,
        "exp": (Utc::now() + chrono::Duration::hours(1)).timestamp(),
    });
    let key = EncodingKey::from_rsa_pem(TEST_PRIVATE_KEY_PEM.as_bytes())
        .expect("the test private key PEM is well-formed");
    jsonwebtoken::encode(&header, &claims, &key).expect("signing a well-formed JWT never fails")
}

/// `(bounded_context, command_type_name)` for every command this demo
/// wants a REST `CommandToken` printed for - kept as one list so main()
/// doesn't have to hand-enumerate both bounded contexts' commands twice.
const COMMAND_TYPES: &[(&str, &str)] = &[
    (skilj_demo::banking::BOUNDED_CONTEXT, "DepositMoney"),
    (skilj_demo::banking::BOUNDED_CONTEXT, "WithdrawMoney"),
    (skilj_demo::courses::BOUNDED_CONTEXT, "OpenCourse"),
    (
        skilj_demo::courses::BOUNDED_CONTEXT,
        "EnrollStudentInCourse",
    ),
    (skilj_demo::courses::BOUNDED_CONTEXT, "DropCourse"),
];

const BOUNDED_CONTEXTS: &[&str] = &[
    skilj_demo::banking::BOUNDED_CONTEXT,
    skilj_demo::courses::BOUNDED_CONTEXT,
];

/// The three OTel SDK providers `init_telemetry` builds when
/// `OTEL_EXPORTER_OTLP_ENDPOINT` is set - held by `main` for the
/// process's lifetime purely so none is dropped early (dropping any one
/// tears down its own exporter). Three separate signals, three separate
/// providers/exporters/processors - OpenTelemetry doesn't unify trace,
/// log, and metric export the way it does for the `tracing` layers
/// consuming all three.
struct TelemetryProviders {
    tracer_provider: opentelemetry_sdk::trace::SdkTracerProvider,
    logger_provider: opentelemetry_sdk::logs::SdkLoggerProvider,
    meter_provider: opentelemetry_sdk::metrics::SdkMeterProvider,
}

impl TelemetryProviders {
    /// Flushes and tears down all three exporters, in `main`'s own
    /// graceful-shutdown path (`shutdown_signal` below) - without this,
    /// a `SIGINT`/`SIGTERM` just drops the process (and everything still
    /// sitting in each batch processor's buffer, unexported) the instant
    /// `axum::serve` returns. Each provider's own `.shutdown()` blocks
    /// until its buffered data is flushed or its own internal timeout
    /// elapses; a failure here is logged, not propagated - there's
    /// nothing a demo binary's own exit path could usefully do about a
    /// flush that didn't fully succeed beyond saying so.
    fn shutdown(&self) {
        if let Err(e) = self.tracer_provider.shutdown() {
            tracing::warn!(error = %e, "failed to shut down the trace provider");
        }
        if let Err(e) = self.logger_provider.shutdown() {
            tracing::warn!(error = %e, "failed to shut down the log provider");
        }
        if let Err(e) = self.meter_provider.shutdown() {
            tracing::warn!(error = %e, "failed to shut down the meter provider");
        }
    }
}

/// Waits for `SIGINT` (`Ctrl+C`) or, on Unix, `SIGTERM` - `axum::serve`'s
/// own `with_graceful_shutdown` future (see `main`), so an operator
/// killing the process (or `docker stop`, systemd, etc., which send
/// `SIGTERM`) still gets in-flight requests drained and telemetry
/// flushed, rather than just cut off. `SIGTERM` handling is Unix-only -
/// there's no equivalent signal to wait for elsewhere, so `ctrl_c` alone
/// covers it there.
async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install a Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install a SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    tracing::info!("shutdown signal received");
}

/// Installs the process-wide `tracing` subscriber - always a console
/// `fmt` layer (`RUST_LOG`, defaulting to `info`), and, only when
/// `OTEL_EXPORTER_OTLP_ENDPOINT` is set, two more layers stacked on top:
///
/// - `tracing-opentelemetry`'s layer, backed by an OTLP/HTTP span
///   exporter, plus a registered W3C `TraceContextPropagator` (what
///   `skilj-rest`/`skilj-graphql`'s own `trace_request` middleware needs
///   something real to parse);
/// - `opentelemetry-appender-tracing`'s `OpenTelemetryTracingBridge`,
///   backed by its own OTLP/HTTP *log* exporter (a separate OTel signal
///   from spans) - every `tracing::info!`/`warn!`/etc. event already in
///   the codebase becomes a log record here too, automatically carrying
///   its enclosing span's trace/span id for correlation in the
///   collector. No change needed anywhere else: this bridges events that
///   already exist, the same ones the console layer already prints.
///
/// A third, separate pipeline - not a `tracing_subscriber` layer, since
/// metrics aren't `tracing` events - registers the OTLP/HTTP metrics
/// exporter as the global `MeterProvider`
/// (`opentelemetry::global::set_meter_provider`), which is what makes
/// every `Counter`/`Histogram` already recorded throughout the codebase
/// (`skilj-core::db::submit_command`/`insert_event`, `skilj-rest`/
/// `skilj-graphql`'s `trace_request`, `skilj`'s two background loops)
/// actually export. **This must happen before any of those instruments
/// is first used** - `opentelemetry::global::meter()`'s own doc comment
/// warns that a `Meter` obtained before the provider changes will never
/// reflect a later change. Safe here only because `init_telemetry` is
/// already `main`'s very first statement - see each instrument's own
/// `LazyLock` doc comment (e.g. `skilj-core::db::meter`) and
/// docs/architecture.md's tracing section for the invariant this relies
/// on.
///
/// `env_filter` is registered first, ahead of the `tracing_subscriber`
/// layers - in `tracing-subscriber`, a callsite's enabled/disabled
/// decision is global to the whole subscriber stack, not per-layer, so
/// this is what makes `RUST_LOG` gate the trace/log OTLP exporters too,
/// not just the console. It has no bearing on the separate metrics
/// pipeline, which isn't `tracing`-driven at all.
fn init_telemetry() -> Option<TelemetryProviders> {
    use tracing_subscriber::prelude::*;

    let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    let fmt_layer = tracing_subscriber::fmt::layer();

    if std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT").is_err() {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt_layer)
            .init();
        return None;
    }

    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name("skilj-demo")
        .build();

    let span_exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
        .expect(
            "OTEL_EXPORTER_OTLP_ENDPOINT is set - building the OTLP/HTTP span exporter \
                 shouldn't fail this early (no network call happens yet)",
        );
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(span_exporter)
        .with_resource(resource.clone())
        .build();

    opentelemetry::global::set_tracer_provider(tracer_provider.clone());
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    let tracer = opentelemetry::trace::TracerProvider::tracer(&tracer_provider, "skilj-demo");
    let otel_trace_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let log_exporter = opentelemetry_otlp::LogExporter::builder()
        .with_http()
        .build()
        .expect(
            "OTEL_EXPORTER_OTLP_ENDPOINT is set - building the OTLP/HTTP log exporter \
                 shouldn't fail this early (no network call happens yet)",
        );
    let logger_provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        .with_batch_exporter(log_exporter)
        .with_resource(resource.clone())
        .build();
    let otel_log_layer =
        opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge::new(&logger_provider);

    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt_layer)
        .with(otel_trace_layer)
        .with(otel_log_layer)
        .init();

    let metric_exporter = opentelemetry_otlp::MetricExporter::builder()
        .with_http()
        .build()
        .expect(
            "OTEL_EXPORTER_OTLP_ENDPOINT is set - building the OTLP/HTTP metric exporter \
                 shouldn't fail this early (no network call happens yet)",
        );
    let metric_reader =
        opentelemetry_sdk::metrics::PeriodicReader::builder(metric_exporter).build();
    let meter_provider = opentelemetry_sdk::metrics::SdkMeterProvider::builder()
        .with_reader(metric_reader)
        .with_resource(resource)
        .build();
    opentelemetry::global::set_meter_provider(meter_provider.clone());

    Some(TelemetryProviders {
        tracer_provider,
        logger_provider,
        meter_provider,
    })
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let telemetry = init_telemetry();

    let database_url = std::env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set, e.g. postgres://user:pass@localhost:5432/skilj_demo");
    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse().ok())
        .unwrap_or(8080);

    let pool = db::connect(&database_url).await?;
    db::migrate(&pool).await?;

    for name in BOUNDED_CONTEXTS {
        if db::get_bounded_context(&pool, name).await?.is_none() {
            db::insert_bounded_context(
                &pool,
                &BoundedContext {
                    name: (*name).to_string(),
                    status: BoundedContextStatus::Active,
                    created_at: Utc::now(),
                    created_by: ContextCreator::SystemCreator,
                    template: None,
                },
            )
            .await?;
            tracing::info!(bounded_context = %name, "created bounded context");
        }
    }

    let external_subject = format!("skilj-demo-admin-{}", generate_token_id());
    let role = Role {
        id: generate_token_id(),
        external_subject: external_subject.clone(),
        name: "skilj-demo admin".into(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: Utc::now(),
        revoked_at: None,
    };
    db::insert_role(&pool, &role).await?;

    let mut mappings = Vec::with_capacity(BOUNDED_CONTEXTS.len());
    for name in BOUNDED_CONTEXTS {
        let bc = db::get_bounded_context(&pool, name)
            .await?
            .expect("just ensured it exists above");
        let mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context: bc,
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            status: RoleStatus::Active,
            created_at: Utc::now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping).await?;
        mappings.push(mapping);
    }

    let jwks_url = serve_local_jwks().await;
    let (skilj, report) = skilj_demo::register(Skilj::builder(database_url))
        .reconciliation_role(external_subject)
        .identity_provider(IdpConfig::new(
            jwks_url
                .parse()
                .expect("serve_local_jwks's own URL is always well-formed"),
            TEST_ISSUER,
            SigningAlgorithm::Rs256,
        ))
        .build()
        .await?;
    tracing::info!(registered = ?report.registered, "reconciliation complete");
    if !report.skipped_no_access.is_empty() {
        tracing::warn!(
            skipped = ?report.skipped_no_access,
            "reconciliation: skipped, no access yet"
        );
    }

    println!(
        "\nGraphQL Role credential (send as `authorization: Bearer <jwt>`, or in a \
         graphql-transport-ws `connection_init` payload the same way):"
    );
    println!("  {}", sign_jwt(&role.external_subject));

    println!("\ncommand tokens (send as `authorization: Bearer <id>.<secret>`):");
    for (bounded_context, command_type_name) in COMMAND_TYPES {
        let mapping = mappings
            .iter()
            .find(|m| m.bounded_context.name == *bounded_context)
            .expect("a mapping was inserted above for every bounded context in BOUNDED_CONTEXTS");
        let command_type = db::get_command_type(&pool, bounded_context, command_type_name)
            .await?
            .unwrap_or_else(|| {
                panic!(
                    "{bounded_context}/{command_type_name} should have just been registered by \
                     skilj_demo::register()"
                )
            });
        let token = access_control::create_command_token(
            mapping,
            &command_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            Utc::now(),
        )?;
        db::insert_command_token(&pool, &token).await?;
        tracing::info!(
            bounded_context = %bounded_context,
            command_type = %command_type_name,
            "minted command token"
        );
        println!(
            "  {bounded_context}/{command_type_name}: {}.{}",
            token.id, token.secret
        );
    }

    let rest = skilj.rest_router();
    let graphql = skilj.graphql_router().await?;
    let app = rest.merge(graphql);

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    println!("\nskilj-demo listening on http://localhost:{port} (REST under /v1/..., GraphQL at /graphql)");
    println!("example - deposit into account \"a1\" (banking):");
    println!(
        "  curl -H 'authorization: Bearer <DepositMoney token>' -H 'content-type: application/json' \\\n\
         \x20      -d '{{\"payload\":{{\"account_id\":\"a1\",\"amount\":100}}}}' \\\n\
         \x20      http://localhost:{port}/v1/commands/trigger"
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    if let Some(telemetry) = telemetry {
        telemetry.shutdown();
    }

    Ok(())
}
