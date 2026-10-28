//! The running library: every rule in the spec is a method on [`Skilj`].

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use sqlx::postgres::{PgPool, PgPoolOptions};
use sqlx::Row;
use tokio::sync::{broadcast, Mutex, RwLock};

use crate::crypto::{self, HmacBlindHasher, MasterKeyProvider};
use crate::decl::BoundedContextDecl;
use crate::error::{Error, Result};
use crate::model::{Config, StoredEvent, ADMIN_CONTEXT};
use crate::tags::{BlindHasher, NoBlindKeys};

mod access;
mod admin;
mod background;
mod cache;
mod commands;
mod events;
mod keys;
mod projections;
mod reads;
mod registry;
mod render;
mod rows;
mod templates;

pub use access::{AccessHistoryAnswer, CapabilityOverviewRow, MintedToken};
pub use admin::ContextDirectoryEntry;
pub use commands::{CommandOutcome, CommandRequest, DryRunAnswer};
pub use events::{ExternalEventItem, ExternalOutcome};
pub use reads::{EventPage, SubscriptionEnd, SubscriptionKind, SubscriptionSpec};
pub use registry::{ReconciliationReport, Registry};
pub use render::Rendered;

/// What travels on the in-process bus to live subscriptions.
#[derive(Debug, Clone)]
pub enum BusMessage {
    /// A committed event (as stored).
    Event(Arc<StoredEvent>),
    /// An access mapping changed (revoked, rescoped ...): subscriptions on it
    /// re-check themselves.
    AccessChanged {
        mapping_id: Option<String>,
        role_id: Option<String>,
    },
    /// This instance may have missed notifications: live subscriptions end.
    Lagged,
}

/// Receives the bootstrap secret.
pub type BootstrapCallback = Arc<dyn Fn(&str) + Send + Sync>;

/// Builder for [`Skilj`].
pub struct SkiljBuilder {
    database_url: Option<String>,
    pool: Option<PgPool>,
    config: Config,
    contexts: Vec<BoundedContextDecl>,
    master: Option<Arc<dyn MasterKeyProvider>>,
    blind_root: Option<Vec<u8>>,
    reconcile_as: Option<String>,
    on_bootstrap: Option<BootstrapCallback>,
    background: bool,
}

impl Default for SkiljBuilder {
    fn default() -> Self {
        SkiljBuilder {
            database_url: None,
            pool: None,
            config: Config::default(),
            contexts: vec![],
            master: None,
            blind_root: None,
            reconcile_as: None,
            on_bootstrap: None,
            background: true,
        }
    }
}

impl SkiljBuilder {
    pub fn database_url(mut self, url: impl Into<String>) -> Self {
        self.database_url = Some(url.into());
        self
    }
    pub fn pool(mut self, pool: PgPool) -> Self {
        self.pool = Some(pool);
        self
    }
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }
    pub fn context(mut self, decl: BoundedContextDecl) -> Self {
        self.contexts.push(decl);
        self
    }
    pub fn master_key(mut self, provider: Arc<dyn MasterKeyProvider>) -> Self {
        self.master = Some(provider);
        self
    }
    pub fn blind_index_root_key(mut self, key: Vec<u8>) -> Self {
        self.blind_root = Some(key);
        self
    }
    /// The external subject of the Role the startup reconciliation loop
    /// registers types as. It needs an admin grant on each context.
    pub fn reconcile_as(mut self, external_subject: impl Into<String>) -> Self {
        self.reconcile_as = Some(external_subject.into());
        self
    }
    /// Receives the bootstrap secret instead of (in addition to) the log.
    pub fn on_bootstrap_secret(mut self, f: impl Fn(&str) + Send + Sync + 'static) -> Self {
        self.on_bootstrap = Some(Arc::new(f));
        self
    }
    /// Disable background tasks (projection consumer, scheduler, listener);
    /// tests drive them by hand with [`Skilj::tick`].
    pub fn without_background_tasks(mut self) -> Self {
        self.background = false;
        self
    }

    pub async fn start(self) -> Result<Skilj> {
        for c in &self.contexts {
            c.check()?;
            if c.name == ADMIN_CONTEXT {
                return Err(Error::Config(
                    "the admin bounded context is SkilJ's own".into(),
                ));
            }
        }
        let pool = match (self.pool, self.database_url) {
            (Some(p), _) => p,
            (None, Some(url)) => {
                PgPoolOptions::new()
                    .max_connections(16)
                    .connect(&url)
                    .await?
            }
            (None, None) => return Err(Error::Config("no database configured".into())),
        };
        let master = self
            .master
            .ok_or_else(|| Error::Config("a master key provider is required".into()))?;
        let blind_hmac = self.blind_root.map(|r| Arc::new(HmacBlindHasher::new(r)));
        let blind: Arc<dyn BlindHasher> = match &blind_hmac {
            Some(h) => h.clone(),
            None => Arc::new(NoBlindKeys),
        };
        migrate(&pool).await?;
        let epoch = read_epoch(&pool).await?;
        let (bus, _) = broadcast::channel(4096);
        let decls: BTreeMap<String, BoundedContextDecl> = self
            .contexts
            .into_iter()
            .map(|c| (c.name.clone(), c))
            .collect();
        let inner = Arc::new(Inner {
            pool,
            config: self.config,
            decls,
            master,
            blind,
            blind_hmac,
            cache: cache::EventCache::default(),
            bus,
            data_keys: Mutex::new(HashMap::new()),
            bootstrap_hash: Mutex::new(None),
            epoch: RwLock::new(epoch),
            registry: RwLock::new(Registry::default()),
            dispatchers: std::sync::Mutex::new(HashMap::new()),
            templates: std::sync::RwLock::new(BTreeMap::new()),
        });
        let skilj = Skilj { inner };
        skilj.stamp_admin_context().await?;
        skilj.register_access_event_types().await?;
        skilj.baseline_access_events().await?;
        skilj.reload_registry().await?;
        skilj.check_blind_tags().await?;
        skilj.warm_cache().await?;
        skilj.generate_bootstrap_secret(self.on_bootstrap).await?;
        let report = match &self.reconcile_as {
            Some(subject) => skilj.reconcile(subject).await?,
            None => ReconciliationReport::default(),
        };
        if !report.skipped_no_access.is_empty() {
            tracing::warn!(contexts = ?report.skipped_no_access, "bounded contexts skipped: no admin grant for the reconciliation role");
        }
        skilj.report_private_projection_inputs().await;
        if self.background {
            background::spawn(skilj.clone());
        }
        Ok(skilj)
    }
}

pub(crate) struct Inner {
    pub(crate) pool: PgPool,
    pub(crate) config: Config,
    pub(crate) decls: BTreeMap<String, BoundedContextDecl>,
    pub(crate) master: Arc<dyn MasterKeyProvider>,
    pub(crate) blind: Arc<dyn BlindHasher>,
    pub(crate) blind_hmac: Option<Arc<HmacBlindHasher>>,
    pub(crate) cache: cache::EventCache,
    pub(crate) bus: broadcast::Sender<BusMessage>,
    pub(crate) data_keys: Mutex<HashMap<String, Option<Vec<u8>>>>,
    pub(crate) bootstrap_hash: Mutex<Option<String>>,
    pub(crate) epoch: RwLock<String>,
    pub(crate) registry: RwLock<Registry>,
    pub(crate) dispatchers: std::sync::Mutex<HashMap<String, Arc<Mutex<Option<i64>>>>>,
    /// tenant -> template, mirrored from the registry for sync lookups.
    pub(crate) templates: std::sync::RwLock<BTreeMap<String, String>>,
}

/// The library handle. Cheap to clone.
#[derive(Clone)]
pub struct Skilj {
    pub(crate) inner: Arc<Inner>,
}

impl Skilj {
    pub fn builder() -> SkiljBuilder {
        SkiljBuilder::default()
    }

    pub fn pool(&self) -> &PgPool {
        &self.inner.pool
    }

    pub fn config(&self) -> &Config {
        &self.inner.config
    }

    pub fn decls(&self) -> &BTreeMap<String, BoundedContextDecl> {
        &self.inner.decls
    }

    /// The compiled declaration that serves a bounded context: its own, or
    /// for a templated tenant, its template's.
    pub fn decl_for(&self, bc: &str) -> Option<&BoundedContextDecl> {
        if let Some(d) = self.inner.decls.get(bc) {
            return Some(d);
        }
        let t = self
            .inner
            .templates
            .read()
            .expect("templates")
            .get(bc)
            .cloned()?;
        self.inner.decls.get(&t)
    }

    /// Subscribe to the in-process bus.
    pub fn bus(&self) -> broadcast::Receiver<BusMessage> {
        self.inner.bus.subscribe()
    }

    /// current_epoch().
    pub async fn current_epoch(&self) -> String {
        self.inner.epoch.read().await.clone()
    }

    pub(crate) async fn check_epoch(&self, epoch: Option<&str>) -> Result<()> {
        match epoch {
            Some(e) if e != self.inner.epoch.read().await.as_str() => Err(Error::EpochMismatch),
            _ => Ok(()),
        }
    }

    pub(crate) fn now() -> DateTime<Utc> {
        // Postgres keeps microseconds; truncate so values round-trip.
        let n = Utc::now();
        n - chrono::Duration::nanoseconds((n.timestamp_subsec_nanos() % 1000) as i64)
    }

    /// The admin context's provenance, stamped once at first startup.
    async fn stamp_admin_context(&self) -> Result<()> {
        sqlx::query(
            "insert into bounded_contexts (name, status, created_at, created_by_role) values ($1, 'active', $2, null)
             on conflict (name) do nothing",
        )
        .bind(ADMIN_CONTEXT)
        .bind(Self::now())
        .execute(&self.inner.pool)
        .await?;
        sqlx::query(
            "insert into sequence_counters (bounded_context) values ($1) on conflict do nothing",
        )
        .bind(ADMIN_CONTEXT)
        .execute(&self.inner.pool)
        .await?;
        Ok(())
    }

    /// Generate the bootstrap secret while no active superadmin exists.
    async fn generate_bootstrap_secret(&self, cb: Option<BootstrapCallback>) -> Result<()> {
        let exists: bool =
            sqlx::query_scalar("select exists(select 1 from roles where superadmin and active)")
                .fetch_one(&self.inner.pool)
                .await?;
        if exists {
            *self.inner.bootstrap_hash.lock().await = None;
            return Ok(());
        }
        let secret = crypto::generate_secret();
        *self.inner.bootstrap_hash.lock().await = Some(crypto::hash_secret(&secret));
        match cb {
            Some(f) => f(&secret),
            None => tracing::warn!("no superadmin exists yet; bootstrap secret: {secret}"),
        }
        Ok(())
    }
}

async fn migrate(pool: &PgPool) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query("select pg_advisory_xact_lock(7243911)")
        .execute(&mut *tx)
        .await?;
    sqlx::raw_sql(include_str!("../migrations.sql"))
        .execute(&mut *tx)
        .await?;
    tx.commit().await?;
    Ok(())
}

/// The database's epoch: system identifier and timeline. Falls back to a
/// value stored in the database itself when the control functions are not
/// readable by this role (which loses the restore-detection half).
async fn read_epoch(pool: &PgPool) -> Result<String> {
    let r = sqlx::query(
        "select (select system_identifier::text from pg_control_system()) as sys,
                (select timeline_id::text from pg_control_checkpoint()) as tl",
    )
    .fetch_one(pool)
    .await;
    match r {
        Ok(row) => {
            let sys: Option<String> = row.try_get("sys")?;
            let tl: Option<String> = row.try_get("tl")?;
            Ok(format!(
                "{}.{}",
                sys.unwrap_or_default(),
                tl.unwrap_or_default()
            ))
        }
        Err(_) => {
            let id = crypto::generate_id();
            sqlx::query(
                "insert into skilj_meta (key, value) values ('epoch', $1) on conflict do nothing",
            )
            .bind(&id)
            .execute(pool)
            .await?;
            Ok(
                sqlx::query_scalar("select value from skilj_meta where key = 'epoch'")
                    .fetch_one(pool)
                    .await?,
            )
        }
    }
}

impl Skilj {
    /// Re-read registered state (tests that edit the database directly).
    pub async fn reload_registry_for_tests(&self) {
        self.reload_registry().await.expect("reload");
    }
}
