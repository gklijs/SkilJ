//! Seeding a development database: bounded contexts, an admin [`Role`]
//! with access to them, and REST command tokens (docs/architecture.md
//! §220).
//!
//! This is the shortcut the template's and `skilj-demo`'s servers take
//! so a fresh checkout runs with one command. It writes the role and its
//! grants straight into the database, which a production deployment
//! doesn't do: there a human claims the once-only bootstrap secret
//! [`SkiljBuilder::build`](crate::SkiljBuilder::build) prints, to create
//! the first superadmin, and grants everything else over the GraphQL
//! admin console (docs/architecture.md §109).
//!
//! ```no_run
//! # async fn run(database_url: String) -> Result<(), Box<dyn std::error::Error>> {
//! use skilj::Skilj;
//!
//! let admin = skilj::dev::seed_admin(&database_url, "wallet admin", &["wallet"]).await?;
//! let (skilj, _report) = Skilj::builder(database_url)
//!     .reconciliation_role(admin.external_subject())
//!     .build()
//!     .await?;
//! let token = admin.create_command_token("wallet", "Deposit").await?;
//! println!("send as `authorization: Bearer {token}`");
//! # Ok(())
//! # }
//! ```

use chrono::Utc;
use skilj_core::access_control::{self, AccessLevel, Role, RoleAccessMapping, RoleStatus};
use skilj_core::bootstrap::ContextCreator;
use skilj_core::db::{self, Pool};
use skilj_core::event_store::{BoundedContext, BoundedContextStatus};
use skilj_core::shared::{generate_token_id, generate_token_secret};
use skilj_core::Error;

/// An admin [`Role`] made by [`seed_admin`], with Admin access to each of
/// its bounded contexts.
pub struct DevAdmin {
    pool: Pool,
    role: Role,
    mappings: Vec<RoleAccessMapping>,
}

/// Connects to `database_url`, migrates it, creates each of
/// `bounded_contexts` that doesn't exist yet, and creates a new [`Role`]
/// named `name` with Admin access to all of them.
///
/// Each call makes a new role, with a random external subject, so it is
/// safe to run on every start against the same database. Pass the role
/// to [`SkiljBuilder::reconciliation_role`](crate::SkiljBuilder::reconciliation_role)
/// so `build()` can register types onto those bounded contexts.
pub async fn seed_admin(
    database_url: &str,
    name: &str,
    bounded_contexts: &[&str],
) -> Result<DevAdmin, Error> {
    let pool = db::connect_with(database_url, db::PgPoolOptions::new().max_connections(2)).await?;
    db::migrate(&pool).await?;

    for bounded_context in bounded_contexts {
        if db::get_bounded_context(&pool, bounded_context)
            .await?
            .is_none()
        {
            db::insert_bounded_context(
                &pool,
                &BoundedContext {
                    name: (*bounded_context).to_string(),
                    status: BoundedContextStatus::Active,
                    created_at: Utc::now(),
                    created_by: ContextCreator::SystemCreator,
                    template: None,
                },
            )
            .await?;
        }
    }

    let id = generate_token_id();
    let role = Role {
        external_subject: format!("dev-admin-{id}"),
        id,
        name: name.to_string(),
        superadmin: false,
        status: RoleStatus::Active,
        created_at: Utc::now(),
        revoked_at: None,
    };
    db::insert_role(&pool, &role).await?;

    let mut mappings = Vec::with_capacity(bounded_contexts.len());
    for bounded_context in bounded_contexts {
        let bounded_context = db::get_bounded_context(&pool, bounded_context)
            .await?
            .ok_or_else(Error::row_not_found)?;
        let mapping = RoleAccessMapping {
            role: role.clone(),
            bounded_context,
            level: AccessLevel::Admin,
            can_read_sensitive: false,
            scope: None,
            capabilities: Default::default(),
            delegable: Default::default(),
            status: RoleStatus::Active,
            created_at: Utc::now(),
            revoked_at: None,
        };
        db::insert_role_access_mapping(&pool, &mapping).await?;
        mappings.push(mapping);
    }

    Ok(DevAdmin {
        pool,
        role,
        mappings,
    })
}

impl DevAdmin {
    /// The role's external subject: what a JWT's `sub` names to act as
    /// it over GraphQL, and what
    /// [`SkiljBuilder::reconciliation_role`](crate::SkiljBuilder::reconciliation_role)
    /// takes.
    pub fn external_subject(&self) -> &str {
        &self.role.external_subject
    }

    /// The role itself.
    pub fn role(&self) -> &Role {
        &self.role
    }

    /// Mints a REST command token for `command_type` in
    /// `bounded_context`, returned as the `<id>.<secret>` an
    /// `authorization: Bearer` header carries.
    ///
    /// The command type must already be registered - call this after
    /// [`SkiljBuilder::build`](crate::SkiljBuilder::build). An
    /// unregistered command type, or a bounded context [`seed_admin`]
    /// wasn't given, is `Error::Database(sqlx::Error::RowNotFound)`.
    pub async fn create_command_token(
        &self,
        bounded_context: &str,
        command_type: &str,
    ) -> Result<String, Error> {
        let mapping = self
            .mappings
            .iter()
            .find(|m| m.bounded_context.name == bounded_context)
            .ok_or_else(Error::row_not_found)?;
        let command_type = db::get_command_type(&self.pool, bounded_context, command_type)
            .await?
            .ok_or_else(Error::row_not_found)?;
        let token = access_control::create_command_token(
            mapping,
            &command_type,
            generate_token_id(),
            generate_token_secret(),
            None,
            Utc::now(),
        )?;
        db::insert_command_token(&self.pool, &token).await?;
        Ok(format!("{}.{}", token.id, token.secret))
    }

    /// Closes the database connections it holds. Dropping it closes them
    /// too, without waiting.
    pub async fn close(self) {
        self.pool.close().await;
    }
}
