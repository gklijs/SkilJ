//! SkilJ: a Rust library for building event-sourced, DDD-style
//! applications backed by a single-instance Postgres database, exposing a
//! GraphQL surface and a REST surface. See `specs/skilj.allium` for the
//! full behavioural specification and `docs/architecture.md` for how
//! it's built.
//!
//! This crate is a thin facade over `skilj-core` + `skilj-graphql` +
//! `skilj-rest`, for the common case of wanting all three. A consumer
//! wanting only one surface can depend on `skilj-core` plus the relevant
//! surface crate directly instead - see docs/architecture.md §3.1.

pub use skilj_core::plugin::{CommandType, EventType, Projection};

/// Entry point - see docs/architecture.md §1.5 for the full worked
/// example and the reasoning behind every choice below.
pub struct Skilj {
    // TODO: the built skilj-core engine, plus skilj-graphql's
    // SchemaRegistry and skilj-rest's routes, ready to be mounted onto an
    // axum::Router (or exposed as two routers for the caller to combine
    // however it likes).
}

/// What `Skilj::builder().build()` did on this startup: which bounded
/// contexts' types registered successfully, which were skipped because
/// the reconciliation Role has no admin access to them yet (expected,
/// not an error - §1.5), and (if `.build()` returned `Err` instead) which
/// registration was genuinely rejected.
#[derive(Debug, Default)]
pub struct ReconciliationReport {
    pub registered: Vec<String>,
    pub skipped_no_access: Vec<String>,
}

pub struct SkiljBuilder {
    current_bounded_context: Option<String>,
    reconciliation_role: Option<String>,
    // TODO: the actual registered EventType/CommandType/Projection set,
    // keyed by bounded context.
}

impl Skilj {
    pub fn builder() -> SkiljBuilder {
        SkiljBuilder {
            current_bounded_context: None,
            reconciliation_role: None,
        }
    }
}

impl SkiljBuilder {
    /// Every `event_type`/`command_type`/`projection` call following this
    /// one registers against `name`, until the next `bounded_context`
    /// call changes it.
    pub fn bounded_context(mut self, name: impl Into<String>) -> Self {
        self.current_bounded_context = Some(name.into());
        self
    }

    pub fn event_type<T: EventType>(self) -> Self {
        // TODO: record T against self.current_bounded_context.
        self
    }

    pub fn command_type<T: CommandType>(self) -> Self {
        // TODO: record T against self.current_bounded_context.
        self
    }

    pub fn projection<T: Projection>(self) -> Self {
        // TODO: record T against self.current_bounded_context.
        self
    }

    /// The Role the startup reconciliation loop authenticates as - named
    /// by `external_subject`, the same identifier every other identity
    /// resolution in the spec keys on. Optional: omitting it skips
    /// reconciliation entirely and cleanly, not an error - see §1.5 for
    /// why this is never a system-identity bypass.
    pub fn reconciliation_role(mut self, external_subject: impl Into<String>) -> Self {
        self.reconciliation_role = Some(external_subject.into());
        self
    }

    /// Runs the startup reconciliation loop automatically (§1.5). Returns
    /// `Err` only for a genuine registration rejection (e.g. an
    /// incompatible schema change) - a bounded context the reconciliation
    /// Role has no admin access to yet is reported in
    /// `ReconciliationReport`, not an error.
    pub async fn build(self) -> Result<(Skilj, ReconciliationReport), skilj_core::Error> {
        todo!("wire up skilj-core, run reconciliation, build skilj-graphql's schema and skilj-rest's routes")
    }
}
