//! `TemplateCache` - Codeberg issue #13's live `bounded_context ->
//! dispatch_template` lookup, and ultra-review bug_001/bug_005's own
//! fix for it. Every `Registered*` dispatch map `skilj`'s facade holds
//! (`command_types`/`event_types`/`projections`/`snapshots`) is keyed
//! by whatever literal name `.bounded_context(name)`/`#[auto_register]`
//! declared at `.build()` time - for a templated tenant, that's always
//! the *template*'s own name, never the tenant's (a tenant's name is
//! chosen at runtime, long after `.build()` ran, so it can never be a
//! map key). `TemplateCache` is what lets a dispatcher resolve a
//! tenant's own runtime-chosen name to its template's before the real
//! lookup, making an already-compiled `decide()`/`project()`/etc.
//! reachable for every tenant with no redeploy.
//!
//! Lives here, not in the `skilj` facade crate where it first did,
//! because `skilj-graphql`'s resolvers need to refresh it too (see
//! below) - `skilj-graphql` cannot depend on `skilj` (the dependency
//! runs the other way), so the shared type has to live somewhere both
//! already depend on. `#[derive(Clone)]` over `Arc`-wrapped internals,
//! the same "hand out a cheap clone" shape `EventCache` already uses.
//!
//! Reads `bounded_contexts.dispatch_template`
//! (`db::list_dispatch_template_mappings`), not `bounded_contexts.
//! template` - a deliberate second, permanent field. `template` is
//! correctly cleared by `DeleteBoundedContext`'s `ON DELETE SET NULL`
//! cascade (that's the right behaviour for *that* field: a tenant whose
//! template was deleted has nothing left to display or resync from).
//! `dispatch_template` is set once, at tenant creation, and never
//! cleared by anything - deleting the template a tenant was stamped
//! from must not also silently stop that tenant from processing
//! commands, which is exactly what reading the nullable `template`
//! column here used to do (ultra-review bug_005).
//!
//! Refreshed two ways: once at `SkiljBuilder::build()` startup, and
//! synchronously by `createBoundedContextFromTemplate`'s own resolver
//! right after it commits a new tenant - closing the same-instance race
//! ultra-review bug_001 found (the cross-instance `RegistrationChanged`
//! NOTIFY listener refreshes every *other* instance, but was the only
//! refresh path at all, so an immediate `submitCommand` on the same
//! instance that just created the tenant could lose the race and see a
//! spurious `no_decider_registered`).

use crate::db::Pool;
use arc_swap::ArcSwap;
use std::collections::HashMap;
use std::sync::Arc;

/// See this module's own doc comment.
#[derive(Clone, Default)]
pub struct TemplateCache {
    current: Arc<ArcSwap<HashMap<String, Option<String>>>>,
}

impl TemplateCache {
    pub fn new() -> Self {
        Self::default()
    }

    /// Re-fetches every `(name, dispatch_template)` pair from Postgres
    /// and swaps it in - the same "build the new snapshot, then one
    /// atomic swap" shape `SchemaRegistry::rebuild` already uses, so a
    /// dispatch call in flight keeps reading a consistent map rather
    /// than observing a partial update.
    pub async fn refresh(&self, pool: &Pool) -> crate::error::Result<()> {
        let mappings = crate::db::list_dispatch_template_mappings(pool).await?;
        self.current.store(Arc::new(mappings.into_iter().collect()));
        Ok(())
    }

    /// `bounded_context` itself when it has no `dispatch_template` on
    /// record (the ordinary case, and also the safe fallback for a
    /// context this cache hasn't heard of yet - a lookup miss behaves
    /// exactly like an untemplated context, never a dispatch failure of
    /// its own).
    pub fn effective_bounded_context(&self, bounded_context: &str) -> String {
        match self.current.load().get(bounded_context) {
            Some(Some(template)) => template.clone(),
            _ => bounded_context.to_string(),
        }
    }
}
