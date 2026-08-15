//! Builds the one unified GraphQL schema from every registered
//! `EventType`/`CommandType`/`Projection`, across every bounded context -
//! see docs/architecture.md §5.1. Namespaced per bounded context (§5.2),
//! not flat-prefixed. Held behind `ArcSwap` and rebuilt atomically
//! whenever registration changes (see [[skilj-language-choice]] in
//! project memory for why `async-graphql::dynamic` was chosen at all).

use arc_swap::ArcSwap;
use async_graphql::dynamic::Schema;
use std::sync::Arc;

pub struct SchemaRegistry {
    current: ArcSwap<Schema>,
}

impl SchemaRegistry {
    pub fn current(&self) -> Arc<Schema> {
        self.current.load_full()
    }

    // TODO: build(), walking every registered bounded context's
    // EventType/CommandType/Projection JSON Schemas into
    // async_graphql::dynamic::Object/Field/TypeRef definitions (§5.1),
    // nested under a per-bounded-context field on Query/Mutation/
    // Subscription (§5.2), and rebuild()/swap() called whenever
    // registration changes.
}
