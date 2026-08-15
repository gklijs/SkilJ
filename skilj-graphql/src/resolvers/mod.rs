//! One module per GraphQL surface, matching `specs/skilj.allium`'s
//! surfaces section: `ProjectionQuery`, `CommandQuery`, `EventQuery`,
//! `EventSubscription`, `CommandSubmission`, `TypeRegistration`,
//! `AccessManagement`, `BoundedContextCreation`, `BoundedContextDirectory`,
//! `SuperadminBootstrap`, `TokenRevocation`, `BoundedContextArchival`,
//! `SubjectErasure`. See docs/architecture.md §3.3.

// TODO: one submodule per surface listed above, each building the
// resolvers §5.2's BankingQueries-style namespacing describes, and
// rendering skilj_core::Error through the code()/message() trait into
// GraphQL's `errors` array (§5.4) - except CommandSubmission, whose
// rejection path returns SubmitCommandPayload-shaped data instead (§5.4).
