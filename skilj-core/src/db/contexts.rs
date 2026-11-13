//! Persistence for contexts rule groups.

use super::*;

// --- BoundedContext ---

#[derive(sqlx::FromRow)]
pub(super) struct BoundedContextRow {
    pub(super) name: String,
    pub(super) status: String,
    pub(super) created_at: DateTime<Utc>,
    pub(super) created_by_kind: String,
    pub(super) created_by_role_id: Option<String>,
    /// issue #13 - just the referenced row's own name; hydrated
    /// into a full `BoundedContext` by `get_bounded_context`/
    /// `list_bounded_contexts`, matching how `created_by_role_id` is
    /// hydrated into a full `Role` below.
    pub(super) template: Option<String>,
}

pub(super) const BOUNDED_CONTEXT_COLUMNS: &str =
    "name, status, created_at, created_by_kind, created_by_role_id, template";

/// Provisions the new context's own `bc_<name>` schema (see
/// `provision_bounded_context_schema`) and inserts its `bounded_contexts`
/// registry row in one transaction - a failure partway through either
/// half rolls back the other, so there's never an orphaned schema with
/// no registry row or vice versa. Callers (test fixtures included) don't
/// need to know any of this happens; the signature is unchanged from
/// before schema-per-context existed.
#[tracing::instrument(skip_all)]
pub async fn insert_bounded_context(pool: &Pool, bc: &BoundedContext) -> crate::error::Result<()> {
    let (kind, role_id) = match &bc.created_by {
        ContextCreator::SystemCreator => ("system", None),
        ContextCreator::SuperadminCreator { role } => ("superadmin", Some(role.id.clone())),
    };

    let mut tx = pool.begin().await?;
    provision_bounded_context_schema(&mut tx, &bc.name).await?;
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO bounded_contexts ({BOUNDED_CONTEXT_COLUMNS}) VALUES ($1,$2,$3,$4,$5,$6)"
    )))
    .bind(&bc.name)
    .bind(bounded_context_status_to_str(bc.status))
    .bind(bc.created_at)
    .bind(kind)
    .bind(role_id)
    .bind(bc.template.as_ref().map(|t| t.name.clone()))
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    notify_registration_changed(pool).await;
    Ok(())
}

/// Persists an `archive_bounded_context` outcome - a status-only update,
/// the only field any rule in this crate ever changes on an existing
/// `bounded_contexts` row (`name`/`created_at`/`created_by` are fixed at
/// creation - `rule AddBoundedContext`'s own "chosen once, at the moment
/// of creation" framing, and there is no rename). Distinct from
/// `insert_bounded_context`, which provisions a brand new schema - this
/// touches only the registry row of one that already exists.
#[tracing::instrument(skip_all, fields(name = %name))]
pub async fn update_bounded_context_status(
    pool: &Pool,
    name: &str,
    status: BoundedContextStatus,
) -> crate::error::Result<()> {
    sqlx::query("UPDATE bounded_contexts SET status = $1 WHERE name = $2")
        .bind(bounded_context_status_to_str(status))
        .bind(name)
        .execute(pool)
        .await?;
    notify_registration_changed(pool).await;
    Ok(())
}

/// Unlike most `into_domain`-style conversions in this module,
/// `ContextCreator::SuperadminCreator` needs a second query (the
/// referenced `roles` row - see the migration's own note on why
/// `bounded_contexts` normalises onto `roles` rather than denormalising
/// its fields directly), so this is a free function taking the row plus
/// an already-resolved `Option<Role>`, not a method on the row type.
pub(super) fn bounded_context_from_row(
    row: BoundedContextRow,
    role: Option<Role>,
    template: Option<Box<BoundedContext>>,
) -> BoundedContext {
    let created_by = match row.created_by_kind.as_str() {
        "system" => ContextCreator::SystemCreator,
        _ => ContextCreator::SuperadminCreator {
            role: role.expect(
                "bounded_contexts row has created_by_kind = 'superadmin' but its \
                 created_by_role_id doesn't resolve to a roles row - the FK should make \
                 this unreachable",
            ),
        },
    };
    BoundedContext {
        name: row.name,
        status: bounded_context_status_from_str(&row.status),
        created_at: row.created_at,
        created_by,
        template,
    }
}

/// Like `get_bounded_context`, for callers that already hold a name they
/// expect to exist. A missing row still comes back as an ordinary
/// `sqlx::Error::RowNotFound` rather than a panic: the caller's own access
/// check can pass just before a concurrent `hard_delete_bounded_context`
/// lands, and that race must surface as an error response, not take the
/// request task down.
pub(super) async fn require_bounded_context(
    pool: &Pool,
    name: &str,
) -> crate::error::Result<BoundedContext> {
    get_bounded_context(pool, name)
        .await?
        .ok_or_else(crate::error::Error::row_not_found)
}

/// Like [`require_bounded_context`], for an `EventType` a caller already
/// has a stored reference to (an `events`/`access_tokens` row's own
/// `event_type_name`) - the identical `hard_delete_bounded_context` race
/// applies: the whole schema, `event_types` included, can vanish between
/// an earlier query in the same function and this one.
pub(super) async fn require_event_type(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<EventType> {
    get_event_type(pool, bounded_context, name)
        .await?
        .ok_or_else(crate::error::Error::row_not_found)
}

/// [`require_event_type`]'s `CommandType` sibling.
pub(super) async fn require_command_type(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<CommandType> {
    get_command_type(pool, bounded_context, name)
        .await?
        .ok_or_else(crate::error::Error::row_not_found)
}

/// [`require_event_type`]'s `Command` sibling, for an `events` row's own
/// `origin_command_id`.
pub(super) async fn require_command(
    pool: &Pool,
    bounded_context: &str,
    id: i64,
) -> crate::error::Result<Command> {
    get_command_by_id(pool, bounded_context, id)
        .await?
        .ok_or_else(crate::error::Error::row_not_found)
}

/// The columns that read a `bounded_contexts` row hydrated in one
/// statement (docs/architecture.md §199), from
/// [`HYDRATED_BOUNDED_CONTEXT_FROM`]: the row as `bc_*`, its creating role
/// as `r_*`, its template as `t_*` and the template's creating role as
/// `tr_*`. A template is never itself templated
/// (`TemplateIsNeverItselfTemplated`), so that is the whole
/// `BoundedContext`. Read back with [`hydrated_bounded_context`].
pub(super) fn hydrated_bounded_context_columns() -> String {
    let aliased = |columns: &str, alias: &str| {
        columns
            .split(", ")
            .map(|column| format!("{alias}.{column} AS {alias}_{column}"))
            .collect::<Vec<_>>()
            .join(", ")
    };
    [
        aliased(BOUNDED_CONTEXT_COLUMNS, "bc"),
        aliased(ROLE_COLUMNS, "r"),
        aliased(BOUNDED_CONTEXT_COLUMNS, "t"),
        aliased(ROLE_COLUMNS, "tr"),
    ]
    .join(", ")
}

/// The joins after `bounded_contexts bc` in
/// [`HYDRATED_BOUNDED_CONTEXT_FROM`], as a literal both constants are
/// built from.
macro_rules! hydrated_bounded_context_joins {
    () => {
        "LEFT JOIN roles r ON r.id = bc.created_by_role_id \
         LEFT JOIN bounded_contexts t ON t.name = bc.template \
         LEFT JOIN roles tr ON tr.id = t.created_by_role_id"
    };
}

/// The joins [`hydrated_bounded_context_columns`] reads from; `bc` is the
/// bounded context itself.
pub(super) const HYDRATED_BOUNDED_CONTEXT_FROM: &str =
    concat!("bounded_contexts bc ", hydrated_bounded_context_joins!());

/// [`HYDRATED_BOUNDED_CONTEXT_FROM`] without its first relation, for a
/// statement that also reads a bounded context's own table: that table
/// goes first in `FROM`, then `bounded_contexts bc`, then these joins.
/// Postgres locks a statement's relations in the order it names them,
/// and `DROP SCHEMA ... CASCADE` locks the bounded context's tables before
/// `roles` (for the foreign keys into it), so naming `roles` first
/// deadlocks with deleting that bounded context (docs/architecture.md
/// §251).
pub(super) const HYDRATED_BOUNDED_CONTEXT_JOINS: &str = hydrated_bounded_context_joins!();

/// The [`BoundedContext`] a [`hydrated_bounded_context_columns`] row holds.
pub(super) fn hydrated_bounded_context(
    row: &sqlx::postgres::PgRow,
) -> crate::error::Result<BoundedContext> {
    use sqlx::Row;
    let bounded_context = |alias: &str| -> Result<Option<BoundedContextRow>, sqlx::Error> {
        let Some(name) = row.try_get::<Option<String>, _>(format!("{alias}_name").as_str())? else {
            return Ok(None);
        };
        Ok(Some(BoundedContextRow {
            name,
            status: row.try_get(format!("{alias}_status").as_str())?,
            created_at: row.try_get(format!("{alias}_created_at").as_str())?,
            created_by_kind: row.try_get(format!("{alias}_created_by_kind").as_str())?,
            created_by_role_id: row.try_get(format!("{alias}_created_by_role_id").as_str())?,
            template: row.try_get(format!("{alias}_template").as_str())?,
        }))
    };
    let role = |alias: &str| -> Result<Option<Role>, sqlx::Error> {
        let Some(id) = row.try_get::<Option<String>, _>(format!("{alias}_id").as_str())? else {
            return Ok(None);
        };
        Ok(Some(
            RoleRow {
                id,
                external_subject: row.try_get(format!("{alias}_external_subject").as_str())?,
                name: row.try_get(format!("{alias}_name").as_str())?,
                superadmin: row.try_get(format!("{alias}_superadmin").as_str())?,
                status: row.try_get(format!("{alias}_status").as_str())?,
                created_at: row.try_get(format!("{alias}_created_at").as_str())?,
                revoked_at: row.try_get(format!("{alias}_revoked_at").as_str())?,
            }
            .into_domain(),
        ))
    };
    let template_role = role("tr")?;
    let template = bounded_context("t")?
        .map(|template| Box::new(bounded_context_from_row(template, template_role, None)));
    let own = bounded_context("bc")?.expect("a hydrated row always has its own bounded context");
    Ok(bounded_context_from_row(own, role("r")?, template))
}

#[tracing::instrument(skip_all, fields(name = %name))]
pub async fn get_bounded_context(
    pool: &Pool,
    name: &str,
) -> crate::error::Result<Option<BoundedContext>> {
    // Its role and template in the same statement (docs/architecture.md
    // §199): read one after another they were up to four round trips, on
    // nearly every request.
    let row = sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT {} FROM {HYDRATED_BOUNDED_CONTEXT_FROM} WHERE bc.name = $1",
        hydrated_bounded_context_columns()
    )))
    .bind(name)
    .fetch_optional(pool)
    .await?;
    row.as_ref().map(hydrated_bounded_context).transpose()
}

/// Every `BoundedContext` this engine currently knows of - the
/// `bounded_contexts` parameter `bootstrap::list_bounded_contexts`
/// expects (see its own doc comment: unrestricted, same full-snapshot
/// treatment `list_roles`/`list_role_access_mappings` get). Backs the
/// `BoundedContextDirectory` surface's `boundedContexts` query - and,
/// issue #15, `SkiljBuilder::build()`'s startup warm-up loop
/// plus all three background pollers, which is why this used to be a
/// real N+1 (one `get_role` query per row) worth fixing: `list_roles`
/// already fetches every `Role` in one query, so one full-table fetch
/// plus an in-memory lookup replaces what was one query per row.
#[tracing::instrument(skip_all)]
pub async fn list_bounded_contexts(pool: &Pool) -> crate::error::Result<Vec<BoundedContext>> {
    let rows: Vec<BoundedContextRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {BOUNDED_CONTEXT_COLUMNS} FROM bounded_contexts"
    )))
    .fetch_all(pool)
    .await?;

    let roles_by_id: std::collections::HashMap<String, Role> = list_roles(pool)
        .await?
        .into_iter()
        .map(|role| (role.id.clone(), role))
        .collect();

    // issue #13: a template is always some other row already in
    // this same full-table fetch (invariant TemplateIsNeverItselfTemplated
    // rules out a template needing a template of its own), so resolving
    // `template` here is a map lookup, not an extra query per row - the
    // same N+1 avoidance this function's own doc comment already commits
    // to for `created_by_role_id`/`Role`. Build every context with
    // `template: None` first, keyed by name, then fill in the box on a
    // second pass.
    let mut contexts_by_name = std::collections::HashMap::with_capacity(rows.len());
    let mut template_names = Vec::with_capacity(rows.len());
    for row in rows {
        let role = row.created_by_role_id.as_ref().map(|role_id| {
            roles_by_id.get(role_id).cloned().expect(
                "bounded_contexts.created_by_role_id references a roles row that no longer \
                 exists",
            )
        });
        let name = row.name.clone();
        let template_name = row.template.clone();
        contexts_by_name.insert(name.clone(), bounded_context_from_row(row, role, None));
        template_names.push((name, template_name));
    }
    for (name, template_name) in template_names {
        let Some(template_name) = template_name else {
            continue;
        };
        let template = contexts_by_name.get(&template_name).cloned().expect(
            "bounded_contexts.template references a bounded_contexts row that no longer exists",
        );
        contexts_by_name.get_mut(&name).unwrap().template = Some(Box::new(template));
    }

    // Name order, deliberately, and the reason is a caller-visible
    // decision rather than tidiness: `skilj-graphql`'s schema build walks
    // these contexts in order and resolves generated type-name collisions
    // first-wins, so which of two colliding projections is served is
    // whatever order it walks them in. `HashMap::into_values()` is
    // `RandomState` order - per-thread, and re-seeded per process - so the
    // winner could change between two schema builds in one run, and a
    // caller could be served one projection and then, after an unrelated
    // registration change, be served the other. Observed in CI as
    // `colliding_projection_type_names_serve_one_and_refuse_the_other`
    // failing with *both* colliding projections served
    // (skilj/tests/projection_query.rs:1760), which is what a mid-test
    // rebuild that re-decided the collision looks like.
    //
    // So this order is now part of the function's contract, unlike
    // `list_roles`/`list_role_access_mappings`'s unordered `Vec` return.
    let mut contexts: Vec<BoundedContext> = contexts_by_name.into_values().collect();
    contexts.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(contexts)
}

/// Ultra-review bug_005's own fix - see migration `0003_add_bounded_
/// context_dispatch_template.sql`'s own doc comment for the full
/// reasoning. `createBoundedContextFromTemplate` calls this once,
/// immediately after `insert_bounded_context` succeeds, to set the
/// permanent, never-cleared record `TemplateCache`'s dispatch resolution
/// reads from (`list_dispatch_template_mappings` below) - deliberately
/// not part of `insert_bounded_context`'s own signature, since every
/// other caller of that function (bootstrap, ordinary `AddBoundedContext`,
/// every test fixture) has no dispatch template to set and shouldn't
/// need to pass one.
#[tracing::instrument(skip_all, fields(name = %name))]
pub async fn set_dispatch_template(
    pool: &Pool,
    name: &str,
    dispatch_template: &str,
) -> crate::error::Result<()> {
    sqlx::query("UPDATE bounded_contexts SET dispatch_template = $1 WHERE name = $2")
        .bind(dispatch_template)
        .bind(name)
        .execute(pool)
        .await?;
    Ok(())
}

/// `TemplateCache::refresh`'s own data source - every `BoundedContext`'s
/// own `name` paired with its `dispatch_template` (`None` for the
/// ordinary, untemplated case), read directly rather than through
/// `list_bounded_contexts`'s full `BoundedContext` hydration: dispatch
/// resolution needs nothing else about a context, and this stays a
/// single flat query with no recursive template lookup, no `Role`
/// join - a smaller, cheaper, single-purpose read for a call this
/// crate's own cross-instance listener makes on every registration
/// change.
#[tracing::instrument(skip_all)]
pub async fn list_dispatch_template_mappings(
    pool: &Pool,
) -> crate::error::Result<Vec<(String, Option<String>)>> {
    let rows: Vec<(String, Option<String>)> =
        sqlx::query_as("SELECT name, dispatch_template FROM bounded_contexts")
            .fetch_all(pool)
            .await?;
    Ok(rows)
}
