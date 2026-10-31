//! Persistence for command records rule groups.

use super::*;

// --- Command ---

#[derive(sqlx::FromRow)]
pub(super) struct CommandRow {
    pub(super) external_id: String,
    pub(super) command_type_name: String,
    pub(super) payload: String,
    pub(super) metadata_type: String,
    pub(super) metadata_version: i64,
    pub(super) metadata_client_id: String,
    pub(super) metadata_created_at: DateTime<Utc>,
    // issue #18 - `None` only for a row written before these
    // columns existed.
    pub(super) metadata_correlation_id: Option<String>,
    pub(super) metadata_causation_id: Option<String>,
    pub(super) consistency_tags: Json<Vec<Tag>>,
    pub(super) consistency_boundary: Option<i64>,
    pub(super) consistency_query: Json<Vec<QueryItem>>,
}

impl CommandRow {
    pub(super) async fn into_domain(
        self,
        pool: &Pool,
        bounded_context: &str,
    ) -> crate::error::Result<Command> {
        let command_type =
            require_command_type(pool, bounded_context, &self.command_type_name).await?;
        Ok(self.with_type(command_type))
    }

    pub(super) fn with_type(self, command_type: CommandType) -> Command {
        Command {
            id: self.external_id,
            bounded_context: command_type.bounded_context.clone(),
            command_type,
            payload: self.payload,
            metadata: Metadata {
                r#type: self.metadata_type,
                version: self.metadata_version,
                client_id: self.metadata_client_id,
                created_at: self.metadata_created_at,
                correlation_id: self.metadata_correlation_id,
                causation_id: self.metadata_causation_id,
            },
            encryption_keys: Vec::new(),
            consistency_tags: self.consistency_tags.0,
            consistency_query: self.consistency_query.0,
            consistency_boundary: self.consistency_boundary,
        }
    }
}

/// `CommandRow::into_domain` for many rows at once: the bounded context
/// once and every command type the rows name in one query, instead of a
/// command type, its bounded context and that context's creator Role per
/// row (docs/architecture.md §126). A row naming a type that's gone is
/// the same `row_not_found` `into_domain` gives.
pub(super) async fn commands_from_rows(
    pool: &Pool,
    bounded_context: &str,
    rows: Vec<CommandRow>,
) -> crate::error::Result<Vec<Command>> {
    if rows.is_empty() {
        return Ok(Vec::new());
    }
    let bc = require_bounded_context(pool, bounded_context).await?;
    let mut names: Vec<&str> = rows.iter().map(|r| r.command_type_name.as_str()).collect();
    names.sort_unstable();
    names.dedup();
    let command_types = get_command_types_by_names_with_bc(pool, &bc, &names).await?;
    rows.into_iter()
        .map(|row| {
            let command_type = command_types
                .get(&row.command_type_name)
                .cloned()
                .ok_or_else(crate::error::Error::row_not_found)?;
            Ok(row.with_type(command_type))
        })
        .collect()
}

// `external_id` - not `id`, this table's own internal `BIGSERIAL` primary
// key, purely an FK-linking detail `insert_command` returns separately
// and never surfaces on `Command` itself - is `Command.id` (drift audit
// finding #12): a real, `generate_token_id()`-assigned identity, added so
// `fetch_commands`'s own `triggered_event` lookup can match a specific
// command precisely rather than by whole-struct content equality.
pub(super) const COMMAND_COLUMNS: &str = "external_id, command_type_name, payload, metadata_type, \
    metadata_version, metadata_client_id, metadata_created_at, metadata_correlation_id, \
    metadata_causation_id, consistency_tags, consistency_boundary, consistency_query";

/// Insert-only, unlike every `upsert_*` above - re-registration/promotion
/// don't apply to a `Command`, so every call is a new row, even though it
/// now has a real identity of its own (`Command.id`, drift audit finding
/// #12 - not this row's internal `BIGSERIAL id`, returned separately
/// below and never surfaced on the domain struct - see the migration's
/// own doc comment on `commands`). Returns the new row's internal `id`,
/// needed to link the `Event`s `process_command` produced back to it (see
/// `insert_event`'s own `command_id` parameter). `encryption_key_ids` is
/// `get_or_create_encryption_key`'s own returned `id`s for
/// `command.encryption_keys` - see `insert_event_and_update_sync_projections`'s
/// own doc comment on why the domain struct alone isn't enough to link
/// the join rows.
///
/// Takes an already-open `tx` rather than opening/committing its own
/// (previously the latter) - `ProcessCommand`'s own REST/GraphQL call
/// sites now share one outer transaction across `next_sequence`'s lock,
/// this insert, and every triggered `Event`'s own insert, so the command
/// row and every one of the events it triggered commit or roll back
/// together - see `DynamicConsistencyBoundaryHonoured` and the note above
/// the rules in specs/skilj.allium.
#[tracing::instrument(skip_all)]
pub async fn insert_command(
    tx: &mut Transaction<'_, Postgres>,
    command: &Command,
    encryption_key_ids: &[i64],
) -> crate::error::Result<i64> {
    let schema = schema_ident(&command.bounded_context.name);
    let (id,): (i64,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.commands ({COMMAND_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12) RETURNING id"
    )))
    .bind(&command.id)
    .bind(&command.command_type.name)
    .bind(&command.payload)
    .bind(&command.metadata.r#type)
    .bind(command.metadata.version)
    .bind(&command.metadata.client_id)
    .bind(command.metadata.created_at)
    .bind(&command.metadata.correlation_id)
    .bind(&command.metadata.causation_id)
    .bind(Json(&command.consistency_tags))
    .bind(command.consistency_boundary)
    .bind(Json(&command.consistency_query))
    .fetch_one(&mut **tx)
    .await?;

    // One multi-row insert via `unnest` instead of one round trip per
    // key - most commands carry zero or one, but a command whose
    // payload touches several `sensitive_field_subjects` can carry
    // several, and each used to be its own serialized `INSERT` inside
    // this transaction's own held lock (docs/architecture.md's load-test
    // §: `commit_command_batch`'s per-command critical section).
    if !encryption_key_ids.is_empty() {
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "INSERT INTO {schema}.command_encryption_keys (command_id, encryption_key_id) \
             SELECT $1, unnest($2::bigint[])"
        )))
        .bind(id)
        .bind(encryption_key_ids)
        .execute(&mut **tx)
        .await?;
    }

    Ok(id)
}

#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_command_by_id(
    pool: &Pool,
    bounded_context: &str,
    id: i64,
) -> crate::error::Result<Option<Command>> {
    let schema = schema_ident(bounded_context);
    let row: Option<CommandRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_COLUMNS} FROM {schema}.commands WHERE id = $1"
    )))
    .bind(id)
    .fetch_optional(pool)
    .await?;
    match row {
        Some(row) => Ok(Some(row.into_domain(pool, bounded_context).await?)),
        None => Ok(None),
    }
}

/// `get_command_by_id`'s batched sibling - one `WHERE id = ANY($1)`
/// round trip for however many distinct commands a caller needs, instead
/// of one round trip (each several deep via `CommandRow::into_domain`'s
/// own `get_command_type`) per id. Built for
/// `list_events_for_bounded_context_matching_tags_with_bc`'s own
/// row-processing loop (docs/architecture.md §61's round four) - a real
/// load test found resolving each `command_triggered` row's origin one
/// at a time was the single biggest cost inside `commit_command_batch`'s
/// own held lock, and a first, *concurrent* attempt at fixing it
/// measured worse (bursting many simultaneous connection requests
/// starved the next batch's own lock acquisition) before this batched
/// version replaced it. `ids` empty returns an empty map without
/// touching Postgres. Takes `bc` (not a bounded context name) for the
/// identical "caller already has this row, don't re-fetch it" reasoning
/// `get_event_type_with_bc` gives.
pub(super) async fn get_commands_by_ids_with_bc(
    conn: &mut sqlx::PgConnection,
    bc: &BoundedContext,
    ids: &[i64],
) -> crate::error::Result<std::collections::HashMap<i64, Command>> {
    if ids.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let schema = schema_ident(&bc.name);
    let rows: Vec<CommandRowWithId> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, {COMMAND_COLUMNS} FROM {schema}.commands WHERE id = ANY($1)"
    )))
    .bind(ids)
    .fetch_all(&mut *conn)
    .await?;

    let mut distinct_type_names: Vec<&str> = Vec::new();
    for row in &rows {
        if !distinct_type_names.contains(&row.command_type_name.as_str()) {
            distinct_type_names.push(&row.command_type_name);
        }
    }
    let command_types = get_command_types_by_names_with_bc(conn, bc, &distinct_type_names).await?;

    let mut commands = std::collections::HashMap::with_capacity(rows.len());
    for row in rows {
        // Unlike `get_command_type`'s own lookup (see `require_command_type`),
        // `get_command_types_by_names_with_bc` queries `{schema}.command_types`
        // directly with the `bc` already in hand - no internal existence
        // recheck to silently swallow. A `hard_delete_bounded_context` race
        // dropping the schema between the `commands` query above and this one
        // surfaces as a real `sqlx::Error` from that `.await?`, not a `None`
        // reaching this lookup - so a genuine miss here is a real bug in the
        // data (a `commands` row outliving the `command_types` row it names),
        // not a race, and this stays a panic.
        let command_type = command_types
            .get(&row.command_type_name)
            .expect("commands row references a command_types row that no longer exists")
            .clone();
        commands.insert(
            row.id,
            Command {
                id: row.external_id,
                bounded_context: command_type.bounded_context.clone(),
                command_type,
                payload: row.payload,
                metadata: Metadata {
                    r#type: row.metadata_type,
                    version: row.metadata_version,
                    client_id: row.metadata_client_id,
                    created_at: row.metadata_created_at,
                    correlation_id: row.metadata_correlation_id,
                    causation_id: row.metadata_causation_id,
                },
                encryption_keys: Vec::new(),
                consistency_tags: row.consistency_tags.0,
                consistency_query: row.consistency_query.0,
                consistency_boundary: row.consistency_boundary,
            },
        );
    }
    Ok(commands)
}

/// `CommandRow`'s own sibling with the internal `BIGSERIAL` id included -
/// `get_commands_by_ids_with_bc`'s own `WHERE id = ANY($1)` needs it to
/// map each returned row back to the id that requested it, which
/// `CommandRow` itself never carries (every other caller already knows
/// the id it asked for).
#[derive(sqlx::FromRow)]
pub(super) struct CommandRowWithId {
    pub(super) id: i64,
    pub(super) external_id: String,
    pub(super) command_type_name: String,
    pub(super) payload: String,
    pub(super) metadata_type: String,
    pub(super) metadata_version: i64,
    pub(super) metadata_client_id: String,
    pub(super) metadata_created_at: DateTime<Utc>,
    pub(super) metadata_correlation_id: Option<String>,
    pub(super) metadata_causation_id: Option<String>,
    pub(super) consistency_tags: Json<Vec<Tag>>,
    pub(super) consistency_boundary: Option<i64>,
    pub(super) consistency_query: Json<Vec<QueryItem>>,
}

/// `get_command_type`'s batched sibling - `get_event_type_with_bc`'s own
/// precedent, extended to a whole set of names in one `WHERE name =
/// ANY($1)` round trip. `names` empty returns an empty map without
/// touching Postgres.
pub(super) async fn get_command_types_by_names_with_bc(
    executor: impl sqlx::PgExecutor<'_>,
    bc: &BoundedContext,
    names: &[&str],
) -> crate::error::Result<std::collections::HashMap<String, CommandType>> {
    if names.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let schema = schema_ident(&bc.name);
    let rows: Vec<CommandTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_TYPE_COLUMNS} FROM {schema}.command_types WHERE name = ANY($1)"
    )))
    .bind(names)
    .fetch_all(executor)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.name.clone(), row.into_domain(bc.clone())))
        .collect())
}

/// `get_command_by_id`'s own sibling, addressed by `Command.id`
/// (`commands.external_id`) instead of the internal `BIGSERIAL` - what a
/// caller holding a domain `Command.id` (the private-field mechanism's
/// own `grantPrivateFieldAccessForCommand` mutation, naming the command
/// being shared by its own wire id) needs, without knowing the internal
/// one at all.
pub async fn get_command_by_external_id(
    pool: &Pool,
    bounded_context: &str,
    external_id: &str,
) -> crate::error::Result<Option<Command>> {
    let schema = schema_ident(bounded_context);
    let row: Option<CommandRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_COLUMNS} FROM {schema}.commands WHERE external_id = $1"
    )))
    .bind(external_id)
    .fetch_optional(pool)
    .await?;
    match row {
        Some(row) => Ok(Some(row.into_domain(pool, bounded_context).await?)),
        None => Ok(None),
    }
}

/// Where the command `external_id` sits in its bounded context's
/// recording order - the internal `commands.id` key, which only ever
/// grows with each insert. Rule `FetchCommands`' `recorded_after`: a
/// `fetchCommands` page continues after this position. `None` when no
/// such command exists here.
pub async fn command_recording_position(
    pool: &Pool,
    bounded_context: &str,
    external_id: &str,
) -> crate::error::Result<Option<i64>> {
    let schema = schema_ident(bounded_context);
    Ok(sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {schema}.commands WHERE external_id = $1"
    )))
    .bind(external_id)
    .fetch_optional(pool)
    .await?)
}

#[derive(sqlx::FromRow)]
pub(super) struct PositionedCommandRow {
    pub(super) id: i64,
    #[sqlx(flatten)]
    pub(super) row: CommandRow,
}

/// One bounded `fetchCommands` page: the bounded context's commands
/// recorded after `after_position` (`-1` for the start), in recording
/// order, `max_commands` at a time, with `select` - the rule's own pure
/// function - choosing what to serve from each chunk (given how many
/// more fit) until the page is full or history ends. Rule
/// `FetchCommands`' `first_by_recording`, the command counterpart to
/// [`collect_event_page`]. `select` runs at least once, so its own
/// validation errors surface even on an empty history.
pub async fn collect_command_page(
    pool: &Pool,
    bounded_context: &str,
    mut after_position: i64,
    max_commands: usize,
    mut select: impl FnMut(&[Command], usize) -> crate::error::Result<Vec<Command>>,
) -> crate::error::Result<Vec<Command>> {
    let chunk_size = max_commands.max(1);
    let limit = i64::try_from(chunk_size).unwrap_or(i64::MAX);
    let schema = schema_ident(bounded_context);
    let mut page = Vec::new();
    loop {
        let rows: Vec<PositionedCommandRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
            "SELECT id, {COMMAND_COLUMNS} FROM {schema}.commands \
             WHERE id > $1 ORDER BY id LIMIT $2"
        )))
        .bind(after_position)
        .bind(limit)
        .fetch_all(pool)
        .await?;
        let exhausted = rows.len() < chunk_size;
        if let Some(last) = rows.last() {
            after_position = last.id;
        }
        let chunk = commands_from_rows(
            pool,
            bounded_context,
            rows.into_iter().map(|p| p.row).collect(),
        )
        .await?;
        page.extend(select(&chunk, chunk_size - page.len())?);
        if page.len() >= chunk_size || exhausted {
            return Ok(page);
        }
    }
}

/// Every `Command` currently stored for a whole bounded context - the
/// full-snapshot parameter `fetch_commands`' own `bounded_context_commands`
/// expects (same treatment `list_events_for_bounded_context` gets for
/// `Event`), added propagating `skilj-graphql`'s `CommandQuery` resolver
/// (Phase 3).
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_commands_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<Command>> {
    let schema = schema_ident(bounded_context);
    let rows: Vec<CommandRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_COLUMNS} FROM {schema}.commands ORDER BY id"
    )))
    .fetch_all(pool)
    .await?;

    commands_from_rows(pool, bounded_context, rows).await
}
