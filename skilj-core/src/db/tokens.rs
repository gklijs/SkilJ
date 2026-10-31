//! Persistence for tokens rule groups.

use super::*;

// --- AccessToken (three of its four variants - see this module's own doc comment) ---

/// `AccessToken.purpose`'s four values, mirroring the Rust `AccessToken`
/// sum type in `access_control` - used by `skilj-rest`'s auth layer to
/// tell "no token with this id exists at all" (401 - an unrecognised
/// credential) from "one does, but not of the variant this route needs"
/// (403 - docs/architecture.md §7.2's "presenting the wrong token
/// variant... is a 403, not a 404") without needing three separate
/// lookups to find out which.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccessTokenKind {
    ExternalEvent,
    DirectCreation,
    EventRead,
    Command,
}

impl AccessTokenKind {
    pub(super) fn as_str(self) -> &'static str {
        match self {
            AccessTokenKind::ExternalEvent => "external_event",
            AccessTokenKind::DirectCreation => "direct_creation",
            AccessTokenKind::EventRead => "event_read",
            AccessTokenKind::Command => "command",
        }
    }

    pub(super) fn from_str(s: &str) -> Self {
        match s {
            "direct_creation" => AccessTokenKind::DirectCreation,
            "event_read" => AccessTokenKind::EventRead,
            "command" => AccessTokenKind::Command,
            _ => AccessTokenKind::ExternalEvent,
        }
    }
}

/// The one thing a bearer credential's bare `id` can resolve on its own,
/// before anything else: which bounded context's schema actually holds
/// it. See `migrations/0001_init.sql`'s own doc comment on
/// `access_token_index` for why this index has to exist at all now that
/// `access_tokens` itself lives per-context.
pub(super) async fn resolve_token_bounded_context(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<String>> {
    let row: Option<(String,)> =
        sqlx::query_as("SELECT bounded_context FROM access_token_index WHERE id = $1")
            .bind(id)
            .fetch_optional(pool)
            .await?;
    Ok(row.map(|(bounded_context,)| bounded_context))
}

pub(super) async fn insert_token_index(
    pool: &Pool,
    id: &str,
    bounded_context: &str,
) -> crate::error::Result<()> {
    sqlx::query("INSERT INTO access_token_index (id, bounded_context) VALUES ($1, $2)")
        .bind(id)
        .bind(bounded_context)
        .execute(pool)
        .await?;
    Ok(())
}

#[derive(sqlx::FromRow)]
pub(super) struct AccessTokenColumns {
    pub(super) id: String,
    pub(super) kind: String,
    pub(super) secret: String,
    pub(super) status: String,
    pub(super) created_at: DateTime<Utc>,
    pub(super) revoked_at: Option<DateTime<Utc>>,
    pub(super) event_type_name: Option<String>,
    pub(super) command_type_name: Option<String>,
    /// One column, all four kinds - see `EventReadToken.scope`'s own doc
    /// comment for the read-side reasoning and
    /// `ExternalEventToken.scope`'s for the write-side one.
    pub(super) scope: Option<String>,
    /// One column, all four kinds, meaningful only for `event_read` -
    /// see `EventReadToken.start_from`'s own doc comment.
    /// `event_read_start_position_from_str` only ever gets called on
    /// this for a genuine `event_read` row; the other three kinds carry
    /// whatever the column's own `DEFAULT` gave it, read here but never
    /// interpreted.
    pub(super) start_from: String,
    /// `EventReadToken.start_at_sequence`/`.start_at_time` - two more
    /// columns, all four kinds, meaningful only for `event_read` on the
    /// identical terms `start_from` above already is.
    pub(super) start_at_sequence: Option<i64>,
    pub(super) start_at_time: Option<DateTime<Utc>>,
}

pub(super) struct AccessTokenRow {
    pub(super) columns: AccessTokenColumns,
    pub(super) bounded_context: String,
}

/// Resolves the index first (see `resolve_token_bounded_context`), then
/// the token's own full row from that context's schema. `None` when
/// either step finds nothing - a bare `id` genuinely unrecognised either
/// way, the same "doesn't distinguish which" treatment
/// `RestError::UnrecognisedCredential` already gives on the wire.
pub(super) async fn fetch_access_token_row(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<AccessTokenRow>> {
    // docs/architecture.md §199: a token never moves to another bounded
    // context, so which one holds it is remembered, and every REST request
    // after a token's first reads its row in one round trip instead of
    // two. Only the location is remembered, never the row: status and
    // revocation are read every time. A remembered location with no row
    // (the token or its bounded context deleted since) is forgotten and
    // the index read again, so a stale entry corrects itself.
    if let Some(bounded_context) = remembered_token_context(id) {
        match read_access_token_columns(pool, &bounded_context, id).await {
            Ok(Some(columns)) => {
                return Ok(Some(AccessTokenRow {
                    columns,
                    bounded_context,
                }))
            }
            Ok(None) => forget_token_context(id),
            Err(crate::error::Error::Database(sqlx::Error::Database(e)))
                if e.code().as_deref() == Some("42P01") =>
            {
                forget_token_context(id)
            }
            Err(e) => return Err(e),
        }
    }
    let Some(bounded_context) = resolve_token_bounded_context(pool, id).await? else {
        return Ok(None);
    };
    let columns = read_access_token_columns(pool, &bounded_context, id).await?;
    if columns.is_some() {
        remember_token_context(id, &bounded_context);
    }
    Ok(columns.map(|columns| AccessTokenRow {
        columns,
        bounded_context,
    }))
}

/// One token's own row from `bounded_context`'s schema.
pub(super) async fn read_access_token_columns(
    pool: &Pool,
    bounded_context: &str,
    id: &str,
) -> crate::error::Result<Option<AccessTokenColumns>> {
    let schema = schema_ident(bounded_context);
    Ok(sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT id, kind, secret, status, created_at, revoked_at, event_type_name, \
         command_type_name, scope, start_from, start_at_sequence, start_at_time \
         FROM {schema}.access_tokens WHERE id = $1"
    )))
    .bind(id)
    .fetch_optional(pool)
    .await?)
}

/// How many token locations [`fetch_access_token_row`] remembers before it
/// starts over - a bound on memory, not a tuning knob: a forgotten
/// location only costs the index read again.
pub(super) const REMEMBERED_TOKEN_CONTEXTS: usize = 10_000;

/// Token id -> the bounded context whose schema holds it.
pub(super) fn token_contexts(
) -> &'static std::sync::Mutex<std::collections::HashMap<String, String>> {
    static CONTEXTS: std::sync::OnceLock<
        std::sync::Mutex<std::collections::HashMap<String, String>>,
    > = std::sync::OnceLock::new();
    CONTEXTS.get_or_init(Default::default)
}

pub(super) fn remembered_token_context(id: &str) -> Option<String> {
    token_contexts()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(id)
        .cloned()
}

pub(super) fn remember_token_context(id: &str, bounded_context: &str) {
    let mut contexts = token_contexts().lock().unwrap_or_else(|e| e.into_inner());
    if contexts.len() >= REMEMBERED_TOKEN_CONTEXTS {
        contexts.clear();
    }
    contexts.insert(id.to_string(), bounded_context.to_string());
}

pub(super) fn forget_token_context(id: &str) {
    token_contexts()
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .remove(id);
}

/// See `AccessTokenKind`'s own doc comment - the one lookup `skilj-rest`'s
/// auth layer needs before deciding 401 vs 403.
#[tracing::instrument(skip_all)]
pub async fn access_token_kind(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<AccessTokenKind>> {
    Ok(fetch_access_token_row(pool, id)
        .await?
        .map(|r| AccessTokenKind::from_str(&r.columns.kind)))
}

/// [`access_token_kind`], but only for a token whose stored secret
/// `hashed_secret` (the presented secret, already through
/// `shared::hash_secret`) matches - so the kind, and that the id exists
/// at all, is told only to a caller holding the whole credential
/// (docs/architecture.md §142).
#[tracing::instrument(skip_all)]
pub async fn access_token_kind_for_credential(
    pool: &Pool,
    id: &str,
    hashed_secret: &str,
) -> crate::error::Result<Option<AccessTokenKind>> {
    Ok(fetch_access_token_row(pool, id)
        .await?
        .filter(|r| crate::shared::secret_matches(hashed_secret, &r.columns.secret))
        .map(|r| AccessTokenKind::from_str(&r.columns.kind)))
}

/// Persists a `revoke_token` outcome - a status-only update, addressed by
/// `id` alone (unambiguous: `access_tokens.id` is that schema's own
/// primary key). Resolves which schema holds the row the same way
/// `fetch_access_token_row` does, via `access_token_index`, rather than
/// asking every call site to already know the bounded context -
/// `revoke_token`'s own `token` parameter carries a whole `EventType`/
/// `CommandType`, not a bare name, so re-deriving it here is simpler than
/// threading an extra parameter through. A no-op (not an error) for an
/// `id` that resolves to nothing - unreachable in practice, since the
/// caller already loaded this exact row to build the `AccessToken` it
/// passed to `revoke_token` in the first place.
#[tracing::instrument(skip_all)]
pub async fn revoke_access_token(
    pool: &Pool,
    id: &str,
    revoked_at: DateTime<Utc>,
) -> crate::error::Result<()> {
    let Some(bounded_context) = resolve_token_bounded_context(pool, id).await? else {
        return Ok(());
    };
    let schema = schema_ident(&bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.access_tokens SET status = 'revoked', revoked_at = $1 WHERE id = $2"
    )))
    .bind(revoked_at)
    .bind(id)
    .execute(pool)
    .await?;
    Ok(())
}

/// One insert shared by `insert_external_event_token`/
/// `insert_direct_creation_token`/`insert_event_read_token` below - same
/// "genuinely this wide, not a struct that exists only to satisfy the
/// lint" reasoning `event_store::process_command`'s own `#[allow]` gives.
/// Writes the token's full row into its own context's schema, then the
/// global index entry that makes it findable by `id` alone - in that
/// order, so a failure partway leaves an unreachable orphan row rather
/// than a dangling index entry pointing at nothing. `secret` is the
/// caller's plaintext (`generate_token_secret`'s own output, still held
/// in memory by the caller to hand back once) - only `hash_secret`'s
/// output of it is ever written, per `AccessToken.secret`'s own "stored
/// hashed and never compared in plaintext" text.
#[allow(clippy::too_many_arguments)]
pub(super) async fn insert_access_token_row(
    pool: &Pool,
    id: &str,
    kind: AccessTokenKind,
    secret: &str,
    status: TokenStatus,
    created_at: DateTime<Utc>,
    revoked_at: Option<DateTime<Utc>>,
    bounded_context: &str,
    event_type_name: &str,
    scope: Option<&str>,
) -> crate::error::Result<()> {
    let schema = schema_ident(bounded_context);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.access_tokens (id, kind, secret, status, created_at, revoked_at, \
         event_type_name, scope) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
    )))
    .bind(id)
    .bind(kind.as_str())
    .bind(crate::shared::hash_secret(secret))
    .bind(token_status_to_str(status))
    .bind(created_at)
    .bind(revoked_at)
    .bind(event_type_name)
    .bind(scope)
    .execute(pool)
    .await?;
    insert_token_index(pool, id, bounded_context).await
}

#[tracing::instrument(skip_all)]
pub async fn insert_external_event_token(
    pool: &Pool,
    token: &ExternalEventToken,
) -> crate::error::Result<()> {
    insert_access_token_row(
        pool,
        &token.id,
        AccessTokenKind::ExternalEvent,
        &token.secret,
        token.status,
        token.created_at,
        token.revoked_at,
        &token.event_type.bounded_context.name,
        &token.event_type.name,
        token.scope.as_deref(),
    )
    .await
}

#[tracing::instrument(skip_all)]
pub async fn insert_direct_creation_token(
    pool: &Pool,
    token: &DirectCreationToken,
) -> crate::error::Result<()> {
    insert_access_token_row(
        pool,
        &token.id,
        AccessTokenKind::DirectCreation,
        &token.secret,
        token.status,
        token.created_at,
        token.revoked_at,
        &token.event_type.bounded_context.name,
        &token.event_type.name,
        token.scope.as_deref(),
    )
    .await
}

/// Not built on `insert_access_token_row` - `EventReadToken` is the only
/// one of the three event-type-scoped kinds that also carries
/// `start_from`, so this hand-writes its own `INSERT` the same way
/// `insert_command_token` already does for its own divergent shape (see
/// that function's own doc comment).
#[tracing::instrument(skip_all)]
pub async fn insert_event_read_token(
    pool: &Pool,
    token: &EventReadToken,
) -> crate::error::Result<()> {
    let schema = schema_ident(&token.event_type.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.access_tokens (id, kind, secret, status, created_at, revoked_at, \
         event_type_name, scope, start_from, start_at_sequence, start_at_time) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)"
    )))
    .bind(&token.id)
    .bind(AccessTokenKind::EventRead.as_str())
    .bind(crate::shared::hash_secret(&token.secret))
    .bind(token_status_to_str(token.status))
    .bind(token.created_at)
    .bind(token.revoked_at)
    .bind(&token.event_type.name)
    .bind(token.scope.as_deref())
    .bind(event_read_start_position_to_str(token.start_from))
    .bind(token.start_at_sequence)
    .bind(token.start_at_time)
    .execute(pool)
    .await?;
    insert_token_index(pool, &token.id, &token.event_type.bounded_context.name).await
}

/// See `insert_access_token_row` above - not built on it directly, since
/// a `CommandToken` fills `command_type_name` instead of the
/// `event_type_name` every other variant does (see the migration's own
/// `access_tokens` `CHECK` constraint), and the same insert-row-then-index
/// ordering. `token.secret` is hashed before storage the same way
/// `insert_access_token_row` does.
#[tracing::instrument(skip_all)]
pub async fn insert_command_token(pool: &Pool, token: &CommandToken) -> crate::error::Result<()> {
    let schema = schema_ident(&token.command_type.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.access_tokens (id, kind, secret, status, created_at, revoked_at, \
         command_type_name, scope) VALUES ($1,$2,$3,$4,$5,$6,$7,$8)"
    )))
    .bind(&token.id)
    .bind(AccessTokenKind::Command.as_str())
    .bind(crate::shared::hash_secret(&token.secret))
    .bind(token_status_to_str(token.status))
    .bind(token.created_at)
    .bind(token.revoked_at)
    .bind(&token.command_type.name)
    .bind(token.scope.as_deref())
    .execute(pool)
    .await?;
    insert_token_index(pool, &token.id, &token.command_type.bounded_context.name).await
}

/// `None` when either no token has this `id` at all, or one does but
/// isn't the expected kind - callers that need to tell those two apart
/// (for the 401-vs-403 split - see `AccessTokenKind`'s own doc comment)
/// call `access_token_kind` first. Covers `get_external_event_token`/
/// `get_direct_creation_token`, identical apart from the `kind` string
/// and return type - both resolve `event_type_name` against
/// `event_types` into an `event_type` field and carry `scope` alike
/// (cross-tenant read/write fix, docs/architecture.md's own write-up of
/// these passes). `get_event_read_token` used to share this shape too,
/// back when all three carried nothing but `scope` beyond the common
/// base - `start_from` (this pass's own write-up) broke that symmetry,
/// so it now stays hand-written below the same way `get_command_token`
/// already does for its own divergent shape.
macro_rules! get_event_type_access_token {
    ($fn_name:ident, $return_type:ident, $kind:literal) => {
        #[tracing::instrument(skip_all)]
        pub async fn $fn_name(pool: &Pool, id: &str) -> crate::error::Result<Option<$return_type>> {
            let Some(row) = fetch_access_token_row(pool, id).await? else {
                return Ok(None);
            };
            if row.columns.kind != $kind {
                return Ok(None);
            }
            let event_type_name = row
                .columns
                .event_type_name
                .expect(concat!($kind, " access_tokens row without event_type_name"));
            let event_type =
                require_event_type(pool, &row.bounded_context, &event_type_name).await?;
            Ok(Some($return_type {
                id: row.columns.id,
                secret: row.columns.secret,
                status: token_status_from_str(&row.columns.status),
                created_at: row.columns.created_at,
                revoked_at: row.columns.revoked_at,
                event_type,
                scope: row.columns.scope,
            }))
        }
    };
}
get_event_type_access_token!(
    get_external_event_token,
    ExternalEventToken,
    "external_event"
);
get_event_type_access_token!(
    get_direct_creation_token,
    DirectCreationToken,
    "direct_creation"
);

/// See `get_external_event_token`'s own doc comment's note on why this
/// one stays hand-written - the only difference from what the macro
/// generates is the extra `start_from` field, read via
/// `event_read_start_position_from_str`.
#[tracing::instrument(skip_all)]
pub async fn get_event_read_token(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<EventReadToken>> {
    let Some(row) = fetch_access_token_row(pool, id).await? else {
        return Ok(None);
    };
    if row.columns.kind != "event_read" {
        return Ok(None);
    }
    let event_type_name = row
        .columns
        .event_type_name
        .expect("event_read access_tokens row without event_type_name");
    let event_type = require_event_type(pool, &row.bounded_context, &event_type_name).await?;
    Ok(Some(EventReadToken {
        id: row.columns.id,
        secret: row.columns.secret,
        status: token_status_from_str(&row.columns.status),
        created_at: row.columns.created_at,
        revoked_at: row.columns.revoked_at,
        event_type,
        scope: row.columns.scope,
        start_from: event_read_start_position_from_str(&row.columns.start_from),
        start_at_sequence: row.columns.start_at_sequence,
        start_at_time: row.columns.start_at_time,
    }))
}

/// See `get_external_event_token`'s own doc comment - same shape and
/// `None` reasoning, resolving `command_type_name` against
/// `command_types` instead of `event_type_name` against `event_types`.
#[tracing::instrument(skip_all)]
pub async fn get_command_token(
    pool: &Pool,
    id: &str,
) -> crate::error::Result<Option<CommandToken>> {
    let Some(row) = fetch_access_token_row(pool, id).await? else {
        return Ok(None);
    };
    if row.columns.kind != "command" {
        return Ok(None);
    }
    let command_type_name = row
        .columns
        .command_type_name
        .expect("command access_tokens row without command_type_name");
    let command_type = require_command_type(pool, &row.bounded_context, &command_type_name).await?;
    Ok(Some(CommandToken {
        id: row.columns.id,
        secret: row.columns.secret,
        status: token_status_from_str(&row.columns.status),
        created_at: row.columns.created_at,
        revoked_at: row.columns.revoked_at,
        command_type,
        scope: row.columns.scope,
    }))
}
