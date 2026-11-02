//! Persistence for event types rule groups.

use super::*;

// --- EventType ---

pub(super) fn missed_occurrence_policy_to_str(policy: MissedOccurrencePolicy) -> &'static str {
    match policy {
        MissedOccurrencePolicy::Skip => "skip",
        MissedOccurrencePolicy::FireOnce => "fire_once",
        MissedOccurrencePolicy::ReplayBacklog => "replay_backlog",
    }
}

pub(super) fn missed_occurrence_policy_from_str(s: &str) -> MissedOccurrencePolicy {
    match s {
        "fire_once" => MissedOccurrencePolicy::FireOnce,
        "replay_backlog" => MissedOccurrencePolicy::ReplayBacklog,
        _ => MissedOccurrencePolicy::Skip,
    }
}

/// issue #95 - a recurring chain's own policy, as stored.
pub(super) fn recurrence_policy_to_str(policy: crate::plugin::MissedRunPolicy) -> &'static str {
    match policy {
        crate::plugin::MissedRunPolicy::Skip => "skip",
        crate::plugin::MissedRunPolicy::FireOnce => "fire_once",
        crate::plugin::MissedRunPolicy::FireAll => "fire_all",
    }
}

/// The inverse of `recurrence_policy_to_str` - `None` for a `NULL`
/// column (a one-shot) or for a value no writer this codebase knows
/// could have stored (defensive; the `CHECK` constraint already refuses
/// those at the door).
pub(super) fn recurrence_policy_from_str(
    s: Option<&str>,
) -> Option<crate::plugin::MissedRunPolicy> {
    match s {
        Some("skip") => Some(crate::plugin::MissedRunPolicy::Skip),
        Some("fire_once") => Some(crate::plugin::MissedRunPolicy::FireOnce),
        Some("fire_all") => Some(crate::plugin::MissedRunPolicy::FireAll),
        _ => None,
    }
}

#[derive(sqlx::FromRow)]
pub(super) struct EventTypeRow {
    pub(super) name: String,
    pub(super) schema: String,
    pub(super) schema_version: i64,
    pub(super) tag_mappings: Json<Vec<TagMapping>>,
    pub(super) owner_tag_key: Option<String>,
    pub(super) sensitive_fields: Json<Vec<SensitiveField>>,
    pub(super) external_creation_allowed: bool,
    pub(super) direct_creation_allowed: bool,
    pub(super) system_triggered_allowed: bool,
    pub(super) system_triggered_schedule: Option<String>,
    pub(super) missed_occurrence_policy: Option<String>,
    pub(super) schedule_position: Option<DateTime<Utc>>,
    pub(super) last_fired_at: Option<DateTime<Utc>>,
    pub(super) event_read_allowed: bool,
    pub(super) private_fields: Json<Vec<PrivateField>>,
}

impl EventTypeRow {
    pub(super) fn into_domain(self, bounded_context: BoundedContext) -> EventType {
        EventType {
            bounded_context,
            name: self.name,
            schema: self.schema,
            schema_version: self.schema_version,
            tag_mappings: self.tag_mappings.0,
            owner_tag_key: self.owner_tag_key,
            sensitive_fields: self.sensitive_fields.0,
            private_fields: self.private_fields.0,
            external_creation_allowed: self.external_creation_allowed,
            direct_creation_allowed: self.direct_creation_allowed,
            system_triggered_allowed: self.system_triggered_allowed,
            system_triggered_schedule: self.system_triggered_schedule,
            missed_occurrence_policy: self
                .missed_occurrence_policy
                .as_deref()
                .map(missed_occurrence_policy_from_str),
            schedule_position: self.schedule_position,
            last_fired_at: self.last_fired_at,
            event_read_allowed: self.event_read_allowed,
        }
    }
}

pub(super) const EVENT_TYPE_COLUMNS: &str =
    "name, schema, schema_version, tag_mappings, owner_tag_key, sensitive_fields, \
    external_creation_allowed, direct_creation_allowed, system_triggered_allowed, \
    system_triggered_schedule, missed_occurrence_policy, schedule_position, last_fired_at, \
    event_read_allowed, private_fields";

/// Upsert, not insert-only - `RegisterEventType`'s own create-or-update
/// shape (see `event_store::register_event_type`), though no surface
/// calls this yet this pass; used directly by tests/seeding until
/// `RegisterEventType` itself has a GraphQL route in front of it.
#[tracing::instrument(skip_all)]
/// Registration's write. `schedule_position` and `last_fired_at` belong to
/// the scheduler once a row exists - it advances them under `FOR UPDATE`
/// while registration reads them unlocked - so on conflict they come from
/// the row as it is now, not from `et` (a possibly stale read):
/// `last_fired_at` is never overwritten, and `schedule_position` only at
/// the opt-in moment, scheduling being newly enabled against the current
/// row (docs/architecture.md §76). Writing `et`'s copies back used to
/// rewind a position the scheduler had just advanced, re-firing the
/// occurrence - every instance re-registers every type at startup.
pub async fn upsert_event_type(pool: &Pool, et: &EventType) -> crate::error::Result<()> {
    check_blind_tag_mappings(pool, &et.bounded_context.name, &et.tag_mappings).await?;
    let schema = schema_ident(&et.bounded_context.name);
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {schema}.event_types ({EVENT_TYPE_COLUMNS}) \
         VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) \
         ON CONFLICT (name) DO UPDATE SET \
            schema = EXCLUDED.schema, schema_version = EXCLUDED.schema_version, \
            tag_mappings = EXCLUDED.tag_mappings, owner_tag_key = EXCLUDED.owner_tag_key, \
            sensitive_fields = EXCLUDED.sensitive_fields, \
            external_creation_allowed = EXCLUDED.external_creation_allowed, \
            direct_creation_allowed = EXCLUDED.direct_creation_allowed, \
            system_triggered_allowed = EXCLUDED.system_triggered_allowed, \
            system_triggered_schedule = EXCLUDED.system_triggered_schedule, \
            missed_occurrence_policy = EXCLUDED.missed_occurrence_policy, \
            schedule_position = CASE \
                WHEN EXCLUDED.system_triggered_allowed \
                    AND NOT {schema}.event_types.system_triggered_allowed \
                THEN EXCLUDED.schedule_position \
                ELSE {schema}.event_types.schedule_position END, \
            event_read_allowed = EXCLUDED.event_read_allowed, \
            private_fields = EXCLUDED.private_fields"
    )))
    .bind(&et.name)
    .bind(&et.schema)
    .bind(et.schema_version)
    .bind(Json(&et.tag_mappings))
    .bind(&et.owner_tag_key)
    .bind(Json(&et.sensitive_fields))
    .bind(et.external_creation_allowed)
    .bind(et.direct_creation_allowed)
    .bind(et.system_triggered_allowed)
    .bind(&et.system_triggered_schedule)
    .bind(
        et.missed_occurrence_policy
            .map(missed_occurrence_policy_to_str),
    )
    .bind(et.schedule_position)
    .bind(et.last_fired_at)
    .bind(et.event_read_allowed)
    .bind(Json(&et.private_fields))
    .execute(pool)
    .await?;
    notify_registration_changed(pool).await;
    Ok(())
}

#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn get_event_type(
    pool: &Pool,
    bounded_context: &str,
    name: &str,
) -> crate::error::Result<Option<EventType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    get_event_type_with_bc(pool, &bc, name).await
}

/// [`get_event_type`]'s own `bc`-in-hand sibling (issue #32,
/// round four - same precedent §60 already set for
/// `list_events_for_bounded_context_matching_tags_with_bc`): skips the
/// `get_bounded_context` call entirely for a caller who already has the
/// row - `list_events_for_bounded_context_matching_tags_with_bc`'s own
/// row-processing loop, one call per distinct event type in a delta
/// result, is exactly the caller this was added for - measured (a real
/// load test, not just reading the code) spending the majority of its
/// own wall-clock time re-fetching a `BoundedContext` (plus, transitively,
/// its own `created_by_role_id`'s `Role`) it never needed to ask for
/// again.
pub(super) async fn get_event_type_with_bc(
    executor: impl sqlx::PgExecutor<'_>,
    bc: &BoundedContext,
    name: &str,
) -> crate::error::Result<Option<EventType>> {
    let schema = schema_ident(&bc.name);
    let row: Option<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types WHERE name = $1"
    )))
    .bind(name)
    .fetch_optional(executor)
    .await?;
    Ok(row.map(|r| r.into_domain(bc.clone())))
}

/// `get_event_type_with_bc`'s batched sibling - `get_command_types_by_names_with_bc`'s
/// own precedent, one `WHERE name = ANY($1)` round trip for a whole set
/// of names. `names` empty returns an empty map without touching
/// Postgres.
pub(super) async fn get_event_types_by_names_with_bc(
    executor: impl sqlx::PgExecutor<'_>,
    bc: &BoundedContext,
    names: &[&str],
) -> crate::error::Result<std::collections::HashMap<String, EventType>> {
    if names.is_empty() {
        return Ok(std::collections::HashMap::new());
    }
    let schema = schema_ident(&bc.name);
    let rows: Vec<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types WHERE name = ANY($1)"
    )))
    .bind(names)
    .fetch_all(executor)
    .await?;
    Ok(rows
        .into_iter()
        .map(|row| (row.name.clone(), row.into_domain(bc.clone())))
        .collect())
}

/// Every `EventType` in `bounded_context` currently opted into
/// scheduling (`system_triggered_allowed = true`) - the background
/// scheduler's own discovery query each tick, and `TypeRegistration`'s
/// `scheduledEventTypes` GraphQL query (`@guarantee ScheduleStateIsShared`'s
/// own admin visibility) both read from this. Ordered by `name` for
/// stable output. `[]`, not an error, for an unknown `bounded_context` -
/// the same "nothing to show" treatment `list_projections_for_bounded_context`
/// gives it.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_scheduled_event_types(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<EventType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types \
         WHERE system_triggered_allowed = true ORDER BY name"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.into_domain(bc.clone()))
        .collect())
}

/// Every `EventType` registered in `bounded_context`, unfiltered -
/// `TypeRegistration`'s own `eventTypes` GraphQL query (issue
/// #6's "5a": the self-describing surface `skilj-tui`'s Commands/Query
/// Events tabs need to offer a real type picker instead of a name typed
/// by hand). Same shape as `list_scheduled_event_types` immediately
/// above, minus its `WHERE system_triggered_allowed = true` filter -
/// every type, not just the scheduled subset. Ordered by `name`, `[]`
/// for an unknown `bounded_context`, same reasoning as its sibling.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_event_types_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<EventType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types ORDER BY name"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.into_domain(bc.clone()))
        .collect())
}

/// `CommandType`'s own equivalent of `list_event_types_for_bounded_context`
/// immediately above - identical reasoning, `TypeRegistration`'s
/// `commandTypes` GraphQL query.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn list_command_types_for_bounded_context(
    pool: &Pool,
    bounded_context: &str,
) -> crate::error::Result<Vec<CommandType>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(Vec::new());
    };
    let schema = schema_ident(bounded_context);
    let rows: Vec<CommandTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {COMMAND_TYPE_COLUMNS} FROM {schema}.command_types ORDER BY name"
    )))
    .fetch_all(pool)
    .await?;
    Ok(rows
        .into_iter()
        .map(|r| r.into_domain(bc.clone()))
        .collect())
}

/// `rule CreateSystemEvent`'s own atomic whole, and `SequenceIsGaplessPerBoundedContext`'s
/// enforcement for a fired occurrence - locks `event_type_name`'s own
/// `event_types` row (`SELECT ... FOR UPDATE`), re-derives eligibility
/// against the now-locked, up-to-date `schedule_position`/`last_fired_at`
/// (another instance may have already advanced them since the caller's
/// own `list_scheduled_event_types` read - see `@guarantee
/// ScheduleStateIsShared`), and only then allocates a sequence number and
/// inserts - the same "peek, lock, re-check, single commit" shape
/// `submit_command` uses for `bounded_context.sequence`, adapted to one
/// `event_types` row instead. `event_dispatcher.scheduled_payload` is
/// resolved *before* the lock, deliberately: it's arbitrary caller code
/// (unlike `decide()`/`project()`, §1.1 never promised it's cheap or
/// I/O-free), so it must never run while this row's lock is held - that
/// would block every other writer to this event type for however long it
/// takes. `Ok(None)` when there is nothing to fire: the occurrence turned
/// out to be ineligible once the lock confirmed the fresh position (not
/// an error - exactly the race this locking exists to make harmless), or
/// this process's own `EventDispatcher` has no `scheduled_payload`
/// producer registered for the type at all (a genuine misconfiguration,
/// tolerated the same way `catch_up_bounded_context` tolerates an
/// unregistered projection dispatcher - logged and skipped by the
/// caller, not propagated as an `Err`).
#[allow(clippy::too_many_arguments)]
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn fire_system_event(
    pool: &Pool,
    projection_dispatcher: &dyn crate::plugin::ProjectionDispatcher,
    event_dispatcher: &dyn crate::plugin::EventDispatcher,
    broadcaster: &crate::event_store::EventBroadcaster,
    event_cache: &crate::event_cache::EventCache,
    bounded_context: &str,
    event_type_name: &str,
    occurrence_at: DateTime<Utc>,
    now: DateTime<Utc>,
    key_service: Option<&KeyService>,
) -> crate::error::Result<Option<Event>> {
    let Some(unlocked_event_type) = get_event_type(pool, bounded_context, event_type_name).await?
    else {
        return Ok(None);
    };
    let Some(payload) = event_dispatcher.scheduled_payload(bounded_context, event_type_name) else {
        return Ok(None);
    };

    let mut resolved = std::collections::HashMap::new();
    resolve_encryption_keys(
        pool,
        bounded_context,
        &unlocked_event_type.sensitive_fields,
        &payload,
        key_service,
        &mut resolved,
    )
    .await?;

    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;
    let row: Option<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types WHERE name = $1 FOR UPDATE"
    )))
    .bind(event_type_name)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let event_type = row.into_domain(unlocked_event_type.bounded_context);

    // Allocated inside `tx`, after the lock above - if `create_system_event`
    // below rejects the occurrence, `tx` is dropped without committing
    // (the early `return Ok(None)`), rolling this allocation back too, so
    // no sequence number is ever burned on an occurrence that didn't
    // actually fire.
    let (next_seq, limits) = next_sequence_with_limits(&mut *tx, bounded_context).await?;
    let Some((event, new_position)) = crate::event_store::try_create_system_event(
        &event_type,
        occurrence_at,
        now,
        next_seq,
        || payload,
        |subject_key, subject_value| {
            let (key, _, data_key) = resolved
                .get(&(subject_key.to_string(), subject_value.to_string()))
                .expect(
                    "resolve_encryption_keys pre-resolved every subject sensitive_field_subjects \
                     named",
                );
            (key.clone(), data_key.clone())
        },
    )?
    else {
        return Ok(None);
    };
    // As stored, after encryption (docs/architecture.md §238). Refused
    // like any other failure to fire: the occurrence stays due, and the
    // scheduler logs and retries it.
    limits
        .apply(projection_dispatcher.payload_limits())
        .check_event(&event)?;
    let blob_claims = event_blob_claims(projection_dispatcher, &event)?;
    check_blob_claims(&mut tx, bounded_context, &blob_claims).await?;

    let encryption_key_ids = encryption_key_ids(&event.encryption_keys, &resolved);
    // Fetched only now - after `next_sequence` above already took this
    // bounded context's own lock - see `insert_event_and_update_sync_projections_in_tx`'s
    // own doc comment on why that ordering, not "as early as possible", is
    // what keeps this read race-free against `promote_projection_rebuild`.
    let sync_projections = sync_projection_names(&mut tx, bounded_context).await?;
    insert_event_and_update_sync_projections_in_tx(
        &mut tx,
        &event,
        None,
        projection_dispatcher,
        &encryption_key_ids,
        &sync_projections,
    )
    .await?;
    record_blob_claims(&mut tx, bounded_context, &blob_claims, None, now).await?;

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.event_types SET schedule_position = $1, last_fired_at = $2 WHERE name = $3"
    )))
    .bind(new_position)
    .bind(new_position)
    .bind(event_type_name)
    .execute(&mut *tx)
    .await?;

    tx.commit().await?;
    broadcaster.publish(&event);
    record_event_appended(&event);
    notify_event_appended(pool, &event, broadcaster.instance_id()).await;
    event_cache.append(&event).await;

    Ok(Some(event))
}

/// `rule SkipMissedOccurrences`'s own atomic whole - locks the same
/// `event_types` row `fire_system_event` does, re-derives eligibility
/// against the locked, up-to-date `schedule_position` (a second instance
/// resuming alongside the first is exactly the harmless race this lock
/// makes a no-op - see `event_store::skip_missed_occurrences`'s own doc
/// comment), and persists the advanced position alone: unlike
/// `fire_system_event`, no event is produced and `last_fired_at` never
/// moves (`FiredOccurrenceIsAccountedFor` only ever relates it to
/// occurrences that actually fired). `Ok(None)` when the resume turns out
/// to be a no-op once the lock is held.
#[tracing::instrument(skip_all, fields(bounded_context = %bounded_context))]
pub async fn skip_missed_occurrences_for_event_type(
    pool: &Pool,
    bounded_context: &str,
    event_type_name: &str,
    now: DateTime<Utc>,
) -> crate::error::Result<Option<DateTime<Utc>>> {
    let Some(bc) = get_bounded_context(pool, bounded_context).await? else {
        return Ok(None);
    };
    let schema = schema_ident(bounded_context);
    let mut tx = pool.begin().await?;
    let row: Option<EventTypeRow> = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT {EVENT_TYPE_COLUMNS} FROM {schema}.event_types WHERE name = $1 FOR UPDATE"
    )))
    .bind(event_type_name)
    .fetch_optional(&mut *tx)
    .await?;
    let Some(row) = row else {
        return Ok(None);
    };
    let event_type = row.into_domain(bc);

    let Some(new_position) = crate::event_store::skip_missed_occurrences(&event_type, now) else {
        return Ok(None);
    };

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "UPDATE {schema}.event_types SET schedule_position = $1 WHERE name = $2"
    )))
    .bind(new_position)
    .bind(event_type_name)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;

    Ok(Some(new_position))
}
