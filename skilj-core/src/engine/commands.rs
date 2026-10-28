//! AuthoriseCommandSubmission, AuthoriseCommandTrigger, ProcessCommand,
//! the dry runs, the batches and FetchCommands.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde_json::{json, Value};
use sqlx::postgres::PgRow;
use sqlx::PgConnection;

use super::events::NewEvent;
use super::render::Rendered;
use super::rows::{command_from_row, event_from_row, tag_arrays, COMMAND_COLUMNS, EVENT_COLUMNS};
use super::Skilj;
use crate::crypto;
use crate::decl::{CommandTypeDecl, Decision, DecisionInput, EventSpec};
use crate::error::{Error, Result};
use crate::model::*;
use crate::schema::Schema;
use crate::tags::{derive_query, derive_tags, owner_scope_satisfied};
use crate::validate;

/// One command as a caller sends it.
#[derive(Debug, Clone, Default)]
pub struct CommandRequest {
    pub command_type: String,
    pub payload: Value,
    pub idempotency_key: Option<String>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

impl CommandRequest {
    pub fn new(command_type: impl Into<String>, payload: Value) -> Self {
        CommandRequest {
            command_type: command_type.into(),
            payload,
            ..Default::default()
        }
    }
    pub fn key(mut self, k: impl Into<String>) -> Self {
        self.idempotency_key = Some(k.into());
        self
    }
}

/// What a submission answers.
#[derive(Debug, Clone)]
pub enum CommandOutcome {
    Accepted {
        command: StoredCommand,
        events: Vec<StoredEvent>,
    },
    /// The key was already accepted: the original outcome.
    Duplicate {
        command_id: String,
        events: Vec<StoredEvent>,
    },
    Rejected {
        reason: String,
        kind: String,
        /// Only for an admin-level GraphQL caller (MatchingEventsRequiresAdminLevel).
        matching_events: Option<Vec<Rendered>>,
        /// Events within scope that were left out of matching_events.
        truncated: bool,
    },
    /// Refused before decide() (only in batch answers; single submissions
    /// return the error).
    Refused { code: &'static str, message: String },
    NotProcessed,
}

impl CommandOutcome {
    pub fn events(&self) -> &[StoredEvent] {
        match self {
            CommandOutcome::Accepted { events, .. } | CommandOutcome::Duplicate { events, .. } => events,
            _ => &[],
        }
    }
    pub fn is_accepted(&self) -> bool {
        matches!(self, CommandOutcome::Accepted { .. } | CommandOutcome::Duplicate { .. })
    }
}

/// DryRunCommand's answer.
#[derive(Debug, Clone)]
pub struct DryRunAnswer {
    pub accepted: bool,
    pub rejection_reason: Option<String>,
    pub rejection_kind: Option<String>,
    pub would_be_events: Vec<Rendered>,
    pub matching_events: Vec<Rendered>,
}

/// Who is submitting.
pub(crate) enum Caller<'a> {
    Grant(&'a Grant),
    Token(&'a AccessToken),
}

impl Caller<'_> {
    fn client_id(&self) -> &str {
        match self {
            Caller::Grant(g) => &g.role.id,
            Caller::Token(t) => &t.id,
        }
    }
    fn scope(&self) -> Option<&str> {
        match self {
            Caller::Grant(g) => g.mapping.scope.as_deref(),
            Caller::Token(t) => t.scope.as_deref(),
        }
    }
}

/// The resolved decision context.
struct Prepared {
    bc: String,
    def: Arc<(CommandTypeDef, Schema)>,
    decl: CommandTypeDecl,
    tags: BTreeSet<Tag>,
    query: Vec<QueryItem>,
}

/// decide()'s inputs and output at one point in the stream.
struct Decided {
    decision: Decision,
    matching: Vec<StoredEvent>,
    boundary: Option<i64>,
}

impl Skilj {
    /// SubmitCommand over GraphQL.
    pub async fn submit_command(&self, grant: &Grant, req: CommandRequest) -> Result<CommandOutcome> {
        self.submit_as(Caller::Grant(grant), req).await
    }

    /// TriggerCommand over REST.
    pub async fn trigger_command(&self, token: &AccessToken, command_type: &str, mut req: CommandRequest) -> Result<CommandOutcome> {
        req.command_type = command_type.to_string();
        self.submit_as(Caller::Token(token), req).await
    }

    /// SubmitCommands.
    pub async fn submit_commands(&self, grant: &Grant, reqs: Vec<CommandRequest>) -> Result<Vec<CommandOutcome>> {
        self.batch(Caller::Grant(grant), reqs, false).await
    }

    /// TriggerCommands.
    pub async fn trigger_commands(
        &self,
        token: &AccessToken,
        command_type: &str,
        reqs: Vec<CommandRequest>,
        stop_at_first_failure: bool,
    ) -> Result<Vec<CommandOutcome>> {
        let reqs = reqs
            .into_iter()
            .map(|mut r| {
                r.command_type = command_type.to_string();
                r
            })
            .collect();
        self.batch(Caller::Token(token), reqs, stop_at_first_failure).await
    }

    async fn batch(&self, caller: Caller<'_>, reqs: Vec<CommandRequest>, stop: bool) -> Result<Vec<CommandOutcome>> {
        if reqs.len() > self.inner.config.max_items_per_batch {
            return Err(Error::invalid(format!(
                "a batch carries at most {} commands",
                self.inner.config.max_items_per_batch
            )));
        }
        let mut out = Vec::with_capacity(reqs.len());
        let mut stopped = false;
        for r in reqs {
            if stopped {
                out.push(CommandOutcome::NotProcessed);
                continue;
            }
            let caller_ref = match &caller {
                Caller::Grant(g) => Caller::Grant(g),
                Caller::Token(t) => Caller::Token(t),
            };
            match self.submit_as(caller_ref, r).await {
                Ok(o) => out.push(o),
                Err(e) => {
                    stopped = stop;
                    out.push(CommandOutcome::Refused {
                        code: e.code(),
                        message: e.to_string(),
                    })
                }
            }
        }
        Ok(out)
    }

    /// The two authorisation rules, then ProcessCommand.
    async fn submit_as(&self, caller: Caller<'_>, req: CommandRequest) -> Result<CommandOutcome> {
        let prepared = self.authorise(&caller, &req.command_type, &req.payload).await?;
        validate::valid_idempotency_key(req.idempotency_key.as_deref())?;
        validate::valid_correlation_id(req.correlation_id.as_deref())?;
        validate::valid_correlation_id(req.causation_id.as_deref())?;
        self.process(&caller, prepared, req).await
    }

    async fn authorise(&self, caller: &Caller<'_>, command_type: &str, payload: &Value) -> Result<Prepared> {
        let bc = match caller {
            Caller::Grant(g) => {
                if !g.has_level(AccessLevel::Write) {
                    return Err(Error::forbidden("submitting needs a write grant"));
                }
                g.mapping.bounded_context.clone()
            }
            Caller::Token(t) => {
                if !t.active {
                    return Err(Error::forbidden("token is revoked"));
                }
                if t.purpose != TokenPurpose::Command || t.type_name != command_type {
                    return Err(Error::forbidden("token is not for this command type"));
                }
                t.bounded_context.clone()
            }
        };
        let def = self.inner.registry.read().await.command_type(&bc, command_type)?;
        if let Caller::Token(_) = caller {
            if !def.0.rest_trigger_allowed {
                return Err(Error::forbidden("this command type may not be triggered over REST"));
            }
        }
        self.inner.registry.read().await.is_active(&bc)?;
        def.1.validate(payload)?;
        let decl = self
            .decl_for(&bc)
            .and_then(|d| d.command_types.iter().find(|c| c.name == command_type))
            .cloned()
            .ok_or_else(|| Error::NotDeclared(format!("{bc}.{command_type} has no decider in this process")))?;
        let tags = derive_tags(&bc, &def.0.tag_mappings, payload, self.inner.blind.as_ref())?;
        if !owner_scope_satisfied(&tags, def.0.owner_tag_key.as_deref(), caller.scope()) {
            return Err(Error::forbidden("scope does not cover this command's owner"));
        }
        let query = derive_query(&bc, &def.0.tag_mappings, &decl.consistency_query, payload, self.inner.blind.as_ref())?;
        Ok(Prepared {
            bc,
            def,
            decl,
            tags,
            query,
        })
    }

    /// query_matching_events + snapshot folding + decide(), against the
    /// pool or a transaction.
    async fn decide_now(&self, conn: &mut PgConnection, p: &Prepared, payload: &Value, now: DateTime<Utc>) -> Result<Decided> {
        let matching = matching_events(conn, &p.bc, &p.tags, &p.query, -1, false).await?;
        let boundary = matching.last().map(|e| e.sequence);
        // decide() sees history decrypted wherever the key still exists.
        let mut plain = Vec::with_capacity(matching.len());
        for e in &matching {
            let mut e2 = e.clone();
            e2.payload = self.decrypt_where(&e.payload, &|_| true).await?;
            plain.push(e2);
        }
        let mut snapshots = BTreeMap::new();
        let snapshot_items: Vec<&QueryItem> = p.query.iter().filter(|i| i.snapshot.is_some()).collect();
        let mut handed: Vec<StoredEvent> = vec![];
        for e in &plain {
            let in_snapshot = snapshot_items.iter().any(|i| i.matches(&e.event_type, &e.tags));
            let in_other = p
                .query
                .iter()
                .filter(|i| i.snapshot.is_none())
                .any(|i| i.matches(&e.event_type, &e.tags));
            if !in_snapshot || in_other || p.query.is_empty() {
                handed.push(e.clone());
            }
        }
        for item in snapshot_items {
            let name = item.snapshot.clone().expect("snapshot");
            let summary = self
                .decl_for(&p.bc)
                .and_then(|d| d.summaries.iter().find(|s| s.name == name))
                .ok_or_else(|| Error::NotDeclared(format!("summary {name}")))?;
            let tag = item.tags.iter().next().expect("one tag");
            let value = tag.value.clone().unwrap_or_default();
            let mut state = summary.initial.clone();
            for e in plain.iter().filter(|e| e.tags.contains(tag)) {
                state = (summary.fold)(&state, e, &value);
            }
            snapshots.insert((name, value), state);
        }
        let decision = (p.decl.decide)(&DecisionInput {
            payload,
            events: &handed,
            snapshots: &snapshots,
            now,
        });
        Ok(Decided {
            decision,
            matching,
            boundary,
        })
    }

    async fn process(&self, caller: &Caller<'_>, p: Prepared, req: CommandRequest) -> Result<CommandOutcome> {
        let client_id = caller.client_id().to_string();
        // Idempotency: an accepted key answers with its original outcome.
        if let Some(key) = &req.idempotency_key {
            if let Some(o) = self.recorded_acceptance(&p.bc, &p.def.0.name, &client_id, key).await? {
                return Ok(o);
            }
        }
        // Optimistic read and decide, outside the lock.
        let now = Self::now();
        let first = {
            let mut conn = self.inner.pool.acquire().await?;
            self.decide_now(&mut conn, &p, &req.payload, now).await?
        };
        if let Decision::Reject { reason, kind } = &first.decision {
            return self.rejection(caller, &p, reason, kind, &first.matching).await;
        }
        // Provision keys outside the transaction.
        let command_protected = self.protect(&p.def.0, &req.payload).await?;
        let mut tx = self.inner.pool.begin().await?;
        let next = self.lock_counter(&mut tx, &p.bc).await?;
        // Re-check: anything new in the consistency query since the read?
        let mut decided = first;
        let conflict = !matching_events(&mut tx, &p.bc, &p.tags, &p.query, decided.boundary.unwrap_or(-1), true)
            .await?
            .is_empty();
        if conflict {
            let now2 = Self::now();
            decided = self.decide_now(&mut tx, &p, &req.payload, now2).await?;
            if let Decision::Reject { reason, kind } = &decided.decision {
                drop(tx);
                return self.rejection(caller, &p, reason, kind, &decided.matching).await;
            }
        }
        let Decision::Accept(specs) = decided.decision else { unreachable!() };
        // Key race: whoever records first wins; the loser answers with
        // the winner's outcome.
        let command_id = crypto::generate_id();
        let correlation_id = req.correlation_id.clone().unwrap_or_else(crypto::generate_id);
        let mut new_events = Vec::with_capacity(specs.len());
        for spec in &specs {
            new_events.push(self.prepare_triggered(&p, spec, &client_id, &command_id, &correlation_id).await?);
        }
        let metadata = Metadata {
            type_name: p.def.0.name.clone(),
            version: p.def.0.schema_version,
            client_id: client_id.clone(),
            created_at: Self::now(),
            correlation_id: correlation_id.clone(),
            causation_id: req.causation_id.clone(),
        };
        let recorded: i64 = sqlx::query_scalar(
            "insert into commands (id, bounded_context, command_type, payload, metadata, encryption_key_ids, consistency_tags, consistency_query, consistency_boundary, created_at)
             values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) returning recorded",
        )
        .bind(&command_id)
        .bind(&p.bc)
        .bind(&p.def.0.name)
        .bind(&command_protected.payload)
        .bind(serde_json::to_value(&metadata)?)
        .bind(&command_protected.key_ids)
        .bind(serde_json::to_value(&p.tags)?)
        .bind(serde_json::to_value(&p.query)?)
        .bind(decided.boundary)
        .bind(metadata.created_at)
        .fetch_one(&mut *tx)
        .await?;
        let written = self.append_locked(&mut tx, &p.bc, next, new_events).await?;
        if let Some(key) = &req.idempotency_key {
            let cutoff = Self::now() - self.inner.config.idempotency_key_retention;
            sqlx::query(
                "delete from idempotency_keys where bounded_context = $1 and command_type = $2 and client_id = $3 and key = $4 and recorded_at < $5",
            )
            .bind(&p.bc)
            .bind(&p.def.0.name)
            .bind(&client_id)
            .bind(key)
            .bind(cutoff)
            .execute(&mut *tx)
            .await?;
            let outcome = json!({"command_id": command_id, "sequences": written.iter().map(|e| e.sequence).collect::<Vec<_>>()});
            let inserted = sqlx::query(
                "insert into idempotency_keys (bounded_context, command_type, client_id, key, outcome, recorded_at)
                 values ($1, $2, $3, $4, $5, $6) on conflict do nothing",
            )
            .bind(&p.bc)
            .bind(&p.def.0.name)
            .bind(&client_id)
            .bind(key)
            .bind(outcome)
            .bind(Self::now())
            .execute(&mut *tx)
            .await?
            .rows_affected();
            if inserted == 0 {
                drop(tx);
                return self
                    .recorded_acceptance(&p.bc, &p.def.0.name, &client_id, key)
                    .await?
                    .ok_or_else(|| Error::conflict("idempotency key raced"));
            }
        }
        tx.commit().await?;
        self.after_commit(&written).await;
        let command = StoredCommand {
            id: command_id,
            bounded_context: p.bc.clone(),
            command_type: p.def.0.name.clone(),
            payload: command_protected.payload,
            metadata,
            encryption_key_ids: command_protected.key_ids,
            consistency_tags: p.tags.clone(),
            consistency_query: p.query.clone(),
            consistency_boundary: decided.boundary,
            recorded,
            triggered_sequences: written.iter().map(|e| e.sequence).collect(),
        };
        Ok(CommandOutcome::Accepted { command, events: written })
    }

    async fn prepare_triggered(
        &self,
        p: &Prepared,
        spec: &EventSpec,
        client_id: &str,
        command_id: &str,
        correlation_id: &str,
    ) -> Result<NewEvent> {
        let ty = self
            .inner
            .registry
            .read()
            .await
            .event_type(&p.bc, &spec.event_type)
            .map_err(|_| Error::invalid(format!("decide() emitted unregistered event type {}", spec.event_type)))?;
        // The spec doesn't re-check decide()'s payloads; a bad one would be
        // stored forever, so we do.
        ty.1.validate(&spec.payload)
            .map_err(|e| Error::invalid(format!("decide() emitted an invalid {}: {e}", spec.event_type)))?;
        let tags = derive_tags(&p.bc, &ty.0.tag_mappings, &spec.payload, self.inner.blind.as_ref())?;
        let protected = self.protect(&ty.0, &spec.payload).await?;
        Ok(NewEvent {
            event_type: ty.0.name.clone(),
            payload: protected.payload,
            tags,
            key_ids: protected.key_ids,
            metadata: Metadata {
                type_name: ty.0.name.clone(),
                version: ty.0.schema_version,
                client_id: client_id.into(),
                created_at: Self::now(),
                correlation_id: correlation_id.into(),
                causation_id: Some(command_id.into()),
            },
            origin: Origin::CommandTriggered {
                command_id: command_id.into(),
            },
            command_id: Some(command_id.into()),
        })
    }

    async fn rejection(
        &self,
        caller: &Caller<'_>,
        p: &Prepared,
        reason: &str,
        kind: &str,
        matching: &[StoredEvent],
    ) -> Result<CommandOutcome> {
        let (matching_events, truncated) = match caller {
            Caller::Grant(g) if g.has_level(AccessLevel::Admin) => {
                let (shown, truncated) = self.shown_matching(g, &p.bc, matching).await?;
                (Some(shown), truncated)
            }
            _ => (None, false),
        };
        Ok(CommandOutcome::Rejected {
            reason: reason.to_string(),
            kind: kind.to_string(),
            matching_events,
            truncated,
        })
    }

    /// Matching events as an admin may see them: in scope, rendered, the
    /// most recent max_events_per_read.
    async fn shown_matching(&self, g: &Grant, bc: &str, matching: &[StoredEvent]) -> Result<(Vec<Rendered>, bool)> {
        let reg = self.inner.registry.read().await;
        let in_scope: Vec<&StoredEvent> = matching
            .iter()
            .filter(|e| {
                let owner = reg.event_type(bc, &e.event_type).ok().and_then(|t| t.0.owner_tag_key.clone());
                owner_scope_satisfied(&e.tags, owner.as_deref(), g.mapping.scope.as_deref())
            })
            .collect();
        drop(reg);
        let max = self.inner.config.max_events_per_read;
        let truncated = in_scope.len() > max;
        let start = in_scope.len().saturating_sub(max);
        let pa = self.private_access(g).await?;
        let mut out = vec![];
        for e in &in_scope[start..] {
            out.push(self.render_event(e, g, &pa).await?);
        }
        Ok((out, truncated))
    }

    async fn recorded_acceptance(&self, bc: &str, ct: &str, client_id: &str, key: &str) -> Result<Option<CommandOutcome>> {
        let cutoff = Self::now() - self.inner.config.idempotency_key_retention;
        let outcome: Option<Value> = sqlx::query_scalar(
            "select outcome from idempotency_keys where bounded_context = $1 and command_type = $2 and client_id = $3 and key = $4 and recorded_at >= $5",
        )
        .bind(bc)
        .bind(ct)
        .bind(client_id)
        .bind(key)
        .bind(cutoff)
        .fetch_optional(&self.inner.pool)
        .await?;
        let Some(o) = outcome else { return Ok(None) };
        let command_id = o["command_id"].as_str().unwrap_or_default().to_string();
        let seqs: Vec<i64> = serde_json::from_value(o["sequences"].clone())?;
        let rows = sqlx::query(&format!(
            "select {EVENT_COLUMNS} from events where bounded_context = $1 and sequence = any($2) order by sequence"
        ))
        .bind(bc)
        .bind(&seqs)
        .fetch_all(&self.inner.pool)
        .await?;
        Ok(Some(CommandOutcome::Duplicate {
            command_id,
            events: rows.iter().map(event_from_row).collect::<Result<_>>()?,
        }))
    }

    /// DryRunCommand.
    pub async fn dry_run_command(&self, grant: &Grant, command_type: &str, payload: Value) -> Result<DryRunAnswer> {
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("a dry run needs an admin grant"));
        }
        let caller = Caller::Grant(grant);
        let p = self.authorise(&caller, command_type, &payload).await?;
        let d = {
            let mut conn = self.inner.pool.acquire().await?;
            self.decide_now(&mut conn, &p, &payload, Self::now()).await?
        };
        let (matching_events, _) = self.shown_matching(grant, &p.bc, &d.matching).await?;
        let pa = self.private_access(grant).await?;
        let (accepted, reason, kind, would) = match d.decision {
            Decision::Accept(specs) => {
                let mut w = vec![];
                for s in &specs {
                    self.inner.registry.read().await.event_type(&p.bc, &s.event_type).map_err(|_| {
                        Error::invalid(format!("decide() emitted unregistered event type {}", s.event_type))
                    })?;
                    w.push(self.render_would_be(&p.bc, s, &grant.role.id, grant, &pa).await?);
                }
                (true, None, None, w)
            }
            Decision::Reject { reason, kind } => (false, Some(reason), Some(kind), vec![]),
        };
        Ok(DryRunAnswer {
            accepted,
            rejection_reason: reason,
            rejection_kind: kind,
            would_be_events: would,
            matching_events,
        })
    }

    /// DryRunTriggerCommand: only the outcome.
    pub async fn dry_run_trigger(&self, token: &AccessToken, command_type: &str, payload: Value) -> Result<DryRunAnswer> {
        let caller = Caller::Token(token);
        let p = self.authorise(&caller, command_type, &payload).await?;
        let d = {
            let mut conn = self.inner.pool.acquire().await?;
            self.decide_now(&mut conn, &p, &payload, Self::now()).await?
        };
        let (accepted, reason, kind) = match d.decision {
            Decision::Accept(_) => (true, None, None),
            Decision::Reject { reason, kind } => (false, Some(reason), Some(kind)),
        };
        Ok(DryRunAnswer {
            accepted,
            rejection_reason: reason,
            rejection_kind: kind,
            would_be_events: vec![],
            matching_events: vec![],
        })
    }

    /// FetchCommands.
    #[allow(clippy::too_many_arguments)]
    pub async fn fetch_commands(
        &self,
        grant: &Grant,
        command_types: &[String],
        after: Option<DateTime<Utc>>,
        before: Option<DateTime<Utc>>,
        triggered_event: Option<i64>,
        after_command: Option<&str>,
    ) -> Result<Vec<Rendered>> {
        if !grant.has_level(AccessLevel::Admin) {
            return Err(Error::forbidden("reading commands needs an admin grant"));
        }
        let bc = &grant.mapping.bounded_context;
        {
            let reg = self.inner.registry.read().await;
            for ct in command_types {
                reg.command_type(bc, ct)?;
            }
        }
        let after_recorded: Option<i64> = match after_command {
            Some(id) => Some(
                sqlx::query_scalar("select recorded from commands where id = $1 and bounded_context = $2")
                    .bind(id)
                    .bind(bc)
                    .fetch_optional(&self.inner.pool)
                    .await?
                    .ok_or_else(|| Error::not_found(format!("command {id}")))?,
            ),
            None => None,
        };
        let mut out = vec![];
        let mut cursor = after_recorded.unwrap_or(0);
        let max = self.inner.config.max_events_per_read;
        let pa = self.private_access(grant).await?;
        // Filter by owner in Rust, paging the SQL until a page is full.
        'outer: loop {
            let rows: Vec<PgRow> = sqlx::query(&format!(
                "select {COMMAND_COLUMNS} from commands c
                 where c.bounded_context = $1 and c.recorded > $2
                   and (cardinality($3::text[]) = 0 or c.command_type = any($3))
                   and ($4::timestamptz is null or c.created_at >= $4)
                   and ($5::timestamptz is null or c.created_at <= $5)
                   and ($6::bigint is null or exists (select 1 from events e where e.bounded_context = c.bounded_context and e.sequence = $6 and e.command_id = c.id))
                 order by c.recorded limit $7"
            ))
            .bind(bc)
            .bind(cursor)
            .bind(command_types)
            .bind(after)
            .bind(before)
            .bind(triggered_event)
            .bind(max as i64)
            .fetch_all(&self.inner.pool)
            .await?;
            if rows.is_empty() {
                break;
            }
            for r in &rows {
                let c = command_from_row(r)?;
                cursor = c.recorded;
                let owner = self
                    .inner
                    .registry
                    .read()
                    .await
                    .command_type(bc, &c.command_type)
                    .ok()
                    .and_then(|t| t.0.owner_tag_key.clone());
                if owner_scope_satisfied(&c.consistency_tags, owner.as_deref(), grant.mapping.scope.as_deref()) {
                    out.push(self.render_command(&c, grant, &pa).await?);
                    if out.len() >= max {
                        break 'outer;
                    }
                }
            }
            if rows.len() < max {
                break;
            }
        }
        Ok(out)
    }
}

/// The SQL behind query_matching_events, over events after `after`.
/// `any_item` ignores each item's latest (the conflict check: an event
/// conflicts if it matches any item at all).
async fn matching_events(
    conn: &mut PgConnection,
    bc: &str,
    tags: &BTreeSet<Tag>,
    query: &[QueryItem],
    after: i64,
    any_item: bool,
) -> Result<Vec<StoredEvent>> {
    let mut by_seq: BTreeMap<i64, StoredEvent> = BTreeMap::new();
    if query.is_empty() {
        if tags.is_empty() {
            return Ok(vec![]);
        }
        let (k, v) = tag_arrays(tags);
        let rows = sqlx::query(&format!(
            "select {EVENT_COLUMNS} from events e where e.bounded_context = $1 and e.sequence > $4 and exists (
                select 1 from event_tags t join unnest($2::text[], $3::text[]) as w(k, v)
                  on t.key = w.k and t.value is not distinct from w.v
                where t.bounded_context = e.bounded_context and t.sequence = e.sequence)
             order by e.sequence"
        ))
        .bind(bc)
        .bind(&k)
        .bind(&v)
        .bind(after)
        .fetch_all(&mut *conn)
        .await?;
        for r in rows {
            let e = event_from_row(&r)?;
            by_seq.insert(e.sequence, e);
        }
        return Ok(by_seq.into_values().collect());
    }
    for item in query {
        if item.event_types.is_empty() && item.tags.is_empty() {
            continue;
        }
        let types: Vec<String> = item.event_types.iter().cloned().collect();
        let (k, v) = tag_arrays(&item.tags);
        let n = item.tags.len() as i64;
        let (order, limit) = match (item.latest, any_item) {
            (Some(l), false) => ("desc", l as i64),
            _ => ("asc", i64::MAX),
        };
        let rows = sqlx::query(&format!(
            "select {EVENT_COLUMNS} from events e where e.bounded_context = $1 and e.sequence > $7
               and (cardinality($2::text[]) = 0 or e.event_type = any($2))
               and $5 = (select count(*) from event_tags t join unnest($3::text[], $4::text[]) as w(k, v)
                          on t.key = w.k and t.value is not distinct from w.v
                         where t.bounded_context = e.bounded_context and t.sequence = e.sequence)
             order by e.sequence {order} limit $6"
        ))
        .bind(bc)
        .bind(&types)
        .bind(&k)
        .bind(&v)
        .bind(n)
        .bind(limit)
        .bind(after)
        .fetch_all(&mut *conn)
        .await?;
        for r in rows {
            let e = event_from_row(&r)?;
            by_seq.insert(e.sequence, e);
        }
    }
    Ok(by_seq.into_values().collect())
}

