//! The event reads: FetchEvents, ConsumeEvents, AcknowledgeEvents,
//! QueryEvents, CountEvents, InspectEvent, InspectSnapshot, and the two
//! subscription rules plus DeliverToSubscriptions.

use std::collections::BTreeSet;
use std::sync::Arc;

use futures::stream::{self, BoxStream, StreamExt};
use serde_json::Value;
use sqlx::Row;

use super::render::{render_rest, Rendered};
use super::rows::{event_from_row, EVENT_COLUMNS};
use super::{BusMessage, Skilj};
use crate::error::{Error, Result};
use crate::filters::{compile_filters, matches_filters, CompiledFilter};
use crate::model::*;
use crate::tags::owner_scope_satisfied;

/// A page of events plus the epoch it was read in.
#[derive(Debug, Clone)]
pub struct EventPage {
    pub events: Vec<Rendered>,
    pub epoch: String,
    /// Highest sequence served, to pass back as after_sequence.
    pub last_sequence: Option<i64>,
}

/// What a created subscription delivers.
#[derive(Debug, Clone)]
pub struct SubscriptionSpec {
    pub bounded_context: String,
    pub mapping_id: String,
    pub from_sequence: i64,
    pub kind: SubscriptionKind,
}

#[derive(Debug, Clone)]
pub enum SubscriptionKind {
    AllEvents {
        event_types: BTreeSet<String>,
    },
    EventType {
        event_type: String,
        filters: Vec<CompiledFilter>,
    },
}

/// Why a subscription ended (on the wire this is the close reason).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionEnd {
    /// The grant was revoked: permanent, don't reconnect.
    Revoked,
    /// This instance may have missed events: reconnect from the last
    /// sequence received.
    Lagged,
}

impl Skilj {
    /// The latest committed sequence of a context (-1 when empty).
    pub(crate) async fn latest_sequence(&self, bc: &str) -> Result<i64> {
        let next: Option<i64> = sqlx::query_scalar(
            "select next_sequence from sequence_counters where bounded_context = $1",
        )
        .bind(bc)
        .fetch_optional(&self.inner.pool)
        .await?;
        Ok(next.ok_or_else(|| Error::not_found(format!("bounded context {bc}")))? - 1)
    }

    /// Events of a context after `after`, optionally of some types, in
    /// order, narrowed by `keep`, at most `max` kept. Served from the cache
    /// when it covers the span; also returns the highest sequence examined.
    async fn scan<F>(
        &self,
        bc: &str,
        types: &BTreeSet<String>,
        after: i64,
        max: usize,
        keep: F,
    ) -> Result<(Vec<Arc<StoredEvent>>, Option<i64>, bool)>
    where
        F: Fn(&StoredEvent) -> bool,
    {
        let latest = self.latest_sequence(bc).await?;
        let mut kept = vec![];
        let mut examined_upto: Option<i64> = None;
        if let Some(span) = self.inner.cache.span(bc, after, latest) {
            for e in span {
                if !types.is_empty() && !types.contains(&e.event_type) {
                    continue;
                }
                examined_upto = Some(e.sequence);
                if keep(&e) {
                    kept.push(e);
                    if kept.len() >= max {
                        return Ok((kept, examined_upto, true));
                    }
                }
            }
            return Ok((kept, examined_upto, false));
        }
        let names: Vec<String> = types.iter().cloned().collect();
        let mut cursor = after;
        loop {
            let rows = sqlx::query(&format!(
                "select {EVENT_COLUMNS} from events where bounded_context = $1 and sequence > $2 and sequence <= $3
                   and (cardinality($4::text[]) = 0 or event_type = any($4)) order by sequence limit 1000"
            ))
            .bind(bc)
            .bind(cursor)
            .bind(latest)
            .bind(&names)
            .fetch_all(&self.inner.pool)
            .await?;
            let n = rows.len();
            for r in rows {
                let e = event_from_row(&r)?;
                cursor = e.sequence;
                examined_upto = Some(e.sequence);
                if keep(&e) {
                    kept.push(Arc::new(e));
                    if kept.len() >= max {
                        return Ok((kept, examined_upto, true));
                    }
                }
            }
            if n < 1000 {
                return Ok((kept, examined_upto, false));
            }
        }
    }

    fn read_token_type(&self, token: &AccessToken) -> Result<()> {
        if !token.active {
            return Err(Error::forbidden("token is revoked"));
        }
        if token.purpose != TokenPurpose::EventRead {
            return Err(Error::forbidden("token is not an event read token"));
        }
        Ok(())
    }

    /// FetchEvents.
    pub async fn fetch_events(
        &self,
        token: &AccessToken,
        event_type: &str,
        filters: &[Filter],
        after_sequence: Option<i64>,
        epoch: Option<&str>,
    ) -> Result<EventPage> {
        self.read_token_type(token)?;
        self.check_epoch(epoch).await?;
        if token.type_name != event_type {
            return Err(Error::forbidden("token is scoped to another event type"));
        }
        let ty = self
            .inner
            .registry
            .read()
            .await
            .event_type(&token.bounded_context, event_type)?;
        if !ty.0.event_read_allowed {
            return Err(Error::forbidden(
                "this event type is not readable over REST",
            ));
        }
        let compiled = compile_filters(&ty.1, filters, &self.inner.config)?;
        let owner = ty.0.owner_tag_key.clone();
        let types: BTreeSet<String> = [event_type.to_string()].into_iter().collect();
        let (served, _, _) = self
            .scan(
                &token.bounded_context,
                &types,
                after_sequence.unwrap_or(-1),
                self.inner.config.max_events_per_read,
                |e| {
                    matches_filters(&compiled, &e.payload)
                        && owner_scope_satisfied(&e.tags, owner.as_deref(), token.scope.as_deref())
                },
            )
            .await?;
        Ok(EventPage {
            last_sequence: served.last().map(|e| e.sequence),
            events: served.iter().map(|e| render_rest(e, &ty.0)).collect(),
            epoch: self.current_epoch().await,
        })
    }

    /// ConsumeEvents.
    pub async fn consume_events(
        &self,
        token: &AccessToken,
        ack_mode: Option<AckMode>,
        filters: &[Filter],
    ) -> Result<EventPage> {
        self.read_token_type(token)?;
        let ty = self
            .inner
            .registry
            .read()
            .await
            .event_type(&token.bounded_context, &token.type_name)?;
        if !ty.0.event_read_allowed {
            return Err(Error::forbidden(
                "this event type is not readable over REST",
            ));
        }
        let compiled = compile_filters(&ty.1, filters, &self.inner.config)?;
        let owner = ty.0.owner_tag_key.clone();
        let scope = token.scope.clone();
        let mut tx = self.inner.pool.begin().await?;
        // Serialise first use per token on the token row.
        sqlx::query("select 1 from tokens where id = $1 for update")
            .bind(&token.id)
            .execute(&mut *tx)
            .await?;
        let cursor = sqlx::query("select ack_mode, sequence, checked_out_at from read_cursors where token_id = $1 for update")
            .bind(&token.id)
            .fetch_optional(&mut *tx)
            .await?;
        let now = Self::now();
        let (is_new, mode, position, checked_out_at) = match cursor {
            None => {
                let mode = ack_mode.ok_or_else(|| {
                    Error::invalid("a token's first consume must name an ack_mode")
                })?;
                let pos = self.start_position(token, owner.as_deref()).await?;
                (true, mode, pos, None)
            }
            Some(r) => {
                let stored =
                    AckMode::parse(&r.try_get::<String, _>("ack_mode")?).expect("stored mode");
                if let Some(m) = ack_mode {
                    if m != stored {
                        return Err(Error::invalid(
                            "this token's cursor is fixed to the other ack_mode",
                        ));
                    }
                }
                (
                    false,
                    stored,
                    r.try_get::<i64, _>("sequence")?,
                    r.try_get::<Option<chrono::DateTime<chrono::Utc>>, _>("checked_out_at")?,
                )
            }
        };
        let leased = mode == AckMode::ManualAck
            && !is_new
            && checked_out_at
                .is_some_and(|t| t + self.inner.config.read_cursor_checkout_lease > now);
        let types: BTreeSet<String> = [token.type_name.clone()].into_iter().collect();
        let max = self.inner.config.max_events_per_read;
        let (served, examined, full) = if leased {
            (vec![], None, false)
        } else {
            self.scan(&token.bounded_context, &types, position, max, |e| {
                matches_filters(&compiled, &e.payload)
                    && owner_scope_satisfied(&e.tags, owner.as_deref(), scope.as_deref())
            })
            .await?
        };
        let caught_up = examined.unwrap_or(position).max(position);
        let next_position = match mode {
            AckMode::AutoAdvance => {
                if !full {
                    caught_up
                } else {
                    served.last().map(|e| e.sequence).unwrap_or(position)
                }
            }
            AckMode::ManualAck if !leased && served.is_empty() => caught_up,
            AckMode::ManualAck => position,
        };
        if is_new {
            sqlx::query(
                "insert into read_cursors (token_id, ack_mode, sequence, updated_at, checked_out_at) values ($1, $2, $3, $4, $5)",
            )
            .bind(&token.id)
            .bind(mode.as_str())
            .bind(next_position)
            .bind(now)
            .bind(if mode == AckMode::ManualAck && !served.is_empty() { Some(now) } else { None })
            .execute(&mut *tx)
            .await?;
        } else if mode == AckMode::AutoAdvance || (!leased && served.is_empty()) {
            sqlx::query(
                "update read_cursors set sequence = $2, updated_at = $3 where token_id = $1",
            )
            .bind(&token.id)
            .bind(next_position)
            .bind(now)
            .execute(&mut *tx)
            .await?;
        } else if !leased {
            sqlx::query("update read_cursors set checked_out_at = $2 where token_id = $1")
                .bind(&token.id)
                .bind(now)
                .execute(&mut *tx)
                .await?;
        }
        tx.commit().await?;
        Ok(EventPage {
            last_sequence: served.last().map(|e| e.sequence),
            events: served.iter().map(|e| render_rest(e, &ty.0)).collect(),
            epoch: self.current_epoch().await,
        })
    }

    /// Where a token's first consume begins.
    async fn start_position(&self, token: &AccessToken, owner: Option<&str>) -> Result<i64> {
        let scoped_highest = |cutoff: Option<chrono::DateTime<chrono::Utc>>| async move {
            // Highest sequence of the token's type within its scope (blind
            // to the call's filters), optionally at or before a time.
            let rows = sqlx::query(&format!(
                "select {EVENT_COLUMNS} from events where bounded_context = $1 and event_type = $2
                   and ($3::timestamptz is null or created_at <= $3) order by sequence desc"
            ))
            .bind(&token.bounded_context)
            .bind(&token.type_name)
            .bind(cutoff)
            .fetch_all(&self.inner.pool)
            .await?;
            for r in rows {
                let e = event_from_row(&r)?;
                if owner_scope_satisfied(&e.tags, owner, token.scope.as_deref()) {
                    return Ok::<i64, Error>(e.sequence);
                }
            }
            Ok(-1)
        };
        Ok(match token.start_from {
            EventReadStartPosition::Beginning => -1,
            EventReadStartPosition::Latest => scoped_highest(None).await?,
            EventReadStartPosition::AtSequence => token.start_at_sequence.unwrap_or(-1).max(-1),
            EventReadStartPosition::AtTime => scoped_highest(token.start_at_time).await?,
        })
    }

    /// AcknowledgeEvents.
    pub async fn acknowledge_events(
        &self,
        token: &AccessToken,
        sequence: i64,
        epoch: Option<&str>,
    ) -> Result<()> {
        self.read_token_type(token)?;
        self.check_epoch(epoch).await?;
        let mut tx = self.inner.pool.begin().await?;
        let row = sqlx::query(
            "select ack_mode, sequence from read_cursors where token_id = $1 for update",
        )
        .bind(&token.id)
        .fetch_optional(&mut *tx)
        .await?
        .ok_or_else(|| Error::conflict("this token has no cursor yet"))?;
        let mode: String = row.try_get("ack_mode")?;
        if mode != AckMode::ManualAck.as_str() {
            return Err(Error::conflict("only a manual_ack cursor is acknowledged"));
        }
        let current: i64 = row.try_get("sequence")?;
        if sequence < current {
            return Err(Error::invalid("acknowledgement moves forward only"));
        }
        sqlx::query("update read_cursors set sequence = $2, updated_at = $3, checked_out_at = null where token_id = $1")
            .bind(&token.id)
            .bind(sequence)
            .bind(Self::now())
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;
        Ok(())
    }

    /// Hash a plaintext blind tag value on the caller's behalf (QueryEvents'
    /// wire contract); only for a caller entitled to sensitive data.
    async fn resolve_query_tags(
        &self,
        grant: &Grant,
        tags: Option<Vec<Tag>>,
    ) -> Result<Option<BTreeSet<Tag>>> {
        let Some(tags) = tags else { return Ok(None) };
        if tags.len() > self.inner.config.max_query_tags {
            return Err(Error::invalid(format!(
                "at most {} tags",
                self.inner.config.max_query_tags
            )));
        }
        let bc = &grant.mapping.bounded_context;
        let blind: BTreeSet<String> = sqlx::query_scalar::<_, String>(
            "select tag_key from blind_tags where bounded_context = $1",
        )
        .bind(bc)
        .fetch_all(&self.inner.pool)
        .await?
        .into_iter()
        .collect();
        let mut out = BTreeSet::new();
        for t in tags {
            if blind.contains(&t.key) {
                if let Some(v) = &t.value {
                    if !grant.mapping.can_read_sensitive {
                        return Err(Error::forbidden(
                            "looking up a blind tag by value needs can_read_sensitive",
                        ));
                    }
                    out.insert(Tag::new(&t.key, self.inner.blind.hash(bc, &t.key, v)?));
                    continue;
                }
            }
            out.insert(t);
        }
        Ok(Some(out))
    }

    fn admin_only(grant: &Grant) -> Result<()> {
        if grant.has_level(AccessLevel::Admin) {
            Ok(())
        } else {
            Err(Error::forbidden("needs an admin grant"))
        }
    }

    async fn check_types(&self, bc: &str, types: &[String]) -> Result<BTreeSet<String>> {
        let reg = self.inner.registry.read().await;
        for t in types {
            reg.event_type(bc, t)?;
        }
        Ok(types.iter().cloned().collect())
    }

    async fn owner_keys(&self, bc: &str) -> std::collections::BTreeMap<String, Option<String>> {
        self.inner
            .registry
            .read()
            .await
            .event_types_of(bc)
            .iter()
            .map(|t| (t.0.name.clone(), t.0.owner_tag_key.clone()))
            .collect()
    }

    /// QueryEvents.
    pub async fn query_events(
        &self,
        grant: &Grant,
        event_types: &[String],
        tags: Option<Vec<Tag>>,
        after_sequence: Option<i64>,
    ) -> Result<EventPage> {
        Self::admin_only(grant)?;
        let bc = grant.mapping.bounded_context.clone();
        let types = self.check_types(&bc, event_types).await?;
        let wanted = self.resolve_query_tags(grant, tags).await?;
        let owners = self.owner_keys(&bc).await;
        let scope = grant.mapping.scope.clone();
        let (served, _, _) = self
            .scan(
                &bc,
                &types,
                after_sequence.unwrap_or(-1),
                self.inner.config.max_events_per_read,
                |e| {
                    wanted
                        .as_ref()
                        .is_none_or(|w| w.iter().any(|t| e.tags.contains(t)))
                        && owner_scope_satisfied(
                            &e.tags,
                            owners.get(&e.event_type).cloned().flatten().as_deref(),
                            scope.as_deref(),
                        )
                },
            )
            .await?;
        let pa = self.private_access(grant).await?;
        let mut out = vec![];
        for e in &served {
            out.push(self.render_event(e, grant, &pa).await?);
        }
        Ok(EventPage {
            last_sequence: served.last().map(|e| e.sequence),
            events: out,
            epoch: self.current_epoch().await,
        })
    }

    /// CountEvents.
    pub async fn count_events(
        &self,
        grant: &Grant,
        event_types: &[String],
        tags: Option<Vec<Tag>>,
    ) -> Result<u64> {
        Self::admin_only(grant)?;
        let bc = grant.mapping.bounded_context.clone();
        let types = self.check_types(&bc, event_types).await?;
        let wanted = self.resolve_query_tags(grant, tags).await?;
        let owners = self.owner_keys(&bc).await;
        let scope = grant.mapping.scope.clone();
        let (served, _, _) = self
            .scan(&bc, &types, -1, usize::MAX, |e| {
                wanted
                    .as_ref()
                    .is_none_or(|w| w.iter().any(|t| e.tags.contains(t)))
                    && owner_scope_satisfied(
                        &e.tags,
                        owners.get(&e.event_type).cloned().flatten().as_deref(),
                        scope.as_deref(),
                    )
            })
            .await?;
        Ok(served.len() as u64)
    }

    /// InspectEvent.
    pub async fn inspect_event(&self, grant: &Grant, sequence: i64) -> Result<Rendered> {
        Self::admin_only(grant)?;
        let bc = &grant.mapping.bounded_context;
        let row = sqlx::query(&format!(
            "select {EVENT_COLUMNS} from events where bounded_context = $1 and sequence = $2"
        ))
        .bind(bc)
        .bind(sequence)
        .fetch_optional(&self.inner.pool)
        .await?
        .ok_or_else(|| Error::not_found(format!("event {bc}#{sequence}")))?;
        let e = event_from_row(&row)?;
        let owners = self.owner_keys(bc).await;
        if !owner_scope_satisfied(
            &e.tags,
            owners.get(&e.event_type).cloned().flatten().as_deref(),
            grant.mapping.scope.as_deref(),
        ) {
            return Err(Error::not_found(format!("event {bc}#{sequence}")));
        }
        let pa = self.private_access(grant).await?;
        self.render_event(&e, grant, &pa).await
    }

    /// InspectSnapshot - specified only as a surface action; this is our
    /// reading of it.
    pub async fn inspect_snapshot(
        &self,
        grant: &Grant,
        snapshot: &str,
        tag_value: &str,
    ) -> Result<Option<Value>> {
        Self::admin_only(grant)?;
        let bc = grant.mapping.bounded_context.clone();
        let summary = self
            .decl_for(&bc)
            .and_then(|d| d.summaries.iter().find(|s| s.name == snapshot))
            .cloned()
            .ok_or_else(|| {
                Error::invalid(format!("no summary named {snapshot} is defined for {bc}"))
            })?;
        let tag = Tag::new(&summary.tag_key, tag_value);
        let rows = sqlx::query(&format!(
            "select {EVENT_COLUMNS} from events e where e.bounded_context = $1 and exists (
               select 1 from event_tags t where t.bounded_context = e.bounded_context and t.sequence = e.sequence
                 and t.key = $2 and t.value = $3) order by e.sequence"
        ))
        .bind(&bc)
        .bind(&tag.key)
        .bind(tag_value)
        .fetch_all(&self.inner.pool)
        .await?;
        let mut owner: Option<String> = None;
        let mut state = summary.initial.clone();
        let mut touched = false;
        for r in rows {
            let mut e = event_from_row(&r)?;
            e.payload = self.decrypt_where(&e.payload, &|_| true).await?;
            if let Some(k) = &summary.owner_tag_key {
                if let Some(v) = crate::decl::tag_value(&e.tags, k) {
                    owner = Some(v.to_string());
                }
            }
            state = (summary.fold)(&state, &e, tag_value);
            touched = true;
        }
        if let (Some(scope), Some(_)) = (&grant.mapping.scope, &summary.owner_tag_key) {
            if owner.as_deref() != Some(scope.as_str()) {
                return Err(Error::not_found(format!(
                    "snapshot {snapshot}[{tag_value}]"
                )));
            }
        }
        Ok(touched.then_some(state))
    }

    /// CreateAllEventsSubscription / CreateEventTypeSubscription. Returns
    /// the spec plus the resume span to deliver first.
    pub async fn create_subscription(
        &self,
        grant: &Grant,
        event_type: Option<&str>,
        event_types: &[String],
        filters: &[Filter],
        from_sequence: Option<i64>,
        epoch: Option<&str>,
    ) -> Result<(SubscriptionSpec, Vec<Arc<StoredEvent>>)> {
        if !grant.has_level(AccessLevel::Read) {
            return Err(Error::forbidden("grant is revoked"));
        }
        self.check_epoch(epoch).await?;
        let bc = grant.mapping.bounded_context.clone();
        self.inner.registry.read().await.is_active(&bc)?;
        let kind = match event_type {
            Some(t) => {
                let ty = self.inner.registry.read().await.event_type(&bc, t)?;
                SubscriptionKind::EventType {
                    event_type: t.to_string(),
                    filters: compile_filters(&ty.1, filters, &self.inner.config)?,
                }
            }
            None => SubscriptionKind::AllEvents {
                event_types: self.check_types(&bc, event_types).await?,
            },
        };
        let latest = self.latest_sequence(&bc).await?;
        let start = from_sequence.unwrap_or(latest);
        let spec = SubscriptionSpec {
            bounded_context: bc.clone(),
            mapping_id: grant.mapping.id.clone(),
            from_sequence: start,
            kind,
        };
        let max = self.inner.config.max_events_per_read;
        let types = match &spec.kind {
            SubscriptionKind::AllEvents { event_types } => event_types.clone(),
            SubscriptionKind::EventType { event_type, .. } => {
                [event_type.clone()].into_iter().collect()
            }
        };
        // The resume span: everything after from_sequence up to now that
        // the subscription would deliver; more than one read is refused.
        let (span, _, full) = self.scan(&bc, &types, start, max + 1, |_| true).await?;
        if full && span.len() > max {
            return Err(Error::invalid(format!(
                "more than {max} events since from_sequence; read them back first"
            )));
        }
        Ok((spec, span))
    }

    /// DeliverToSubscriptions for one live subscription, as a stream that
    /// ends (with the reason) on revocation or lag.
    pub fn subscription_stream(
        &self,
        grant: Grant,
        spec: SubscriptionSpec,
        resume: Vec<Arc<StoredEvent>>,
    ) -> BoxStream<'static, std::result::Result<Rendered, SubscriptionEnd>> {
        let skilj = self.clone();
        // Subscribe before anything else so nothing committed after the
        // resume read is missed; duplicates are dropped by sequence.
        let rx = self.bus();
        let state = (skilj, grant, spec, rx, resume.into_iter(), -1i64, false);
        stream::unfold(
            state,
            |(skilj, mut grant, spec, mut rx, mut resume, mut last, done)| async move {
                if done {
                    return None;
                }
                loop {
                    let next: Option<Arc<StoredEvent>> = match resume.next() {
                        Some(e) => Some(e),
                        None => match rx.recv().await {
                            Ok(BusMessage::Event(e)) => Some(e),
                            Ok(BusMessage::AccessChanged {
                                mapping_id,
                                role_id,
                            }) => {
                                let concerns = mapping_id.as_deref()
                                    == Some(grant.mapping.id.as_str())
                                    || role_id.as_deref() == Some(grant.role.id.as_str());
                                if concerns {
                                    match skilj.reload_grant(&grant).await {
                                        Ok(Some(g)) => grant = g,
                                        _ => {
                                            return Some((
                                                Err(SubscriptionEnd::Revoked),
                                                (skilj, grant, spec, rx, resume, last, true),
                                            ))
                                        }
                                    }
                                }
                                None
                            }
                            Ok(BusMessage::Lagged)
                            | Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                                return Some((
                                    Err(SubscriptionEnd::Lagged),
                                    (skilj, grant, spec, rx, resume, last, true),
                                ));
                            }
                            Err(tokio::sync::broadcast::error::RecvError::Closed) => return None,
                        },
                    };
                    let Some(e) = next else { continue };
                    if e.bounded_context != spec.bounded_context
                        || e.sequence <= spec.from_sequence
                        || e.sequence <= last
                    {
                        continue;
                    }
                    last = e.sequence;
                    if !grant.mapping.active || !grant.role.active {
                        return Some((
                            Err(SubscriptionEnd::Revoked),
                            (skilj, grant, spec, rx, resume, last, true),
                        ));
                    }
                    let kind_matches = match &spec.kind {
                        SubscriptionKind::AllEvents { event_types } => {
                            event_types.is_empty() || event_types.contains(&e.event_type)
                        }
                        SubscriptionKind::EventType {
                            event_type,
                            filters,
                        } => &e.event_type == event_type && matches_filters(filters, &e.payload),
                    };
                    if !kind_matches {
                        continue;
                    }
                    let owner = skilj
                        .inner
                        .registry
                        .read()
                        .await
                        .event_type(&e.bounded_context, &e.event_type)
                        .ok()
                        .and_then(|t| t.0.owner_tag_key.clone());
                    if !owner_scope_satisfied(
                        &e.tags,
                        owner.as_deref(),
                        grant.mapping.scope.as_deref(),
                    ) {
                        continue;
                    }
                    let rendered = match skilj.private_access(&grant).await {
                        Ok(pa) => skilj.render_event(&e, &grant, &pa).await,
                        Err(err) => Err(err),
                    };
                    match rendered {
                        Ok(r) => {
                            return Some((Ok(r), (skilj, grant, spec, rx, resume, last, false)))
                        }
                        Err(_) => {
                            return Some((
                                Err(SubscriptionEnd::Lagged),
                                (skilj, grant, spec, rx, resume, last, true),
                            ))
                        }
                    }
                }
            },
        )
        .boxed()
    }
}
