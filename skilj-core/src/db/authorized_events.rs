//! Authorized event selection shared by the administrative and token read surfaces.

use super::{events::events_from_rows, events::EventRowAnyType, *};
use crate::event_store;
use crate::shared::Filter;

/// Validated event selection. Owner policy is read from current registrations
/// by each SQL statement; this value retains no event or decrypted payload.
#[derive(Clone, Debug)]
pub struct AuthorizedEventQuery {
    bounded_context: String,
    types: Vec<String>,
    tags: Option<Vec<Tag>>,
    scope: Option<String>,
    after: i64,
    up_to: Option<i64>,
    correlation_id: Option<String>,
}

impl AuthorizedEventQuery {
    /// Validate an administrative query, including when the history is empty.
    pub fn for_admin(
        mapping: &RoleAccessMapping,
        types: &[EventType],
        tags: Option<&[Tag]>,
        after: Option<i64>,
        up_to: Option<i64>,
        correlation_id: Option<&str>,
    ) -> crate::error::Result<Self> {
        event_store::query_events_select(mapping, types, tags, after, correlation_id, &[], 0)?;
        Ok(Self {
            bounded_context: mapping.bounded_context.name.clone(),
            types: types.iter().map(|ty| ty.name.clone()).collect(),
            tags: tags.map(<[Tag]>::to_vec),
            scope: mapping.scope.clone(),
            after: after.unwrap_or(-1),
            up_to,
            correlation_id: correlation_id.map(str::to_owned),
        })
    }

    /// Validate a token read. Payload filters remain the caller's pure predicate;
    /// SQL narrows its candidates by type, owner, position and correlation.
    pub fn for_token(
        token: &EventReadToken,
        filters: &[Filter],
        after: i64,
        up_to: Option<i64>,
        correlation_id: Option<&str>,
    ) -> crate::error::Result<Self> {
        event_store::check_fetch_events(token, filters)?;
        Ok(Self {
            bounded_context: token.event_type.bounded_context.name.clone(),
            types: vec![token.event_type.name.clone()],
            tags: None,
            scope: token.scope.clone(),
            after,
            up_to,
            correlation_id: correlation_id.map(str::to_owned),
        })
    }

    fn predicate(&self, limit: Option<&str>) -> String {
        let schema = schema_ident(&self.bounded_context);
        let mut predicate = String::from(
            "e.sequence > $1 AND ($2::bigint IS NULL OR e.sequence <= $2) \
             AND ",
        );
        predicate.push_str(if self.types.is_empty() {
            "$3::text[] IS NOT NULL"
        } else {
            "e.event_type_name = ANY($3::text[])"
        });
        if self.correlation_id.is_some() {
            predicate.push_str(" AND e.metadata_correlation_id = $4");
        } else {
            predicate.push_str(" AND $4::text IS NULL");
        }
        if self.scope.is_some() {
            // Resolve the current owner key through the dictionary and reuse
            // its reference index. Ownerless types contribute their type
            // index ranges. ANY avoids multiplying events with duplicate tags.
            let types = if self.types.is_empty() {
                "TRUE"
            } else {
                "policy.name = ANY($3::text[])"
            };
            predicate.push_str(&format!(
                " AND e.sequence = ANY(ARRAY(\
                    SELECT r.sequence FROM {schema}.event_types policy \
                    JOIN {schema}.event_tag_values v \
                      ON v.tag_key = policy.owner_tag_key AND v.tag_value = $5::text \
                    JOIN {schema}.event_tag_refs r \
                      ON r.tag_id = v.id AND r.event_type_name = policy.name \
                    WHERE {types} AND r.sequence > $1 \
                      AND ($2::bigint IS NULL OR r.sequence <= $2) \
                    UNION ALL \
                    SELECT free.sequence FROM {schema}.event_types policy \
                    JOIN {schema}.events free ON free.event_type_name = policy.name \
                    WHERE policy.owner_tag_key IS NULL AND {types} \
                      AND free.sequence > $1 AND ($2::bigint IS NULL OR free.sequence <= $2)))"
            ));
        } else {
            predicate.push_str(" AND $5::text IS NULL");
        }
        if let Some(tags) = &self.tags {
            if tags.is_empty() {
                predicate.push_str(" AND FALSE");
            } else {
                let types = if self.types.is_empty() {
                    super::events::all_event_types(&schema)
                } else {
                    "$3::text[]".into()
                };
                // Limit each tag only after every other predicate has matched.
                // The union's first N matches must occur in a tag's first N.
                let filter = if self.scope.is_some() || self.correlation_id.is_some() {
                    format!(
                        " AND EXISTS (SELECT 1 FROM {schema}.events e \
                        JOIN {schema}.event_types et ON et.name = e.event_type_name \
                        WHERE e.sequence = t.sequence AND {predicate})"
                    )
                } else {
                    " AND t.sequence > $1 AND ($2::bigint IS NULL OR t.sequence <= $2)".into()
                };
                let selects: Vec<_> = tags
                    .iter()
                    .enumerate()
                    .map(|(i, tag)| {
                        let key = format!("${}::text", 6 + 2 * i);
                        let value = if tag.value.is_some() {
                            format!("= ${}::text", 7 + 2 * i)
                        } else {
                            format!("IS NULL AND ${}::text IS NULL", 7 + 2 * i)
                        };
                        match limit {
                            None => super::events::event_tags_select(
                                &schema, &key, &value, &types, &filter, None,
                            ),
                            Some(limit) => {
                                // Duplicate stored tags can create duplicate reference
                                // rows. Deduplicate before limiting, or a short candidate
                                // page could incorrectly declare the history exhausted.
                                format!(
                                    "(SELECT l.sequence FROM unnest({types}) AS ty(name) \
                                     CROSS JOIN LATERAL (SELECT DISTINCT t.sequence \
                                        FROM {schema}.event_tag_refs t \
                                        WHERE t.tag_id = (SELECT id FROM {schema}.event_tag_values \
                                            WHERE tag_key = {key} AND tag_value {value}) \
                                        AND t.event_type_name = ty.name{filter} \
                                        ORDER BY t.sequence LIMIT {limit}) l \
                                     ORDER BY l.sequence LIMIT {limit})"
                                )
                            }
                        }
                    })
                    .collect();
                predicate.push_str(&format!(
                    " AND e.sequence = ANY(ARRAY({}))",
                    selects.join(" UNION ")
                ));
            }
        }
        predicate
    }

    fn bind<'q, O>(
        &'q self,
        mut query: sqlx::query::QueryAs<'q, Postgres, O, sqlx::postgres::PgArguments>,
    ) -> sqlx::query::QueryAs<'q, Postgres, O, sqlx::postgres::PgArguments> {
        query = query
            .bind(self.after)
            .bind(self.up_to)
            .bind(&self.types)
            .bind(&self.correlation_id)
            .bind(&self.scope);
        if let Some(tags) = &self.tags {
            for tag in tags {
                query = query.bind(&tag.key).bind(&tag.value);
            }
        }
        query
    }
}

/// Count matching rows in one statement snapshot, without loading payloads,
/// commands or encryption keys. Concurrent appends after that snapshot are
/// visible to a later call, rather than to later chunks of the same count.
pub async fn count_authorized_events(
    pool: &Pool,
    query: &AuthorizedEventQuery,
) -> crate::error::Result<i64> {
    let schema = schema_ident(&query.bounded_context);
    let sql = format!(
        "SELECT count(*) FROM {schema}.events e \
         JOIN {schema}.event_types et ON et.name = e.event_type_name WHERE {}",
        query.predicate(None)
    );
    let (count,): (i64,) = query
        .bind(sqlx::query_as(sqlx::AssertSqlSafe(sql)))
        .fetch_one(pool)
        .await?;
    Ok(count)
}

/// Select a bounded page before hydration. Each statement uses current owner
/// registrations. `select` applies any remaining pure payload predicates.
///
/// A request fixes its upper sequence to the type-filtered committed head at
/// entry (or the smaller explicit bound). A short page reports that position
/// even when every candidate was filtered out, so token cursors make progress.
/// Across requests the history remains live; no transaction spans page walks.
pub async fn collect_authorized_event_page(
    pool: &Pool,
    query: &AuthorizedEventQuery,
    max_events: usize,
    mut select: impl FnMut(&[Event], usize) -> crate::error::Result<Vec<Event>>,
) -> crate::error::Result<super::ScannedEventPage> {
    let bc = require_bounded_context(pool, &query.bounded_context).await?;
    let schema = schema_ident(&query.bounded_context);
    let type_predicate = if query.types.is_empty() {
        "$3::text[] IS NOT NULL"
    } else {
        "event_type_name = ANY($3::text[])"
    };
    let (head,): (Option<i64>,) = sqlx::query_as(sqlx::AssertSqlSafe(format!(
        "SELECT max(sequence) FROM {schema}.events WHERE sequence > $1 \
         AND ($2::bigint IS NULL OR sequence <= $2) \
         AND {type_predicate}"
    )))
    .bind(query.after)
    .bind(query.up_to)
    .bind(&query.types)
    .fetch_one(pool)
    .await?;
    let mut page = super::ScannedEventPage {
        events: Vec::new(),
        scanned_through: None,
    };
    let Some(head) = head else {
        return Ok(page);
    };
    let mut query = query.clone();
    query.up_to = Some(head);
    while page.events.len() < max_events {
        let remaining = max_events - page.events.len();
        let limit = i64::try_from(remaining).unwrap_or(i64::MAX);
        let limit_parameter = 6 + query.tags.as_ref().map_or(0, |tags| 2 * tags.len());
        let sql = format!(
            "SELECT e.* FROM {schema}.events e \
             JOIN {schema}.event_types et ON et.name = e.event_type_name \
             WHERE {} ORDER BY e.sequence LIMIT ${limit_parameter}",
            query.predicate(Some(&format!("${limit_parameter}::bigint")))
        );
        let rows: Vec<EventRowAnyType> = query
            .bind(sqlx::query_as(sqlx::AssertSqlSafe(sql)))
            .bind(limit)
            .fetch_all(pool)
            .await?;
        let short = rows.len() < remaining;
        if let Some(last) = rows.last() {
            query.after = last.sequence;
            page.scanned_through = Some(last.sequence);
        }
        let events = events_from_rows(&mut *pool.acquire().await?, &bc, rows, None).await?;
        page.events.extend(select(&events, remaining)?);
        if short {
            page.scanned_through = Some(head);
            break;
        }
    }
    Ok(page)
}
