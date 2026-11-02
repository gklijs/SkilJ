//! Exact tag dictionaries and bounded discovery of tags used by events.

use super::*;

/// The least UTF-8 string above every string with `prefix`, under C collation.
/// `None` means that the prefix has no finite upper bound (including empty).
fn prefix_upper(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let next = match u32::from(last) {
            0xd7ff => 0xe000,
            code => code.saturating_add(1),
        };
        if let Some(next) = char::from_u32(next) {
            let mut upper: String = chars.into_iter().collect();
            upper.push(next);
            return Some(upper);
        }
    }
    None
}

/// Searches actually used tag values under one key, by literal prefix.
///
/// Returns at most `min(limit, 100)` distinct tags, ordered by value with
/// nulls first. Null-valued tags are included only for an empty prefix.
/// Dictionary registrations without an event reference are excluded.
/// `event_type` restricts the referring events, and `scope` applies the same
/// owner-tag restriction as event reads. This is a low-level persistence API:
/// callers must authorize the context and event type before calling it.
pub async fn search_used_tags(
    pool: &Pool,
    bounded_context: &str,
    key: &str,
    prefix: &str,
    limit: usize,
    event_type: Option<&str>,
    scope: Option<&str>,
) -> crate::error::Result<Vec<Tag>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let bc = require_bounded_context(pool, bounded_context).await?;
    ensure_bounded_context_current(pool, &bc).await?;
    let schema = schema_ident(bounded_context);
    let upper = prefix_upper(prefix);
    let type_filter = if event_type.is_some() {
        "AND r.event_type_name = $5"
    } else {
        "AND $5::text IS NULL"
    };
    let (joins, owner_filter) = if scope.is_some() {
        (
            format!(
                "JOIN {schema}.events e ON e.sequence = r.sequence \
                 JOIN {schema}.event_types et ON et.name = r.event_type_name"
            ),
            "AND (et.owner_tag_key IS NULL OR e.tags @> \
             jsonb_build_array(jsonb_build_object('key', et.owner_tag_key, 'value', $6::text)))",
        )
    } else {
        (String::new(), "AND $6::text IS NULL")
    };
    // OFFSET 0 prevents PostgreSQL's generic plan from hashing all event
    // references to answer a bounded prefix request (issue #158).
    let used = format!(
        "EXISTS (SELECT 1 FROM {schema}.event_tag_refs r {joins} \
         WHERE r.tag_id = d.id {type_filter} {owner_filter} OFFSET 0)"
    );
    let range = if upper.is_some() {
        "d.tag_value >= $2 AND d.tag_value < $3"
    } else {
        "$3::text IS NULL AND d.tag_value >= $2"
    };
    let sql = format!(
        "SELECT tag_value FROM (\
            (SELECT d.tag_value FROM {schema}.event_tag_values d \
             WHERE d.tag_key = $1 AND {range} AND {used} \
             ORDER BY d.tag_value LIMIT $4) \
            UNION ALL \
            (SELECT d.tag_value FROM {schema}.event_tag_values d \
             WHERE d.tag_key = $1 AND d.tag_value IS NULL AND $2 = '' AND {used} \
             LIMIT $4)\
         ) suggestions ORDER BY tag_value NULLS FIRST LIMIT $4"
    );
    let values: Vec<Option<String>> = sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .bind(key)
        .bind(prefix)
        .bind(upper)
        .bind(limit.min(100) as i64)
        .bind(event_type)
        .bind(scope)
        .fetch_all(pool)
        .await?;
    Ok(values
        .into_iter()
        .map(|value| Tag {
            key: key.to_string(),
            value,
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::prefix_upper;

    #[test]
    fn prefix_bounds_cover_literal_and_unicode_edges() {
        assert_eq!(prefix_upper(""), None);
        assert_eq!(prefix_upper("abc"), Some("abd".into()));
        assert_eq!(prefix_upper("a\u{10ffff}"), Some("b".into()));
        assert_eq!(prefix_upper("\u{10ffff}"), None);
        assert_eq!(prefix_upper("\u{d7ff}"), Some("\u{e000}".into()));
        assert_eq!(prefix_upper("literal_%"), Some("literal_&".into()));
    }
}
