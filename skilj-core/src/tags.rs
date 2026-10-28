//! derive_tags, derive_query, query_matching_events selection and the
//! owner-scope predicates.

use std::collections::BTreeSet;

use serde_json::Value;

use crate::error::{Error, Result};
use crate::model::{QueryItem, QueryItemMapping, StoredEvent, Tag, TagMapping};
use crate::schema::read_path;

/// Computes blind tag values (keyed hash of the normalized value).
pub trait BlindHasher: Send + Sync {
    fn hash(&self, bounded_context: &str, tag_key: &str, value: &str) -> Result<String>;
}

/// A hasher for contexts with no blind keys configured: refuses.
pub struct NoBlindKeys;

impl BlindHasher for NoBlindKeys {
    fn hash(&self, bc: &str, key: &str, _: &str) -> Result<String> {
        Err(Error::Config(format!(
            "no blind index key configured for {bc}/{key}"
        )))
    }
}

/// Render a scalar as a tag value. Strings verbatim, other scalars in their
/// JSON spelling (so 5 and "5" are the same tag - the spec doesn't say).
pub fn scalar_tag_value(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        _ => None,
    }
}

/// Tags one mapping derives. `None` result means "derives no tag at all"
/// (a list holding only non-scalars), which only derive_query cares about.
fn derive_one(
    bc: &str,
    m: &TagMapping,
    payload: &Value,
    blind: &dyn BlindHasher,
) -> Result<Option<BTreeSet<Tag>>> {
    let mut out = BTreeSet::new();
    let make = |raw: String| -> Result<Tag> {
        if m.blind {
            Ok(Tag::new(&m.key, blind.hash(bc, &m.key, &raw)?))
        } else {
            Ok(Tag::new(&m.key, raw))
        }
    };
    match read_path(payload, &m.field) {
        None | Some(Value::Null) => {
            out.insert(Tag::none(&m.key));
        }
        Some(Value::Array(items)) => {
            if items.is_empty() {
                out.insert(Tag::none(&m.key));
            } else {
                for item in items {
                    if let Some(s) = scalar_tag_value(item) {
                        out.insert(make(s)?);
                    }
                }
                if out.is_empty() {
                    return Ok(None);
                }
            }
        }
        Some(v) => match scalar_tag_value(v) {
            Some(s) => {
                out.insert(make(s)?);
            }
            None => return Ok(None),
        },
    }
    Ok(Some(out))
}

/// derive_tags(type, payload).
pub fn derive_tags(
    bc: &str,
    mappings: &[TagMapping],
    payload: &Value,
    blind: &dyn BlindHasher,
) -> Result<BTreeSet<Tag>> {
    let mut tags = BTreeSet::new();
    for m in mappings {
        if let Some(t) = derive_one(bc, m, payload, blind)? {
            tags.extend(t);
        }
    }
    Ok(tags)
}

/// derive_query(command_type, payload): one item per declared item (a
/// snapshot item: one per derived value).
pub fn derive_query(
    bc: &str,
    mappings: &[TagMapping],
    query: &[QueryItemMapping],
    payload: &Value,
    blind: &dyn BlindHasher,
) -> Result<Vec<QueryItem>> {
    let mut out = Vec::new();
    for item in query {
        let item_mappings: Vec<&TagMapping> = mappings
            .iter()
            .filter(|m| item.tag_keys.contains(&m.key))
            .collect();
        if item.snapshot.is_some() {
            // One QueryItem per value the single mapping derives.
            let m = item_mappings
                .first()
                .ok_or_else(|| Error::Config("snapshot item has no tag mapping".into()))?;
            match derive_one(bc, m, payload, blind)? {
                Some(tags) => {
                    let valued: Vec<Tag> = tags.into_iter().filter(|t| t.value.is_some()).collect();
                    if valued.is_empty() {
                        out.push(QueryItem::nothing());
                    }
                    for t in valued {
                        out.push(QueryItem {
                            event_types: BTreeSet::new(),
                            tags: [t].into_iter().collect(),
                            latest: None,
                            snapshot: item.snapshot.clone(),
                        });
                    }
                }
                None => out.push(QueryItem::nothing()),
            }
            continue;
        }
        let mut tags = BTreeSet::new();
        let mut dead = false;
        for m in item_mappings {
            match derive_one(bc, m, payload, blind)? {
                Some(t) => tags.extend(t),
                None => dead = true,
            }
        }
        if dead {
            out.push(QueryItem::nothing());
        } else {
            out.push(QueryItem {
                event_types: item.event_types.clone(),
                tags,
                latest: item.latest,
                snapshot: None,
            });
        }
    }
    Ok(out)
}

/// Whether an event is in a command's consistency query (any item,
/// regardless of latest) - Command.in_consistency_query.
pub fn in_consistency_query(
    consistency_tags: &BTreeSet<Tag>,
    query: &[QueryItem],
    event_type: &str,
    tags: &BTreeSet<Tag>,
) -> bool {
    if query.is_empty() {
        consistency_tags.iter().any(|t| tags.contains(t))
    } else {
        query.iter().any(|i| i.matches(event_type, tags))
    }
}

/// query_matching_events over a candidate list already narrowed to events
/// that are in the consistency query, in ascending sequence order.
/// Applies each item's `latest`, returns the union in sequence order.
pub fn select_matching<'a>(
    consistency_tags: &BTreeSet<Tag>,
    query: &[QueryItem],
    candidates: &'a [StoredEvent],
) -> Vec<&'a StoredEvent> {
    if query.is_empty() {
        return candidates
            .iter()
            .filter(|e| consistency_tags.iter().any(|t| e.tags.contains(t)))
            .collect();
    }
    let mut chosen: BTreeSet<i64> = BTreeSet::new();
    for item in query {
        let matches: Vec<&StoredEvent> = candidates
            .iter()
            .filter(|e| item.matches(&e.event_type, &e.tags))
            .collect();
        let take: Box<dyn Iterator<Item = &&StoredEvent>> = match item.latest {
            Some(n) => Box::new(matches.iter().rev().take(n as usize)),
            None => Box::new(matches.iter()),
        };
        for e in take {
            chosen.insert(e.sequence);
        }
    }
    candidates
        .iter()
        .filter(|e| chosen.contains(&e.sequence))
        .collect()
}

/// tag_owner_scope_satisfied / event_owner_scope_satisfied /
/// command_owner_scope_satisfied: holds when scope is null, or the type
/// declares no owner key, or a tag under the owner key equals the scope.
pub fn owner_scope_satisfied(
    tags: &BTreeSet<Tag>,
    owner_tag_key: Option<&str>,
    scope: Option<&str>,
) -> bool {
    match (scope, owner_tag_key) {
        (None, _) | (_, None) => true,
        (Some(scope), Some(key)) => tags
            .iter()
            .any(|t| t.key == key && t.value.as_deref() == Some(scope)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn derive_cases() {
        let m = vec![
            TagMapping::new("account", "account_id"),
            TagMapping::new("label", "labels"),
            TagMapping::new("country", "address.country"),
        ];
        let tags = derive_tags(
            "b",
            &m,
            &json!({"account_id": "a1", "labels": ["x", "y", "x"]}),
            &NoBlindKeys,
        )
        .unwrap();
        assert!(tags.contains(&Tag::new("account", "a1")));
        assert!(tags.contains(&Tag::new("label", "x")));
        assert!(tags.contains(&Tag::new("label", "y")));
        assert!(tags.contains(&Tag::none("country")));
        assert_eq!(tags.len(), 4);
        let tags = derive_tags(
            "b",
            &m,
            &json!({"account_id": 7, "labels": []}),
            &NoBlindKeys,
        )
        .unwrap();
        assert!(tags.contains(&Tag::new("account", "7")));
        assert!(tags.contains(&Tag::none("label")));
    }

    #[test]
    fn owner_scope() {
        let tags: BTreeSet<Tag> = [Tag::new("company", "c1"), Tag::none("x")]
            .into_iter()
            .collect();
        assert!(owner_scope_satisfied(&tags, None, Some("c2")));
        assert!(owner_scope_satisfied(&tags, Some("company"), None));
        assert!(owner_scope_satisfied(&tags, Some("company"), Some("c1")));
        assert!(!owner_scope_satisfied(&tags, Some("company"), Some("c2")));
        assert!(!owner_scope_satisfied(&tags, Some("x"), Some("c1")));
    }
}
