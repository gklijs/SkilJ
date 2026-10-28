//! The `valid_*` black boxes that need no database.

use std::collections::BTreeSet;

use crate::error::{Error, Result};
use crate::model::{PrivateField, PrivateFieldKind, SensitiveField, TagMapping};
use crate::schema::Schema;

/// Lowercase letters, digits and underscores, starting with a letter, at
/// most 40 characters.
pub fn valid_bounded_context_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 40
        && name.chars().next().is_some_and(|c| c.is_ascii_lowercase())
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(Error::invalid(format!(
            "bounded context name '{name}' must be lowercase letters, digits and underscores, start with a letter, at most 40 characters"
        )))
    }
}

/// Type and projection names. The spec gives no rule for these at all; a
/// name ends up in GraphQL type names, so we apply GraphQL's.
pub fn valid_type_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 100
        && name
            .chars()
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if ok {
        Ok(())
    } else {
        Err(Error::invalid(format!(
            "type name '{name}' is not a valid identifier"
        )))
    }
}

/// Null/empty or at most 200 characters, no charset.
pub fn valid_correlation_id(id: Option<&str>) -> Result<()> {
    match id {
        Some(s) if s.chars().count() > 200 => Err(Error::invalid(
            "correlation/causation id longer than 200 characters",
        )),
        _ => Ok(()),
    }
}

/// Idempotency keys: at most 255 characters (ProcessCommand prose).
pub fn valid_idempotency_key(key: Option<&str>) -> Result<()> {
    match key {
        Some(s) if s.chars().count() > 255 => {
            Err(Error::invalid("idempotency key longer than 255 characters"))
        }
        Some("") => Err(Error::invalid("idempotency key may not be empty")),
        _ => Ok(()),
    }
}

/// Lowercase letters, digits, underscore and hyphen, 1-64 characters.
pub fn valid_capability_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 64
        && name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_' || c == '-');
    if ok {
        Ok(())
    } else {
        Err(Error::invalid(format!(
            "capability name '{name}' is malformed"
        )))
    }
}

/// Everything RegisterEventType/RegisterCommandType check about the field
/// declarations, in the order the spec checks them.
pub fn validate_field_declarations(
    schema: &Schema,
    tag_mappings: &[TagMapping],
    sensitive_fields: &[SensitiveField],
    private_fields: &[PrivateField],
    owner_tag_key: Option<&str>,
) -> Result<()> {
    // valid_tag_mappings
    let mut keys_seen: BTreeSet<(&str, bool)> = BTreeSet::new();
    for m in tag_mappings {
        if m.key.is_empty() {
            return Err(Error::invalid("tag mapping key may not be empty"));
        }
        schema.resolve(&m.field)?;
        keys_seen.insert((&m.key, m.blind));
    }
    // A key is blind on every mapping of it or on none (within one type here;
    // across the context is checked by the registry).
    for (k, _) in &keys_seen {
        let blind = keys_seen.contains(&(k, true));
        let plain = keys_seen.contains(&(k, false));
        if blind && plain {
            return Err(Error::invalid(format!(
                "tag key '{k}' is mapped both blind and plain"
            )));
        }
    }
    // valid_sensitive_fields
    for s in sensitive_fields {
        schema.resolve(&s.field)?;
        schema.resolve(&s.subject_field)?;
        if s.subject_key.is_empty() {
            return Err(Error::invalid(
                "sensitive field subject_key may not be empty",
            ));
        }
        if s.field == s.subject_field {
            // Not in the spec. A field that is its own subject would have to
            // be read in plaintext to find the key it is encrypted under.
            return Err(Error::invalid(format!(
                "sensitive field '{}' cannot be its own subject_field",
                s.field
            )));
        }
    }
    // valid_private_fields
    for p in private_fields {
        schema.resolve(&p.field)?;
        match p.kind {
            PrivateFieldKind::Own => {
                if p.capability.is_some() || p.addressee_field.is_some() {
                    return Err(Error::invalid(format!(
                        "own-kind private field '{}' names neither a capability nor an addressee",
                        p.field
                    )));
                }
            }
            PrivateFieldKind::Capability => {
                let cap = p.capability.as_deref().ok_or_else(|| {
                    Error::invalid(format!(
                        "capability-kind private field '{}' needs a capability",
                        p.field
                    ))
                })?;
                valid_capability_name(cap)?;
                if p.addressee_field.is_some() {
                    return Err(Error::invalid(format!(
                        "capability-kind private field '{}' may not name an addressee",
                        p.field
                    )));
                }
            }
            PrivateFieldKind::Addressed => {
                let a = p.addressee_field.as_deref().ok_or_else(|| {
                    Error::invalid(format!(
                        "addressed private field '{}' needs an addressee_field",
                        p.field
                    ))
                })?;
                schema.resolve(a)?;
                if p.capability.is_some() {
                    return Err(Error::invalid(format!(
                        "addressed private field '{}' may not name a capability",
                        p.field
                    )));
                }
            }
        }
    }
    // valid_owner_tag_key
    if let Some(owner) = owner_tag_key {
        let produced = tag_mappings.iter().find(|m| m.key == owner);
        match produced {
            None => {
                return Err(Error::invalid(format!(
                    "owner_tag_key '{owner}' is not a key these tag mappings produce"
                )))
            }
            Some(m) if m.blind => {
                return Err(Error::invalid(format!(
                    "owner_tag_key '{owner}' is blind; a scope is compared with a plain tag"
                )))
            }
            _ => {}
        }
    }
    // Overlaps: a field is at most one of tagged (plain), sensitive, private.
    for m in tag_mappings.iter().filter(|m| !m.blind) {
        if sensitive_fields.iter().any(|s| s.field == m.field) {
            return Err(Error::invalid(format!(
                "field '{}' is both tagged and sensitive; the tag would outlive crypto-shredding",
                m.field
            )));
        }
        if private_fields.iter().any(|p| p.field == m.field) {
            return Err(Error::invalid(format!(
                "field '{}' is both tagged and private; the tag would leak it",
                m.field
            )));
        }
    }
    for s in sensitive_fields {
        if private_fields.iter().any(|p| p.field == s.field) {
            return Err(Error::invalid(format!(
                "field '{}' is both sensitive and private",
                s.field
            )));
        }
    }
    // Duplicates within one declaration.
    let mut f = BTreeSet::new();
    for s in sensitive_fields {
        if !f.insert(&s.field) {
            return Err(Error::invalid(format!(
                "field '{}' declared sensitive twice",
                s.field
            )));
        }
    }
    let mut f = BTreeSet::new();
    for p in private_fields {
        if !f.insert(&p.field) {
            return Err(Error::invalid(format!(
                "field '{}' declared private twice",
                p.field
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn names() {
        valid_bounded_context_name("banking_1").unwrap();
        assert!(valid_bounded_context_name("1bank").is_err());
        assert!(valid_bounded_context_name("Bank").is_err());
        assert!(valid_bounded_context_name(&"a".repeat(41)).is_err());
        valid_capability_name("staff-only").unwrap();
        assert!(valid_capability_name("Staff").is_err());
    }

    #[test]
    fn overlap_is_refused() {
        let s = Schema::parse(&json!({"type":"object","properties":{
            "user_id":{"type":"string"},"email":{"type":"string"}}}))
        .unwrap();
        let tags = vec![TagMapping::new("email", "email")];
        let sens = vec![SensitiveField {
            field: "email".into(),
            subject_key: "user".into(),
            subject_field: "user_id".into(),
        }];
        assert!(validate_field_declarations(&s, &tags, &sens, &[], None).is_err());
        // blind is exempt
        let tags = vec![TagMapping::blind("email", "email")];
        validate_field_declarations(&s, &tags, &sens, &[], None).unwrap();
    }
}
