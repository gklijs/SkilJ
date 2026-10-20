//! Blind-index tags: a tag whose value is a keyed hash of a payload field
//! rather than the field itself (docs/architecture.md §205).
//!
//! A plain tag is stored in the clear - in `events.tags`, `event_tags`,
//! `commands.consistency_tags`, the event cache and everything that serves
//! them - which is why a tag mapping may not name a sensitive field. A
//! blind tag may: its value is `HMAC-SHA256(constraint key,
//! normalize(value))`, hex-encoded, so two payloads with the same
//! normalized value carry the same tag and a command's consistency query
//! finds the earlier one, while the value itself is never stored. That is
//! what a uniqueness check on personal data (an email address, a national
//! id) needs: the ordinary DCB conflict check, over a value nobody can read
//! back.
//!
//! Each constraint - one `(bounded context, tag key)` pair, declared once
//! by a [`crate::plugin::BlindTag`] - gets its own HMAC key, derived with
//! HKDF-SHA256 from the process's [`BlindIndexKey`]. The root key is
//! separate from the [`crate::encryption::EncryptionMasterKey`], so either
//! can be rotated or kept elsewhere without touching the other; deriving
//! per constraint means the same value hashes differently under two
//! constraints or two bounded contexts, so the hashes can't be correlated
//! across them.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use ring::{hkdf, hmac};
use serde::{Deserialize, Serialize};

const KEY_LEN: usize = 32;

/// Salt for every constraint key derivation. Changing it changes every
/// blind tag ever written, so it is part of the stored format.
const HKDF_SALT: &[u8] = b"skilj-blind-index-v1";

/// Errors deriving a blind tag.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A tag mapping is blind but this process can't hash it: it doesn't
    /// declare the `BlindTag` for that key, or has no [`BlindIndexKey`].
    /// An instance that does can (`skilj_retry::ANOTHER_INSTANCE_CODES`).
    #[error(
        "tag key `{tag_key}` of bounded context `{bounded_context}` is blind, but this \
         process declares no BlindTag for it (or has no blind_index_key)"
    )]
    NotDeclared {
        /// The bounded context.
        bounded_context: String,
        /// The blind tag key.
        tag_key: String,
    },

    /// A query asked for a blind tag by its plaintext value without a
    /// grant that may read sensitive fields (docs/architecture.md §207):
    /// the answer would say whether a value is in use, which is the value's
    /// own sensitivity.
    #[error(
        "looking up blind tag `{tag_key}` by its plaintext value needs a grant that can read \
         sensitive fields"
    )]
    LookupNotGranted {
        /// The blind tag key.
        tag_key: String,
    },

    /// The constraint is stored at a key generation this process has no
    /// root key for (docs/architecture.md §209): it was rotated to a key
    /// this process isn't configured with. An instance that is can do it
    /// (`skilj_retry::ANOTHER_INSTANCE_CODES`).
    #[error(
        "blind tag `{tag_key}` of bounded context `{bounded_context}` is at key generation \
         {generation}, and this process has no blind_index_key for it"
    )]
    KeyNotConfigured {
        /// The bounded context.
        bounded_context: String,
        /// The blind tag key.
        tag_key: String,
        /// The key generation the constraint is stored at.
        generation: u32,
    },

    /// A command's blind tags were hashed before a key rotation of its
    /// bounded context committed (docs/architecture.md §209). Submission
    /// retries it once, rehashed; it surfaces only if a second rotation
    /// lands in between.
    #[error("the blind tags of bounded context `{bounded_context}` were rotated meanwhile; retry")]
    Rotated {
        /// The bounded context.
        bounded_context: String,
    },
}

impl crate::error::SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::NotDeclared { .. } => "blind_tag_not_declared",
            Error::LookupNotGranted { .. } => "blind_tag_lookup_not_granted",
            Error::KeyNotConfigured { .. } => "blind_index_key_not_configured",
            Error::Rotated { .. } => "blind_tags_rotated",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

/// The root secret every constraint's HMAC key is derived from -
/// `SkiljBuilder::blind_index_key`. Never persisted; like the encryption
/// master key, the application keeps it stable across restarts and
/// identical across instances. Losing or changing it changes every blind
/// tag, so values already stored no longer match new ones; replacing a
/// leaked one is a rotation (docs/architecture.md §209), which keeps it
/// configured as a previous key.
#[derive(Clone)]
pub struct BlindIndexKey([u8; KEY_LEN]);

impl BlindIndexKey {
    /// The key from its 32 bytes, which should come from a secure random
    /// source and be kept secret.
    pub fn from_bytes(bytes: [u8; KEY_LEN]) -> Self {
        Self(bytes)
    }

    /// The HMAC key of one constraint. The info string length-prefixes
    /// both parts, so no `(bounded_context, tag_key)` pair can collide
    /// with another by moving a separator.
    fn constraint_key(&self, bounded_context: &str, tag_key: &str) -> hmac::Key {
        self.derive(&[], bounded_context, tag_key)
    }

    /// What `blind_tags.key_fingerprint` stores for one constraint
    /// (docs/architecture.md §208): the hex HMAC of an empty message under
    /// a key derived like the constraint's but with [`FINGERPRINT_LABEL`]
    /// ahead of the info, so it reveals nothing about the constraint key
    /// and can't equal the tag of any value. Equal for two root keys
    /// exactly when they hash every value of the constraint alike.
    ///
    /// `previous` is the fingerprint of the key chain before this key
    /// (docs/architecture.md §209), empty for the first.
    fn fingerprint(&self, bounded_context: &str, tag_key: &str, previous: &str) -> String {
        let key = self.derive(FINGERPRINT_LABEL, bounded_context, tag_key);
        hex(hmac::sign(&key, previous.as_bytes()).as_ref())
    }

    /// `tag`, a tag of the previous generation, wrapped under this key's
    /// constraint key (docs/architecture.md §209).
    fn wrap(&self, bounded_context: &str, tag_key: &str, tag: &str) -> String {
        let key = self.constraint_key(bounded_context, tag_key);
        hex(hmac::sign(&key, tag.as_bytes()).as_ref())
    }

    /// HKDF-SHA256 over the root key, with `label` (empty for the
    /// constraint key itself, the stored format) ahead of the
    /// length-prefixed `(bounded_context, tag_key)`.
    fn derive(&self, label: &[u8], bounded_context: &str, tag_key: &str) -> hmac::Key {
        let salt = hkdf::Salt::new(hkdf::HKDF_SHA256, HKDF_SALT);
        let prk = salt.extract(&self.0);
        let bc_len = (bounded_context.len() as u64).to_be_bytes();
        let key_len = (tag_key.len() as u64).to_be_bytes();
        let info: [&[u8]; 5] = [
            label,
            &bc_len,
            bounded_context.as_bytes(),
            &key_len,
            tag_key.as_bytes(),
        ];
        let okm = prk
            .expand(&info, hmac::HMAC_SHA256)
            .expect("HMAC_SHA256's key length is a valid HKDF output length");
        hmac::Key::from(okm)
    }
}

/// Prefixes the HKDF info of a key fingerprint. A constraint key's info
/// starts with the bounded context's 8-byte length, and these 8 bytes read
/// as a length no name has, so the two derivations never share an info.
const FINGERPRINT_LABEL: &[u8] = b"skilj-blind-index-fingerprint-v1";

impl std::fmt::Debug for BlindIndexKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("BlindIndexKey(REDACTED)")
    }
}

/// What `forgetSubject` does with a blind tag (docs/architecture.md §205).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "kind")]
#[non_exhaustive]
pub enum Erasure {
    /// The hash stays on the subject's events and commands after its key
    /// is destroyed, so the value stays taken: "this address may never
    /// sign up again".
    Keep,
    /// Forgetting the subject removes the tag from every event and command
    /// carrying `subject_tag` with the forgotten subject's value, so the
    /// value is free again and no hash of it remains.
    WithSubject {
        /// The `SensitiveField.subject_key` whose `forgetSubject` erases.
        subject_key: String,
        /// The plain tag key whose value is that subject's value - how the
        /// subject's events and commands are found without decrypting.
        subject_tag: String,
    },
}

/// One constraint as stored in a bounded context's `blind_tags` table:
/// what every instance agrees on. The normalizer itself is code; its
/// version is what's compared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BlindTagDeclaration {
    /// The blind tag key.
    pub tag_key: String,
    /// The version of the normalizer values are hashed after.
    pub normalizer_version: u32,
    /// What `forgetSubject` does with the hashes.
    pub erasure: Erasure,
    /// The constraint's key fingerprint (docs/architecture.md §208), so an
    /// instance with a different `blind_index_key` refuses to start rather
    /// than hash values that never match the stored ones. `None` when the
    /// writer couldn't compute it - a tenant created by an instance that
    /// doesn't declare its template's `BlindTag` - and recorded by the
    /// first instance that can.
    pub key_fingerprint: Option<String>,
    /// How many times the constraint's tags have been wrapped under a
    /// later root key (docs/architecture.md §209); `key_fingerprint` is
    /// the chain's up to it.
    pub key_generation: u32,
}

/// A normalizer: what a payload value becomes before it's hashed.
pub type Normalizer = fn(&str) -> String;

/// Ready-made normalizers for [`crate::plugin::BlindTag::normalize`].
pub mod normalize {
    /// Leading and trailing whitespace removed, lowercased - for an email
    /// address. Deliberately nothing provider-specific: `a.b+x@gmail.com`
    /// and `ab@gmail.com` stay different addresses.
    pub fn email(value: &str) -> String {
        value.trim().to_lowercase()
    }

    /// Every whitespace character removed, uppercased - for identifiers
    /// written with optional spacing, like an IBAN or a postcode.
    pub fn compact_uppercase(value: &str) -> String {
        value
            .chars()
            .filter(|c| !c.is_whitespace())
            .flat_map(char::to_uppercase)
            .collect()
    }
}

struct Inner {
    /// Every root key, oldest first; the last is the current one
    /// (docs/architecture.md §209). One key unless a rotation is underway
    /// or done.
    keys: Vec<BlindIndexKey>,
    normalizers: HashMap<(String, String), Normalizer>,
    templates: Option<crate::template_cache::TemplateCache>,
    /// The stored `key_generation` of each constraint this process has
    /// seen, by `(bounded context, tag key)` - startup loads it, and the
    /// check under each write's bounded-context lock corrects it after a
    /// rotation (§209). Shared by every clone.
    generations: Arc<RwLock<HashMap<(String, String), u32>>>,
}

impl Inner {
    /// The constraint key of generation `generation`.
    fn key(
        &self,
        generation: u32,
        bounded_context: &str,
        tag_key: &str,
    ) -> Result<&BlindIndexKey, Error> {
        self.keys
            .get(generation as usize)
            .ok_or_else(|| Error::KeyNotConfigured {
                bounded_context: bounded_context.to_string(),
                tag_key: tag_key.to_string(),
                generation,
            })
    }
}

/// Every constraint this process can hash, by `(bounded context, tag
/// key)`. Cheap to clone. Empty when no `BlindTag` is declared, which is
/// every deployment that doesn't use them: deriving a plain tag never
/// looks here.
///
/// A tenant created from a template (issue #13) is resolved to
/// its template's declaration through the [`TemplateCache`]: the template's
/// normalizer, but a key derived from the tenant's own name, so two
/// tenants' hashes of the same value differ.
///
/// **Generations** (docs/architecture.md §209). A constraint's tags are
/// hashed under the first root key and then wrapped once under each later
/// one, up to the constraint's stored `key_generation`: generation 0 is
/// `HMAC(k0, normalize(value))`, generation `g` is `HMAC(k_g, tag of g -
/// 1)`. A rotation can therefore rewrite every stored tag from the tag
/// alone, without the value - which is encrypted, or forgotten - and a
/// leaked earlier key no longer lets anyone test a value against the
/// stored tags.
///
/// [`TemplateCache`]: crate::template_cache::TemplateCache
#[derive(Clone, Default)]
pub struct BlindIndex(Option<Arc<Inner>>);

impl std::fmt::Debug for BlindIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let keys = self.0.iter().flat_map(|inner| inner.normalizers.keys());
        f.debug_set().entries(keys).finish()
    }
}

impl BlindIndex {
    /// An index over `constraints`, each `(bounded context, tag key,
    /// normalizer)`, its keys derived from `root`.
    pub fn new(
        root: BlindIndexKey,
        constraints: impl IntoIterator<Item = (String, String, Normalizer)>,
    ) -> Self {
        let normalizers = constraints
            .into_iter()
            .map(|(bounded_context, tag_key, normalize)| ((bounded_context, tag_key), normalize))
            .collect();
        Self(Some(Arc::new(Inner {
            keys: vec![root],
            normalizers,
            templates: None,
            generations: Arc::default(),
        })))
    }

    /// The root keys `new`'s replaced, oldest first, for a rotation
    /// (docs/architecture.md §209): `new`'s key becomes the newest.
    pub fn with_previous_keys(self, previous: impl IntoIterator<Item = BlindIndexKey>) -> Self {
        Self(self.0.map(|inner| {
            let mut keys: Vec<BlindIndexKey> = previous.into_iter().collect();
            keys.extend(inner.keys.iter().cloned());
            Arc::new(Inner {
                keys,
                normalizers: inner.normalizers.clone(),
                templates: inner.templates.clone(),
                generations: inner.generations.clone(),
            })
        }))
    }

    /// Resolves a bounded context that declares nothing itself through
    /// `templates`, to the template it was created from.
    #[doc(hidden)]
    pub fn with_templates(self, templates: crate::template_cache::TemplateCache) -> Self {
        Self(self.0.map(|inner| {
            Arc::new(Inner {
                keys: inner.keys.clone(),
                normalizers: inner.normalizers.clone(),
                templates: Some(templates),
                generations: inner.generations.clone(),
            })
        }))
    }

    fn normalizer(&self, bounded_context: &str, tag_key: &str) -> Option<(&Inner, Normalizer)> {
        let inner = self.0.as_deref()?;
        let lookup = |bc: &str| {
            inner
                .normalizers
                .get(&(bc.to_string(), tag_key.to_string()))
                .copied()
        };
        let normalize = lookup(bounded_context).or_else(|| {
            let template = inner
                .templates
                .as_ref()?
                .effective_bounded_context(bounded_context);
            (template != bounded_context)
                .then(|| lookup(&template))
                .flatten()
        })?;
        Some((inner, normalize))
    }

    /// Whether this process declares the constraint, itself or through
    /// the bounded context's template.
    pub fn declares(&self, bounded_context: &str, tag_key: &str) -> bool {
        self.normalizer(bounded_context, tag_key).is_some()
    }

    /// Whether this process declares any constraint of `bounded_context`,
    /// itself or through its template.
    pub fn declares_any(&self, bounded_context: &str) -> bool {
        let Some(inner) = self.0.as_deref() else {
            return false;
        };
        let template = inner
            .templates
            .as_ref()
            .map(|templates| templates.effective_bounded_context(bounded_context));
        inner
            .normalizers
            .keys()
            .any(|(bc, _)| bc == bounded_context || template.as_deref() == Some(bc.as_str()))
    }

    /// The newest generation this process can hash: one less than its
    /// number of root keys. What a new constraint is stored with.
    pub fn current_generation(&self) -> u32 {
        self.0
            .as_deref()
            .map_or(0, |inner| inner.keys.len().saturating_sub(1) as u32)
    }

    /// The constraint's generation as this process last saw it stored;
    /// [`current_generation`](Self::current_generation) for one it hasn't
    /// seen, which the check under the write lock corrects.
    pub fn generation(&self, bounded_context: &str, tag_key: &str) -> u32 {
        let Some(inner) = self.0.as_deref() else {
            return 0;
        };
        inner
            .generations
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&(bounded_context.to_string(), tag_key.to_string()))
            .copied()
            .unwrap_or_else(|| self.current_generation())
    }

    /// Records the constraint's stored generation; whether it differed
    /// from what this process hashed with.
    pub fn observe_generation(
        &self,
        bounded_context: &str,
        tag_key: &str,
        generation: u32,
    ) -> bool {
        let Some(inner) = self.0.as_deref() else {
            return false;
        };
        let changed = self.generation(bounded_context, tag_key) != generation;
        inner
            .generations
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .insert(
                (bounded_context.to_string(), tag_key.to_string()),
                generation,
            );
        changed
    }

    /// The tag value `value` becomes under the constraint: the hex
    /// HMAC-SHA256 of its normalized form, keyed for `bounded_context`,
    /// wrapped up to the constraint's generation.
    pub fn blind(
        &self,
        bounded_context: &str,
        tag_key: &str,
        value: &str,
    ) -> Result<String, Error> {
        let (inner, normalize) =
            self.normalizer(bounded_context, tag_key)
                .ok_or_else(|| Error::NotDeclared {
                    bounded_context: bounded_context.to_string(),
                    tag_key: tag_key.to_string(),
                })?;
        let generation = self.generation(bounded_context, tag_key);
        let first = inner.key(0, bounded_context, tag_key)?;
        let key = first.constraint_key(bounded_context, tag_key);
        let mut tag = hex(hmac::sign(&key, normalize(value).as_bytes()).as_ref());
        for g in 1..=generation {
            tag = inner
                .key(g, bounded_context, tag_key)?
                .wrap(bounded_context, tag_key, &tag);
        }
        Ok(tag)
    }

    /// A tag of generation `generation - 1` as generation `generation`
    /// stores it - what a rotation rewrites each stored tag with
    /// (docs/architecture.md §209).
    pub fn wrap(
        &self,
        bounded_context: &str,
        tag_key: &str,
        generation: u32,
        tag: &str,
    ) -> Result<String, Error> {
        let inner = self.0.as_deref().ok_or_else(|| Error::NotDeclared {
            bounded_context: bounded_context.to_string(),
            tag_key: tag_key.to_string(),
        })?;
        Ok(inner
            .key(generation, bounded_context, tag_key)?
            .wrap(bounded_context, tag_key, tag))
    }

    /// The constraint's key fingerprint at `generation` (docs/architecture.md
    /// §208, §209), or `None` when this process doesn't declare it or
    /// lacks a key of that generation.
    pub fn fingerprint(
        &self,
        bounded_context: &str,
        tag_key: &str,
        generation: u32,
    ) -> Option<String> {
        let (inner, _) = self.normalizer(bounded_context, tag_key)?;
        chain_fingerprint(inner, bounded_context, tag_key, generation)
    }

    /// The key fingerprint `tenant` gets for its template's constraint at
    /// `generation`, or `None` when this process can't compute it - for
    /// creating a tenant, which the template cache may not know yet.
    pub fn tenant_fingerprint(
        &self,
        template: &str,
        tenant: &str,
        tag_key: &str,
        generation: u32,
    ) -> Option<String> {
        let (inner, _) = self.normalizer(template, tag_key)?;
        chain_fingerprint(inner, tenant, tag_key, generation)
    }
}

/// The fingerprint of the key chain up to `generation`: the first key's,
/// then each later key's fingerprint over the previous one. Equal for two
/// chains exactly when every key up to `generation` is.
fn chain_fingerprint(
    inner: &Inner,
    bounded_context: &str,
    tag_key: &str,
    generation: u32,
) -> Option<String> {
    let mut fingerprint = inner
        .keys
        .first()?
        .fingerprint(bounded_context, tag_key, "");
    for g in 1..=generation {
        fingerprint =
            inner
                .keys
                .get(g as usize)?
                .fingerprint(bounded_context, tag_key, &fingerprint);
    }
    Some(fingerprint)
}

fn hex(bytes: &[u8]) -> String {
    const DIGITS: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(DIGITS[(byte >> 4) as usize] as char);
        out.push(DIGITS[(byte & 0x0f) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn index(root: u8) -> BlindIndex {
        BlindIndex::new(
            BlindIndexKey::from_bytes([root; KEY_LEN]),
            [
                ("crm", "email"),
                ("crm", "backup_email"),
                ("billing", "email"),
            ]
            .map(|(bc, key)| {
                (
                    bc.to_string(),
                    key.to_string(),
                    normalize::email as Normalizer,
                )
            }),
        )
    }

    #[test]
    fn the_same_normalized_value_blinds_the_same() {
        let index = index(1);
        let a = index.blind("crm", "email", "Alice@Example.com ").unwrap();
        let b = index.blind("crm", "email", "alice@example.com").unwrap();
        assert_eq!(a, b);
        assert_eq!(a.len(), 64);
        assert!(!a.contains("alice"));
    }

    #[test]
    fn each_constraint_and_root_key_blinds_differently() {
        let blind = index(1);
        let crm = blind.blind("crm", "email", "alice@example.com").unwrap();
        let backup = blind
            .blind("crm", "backup_email", "alice@example.com")
            .unwrap();
        let billing = blind
            .blind("billing", "email", "alice@example.com")
            .unwrap();
        let other_root = index(2).blind("crm", "email", "alice@example.com").unwrap();
        assert_ne!(crm, backup);
        assert_ne!(crm, billing);
        assert_ne!(crm, other_root);
    }

    #[test]
    fn an_undeclared_constraint_is_refused() {
        let err = index(1).blind("crm", "phone", "123").unwrap_err();
        assert!(matches!(err, Error::NotDeclared { .. }));
        let err = BlindIndex::default()
            .blind("crm", "email", "a")
            .unwrap_err();
        assert!(matches!(err, Error::NotDeclared { .. }));
    }

    #[test]
    fn a_fingerprint_tells_root_keys_apart_and_is_no_tag() {
        let one = index(1);
        let fingerprint = one.fingerprint("crm", "email", 0).unwrap();
        assert_eq!(fingerprint.len(), 64);
        assert_eq!(
            Some(fingerprint.clone()),
            index(1).fingerprint("crm", "email", 0)
        );
        assert_ne!(
            Some(fingerprint.clone()),
            index(2).fingerprint("crm", "email", 0)
        );
        assert_ne!(
            Some(fingerprint.clone()),
            one.fingerprint("crm", "backup_email", 0)
        );
        assert_ne!(
            Some(fingerprint.clone()),
            one.fingerprint("billing", "email", 0)
        );
        assert_ne!(fingerprint, one.blind("crm", "email", "").unwrap());
        assert_eq!(one.fingerprint("crm", "phone", 0), None);
        assert_eq!(
            one.tenant_fingerprint("crm", "acme", "email", 0),
            BlindIndex::new(
                BlindIndexKey::from_bytes([1; KEY_LEN]),
                [(
                    "acme".to_string(),
                    "email".to_string(),
                    normalize::email as Normalizer
                )],
            )
            .fingerprint("acme", "email", 0)
        );
        assert_eq!(one.tenant_fingerprint("acme", "acme", "email", 0), None);
        assert_eq!(BlindIndex::default().fingerprint("crm", "email", 0), None);
    }

    #[test]
    fn a_rotated_tag_wraps_the_previous_generation() {
        let old = index(1);
        let rotated = index(2).with_previous_keys([BlindIndexKey::from_bytes([1; KEY_LEN])]);
        assert_eq!(rotated.current_generation(), 1);

        // Before the rotation is stored, generation 0: the old key's hash.
        let before = old.blind("crm", "email", "Alice@Example.com").unwrap();
        rotated.observe_generation("crm", "email", 0);
        assert_eq!(
            rotated.blind("crm", "email", "alice@example.com").unwrap(),
            before
        );

        // After: the old tag wrapped, which is what a rotation rewrites to,
        // and neither key alone gives it.
        let wrapped = rotated.wrap("crm", "email", 1, &before).unwrap();
        assert!(rotated.observe_generation("crm", "email", 1));
        assert!(!rotated.observe_generation("crm", "email", 1));
        let after = rotated.blind("crm", "email", " alice@example.com").unwrap();
        assert_eq!(after, wrapped);
        assert_ne!(after, before);
        assert_ne!(
            after,
            index(2).blind("crm", "email", "alice@example.com").unwrap()
        );

        // The chain's fingerprint at generation 0 is the old key's.
        assert_eq!(
            rotated.fingerprint("crm", "email", 0),
            old.fingerprint("crm", "email", 0)
        );
        assert_ne!(
            rotated.fingerprint("crm", "email", 1),
            rotated.fingerprint("crm", "email", 0)
        );
        assert_ne!(
            rotated.fingerprint("crm", "email", 1),
            index(2).fingerprint("crm", "email", 0)
        );
        assert_eq!(rotated.fingerprint("crm", "email", 2), None);
    }

    #[test]
    fn a_generation_past_the_configured_keys_is_refused() {
        let index = index(1);
        index.observe_generation("crm", "email", 1);
        let err = index.blind("crm", "email", "a").unwrap_err();
        assert!(matches!(err, Error::KeyNotConfigured { generation: 1, .. }));
        assert!(index.declares_any("crm"));
        assert!(!index.declares_any("sales"));
    }

    #[test]
    fn compact_uppercase_drops_every_space() {
        assert_eq!(
            normalize::compact_uppercase(" nl91 abna 0417 1643 00 "),
            "NL91ABNA0417164300"
        );
    }
}
