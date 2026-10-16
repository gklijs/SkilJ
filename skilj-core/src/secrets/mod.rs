//! Secrets: rotatable, versioned credentials that an explicitly granted
//! adapter may fetch and that normal queries never reveal.
//!
//! A secret is *not* personal data. `sensitive_fields` (docs/architecture.md
//! §SubjectErasure) protects PII keyed by the person the payload names, so a
//! GDPR erasure can crypto-shred it. A secret is keyed by *the secret's own
//! version*, ends when it is **rotated** (which shreds the previous
//! version's key) or explicitly **revoked**, and is never returned by a
//! normal read at all - only an explicit `fetchSecret`, by the one adapter
//! granted it, ever decrypts one.
//!
//! What this module reuses from the personal-data path, deliberately, rather
//! than inventing a second crypto scheme: the same envelope encryption
//! (`crate::encryption` - a per-version `DataKey` wrapped under the
//! process-wide [`crate::encryption::EncryptionMasterKey`]) and the same
//! irreversible crypto-shredding (`db::destroy_encryption_key` nulls the
//! wrapped key, so even someone holding the master key can no longer recover
//! it). A secret version is provisioned under the `"secret"` subject
//! namespace, `subject_value = "{name}#{version}"` - the existing
//! `encryption_keys` table, `get_or_create_encryption_key` and
//! `destroy_encryption_key` are already fully generic over that namespace,
//! so nothing about the key lifecycle had to change to serve secrets.
//!
//! The one type that must never be printable is [`SecretValue`]: unlike
//! [`crate::encryption::DataKey`], a fetched secret is plaintext application
//! data on its way back to exactly one caller, so the risk isn't that it
//! reaches disk (it never does - only ciphertext is stored), it's that an
//! accidental `{:?}`, a `Display`, or a `serde_json::to_string` folds it into
//! a rejection reason, a tracing span, a command payload, or a response
//! nobody intended. `SecretValue` therefore implements **no `Debug`, no
//! `Display`, and neither `Serialize` nor `Deserialize`**: a struct that
//! embeds one cannot derive `Debug`, `format!` cannot render it, and it
//! cannot be embedded in a serde payload at all. The single deliberate
//! escape hatch is [`SecretValue::expose`], which every caller has to spell
//! out - so a secret reaching a payload, an error or a log is always a
//! visible, reviewable choice, never an accident.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::error::SkiljRejection;
use crate::event_store::BoundedContext;
use crate::shared::generate_token_id;

/// The `encryption_keys.subject_key` namespace every secret version's
/// `DataKey` lives under. Distinct from any `SensitiveField.subject_key` an
/// application declares, so a secret's key is never reachable by the
/// per-subject decrypt a `sensitive_fields` read performs.
pub const SUBJECT_KEY: &str = "secret";

/// The longest a secret name may be. Long enough for any real credential
/// name (`"github-oauth-refresh-token"`), short enough that a name is never
/// a smuggling channel for the value itself.
pub const MAX_NAME_LEN: usize = 256;

/// The `encryption_keys.subject_value` a secret version's key lives under -
/// `"{name}#{version}"`. The `#` cannot appear in a validated name
/// ([`validate_secret_name`]), so no two `(name, version)` pairs ever
/// collide on the same subject value.
pub fn subject_value(name: &str, version: i64) -> String {
    format!("{name}#{version}")
}

/// See `entity SecretVersion`'s `status` field - the same "one active,
/// append-only history" shape `EncryptionKey` already has, with an explicit
/// terminal `Rotated` distinct from `Revoked` so an audit can tell a
/// scheduled rotation from a deliberate revocation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretStatus {
    Active,
    Rotated,
    Revoked,
}

impl SecretStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SecretStatus::Active => "active",
            SecretStatus::Rotated => "rotated",
            SecretStatus::Revoked => "revoked",
        }
    }
}

/// One version of a named secret. Carries **metadata only** - never the
/// value. This is what every normal query returns, so it is safe to derive
/// `Debug`: there is nothing in it to leak. (`BoundedContext` carries no
/// `serde` impls of its own, so neither does this - the GraphQL layer builds
/// its object field by field, as it does for every other entity.)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Secret {
    pub bounded_context: BoundedContext,
    pub name: String,
    pub version: i64,
    pub status: SecretStatus,
    /// When this version stops being valid and the adapter should refresh
    /// it. `None` for a secret that does not expire. Common for a secret,
    /// unlike personal data - an OAuth refresh token has a real lifetime.
    pub expires_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    /// Set when a later `put_secret_value` superseded this version.
    pub rotated_at: Option<DateTime<Utc>>,
    /// Set when this version was explicitly revoked.
    pub revoked_at: Option<DateTime<Utc>>,
    /// A **summary**, not an event per read (docs/architecture.md §SecretReadAudit):
    /// the last time an adapter fetched this secret, and by which Role. A
    /// per-fetch event table would grow without bound for a credential an
    /// adapter refreshes on a timer; "who last read it, when" is what an
    /// audit actually needs.
    pub last_read_at: Option<DateTime<Utc>>,
    pub last_read_by: Option<String>,
}

/// Whether a version is one an adapter may still fetch. Rotated/revoked are
/// terminal.
impl Secret {
    pub fn is_active(&self) -> bool {
        self.status == SecretStatus::Active
    }
}

/// A grant letting one Role (the "adapter") fetch one named secret. Unlike
/// every other grant in the codebase this is **not** an access level on a
/// bounded context - it names a single secret, and it is the only thing that
/// lets a caller past [`require_secret_access`]. A fresh adapter with no
/// grant is refused, by construction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SecretGrant {
    pub id: String,
    pub bounded_context: BoundedContext,
    pub name: String,
    pub grantee_role_id: String,
    pub status: SecretGrantStatus,
    pub created_at: DateTime<Utc>,
    pub revoked_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SecretGrantStatus {
    Active,
    Revoked,
}

impl SecretGrantStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            SecretGrantStatus::Active => "active",
            SecretGrantStatus::Revoked => "revoked",
        }
    }
}

/// The plaintext value of a secret, handed to exactly one caller at exactly
/// one place: an explicit, granted fetch.
///
/// Deliberately implements no `Debug`, no `Display`, and neither `Serialize`
/// nor `Deserialize` (see this module's own doc comment). The only way out
/// is [`SecretValue::expose`].
#[derive(Clone)]
pub struct SecretValue(String);

impl SecretValue {
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The deliberate, greppable escape hatch - the one call that makes a
    /// secret visible to the rest of a program.
    pub fn expose(&self) -> &str {
        &self.0
    }

    /// Consumes the value, returning the owned plaintext. The other half of
    /// the escape hatch, for a caller that genuinely needs to move it.
    pub fn into_inner(self) -> String {
        self.0
    }
}

/// Library-level errors this module's own rules reject for.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error(
        "a secret name must be 1-{MAX_NAME_LEN} characters of ASCII letters, digits, '_', '.' or '-'"
    )]
    InvalidName,

    #[error("no secret named \"{name}\" has an active version")]
    NotActive { name: String },

    #[error("this caller is not granted read access to secret \"{name}\"")]
    AccessNotGranted { name: String },
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::InvalidName => "secret_name_invalid",
            Error::NotActive { .. } => "secret_not_active",
            Error::AccessNotGranted { .. } => "secret_access_not_granted",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

/// A secret name is a stable, human-chosen identifier, not the value. Bound
/// it to a safe alphabet so it is safe to log and to use as part of an
/// `EncryptionKey.subject_value`.
pub fn validate_secret_name(name: &str) -> Result<(), Error> {
    if name.is_empty()
        || name.len() > MAX_NAME_LEN
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'.' | b'-'))
    {
        return Err(Error::InvalidName);
    }
    Ok(())
}

/// The pure half of superseding a version: an active version becomes
/// `Rotated`. Its `DataKey` is destroyed by the caller
/// (`db::put_secret_value`), which is what actually shreds it - this only
/// computes the status transition.
pub fn rotate_secret(current: &Secret, now: DateTime<Utc>) -> Result<Secret, Error> {
    if !current.is_active() {
        return Err(Error::NotActive {
            name: current.name.clone(),
        });
    }
    let mut rotated = current.clone();
    rotated.status = SecretStatus::Rotated;
    rotated.rotated_at = Some(now);
    Ok(rotated)
}

/// The pure half of revoking a version: an active version becomes
/// `Revoked`. Like [`rotate_secret`], the crypto-shredding is the caller's.
pub fn revoke_secret(current: &Secret, now: DateTime<Utc>) -> Result<Secret, Error> {
    if !current.is_active() {
        return Err(Error::NotActive {
            name: current.name.clone(),
        });
    }
    let mut revoked = current.clone();
    revoked.status = SecretStatus::Revoked;
    revoked.revoked_at = Some(now);
    Ok(revoked)
}

/// Mints an active grant for one Role on one secret. `grantee_role_id` is a
/// `Role.id` (the same identity `RoleAccessMapping`/`PrivateFieldGrant` use),
/// so an adapter is whatever Role the integration authenticates as.
pub fn grant_secret_access(
    bounded_context: &BoundedContext,
    name: &str,
    grantee_role_id: &str,
    now: DateTime<Utc>,
) -> Result<SecretGrant, Error> {
    validate_secret_name(name)?;
    if grantee_role_id.is_empty() {
        return Err(Error::InvalidName);
    }
    Ok(SecretGrant {
        id: generate_token_id(),
        bounded_context: bounded_context.clone(),
        name: name.to_string(),
        grantee_role_id: grantee_role_id.to_string(),
        status: SecretGrantStatus::Active,
        created_at: now,
        revoked_at: None,
    })
}

/// The pure half of ending a grant - the caller persists the transition.
pub fn revoke_secret_access(grant: &SecretGrant, now: DateTime<Utc>) -> SecretGrant {
    let mut revoked = grant.clone();
    revoked.status = SecretGrantStatus::Revoked;
    revoked.revoked_at = Some(now);
    revoked
}

/// Whether `caller_role_id` may fetch secret `name`, given that caller's own
/// active grants. Only an active grant naming this exact secret and this
/// exact Role counts - there is no admin override, deliberately: an admin
/// manages secrets and grants, but reading one is a separate, explicit
/// entitlement.
pub fn secret_is_readable_by(name: &str, caller_role_id: &str, grants: &[SecretGrant]) -> bool {
    grants.iter().any(|g| {
        g.status == SecretGrantStatus::Active
            && g.name == name
            && g.grantee_role_id == caller_role_id
    })
}

/// [`secret_is_readable_by`] as a [`Result`], the gate
/// `db::fetch_secret_value` applies before it ever looks up a secret - so an
/// ungranted caller learns nothing about whether the secret exists.
pub fn require_secret_access(
    name: &str,
    caller_role_id: &str,
    grants: &[SecretGrant],
) -> Result<(), Error> {
    if secret_is_readable_by(name, caller_role_id, grants) {
        Ok(())
    } else {
        Err(Error::AccessNotGranted {
            name: name.to_string(),
        })
    }
}

/// Whether an active version is due to be refreshed - the predicate a
/// scheduler or a reusing deadline (§46) evaluates to raise "refresh this
/// before it expires". A version with no `expires_at` never needs one.
pub fn secret_needs_refresh(secret: &Secret, now: DateTime<Utc>) -> bool {
    secret.is_active() && secret.expires_at.is_some_and(|expires| expires <= now)
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use crate::bootstrap::ContextCreator;
    use crate::event_store::BoundedContextStatus;

    fn bc() -> BoundedContext {
        BoundedContext {
            name: "banking".into(),
            status: BoundedContextStatus::Active,
            created_at: Utc::now(),
            created_by: ContextCreator::SystemCreator,
            template: None,
        }
    }

    fn active_secret() -> Secret {
        Secret {
            bounded_context: bc(),
            name: "github-oauth-refresh-token".into(),
            version: 1,
            status: SecretStatus::Active,
            expires_at: None,
            created_at: Utc::now(),
            rotated_at: None,
            revoked_at: None,
            last_read_at: None,
            last_read_by: None,
        }
    }

    #[test]
    fn name_validation_accepts_reasonable_names_and_rejects_the_rest() {
        assert!(validate_secret_name("github-oauth-refresh-token").is_ok());
        assert!(validate_secret_name("a.b_c-1").is_ok());
        assert!(validate_secret_name("").is_err());
        assert!(validate_secret_name("has#hash").is_err()); // the subject separator
        assert!(validate_secret_name("has space").is_err());
        assert!(validate_secret_name(&"x".repeat(MAX_NAME_LEN + 1)).is_err());
    }

    #[test]
    fn subject_value_cannot_collide_because_names_cannot_contain_hash() {
        assert_eq!(subject_value("oauth", 2), "oauth#2");
        assert!(validate_secret_name("oauth#2").is_err());
    }

    #[test]
    fn rotate_only_accepts_an_active_version() {
        let now = Utc::now();
        let rotated = rotate_secret(&active_secret(), now).unwrap();
        assert_eq!(rotated.status, SecretStatus::Rotated);
        assert_eq!(rotated.rotated_at, Some(now));
        assert!(rotate_secret(&rotated, now).is_err());
    }

    #[test]
    fn revoke_only_accepts_an_active_version() {
        let now = Utc::now();
        let revoked = revoke_secret(&active_secret(), now).unwrap();
        assert_eq!(revoked.status, SecretStatus::Revoked);
        assert_eq!(revoked.revoked_at, Some(now));
        assert!(revoke_secret(&revoked, now).is_err());
    }

    #[test]
    fn a_fresh_adapter_with_no_grant_is_refused() {
        let grants = vec![grant_secret_access(&bc(), "oauth", "role-1", Utc::now()).unwrap()];
        assert!(!secret_is_readable_by("oauth", "role-2", &grants));
        assert_eq!(
            require_secret_access("oauth", "role-2", &grants)
                .unwrap_err()
                .code(),
            "secret_access_not_granted"
        );
        // and a grant on a *different* secret does not open this one
        assert!(!secret_is_readable_by("payments", "role-1", &grants));
    }

    #[test]
    fn a_revoked_grant_stops_being_readable() {
        let grant = grant_secret_access(&bc(), "oauth", "role-1", Utc::now()).unwrap();
        assert!(secret_is_readable_by(
            "oauth",
            "role-1",
            std::slice::from_ref(&grant)
        ));
        let revoked = revoke_secret_access(&grant, Utc::now());
        assert!(!secret_is_readable_by("oauth", "role-1", &[revoked]));
    }

    #[test]
    fn refresh_is_due_only_when_expired() {
        let now = Utc::now();
        let mut secret = active_secret();
        assert!(!secret_needs_refresh(&secret, now));
        secret.expires_at = Some(now + chrono::Duration::seconds(1));
        assert!(!secret_needs_refresh(&secret, now));
        secret.expires_at = Some(now - chrono::Duration::seconds(1));
        assert!(secret_needs_refresh(&secret, now));
    }

    #[test]
    fn secret_value_exposes_only_through_an_explicit_call() {
        let value = SecretValue::new("plaintext-credential");
        assert_eq!(value.expose(), "plaintext-credential");
        assert_eq!(value.into_inner(), "plaintext-credential");
    }
}
