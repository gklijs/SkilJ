//! `Role`, `RoleAccessMapping`, `AccessToken` (and its four variants);
//! `CreateRole`, `RevokeRole`, `GrantRoleAccessMapping`,
//! `RevokeRoleAccessMapping`, `TokenRevocation`; actor resolution
//! (`ReadAccess`/`WriteAccess`/`AdminAccess`/`SensitiveDataAccess`/
//! `Superadmin`) and the JWT-to-Role identity resolution entry point. See
//! docs/architecture.md §3.2 and §6 (JWKS/JWT verification lives here,
//! behind the `jsonwebtoken` crate).

use crate::error::SkiljRejection;
use crate::event_store::BoundedContext;

// Real JWKS fetching and JWT signature verification (step 1 of the
// identity resolution note above `entity Role`) now live below
// (`IdpConfig`/`SigningAlgorithm`/`JwksCache`/`verify_and_extract_subject`)
// - genuine I/O, so unlike every pure function in this module they
// aren't tested by construction alone; see their own doc comments and
// docs/architecture.md §6 for the concrete design the spec's own "black
// box" text left this document to fill in. The four actors
// (`ReadAccess`/`WriteAccess`/`AdminAccess`/`SensitiveDataAccess`)
// and `Superadmin` have no resolution function of their own to add
// beyond that: each is a predicate over an already-resolved
// `RoleAccessMapping`/`Role` (see `actor ReadAccess` etc. in the spec),
// and every rule elsewhere in this crate that faces one already inlines
// exactly that predicate as its own `requires` checks - there is no
// separate obligation for the actor declarations themselves to
// propagate, only each surface's own still-deferred `surface-actor`
// obligation (the GraphQL-scaffolding gap every test file in this crate
// already defers for the same reason).
// `EventReadToken`/`ExternalEventToken`/`DirectCreationToken`/
// `CommandToken`/`Role`/`RoleAccessMapping` below are first, partial cuts
// - see their own doc comments - added while propagating tests for the
// EventFetch/ExternalEventIngestion/DirectEventCreation/CommandTrigger/
// AccessManagement surfaces (docs/architecture.md §9's pilot and its
// follow-ups). The base `AccessToken` shape (`secret`/`created_at`/
// `revoked_at`, plus `id` on `EventReadToken`) and the `AccessToken` sum
// type itself, `create_external_event_token`/`create_direct_creation_token`/
// `create_command_token`/`create_event_read_token`, and `revoke_token` were
// added next, propagating EventTypeAdminOperations/
// CommandTypeAdminOperations/TokenRevocation - the token lifecycle's
// minting and revocation halves, completing `AccessToken`'s entity shape.
// `resolve_role_by_external_subject` was added last - the one pure
// fragment of JWT-to-Role identity resolution (see its own doc comment
// for why the JWT/JWKS verification itself stays unmodelled).

/// See `entity Role`'s `status` field/transition graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoleStatus {
    Active,
    Revoked,
}

/// See `entity Role`. `access_mappings` (a relationship projection, not a
/// stored field - the same treatment `EventType`'s `*_tokens` get) is
/// omitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Role {
    pub id: String,
    pub external_subject: String,
    pub name: String,
    pub superadmin: bool,
    pub status: RoleStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// See `enum AccessLevel`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessLevel {
    Read,
    Write,
    Admin,
}

/// See `entity RoleAccessMapping`. Reuses `RoleStatus` for its own
/// `status` - the spec declares both as the identical `active | revoked`
/// shape, and nothing here needs to tell "a revoked Role" apart from "a
/// revoked grant" by type alone (each already lives on its own struct).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoleAccessMapping {
    pub role: Role,
    pub bounded_context: BoundedContext,
    pub level: AccessLevel,
    pub can_read_sensitive: bool,
    /// Orthogonal to `level`, exactly as `can_read_sensitive` is: `None`
    /// (the default for every mapping today) means unrestricted within
    /// `bounded_context`, identical to this grant's own behaviour before
    /// this field existed. `Some(v)` restricts this grant to projection
    /// instances whose own derived "owner" value equals `v`, for a
    /// projection that declares an owner-tag dimension (see
    /// `plugin::Projection::OWNER_TAG_KEY`) - a projection with no such
    /// declaration is unaffected by this field regardless of its value.
    /// No validation on the value itself, the same unchecked
    /// pass-through `can_read_sensitive`'s boolean already gets. See
    /// `projections::query_projection`'s own enforcement and
    /// specs/skilj.allium's `owner_scope_satisfied`.
    pub scope: Option<String>,
    pub status: RoleStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
}

/// See `entity AccessToken`'s `status` field/transition graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TokenStatus {
    Active,
    Revoked,
}

/// See `variant EventReadToken` in the spec. Now carries the full
/// `entity AccessToken` base shape - `id` included, promoted from the
/// earlier EventFetch-only cut (nothing in EventFetch's own rules reads
/// it, but `RevokeToken` and its `AccessToken` sum type below need every
/// variant to carry the same base fields uniformly).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EventReadToken {
    pub id: String,
    pub secret: String,
    pub status: TokenStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub event_type: crate::event_store::EventType,
    /// Cross-tenant read fix (docs/architecture.md's own write-up of
    /// these passes) - the REST track's own counterpart to
    /// `RoleAccessMapping.scope`, set at minting time
    /// (`create_event_read_token`) by whichever admin issues this token,
    /// independent of that admin's own `access_mapping.scope`: an
    /// unscoped staff admin can mint a company-scoped token. Same
    /// null-is-unrestricted semantics, checked by
    /// `event_store::event_owner_scope_satisfied` in
    /// `fetch_events`/`consume_events`.
    pub scope: Option<String>,
}

/// See `variant ExternalEventToken`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ExternalEventToken {
    pub id: String,
    pub secret: String,
    pub status: TokenStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub event_type: crate::event_store::EventType,
}

/// See `variant DirectCreationToken`. Same shape as `ExternalEventToken`
/// above.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DirectCreationToken {
    pub id: String,
    pub secret: String,
    pub status: TokenStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub event_type: crate::event_store::EventType,
}

/// See `variant CommandToken`. Same shape as `ExternalEventToken`/
/// `DirectCreationToken` above, scoped to a `CommandType` instead of an
/// `EventType`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommandToken {
    pub id: String,
    pub secret: String,
    pub status: TokenStatus,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub revoked_at: Option<chrono::DateTime<chrono::Utc>>,
    pub command_type: crate::event_store::CommandType,
}

/// See `entity AccessToken`'s `purpose` field - a Rust sum type over the
/// four variants stands in for it directly, the same "the enum variant
/// tag is the purpose" treatment `Event.origin`/`EventOrigin` gets, so
/// there is no separate `purpose` field to carry: matching on this enum
/// already tells the four apart. Exists for `RevokeToken`, the one rule
/// that operates on "any `AccessToken`" polymorphically rather than on one
/// concrete variant.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AccessToken {
    ExternalEventToken(ExternalEventToken),
    DirectCreationToken(DirectCreationToken),
    CommandToken(CommandToken),
    EventReadToken(EventReadToken),
}

impl AccessToken {
    fn status(&self) -> TokenStatus {
        match self {
            AccessToken::ExternalEventToken(t) => t.status,
            AccessToken::DirectCreationToken(t) => t.status,
            AccessToken::CommandToken(t) => t.status,
            AccessToken::EventReadToken(t) => t.status,
        }
    }

    /// `token_scope` from `rule RevokeToken`: `token.command_type.bounded_context`
    /// for a `CommandToken`, `token.event_type.bounded_context` for the
    /// other three.
    fn scope(&self) -> &BoundedContext {
        match self {
            AccessToken::ExternalEventToken(t) => &t.event_type.bounded_context,
            AccessToken::DirectCreationToken(t) => &t.event_type.bounded_context,
            AccessToken::CommandToken(t) => &t.command_type.bounded_context,
            AccessToken::EventReadToken(t) => &t.event_type.bounded_context,
        }
    }
}

/// Library-level errors this module's own rules reject for - an
/// enumerable, closed set, unlike a bounded context's own
/// `CommandDecision::Rejected` (see crate::error and
/// docs/architecture.md §4.1).
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    #[error("this Role is not active")]
    RoleNotActive,

    #[error("this RoleAccessMapping is not active")]
    GrantNotActive,

    #[error("this grant's access level is too low for this action")]
    InsufficientAccessLevel,

    #[error("this RoleAccessMapping is scoped to a different bounded context")]
    GrantBoundedContextMismatch,

    /// Cross-tenant read fix (docs/architecture.md's own write-up of
    /// these passes): a grant/token names a `scope`, the record's own
    /// type declares an owner dimension, and the record's own derived
    /// owner either differs from `scope` or is not yet established -
    /// fail-closed, the same "affirmatively provable, not merely
    /// un-contradicted" framing every caller of this variant shares. A
    /// single-record surface (`projections::query_projection`,
    /// `event_store::inspect_event`) rejects outright with this;
    /// multi-record ones (`event_store::query_events`/`count_events`/
    /// `deliver_to_subscriptions`/`fetch_events`/`consume_events`) never
    /// construct it at all - a record that fails this check is filtered
    /// out of the result, not a reason to fail the whole call.
    #[error("this grant is scoped to a value that does not match this record's own owner")]
    GrantScopeMismatch,

    #[error("this AccessToken is not active")]
    TokenNotActive,

    #[error("only an active superadmin Role may perform this action")]
    NotSuperadmin,

    #[error("no active Role's external_subject matches this JWT's subject claim")]
    UnrecognisedSubject,

    #[error("this external_subject is already claimed by another active Role")]
    ExternalSubjectAlreadyClaimed,

    #[error("this Role already holds an active RoleAccessMapping for this bounded context")]
    DuplicateActiveMapping,

    #[error("the presented JWT is malformed: {0}")]
    MalformedJwt(String),

    #[error("the presented JWT has no key id (kid) header, so no JWKS entry can verify it")]
    JwtMissingKeyId,

    #[error(
        "the presented JWT's key id (kid) doesn't match any key in the trusted IdP's JWKS, \
         even after a refetch"
    )]
    UnknownSigningKey,

    #[error("the presented JWT failed verification: {0}")]
    JwtVerificationFailed(String),

    #[error("the presented JWT's verified claims have no {0} claim to trust as the subject")]
    MissingSubjectClaim(String),

    #[error("fetching the trusted IdP's JWKS failed: {0}")]
    JwksFetchFailed(String),
}

impl SkiljRejection for Error {
    fn code(&self) -> &str {
        match self {
            Error::RoleNotActive => "role_not_active",
            Error::GrantNotActive => "grant_not_active",
            Error::InsufficientAccessLevel => "insufficient_access_level",
            Error::GrantBoundedContextMismatch => "grant_bounded_context_mismatch",
            Error::GrantScopeMismatch => "grant_scope_mismatch",
            Error::TokenNotActive => "token_not_active",
            Error::NotSuperadmin => "not_superadmin",
            Error::UnrecognisedSubject => "unrecognised_subject",
            Error::ExternalSubjectAlreadyClaimed => "external_subject_already_claimed",
            Error::DuplicateActiveMapping => "duplicate_active_mapping",
            Error::MalformedJwt(_) => "malformed_jwt",
            Error::JwtMissingKeyId => "jwt_missing_key_id",
            Error::UnknownSigningKey => "unknown_signing_key",
            Error::JwtVerificationFailed(_) => "jwt_verification_failed",
            Error::MissingSubjectClaim(_) => "missing_subject_claim",
            Error::JwksFetchFailed(_) => "jwks_fetch_failed",
        }
    }

    fn message(&self) -> String {
        self.to_string()
    }
}

/// `pub(crate)`: reused by `bootstrap::add_bounded_context`/
/// `bootstrap::list_bounded_contexts` (both `facing caller: Superadmin`,
/// the same actor every rule in this module already checks this way).
pub(crate) fn require_active_superadmin(caller: &Role) -> crate::error::Result<()> {
    if caller.status != RoleStatus::Active {
        return Err(Error::RoleNotActive.into());
    }
    if !caller.superadmin {
        return Err(Error::NotSuperadmin.into());
    }
    Ok(())
}

/// A deployment's trust configuration for exactly one external IdP - see
/// the identity resolution note above `entity Role` and docs/
/// architecture.md §6. Runtime/library configuration, not domain state,
/// the same register the spec's own note gives the in-memory event
/// cache's startup warm-up count: nothing in the domain reads or writes
/// these values, they only parameterise `verify_and_extract_subject`
/// below.
#[derive(Debug, Clone)]
pub struct IdpConfig {
    pub jwks_endpoint: reqwest::Url,
    pub issuer: String,
    pub signing_algorithm: SigningAlgorithm,
    /// Defaults to `"sub"`, the standard JWT subject claim - the one
    /// degree of freedom the spec's own note leaves step 2 ("SkilJ
    /// trusts the subject claim of a verified JWT... configurable" per
    /// the identity resolution note's closing paragraph).
    pub subject_claim: String,
}

impl IdpConfig {
    pub fn new(
        jwks_endpoint: reqwest::Url,
        issuer: impl Into<String>,
        signing_algorithm: SigningAlgorithm,
    ) -> Self {
        Self {
            jwks_endpoint,
            issuer: issuer.into(),
            signing_algorithm,
            subject_claim: "sub".to_string(),
        }
    }

    pub fn with_subject_claim(mut self, claim: impl Into<String>) -> Self {
        self.subject_claim = claim.into();
        self
    }
}

/// The signing algorithms `IdpConfig` accepts - asymmetric only.
/// `jsonwebtoken::Algorithm` also offers HMAC variants, deliberately not
/// wrapped here: an HMAC signature is verified with the same secret it
/// was signed with, which would mean this process and the external IdP
/// sharing a symmetric secret - a trust shape the spec's own "SkilJ does
/// not authenticate GraphQL callers itself; it delegates to a trusted
/// external identity provider" framing doesn't fit. SkilJ only ever
/// verifies a JWT an external IdP already signed with its own private
/// key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SigningAlgorithm {
    Rs256,
    Rs384,
    Rs512,
    Es256,
    Es384,
}

impl SigningAlgorithm {
    fn to_jsonwebtoken(self) -> jsonwebtoken::Algorithm {
        match self {
            SigningAlgorithm::Rs256 => jsonwebtoken::Algorithm::RS256,
            SigningAlgorithm::Rs384 => jsonwebtoken::Algorithm::RS384,
            SigningAlgorithm::Rs512 => jsonwebtoken::Algorithm::RS512,
            SigningAlgorithm::Es256 => jsonwebtoken::Algorithm::ES256,
            SigningAlgorithm::Es384 => jsonwebtoken::Algorithm::ES384,
        }
    }
}

/// Caches an IdP's JWKS (JSON Web Key Set), keyed by each key's own
/// `kid` - see docs/architecture.md §6 for why this is reactive-refresh
/// rather than a background-timer poll: a JWT whose `kid` isn't already
/// cached triggers exactly one refetch of the whole set before giving
/// up, rather than a task refreshing on a schedule nothing here needs to
/// manage the lifecycle of. `min_refetch_interval` (a few seconds', not
/// configurable - an internal safety margin, not a tuning knob) is what
/// keeps a caller presenting JWTs with garbage `kid` values from cheaply
/// forcing a refetch on every single request.
///
/// One `JwksCache` per `IdpConfig`, held for the process's lifetime
/// (`Skilj` holds one, built once in `.build()`) - never reconstructed
/// per request, so its cached keys and refetch timer persist exactly the
/// way this design depends on.
pub struct JwksCache {
    client: reqwest::Client,
    jwks_endpoint: reqwest::Url,
    min_refetch_interval: std::time::Duration,
    keys: tokio::sync::RwLock<std::collections::HashMap<String, jsonwebtoken::DecodingKey>>,
    last_refetch: tokio::sync::RwLock<Option<std::time::Instant>>,
}

impl JwksCache {
    pub fn new(jwks_endpoint: reqwest::Url) -> Self {
        Self {
            client: reqwest::Client::new(),
            jwks_endpoint,
            min_refetch_interval: std::time::Duration::from_secs(5),
            keys: tokio::sync::RwLock::new(std::collections::HashMap::new()),
            last_refetch: tokio::sync::RwLock::new(None),
        }
    }

    /// The cached `DecodingKey` for `kid`, refetching the whole JWKS
    /// document once first when it isn't already cached and the minimum
    /// refetch interval has elapsed since the last attempt (whether or
    /// not that attempt found the key being looked for now) - `None`
    /// either way it still can't be found, which
    /// `verify_and_extract_subject` below turns into
    /// `Error::UnknownSigningKey`.
    async fn key_for(&self, kid: &str) -> crate::error::Result<Option<jsonwebtoken::DecodingKey>> {
        if let Some(key) = self.keys.read().await.get(kid) {
            return Ok(Some(key.clone()));
        }

        {
            let mut last_refetch = self.last_refetch.write().await;
            let now = std::time::Instant::now();
            if last_refetch.is_some_and(|last| now.duration_since(last) < self.min_refetch_interval)
            {
                return Ok(None);
            }
            *last_refetch = Some(now);
        }

        self.refetch().await?;
        Ok(self.keys.read().await.get(kid).cloned())
    }

    #[tracing::instrument(skip_all, fields(jwks_endpoint = %self.jwks_endpoint))]
    async fn refetch(&self) -> crate::error::Result<()> {
        let jwks: jsonwebtoken::jwk::JwkSet = self
            .client
            .get(self.jwks_endpoint.clone())
            .send()
            .await
            .map_err(|e| Error::JwksFetchFailed(e.to_string()))?
            .json()
            .await
            .map_err(|e| Error::JwksFetchFailed(e.to_string()))?;

        let mut keys = self.keys.write().await;
        keys.clear();
        for jwk in &jwks.keys {
            let (Some(kid), Ok(decoding_key)) = (
                jwk.common.key_id.clone(),
                jsonwebtoken::DecodingKey::from_jwk(jwk),
            ) else {
                continue;
            };
            keys.insert(kid, decoding_key);
        }
        Ok(())
    }
}

/// Step 1 of the identity resolution note above `entity Role`: verifies
/// `jwt`'s signature against `config`'s trusted IdP (fetching/caching its
/// JWKS via `cache` as needed), then step 2, pulling out - and trusting -
/// only `config.subject_claim`. Everything else in the JWT's claims is
/// read by nobody: `jsonwebtoken::decode` verifies the signature,
/// `issuer` and algorithm, but claims are decoded into a generic JSON map
/// rather than a fixed struct, since `subject_claim` is configurable
/// rather than always `"sub"`.
///
/// Genuine I/O (JWKS fetch on a cache miss), so - unlike every function
/// above in this module - not a pure function `skilj-core`'s own test
/// suite exercises via plain unit tests; see this crate's `tests/`
/// directory for its own JWKS-serving test harness instead. Called by
/// `skilj-graphql`'s auth extractor, once per request that presents a
/// bearer JWT; its output is what `resolve_role_by_external_subject`
/// below takes as `verified_subject`.
pub async fn verify_and_extract_subject(
    jwt: &str,
    config: &IdpConfig,
    cache: &JwksCache,
) -> crate::error::Result<String> {
    let header =
        jsonwebtoken::decode_header(jwt).map_err(|e| Error::MalformedJwt(e.to_string()))?;
    let kid = header.kid.ok_or(Error::JwtMissingKeyId)?;
    let Some(decoding_key) = cache.key_for(&kid).await? else {
        return Err(Error::UnknownSigningKey.into());
    };

    let mut validation = jsonwebtoken::Validation::new(config.signing_algorithm.to_jsonwebtoken());
    validation.set_issuer(&[&config.issuer]);
    // `IdpConfig` has no audience field at all - deliberately, per this
    // function's own doc comment above ("nothing else from it" but the
    // subject claim). `jsonwebtoken::Validation::new`'s own default is
    // `validate_aud: true` with no configured value, which rejects any
    // token carrying an `aud` claim outright rather than skipping the
    // check - and every spec-compliant OIDC ID token carries one. Found
    // against a real external IdP (self-hosted Dex, skilj-helpdesk):
    // signature and issuer verified correctly, then every real token
    // rejected with InvalidAudience regardless of its actual audience
    // value. The local JWKS/JWT test fixtures elsewhere in this
    // workspace never carry an `aud` claim, so they never exercised
    // this path.
    validation.validate_aud = false;

    let token_data = jsonwebtoken::decode::<serde_json::Map<String, serde_json::Value>>(
        jwt,
        &decoding_key,
        &validation,
    )
    .map_err(|e| Error::JwtVerificationFailed(e.to_string()))?;

    token_data
        .claims
        .get(&config.subject_claim)
        .and_then(|v| v.as_str())
        .map(str::to_string)
        .ok_or_else(|| Error::MissingSubjectClaim(config.subject_claim.clone()).into())
}

/// See the GraphQL identity resolution note above `entity Role`, step 3:
/// "That claim value resolves to the one active Role whose
/// external_subject matches it". The one pure part of the JWT-to-Role
/// resolution pipeline - step 1, verifying the presented JWT's signature
/// against the trusted IdP's JWKS, is a black box this engine doesn't own
/// ("a black box here, the same register as `protect_sensitive_fields`
/// and `generate_token_secret`" per the spec's own text), so it stays a
/// caller-supplied `verified_subject: &str` rather than something this
/// function fetches or verifies itself - the caller has already done
/// that and is handing over the JWT's trusted subject claim, nothing
/// else from it (step 2: "SkilJ trusts the subject claim of a verified
/// JWT, and nothing else in it"). `existing_roles` is every `Role` this
/// engine currently knows of, the same full-snapshot treatment
/// `create_role`'s own `existing_roles` gets below.
/// `UniqueActiveExternalSubject` is what makes "the one" well defined -
/// at most one active `Role` can ever match.
pub fn resolve_role_by_external_subject<'a>(
    verified_subject: &str,
    existing_roles: &'a [Role],
) -> crate::error::Result<&'a Role> {
    existing_roles
        .iter()
        .find(|r| r.status == RoleStatus::Active && r.external_subject == verified_subject)
        .ok_or_else(|| Error::UnrecognisedSubject.into())
}

/// See `rule CreateRole`. `id` is the value the caller's own
/// `generate_token_id()` already produced - not this function's to
/// generate, the same caller-supplied-black-box-output treatment
/// `next_sequence` gets elsewhere in this codebase (`generate_token_id`
/// is real, in `crate::shared`, a plain UUIDv4). `existing_roles` is
/// every `Role` this engine currently knows of, for the
/// `UniqueActiveExternalSubject` check - a full-snapshot parameter, the
/// same shape `consistency_boundary_and_matching_events` takes `Event`s
/// in, since there's no single key to look this existential check up by.
pub fn create_role(
    caller: &Role,
    name: String,
    superadmin: bool,
    external_subject: String,
    existing_roles: &[Role],
    id: String,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<Role> {
    require_active_superadmin(caller)?;
    if existing_roles
        .iter()
        .any(|r| r.external_subject == external_subject && r.status == RoleStatus::Active)
    {
        return Err(Error::ExternalSubjectAlreadyClaimed.into());
    }

    Ok(Role {
        id,
        external_subject,
        name,
        superadmin,
        status: RoleStatus::Active,
        created_at: now,
        revoked_at: None,
    })
}

/// See `rule RevokeRole`. `active_mappings` is `role.access_mappings
/// where status = active` as already looked up by the caller - the same
/// "relationship projection resolved by the caller" treatment
/// `Role.access_mappings` gets everywhere else (see the doc comment on
/// `Role`). Returns the revoked `Role` and every mapping it just
/// cascaded the revocation to - together they *are*
/// `RevokedRoleImpliesMappingsRevoked`, established in the same
/// transaction rather than checked after the fact.
pub fn revoke_role(
    caller: &Role,
    role: &Role,
    active_mappings: &[RoleAccessMapping],
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<(Role, Vec<RoleAccessMapping>)> {
    require_active_superadmin(caller)?;
    if role.status != RoleStatus::Active {
        return Err(Error::RoleNotActive.into());
    }

    let revoked_role = Role {
        status: RoleStatus::Revoked,
        revoked_at: Some(now),
        ..role.clone()
    };
    let revoked_mappings = active_mappings
        .iter()
        .cloned()
        .map(|m| RoleAccessMapping {
            status: RoleStatus::Revoked,
            revoked_at: Some(now),
            ..m
        })
        .collect();

    Ok((revoked_role, revoked_mappings))
}

/// See `rule GrantRoleAccessMapping`. `existing_mappings` is every
/// `RoleAccessMapping` this engine currently knows of, for the `not
/// exists RoleAccessMapping{role, bounded_context, status: active}`
/// check - same full-snapshot treatment as `create_role`'s
/// `existing_roles`.
#[allow(clippy::too_many_arguments)]
pub fn grant_role_access_mapping(
    caller: &Role,
    role: &Role,
    bounded_context: &BoundedContext,
    level: AccessLevel,
    can_read_sensitive: bool,
    scope: Option<String>,
    existing_mappings: &[RoleAccessMapping],
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<RoleAccessMapping> {
    require_active_superadmin(caller)?;
    if role.status != RoleStatus::Active {
        return Err(Error::RoleNotActive.into());
    }
    if bounded_context.status != crate::event_store::BoundedContextStatus::Active {
        return Err(crate::event_store::Error::BoundedContextArchived.into());
    }
    if existing_mappings.iter().any(|m| {
        &m.role == role && &m.bounded_context == bounded_context && m.status == RoleStatus::Active
    }) {
        return Err(Error::DuplicateActiveMapping.into());
    }

    Ok(RoleAccessMapping {
        role: role.clone(),
        bounded_context: bounded_context.clone(),
        level,
        can_read_sensitive,
        scope,
        status: RoleStatus::Active,
        created_at: now,
        revoked_at: None,
    })
}

/// See `rule RevokeRoleAccessMapping`.
pub fn revoke_role_access_mapping(
    caller: &Role,
    access_mapping: &RoleAccessMapping,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<RoleAccessMapping> {
    require_active_superadmin(caller)?;
    if access_mapping.status != RoleStatus::Active {
        return Err(Error::GrantNotActive.into());
    }

    Ok(RoleAccessMapping {
        status: RoleStatus::Revoked,
        revoked_at: Some(now),
        ..access_mapping.clone()
    })
}

/// One now-inactive `RoleAccessMapping`, identified the same way a live
/// `EventSubscription` connection already scopes itself - `role_id` plus
/// `bounded_context` name, never the full entity. `RevocationBroadcaster`'s
/// own publish payload.
#[derive(Debug, Clone)]
pub struct RevokedMapping {
    pub role_id: String,
    pub bounded_context: String,
}

/// `EventSubscription`'s own second broadcast channel, alongside
/// `event_store::EventBroadcaster` - drift audit finding #4 (2026-08-20,
/// see project memory `skilj-drift-audit-2026-08-20`). Fixes
/// `RevocationClosesTheConnection`'s real gap: the per-delivered-event
/// re-check `resolvers::event_subscription` already did only ever ran
/// when an event happened to arrive, so a subscription in a bounded
/// context that had gone quiet stayed open indefinitely after its own
/// grant was revoked - exactly the scenario that guarantee's own spec
/// text exists to rule out ("the caller would wait on it indefinitely
/// believing itself current"). This is the push half of the fix: every
/// site that revokes a `RoleAccessMapping` - `revoke_role_and_mappings`'s
/// own cascade and `revoke_active_role_access_mapping` directly, both in
/// `skilj-graphql/src/resolvers/access_management.rs` - publishes here
/// right after the revocation is durably committed, and both subscription
/// resolvers `tokio::select!` against it alongside their own
/// `EventBroadcaster` receiver, closing the instant a notification names
/// their own `(role_id, bounded_context)` rather than waiting for the
/// next matching event. The identical "same choke point, one shared
/// process-wide instance, silently-ignored `SendError` when nobody's
/// listening" shape `EventBroadcaster` already establishes - see its own
/// doc comment, not repeated here.
///
/// Revocations are rare and low-volume compared to events, so unlike
/// `EventBroadcaster`'s own `capacity` this has no builder-configurable
/// knob - a fixed, generous capacity is enough headroom that a lagged
/// receiver is already an exceptional case, handled defensively (a direct
/// database re-check, not assumed-still-active) rather than tuned around.
#[derive(Clone)]
pub struct RevocationBroadcaster {
    sender: tokio::sync::broadcast::Sender<RevokedMapping>,
    instance_id: String,
}

impl RevocationBroadcaster {
    pub fn new() -> Self {
        let (sender, _receiver) = tokio::sync::broadcast::channel(256);
        Self {
            sender,
            instance_id: crate::shared::generate_token_id(),
        }
    }

    /// See `EventBroadcaster::instance_id`'s own doc comment - identical
    /// role, for `db::notify_revocation`/this instance's cross-instance
    /// dispatch loop instead of `db::notify_event_appended`/the
    /// `EventBroadcaster` one.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// A fresh, independent receiver - one call per live GraphQL
    /// subscription, the same as `EventBroadcaster::subscribe`.
    pub fn subscribe(&self) -> tokio::sync::broadcast::Receiver<RevokedMapping> {
        self.sender.subscribe()
    }

    /// Called once per revoked `RoleAccessMapping`, right after that
    /// revocation is durably committed - see this type's own doc comment
    /// for the two real call sites. A `SendError` (zero receivers
    /// currently subscribed) is the expected steady state, not a
    /// failure - identical reasoning to `EventBroadcaster::publish`.
    pub fn publish(&self, revoked: RevokedMapping) {
        let _ = self.sender.send(revoked);
    }
}

impl Default for RevocationBroadcaster {
    fn default() -> Self {
        Self::new()
    }
}

/// `access_mapping.status = active`/`access_mapping.level = admin` -
/// shared by every `Create*Token` rule below and by `revoke_token`, all
/// four of which require this exact pair (see
/// `EventTypeAdminOperations`/`CommandTypeAdminOperations`/
/// `TokenRevocation`'s own `facing access_mapping: AdminAccess`).
fn require_active_admin(access_mapping: &RoleAccessMapping) -> crate::error::Result<()> {
    if access_mapping.status != RoleStatus::Active {
        return Err(Error::GrantNotActive.into());
    }
    if access_mapping.level != AccessLevel::Admin {
        return Err(Error::InsufficientAccessLevel.into());
    }
    Ok(())
}

/// See `rule CreateExternalEventToken`. `id`/`secret` are the caller's own
/// `generate_token_id()`/`generate_token_secret()` output - not this
/// function's to generate, the same treatment `create_role`'s `id` gets
/// (see its own doc comment; both are real, in `crate::shared`).
pub fn create_external_event_token(
    access_mapping: &RoleAccessMapping,
    event_type: &crate::event_store::EventType,
    id: String,
    secret: String,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<ExternalEventToken> {
    require_active_admin(access_mapping)?;
    if access_mapping.bounded_context != event_type.bounded_context {
        return Err(Error::GrantBoundedContextMismatch.into());
    }

    Ok(ExternalEventToken {
        id,
        secret,
        status: TokenStatus::Active,
        created_at: now,
        revoked_at: None,
        event_type: event_type.clone(),
    })
}

/// See `rule CreateDirectCreationToken`. Same shape and reasoning as
/// `create_external_event_token` above.
pub fn create_direct_creation_token(
    access_mapping: &RoleAccessMapping,
    event_type: &crate::event_store::EventType,
    id: String,
    secret: String,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<DirectCreationToken> {
    require_active_admin(access_mapping)?;
    if access_mapping.bounded_context != event_type.bounded_context {
        return Err(Error::GrantBoundedContextMismatch.into());
    }

    Ok(DirectCreationToken {
        id,
        secret,
        status: TokenStatus::Active,
        created_at: now,
        revoked_at: None,
        event_type: event_type.clone(),
    })
}

/// See `rule CreateEventReadToken`. Same shape and reasoning as
/// `create_external_event_token` above, plus `scope` -
/// `EventReadToken.scope`'s own doc comment - carried through
/// unvalidated, the same "no validation on this value" treatment
/// `RoleAccessMapping.scope` already gets from `grant_role_access_mapping`.
pub fn create_event_read_token(
    access_mapping: &RoleAccessMapping,
    event_type: &crate::event_store::EventType,
    id: String,
    secret: String,
    scope: Option<String>,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<EventReadToken> {
    require_active_admin(access_mapping)?;
    if access_mapping.bounded_context != event_type.bounded_context {
        return Err(Error::GrantBoundedContextMismatch.into());
    }

    Ok(EventReadToken {
        id,
        secret,
        status: TokenStatus::Active,
        created_at: now,
        revoked_at: None,
        event_type: event_type.clone(),
        scope,
    })
}

/// See `rule CreateCommandToken`. Same shape and reasoning as
/// `create_external_event_token` above, scoped to a `CommandType` instead
/// of an `EventType`.
pub fn create_command_token(
    access_mapping: &RoleAccessMapping,
    command_type: &crate::event_store::CommandType,
    id: String,
    secret: String,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<CommandToken> {
    require_active_admin(access_mapping)?;
    if access_mapping.bounded_context != command_type.bounded_context {
        return Err(Error::GrantBoundedContextMismatch.into());
    }

    Ok(CommandToken {
        id,
        secret,
        status: TokenStatus::Active,
        created_at: now,
        revoked_at: None,
        command_type: command_type.clone(),
    })
}

/// See `rule RevokeToken`. One function for all four `AccessToken`
/// variants, matching the spec's own "one revocation rule for every
/// variant" framing - `token_scope` is `AccessToken::scope`, the only
/// place the variants differ here.
pub fn revoke_token(
    access_mapping: &RoleAccessMapping,
    token: &AccessToken,
    now: chrono::DateTime<chrono::Utc>,
) -> crate::error::Result<AccessToken> {
    require_active_admin(access_mapping)?;
    if &access_mapping.bounded_context != token.scope() {
        return Err(Error::GrantBoundedContextMismatch.into());
    }
    if token.status() != TokenStatus::Active {
        return Err(Error::TokenNotActive.into());
    }

    Ok(match token.clone() {
        AccessToken::ExternalEventToken(t) => AccessToken::ExternalEventToken(ExternalEventToken {
            status: TokenStatus::Revoked,
            revoked_at: Some(now),
            ..t
        }),
        AccessToken::DirectCreationToken(t) => {
            AccessToken::DirectCreationToken(DirectCreationToken {
                status: TokenStatus::Revoked,
                revoked_at: Some(now),
                ..t
            })
        }
        AccessToken::CommandToken(t) => AccessToken::CommandToken(CommandToken {
            status: TokenStatus::Revoked,
            revoked_at: Some(now),
            ..t
        }),
        AccessToken::EventReadToken(t) => AccessToken::EventReadToken(EventReadToken {
            status: TokenStatus::Revoked,
            revoked_at: Some(now),
            ..t
        }),
    })
}
