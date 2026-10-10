//! Cross-cutting value types, used across every domain module - and the
//! home for primitives (once implemented) reused by both
//! `access_control`'s `AccessToken` and `bootstrap`'s `BootstrapSecret`
//! (`generate_token_secret`, `secret_matches`, `generate_token_id`). See
//! docs/architecture.md §3.2 and `specs/skilj.allium`'s Value Types
//! section, which this module mirrors closely.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Names a payload field to filter, tag, or mark sensitive: a bare
/// top-level name, or a two-segment dotted path reaching one leaf inside
/// the spec's one permitted level of nesting (e.g. `"address.country"`).
/// See the payload schema shape note above `entity CommandType` in
/// `specs/skilj.allium`.
pub type FieldPath = String;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Filter {
    pub field: FieldPath,
    pub operator: FilterOperator,
    pub value: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Equals,
    Contains,
    IsLike,
    GreaterThan,
    LessThan,
    /// Only valid against a string leaf with `format: "geo-point"`
    /// (`"lat,lng"`). `Filter.value` is `"lat,lng,radius_meters"` - the
    /// target point plus how close counts as "near". See
    /// `event_store::geo_distance_within`.
    Near,
    /// Only valid against a string leaf with `format: "color"`
    /// (`"#RRGGBB"`). `Filter.value` is `"#RRGGBB,max_distance"` - the
    /// target color plus a similarity threshold (plain Euclidean RGB
    /// distance, not a perceptual metric). See
    /// `event_store::color_similarity_within`.
    SimilarColor,
    /// Only valid against a string leaf with `format: "ip"` (IPv4 or
    /// IPv6, via `std::net::IpAddr`). `Filter.value` is a CIDR, e.g.
    /// `"192.168.1.0/24"`. See `event_store::ip_in_subnet`.
    InSubnet,
    /// Valid against any scalar leaf (string/integer/number/boolean) -
    /// not tied to any particular `format`. `Filter.value` is a
    /// comma-separated list of candidates, e.g. `"a,b,c"` - no escaping,
    /// same as `IsLike`'s `%`/`_` wildcards already being unescaped.
    In,
}

/// See `value Metadata` in the spec. `version` and every other
/// sequence-adjacent integer in this codebase is `i64` - see
/// docs/architecture.md §2.2.1.
///
/// `correlation_id`/`causation_id` (Codeberg issue #18) are `Option`
/// here regardless of the "every stored `Command`/`Event` ends up with a
/// `correlation_id`" guarantee the spec's `CorrelationIdIsAlwaysRecorded`
/// invariant makes - that guarantee is a property of the write path
/// (`event_store::process_command`/`create_external_event`/
/// `create_direct_event`/`create_system_event` all generate one when the
/// caller didn't supply one), not of this type. A row written before
/// this field existed genuinely has neither, and decoding it as
/// `Option::None` rather than inventing a value is the honest read.
/// Deliberately not the OTel trace id (`skilj-rest`/`skilj-graphql`'s own
/// `current_trace_id()`, docs/architecture.md §10b) - see the spec's own
/// note above `value Metadata` for why the two are unrelated: a trace id
/// is ephemeral and exporter-dependent, these are durable and queryable
/// from the store itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct Metadata {
    pub r#type: String,
    pub version: i64,
    pub client_id: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub correlation_id: Option<String>,
    pub causation_id: Option<String>,
}

/// `value` is `None` (absent) rather than the tag being omitted entirely
/// when the mapped payload field was itself absent - see `Tag.value` in
/// the spec for why that's deliberate, not an oversight.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub struct Tag {
    pub key: String,
    pub value: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct TagMapping {
    pub key: String,
    pub field: FieldPath,
}

/// One item of a command type's consistency query (docs/architecture.md
/// §198, `value QueryItemMapping` in specs/skilj.allium), declared by
/// `CommandType::consistency_query`. Derived per command into a
/// [`QueryItem`], as a [`TagMapping`] is into a [`Tag`]. An event matches
/// the item when its type is one of `event_types` (any type when empty)
/// and it carries every tag `tag_mappings` derives. `latest`, when set,
/// asks for only that many matching events, those with the highest
/// sequences; `None` for every match.
///
/// ```
/// # use skilj_core::shared::{QueryItemMapping, TagMapping};
/// let company = TagMapping { key: "company".into(), field: "company_id".into() };
/// // The company's latest lifecycle event, whatever kind it is.
/// let status = QueryItemMapping::types(&["CompanySignedUp", "CompanyExpired"])
///     .tagged(vec![company])
///     .latest();
/// assert_eq!(status.latest, Some(1));
/// // The requester's last three tickets.
/// let recent = QueryItemMapping::types(&["TicketCreated"]).last(3);
/// assert_eq!(recent.latest, Some(3));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QueryItemMapping {
    pub event_types: Vec<String>,
    pub tag_mappings: Vec<TagMapping>,
    pub latest: Option<u32>,
}

impl QueryItemMapping {
    /// An item matching these event types, every match, no tags yet.
    pub fn types(event_types: &[&str]) -> Self {
        Self {
            event_types: event_types.iter().map(|t| t.to_string()).collect(),
            tag_mappings: Vec::new(),
            latest: None,
        }
    }

    /// An item matching any event type carrying these tags.
    pub fn tags(tag_mappings: Vec<TagMapping>) -> Self {
        Self {
            event_types: Vec::new(),
            tag_mappings,
            latest: None,
        }
    }

    /// Also requires these tags.
    pub fn tagged(mut self, tag_mappings: Vec<TagMapping>) -> Self {
        self.tag_mappings.extend(tag_mappings);
        self
    }

    /// Only the matching event with the highest sequence: `last(1)`.
    pub fn latest(self) -> Self {
        self.last(1)
    }

    /// Only the `count` matching events with the highest sequences.
    /// `build()` refuses 0.
    pub fn last(mut self, count: u32) -> Self {
        self.latest = Some(count);
        self
    }
}

/// A [`QueryItemMapping`] derived from one command's payload - `value
/// QueryItem` in specs/skilj.allium, recorded on `Command.consistency_query`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct QueryItem {
    pub event_types: Vec<String>,
    pub tags: Vec<Tag>,
    /// See [`QueryItemMapping::latest`].
    pub latest: Option<u32>,
}

/// `subject_field` is never encrypted - it's the plaintext identifier the
/// `EncryptionKey` is looked up or derived by. Only `field` is what gets
/// swapped for ciphertext (at the leaf, for a dotted path - see
/// `protect_sensitive_fields` in `event_store`).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct SensitiveField {
    pub field: FieldPath,
    pub subject_key: String,
    pub subject_field: FieldPath,
}

/// See `value PrivateField` in the spec - the private-field mechanism's
/// own three-way visibility rule, standing beside `SensitiveField` rather
/// than inside it: a plain read-time redaction, no `EncryptionKey`
/// anywhere behind it. `team`/`addressee_field` are each set exactly when
/// `kind` calls for them, checked by `event_store::valid_private_fields`
/// at registration - `None` for whichever the other two kinds leave
/// unused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PrivateField {
    pub field: FieldPath,
    pub kind: PrivateFieldKind,
    pub team: Option<String>,
    pub addressee_field: Option<FieldPath>,
}

/// See `enum PrivateFieldKind` in the spec for what each variant means in
/// full - `Own` (default reader: the record's own creator),
/// `Team` (default reader: any `Role` named `PrivateField.team`),
/// `Addressed` (default reader: the party `PrivateField.addressee_field`
/// names in the payload). A fourth kind, `Draft` - visible only until an
/// external status changes - is a real, deliberately deferred future
/// kind: it needs a live `Projection` lookup at read time, a materially
/// different evaluability shape from these three, which are decidable
/// from the record and the reading caller's own identity alone. Nothing
/// here is shaped around it and nothing here forecloses it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum PrivateFieldKind {
    Own,
    Team,
    Addressed,
}

/// The whole contract a bounded context's `decide()` returns - see
/// `value CommandDecision` in the spec, and docs/architecture.md
/// §4.1/§5.4/§7.3 for why a rejection is typed data, never a
/// GraphQL/HTTP-level error.
#[derive(Debug, Clone)]
pub enum CommandDecision {
    Accepted { events: Vec<EventSpec> },
    Rejected { reason: String, kind: String },
}

/// One event a `decide()` call wants appended, on acceptance.
#[derive(Debug, Clone)]
pub struct EventSpec {
    pub event_type: String,
    pub payload: serde_json::Value,
}

// `secret_matches` below was added propagating `bootstrap::create_superadmin`
// - unlike `generate_token_secret`/`generate_token_id` below, it needed no
// caller-supplied-output treatment (it takes strings, not producing an
// opaque one), so it was real from the start rather than deferred.

/// Generates the opaque `id` half of an `AccessToken`/`Role` - a UUIDv4,
/// hyphens stripped (32 lowercase hex characters), matching every other
/// identifier this codebase already treats as an opaque `String` rather
/// than a typed newtype. Not a secret itself - safe to log, appear in a
/// URL path, etc. - only `generate_token_secret` below carries the
/// confidentiality requirement `secret_matches` is built to compare
/// without leaking.
pub fn generate_token_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()
}

/// Generates the `secret` half of an `AccessToken`/`BootstrapSecret` -
/// two concatenated UUIDv4s (244 bits of randomness, comfortably more
/// than a single v4's 122), hyphens stripped. `uuid`'s `v4` feature reads
/// from the OS CSPRNG (`getrandom`), so this is cryptographically
/// unpredictable, not merely unique like `generate_token_id` only needs
/// to be.
pub fn generate_token_secret() -> String {
    format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    )
}

/// Hashes the `secret` half of an `AccessToken` for storage - see the
/// REST token presentation note above the surfaces in the spec: "the
/// secret half is then verified against that row's stored hash... Both
/// the hashing scheme and the comparison are black boxes here, the same
/// register as generate_token_secret and generate_token_id: the spec
/// owns that the secret is never stored or compared in plaintext, not
/// which primitive does it." `generate_token_secret` already gives 244
/// bits of CSPRNG randomness (see its own doc comment) - high-entropy,
/// unlike a user-chosen password - so a plain cryptographic hash
/// (SHA-256, via `ring`, already a dependency for `encryption`) is the
/// appropriate primitive: brute-forcing a 244-bit secret back out of its
/// hash is infeasible regardless of how fast the hash is, and a slow
/// password KDF (bcrypt/argon2/scrypt) would only add latency to every
/// request's auth check for no security benefit. Base64-encoded
/// (`base64::engine::general_purpose::STANDARD`), the same encoding
/// `encryption::encrypt` already uses for its own opaque byte output.
/// Deterministic (no per-secret salt) - safe here specifically because
/// the input is never a low-entropy human-chosen value a rainbow table
/// could target, unlike a password hash.
pub fn hash_secret(secret: &str) -> String {
    use base64::Engine;
    let digest = ring::digest::digest(&ring::digest::SHA256, secret.as_bytes());
    base64::engine::general_purpose::STANDARD.encode(digest.as_ref())
}

/// See the `secret_matches` note above `rule CreateSuperadmin`: compares
/// two secrets in time that depends only on their lengths, never on where
/// they first differ, so a failed presentation leaks nothing about how
/// much of the real secret was guessed correctly. Real rather than
/// deferred - simple enough that there's no trivial/reachable-case split
/// the way `protect_sensitive_fields`/`derive_tags` etc. have. A length
/// mismatch still short-circuits (the length itself isn't the secret -
/// only its content is), but every byte of an equal-length comparison is
/// folded in regardless of an early difference.
pub fn secret_matches(presented: &str, actual: &str) -> bool {
    let (a, b) = (presented.as_bytes(), actual.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}
